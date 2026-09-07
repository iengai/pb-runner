//! P5.1-lite: compare the runner's cancel/create plan with the requests the
//! Python bot actually sent to the fake exchange, cycle by cycle.
//!
//! Inputs are a full fake-exchange run (`tools/record_fake_v8.py` output):
//! - `<recordings>/*.in.json|*.out.json`: one engine call per step;
//! - `<artifacts>/remote_calls.json`: every request the fake exchange saw,
//!   with `step_index`/`timestamp`, including `create_order` (symbol, side,
//!   amount, price, client id) and `cancel_order` (order id);
//! - `<artifacts>/fills.json`: fills (order ids), to retire filled orders.
//!
//! For each step: the open-order book is replayed from the request log up to
//! (excluding) that step, the ideal orders are the recorded engine output,
//! positions come from the recorded input, `PB_modes` from the recorded
//! diagnostics, and `reconcile()` must produce the same create set and the
//! same cancel set as the Python bot's requests at that step.
//!
//!     cargo run -p pb-runner --bin pb-plancheck -- --config tests/fixtures/configs/fake_v8/grid_v7.json \
//!       --recordings .local/fake_v8_public/grid_v7/recordings --artifacts .local/fake_v8_public/grid_v7/artifacts/<run>

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use pb_exchange_bybit::{OpenOrder, PositionSide, Side};
use pb_runner::bot_params::ConfigView;
use pb_runner::jsonexact::parse_exact;
use pb_runner::live::PlannedOrder;
use pb_runner::reconcile::{self, OrderRec, PbMode, ReconcileParams};
use pb_runner::snapshot::{MarketParams, SideState, SnapshotBuilder, SymbolState};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    recordings: PathBuf,
    #[arg(long)]
    artifacts: PathBuf,
    #[arg(long, default_value_t = 0)]
    limit: usize,
    #[arg(long)]
    verbose: bool,
}

fn num(v: &Value) -> f64 {
    v.as_f64().unwrap_or(0.0)
}

fn side_of(s: &str) -> Side {
    if s.eq_ignore_ascii_case("buy") {
        Side::Buy
    } else {
        Side::Sell
    }
}

fn pside_of(s: &str) -> PositionSide {
    if s.eq_ignore_ascii_case("long") {
        PositionSide::Long
    } else {
        PositionSide::Short
    }
}

/// Comparable identity of an order (custom ids are random).
fn key(
    symbol: &str,
    side: Side,
    pside: PositionSide,
    qty: f64,
    price: f64,
    reduce_only: bool,
    t: &str,
) -> String {
    format!("{symbol} {side:?} {pside:?} qty={qty} px={price} ro={reduce_only} {t}")
}

fn main() -> Result<()> {
    let args = Args::parse();
    let cfg = ConfigView::new(parse_exact(&std::fs::read_to_string(&args.config)?)?)?;
    let builder = SnapshotBuilder::new(&cfg)?;
    let symbols = builder.universe(&[]);
    let hedge_mode = cfg
        .live("hedge_mode")
        .map(|v| v.as_bool().unwrap_or(true))
        .unwrap_or(true);
    let params = ReconcileParams {
        hedge_mode,
        match_tolerance: cfg
            .live("order_match_tolerance_pct")
            .and_then(Value::as_f64)
            .unwrap_or(0.0002),
        max_cancels_per_batch: cfg
            .live("max_n_cancellations_per_batch")
            .and_then(Value::as_u64)
            .unwrap_or(5) as usize,
        max_creates_per_batch: cfg
            .live("max_n_creations_per_batch")
            .and_then(Value::as_u64)
            .unwrap_or(3) as usize,
    };
    let stop = PbMode::parse(builder.stop_mode());

    let calls: Vec<Value> = parse_exact(&std::fs::read_to_string(
        args.artifacts.join("remote_calls.json"),
    )?)?
    .as_array()
    .cloned()
    .ok_or_else(|| anyhow!("remote_calls.json is not a list"))?;
    let fills: Vec<Value> = std::fs::read_to_string(args.artifacts.join("fills.json"))
        .ok()
        .and_then(|t| parse_exact(&t).ok())
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default();
    // Fill timestamps by order id (an order is retired once fully filled; the
    // fake exchange fills resting limit orders entirely).
    let mut filled_at: HashMap<String, u64> = HashMap::new();
    for f in &fills {
        let id = f
            .get("order")
            .or(f.get("order_id"))
            .or(f.get("orderId"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        let ts = f.get("timestamp").and_then(Value::as_u64).unwrap_or(0);
        if !id.is_empty() {
            let e = filled_at.entry(id.to_string()).or_insert(ts);
            *e = (*e).max(ts);
        }
    }

    let mut files: Vec<PathBuf> = std::fs::read_dir(&args.recordings)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.to_string_lossy().ends_with(".in.json"))
        .collect();
    files.sort();
    if args.limit > 0 {
        files.truncate(args.limit);
    }

    // Requests grouped by timestamp.
    let mut creates_at: BTreeMap<u64, Vec<&Value>> = BTreeMap::new();
    let mut cancels_at: BTreeMap<u64, Vec<&Value>> = BTreeMap::new();
    for c in &calls {
        let ts = c["timestamp"].as_u64().unwrap_or(0);
        match c["method"].as_str() {
            Some("create_order") => creates_at.entry(ts).or_default().push(c),
            Some("cancel_order") => cancels_at.entry(ts).or_default().push(c),
            _ => {}
        }
    }

    let mut book: BTreeMap<String, OpenOrder> = BTreeMap::new();
    let mut applied_ts: u64 = 0;
    let mut steps = 0usize;
    let mut ok_steps = 0usize;
    let mut create_mismatch = 0usize;
    let mut cancel_mismatch = 0usize;
    let mut examples: Vec<String> = Vec::new();
    let apply_until = |book: &mut BTreeMap<String, OpenOrder>, until_excl: u64, from: u64| {
        for (ts, cs) in creates_at.range(from..until_excl) {
            for c in cs {
                let id = c["order_id"].as_str().unwrap_or_default().to_string();
                book.insert(
                    id.clone(),
                    OpenOrder {
                        id,
                        client_id: c["client_order_id"].as_str().map(str::to_string),
                        symbol: c["symbol"].as_str().unwrap_or_default().to_string(),
                        side: side_of(c["side"].as_str().unwrap_or("buy")),
                        pside: pside_of(c["position_side"].as_str().unwrap_or("long")),
                        qty: num(&c["amount"]),
                        price: num(&c["price"]),
                        reduce_only: c["reduce_only"].as_bool().unwrap_or(false),
                        created_ms: Some(*ts),
                    },
                );
            }
        }
        for (_, cs) in cancels_at.range(from..until_excl) {
            for c in cs {
                book.remove(c["order_id"].as_str().unwrap_or_default());
            }
        }
        book.retain(|id, _| filled_at.get(id).is_none_or(|t| *t >= until_excl));
    };

    for file in &files {
        let rec = parse_exact(&std::fs::read_to_string(file)?)?;
        let out_path = PathBuf::from(file.to_string_lossy().replace(".in.json", ".out.json"));
        let out = parse_exact(
            &std::fs::read_to_string(&out_path).with_context(|| out_path.display().to_string())?,
        )?;
        let ts = rec["timestamp_ms"].as_u64().unwrap();
        apply_until(&mut book, ts, applied_ts);
        applied_ts = ts;

        // State from the recording.
        let mut states: Vec<SymbolState> = Vec::new();
        let mut positions: HashMap<(String, PositionSide), f64> = HashMap::new();
        let mut last_price: HashMap<String, f64> = HashMap::new();
        for (idx, symbol) in symbols.iter().enumerate() {
            let rs = &rec["symbols"][idx];
            let ex = &rs["exchange"];
            let side = |p: &str| SideState {
                position_size: num(&rs[p]["position"]["size"]),
                position_price: num(&rs[p]["position"]["price"]),
                ..Default::default()
            };
            let (long, short) = (side("long"), side("short"));
            positions.insert((symbol.clone(), PositionSide::Long), long.position_size);
            positions.insert((symbol.clone(), PositionSide::Short), short.position_size);
            last_price.insert(symbol.clone(), num(&rs["order_book"]["bid"]));
            states.push(SymbolState {
                symbol: symbol.clone(),
                market: MarketParams {
                    qty_step: num(&ex["qty_step"]),
                    price_step: num(&ex["price_step"]),
                    min_qty: num(&ex["min_qty"]),
                    min_cost: num(&ex["min_cost"]),
                    c_mult: num(&ex["c_mult"]),
                    maker_fee: num(&ex["maker_fee"]),
                    taker_fee: num(&ex["taker_fee"]),
                },
                active: true,
                bid: num(&rs["order_book"]["bid"]),
                ask: num(&rs["order_book"]["ask"]),
                min_cost_price: num(&rs["order_book"]["bid"]),
                candles_1m: Vec::new(),
                candles_1h: None,
                candles_available: true,
                long,
                short,
            });
        }
        // PB_modes from the recorded diagnostics.
        let mut modes: HashMap<(String, PositionSide), PbMode> = HashMap::new();
        for st in out["diagnostics"]["symbol_states"]
            .as_array()
            .into_iter()
            .flatten()
        {
            let idx = st["symbol_idx"].as_u64().unwrap() as usize;
            let symbol = &symbols[idx];
            let state = &states[idx];
            for (pside, name) in [(PositionSide::Long, "long"), (PositionSide::Short, "short")] {
                let explicit = builder.mode_override(name, state).ok().flatten();
                let active = st[name]["active"].as_bool().unwrap_or(false);
                let m = match explicit {
                    Some(m) => PbMode::parse(&m),
                    None if active => PbMode::Normal,
                    None => stop,
                };
                modes.insert((symbol.clone(), pside), m);
            }
        }
        // Ideal orders from the recorded engine output.
        let planned: Vec<PlannedOrder> = out["orders"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|o| {
                let idx = o["symbol_idx"].as_u64().unwrap() as usize;
                let qty = num(&o["qty"]);
                let t = o["order_type"].as_str().unwrap_or("unknown").to_string();
                PlannedOrder {
                    symbol: symbols[idx].clone(),
                    side: if qty > 0.0 { Side::Buy } else { Side::Sell },
                    pside: pside_of(o["pside"].as_str().unwrap_or("long")),
                    qty: qty.abs(),
                    price: num(&o["price"]),
                    reduce_only: t.contains("close"),
                    market: o["execution_type"].as_str() == Some("market"),
                    risk_critical: o["execution_priority"].as_str() == Some("risk_critical"),
                    order_type: t,
                }
            })
            .collect();
        let pos =
            |s: &str, p: PositionSide| positions.get(&(s.to_string(), p)).copied().unwrap_or(0.0);
        let last = |s: &str| last_price.get(s).copied().unwrap_or(0.0);
        let ideal: Vec<OrderRec> = reconcile::to_executable(&planned, &pos, &last);
        let open: Vec<OrderRec> = book
            .values()
            .map(|o| reconcile::normalize_open_order(o, hedge_mode))
            .collect();
        let mode_fn = |s: &str, p: PositionSide| {
            modes
                .get(&(s.to_string(), p))
                .copied()
                .unwrap_or(PbMode::Normal)
        };
        let plan = reconcile::reconcile(&ideal, &open, &mode_fn, &last, &[], ts, &params);

        // Expected from the request log at this step.
        let mut exp_creates: Vec<String> = creates_at
            .get(&ts)
            .into_iter()
            .flatten()
            .map(|c| {
                let t = reconcile::pb_order_type_from_custom_id(c["client_order_id"].as_str());
                key(
                    c["symbol"].as_str().unwrap_or(""),
                    side_of(c["side"].as_str().unwrap_or("buy")),
                    pside_of(c["position_side"].as_str().unwrap_or("long")),
                    num(&c["amount"]),
                    num(&c["price"]),
                    c["reduce_only"].as_bool().unwrap_or(false),
                    &t,
                )
            })
            .collect();
        let mut got_creates: Vec<String> = plan
            .creates
            .iter()
            .map(|o| {
                key(
                    &o.symbol,
                    o.side,
                    o.pside,
                    o.qty,
                    o.price,
                    o.reduce_only,
                    &o.pb_order_type,
                )
            })
            .collect();
        let mut exp_cancels: Vec<String> = cancels_at
            .get(&ts)
            .into_iter()
            .flatten()
            .filter_map(|c| {
                book.get(c["order_id"].as_str().unwrap_or("")).map(|o| {
                    format!(
                        "{} {:?} {:?} qty={} px={}",
                        o.symbol, o.side, o.pside, o.qty, o.price
                    )
                })
            })
            .collect();
        let mut got_cancels: Vec<String> = plan
            .cancels
            .iter()
            .map(|o| {
                format!(
                    "{} {:?} {:?} qty={} px={}",
                    o.symbol, o.side, o.pside, o.qty, o.price
                )
            })
            .collect();
        exp_creates.sort();
        got_creates.sort();
        exp_cancels.sort();
        got_cancels.sort();
        steps += 1;
        let mut bad = false;
        if exp_creates != got_creates {
            create_mismatch += 1;
            bad = true;
            if args.verbose || examples.len() < 12 {
                examples.push(format!(
                    "{ts} creates: expected {exp_creates:?}\n            got      {got_creates:?}"
                ));
            }
        }
        if exp_cancels != got_cancels {
            cancel_mismatch += 1;
            bad = true;
            if args.verbose || examples.len() < 12 {
                examples.push(format!(
                    "{ts} cancels: expected {exp_cancels:?}\n            got      {got_cancels:?}"
                ));
            }
        }
        if !bad {
            ok_steps += 1;
        }
    }
    println!("{steps} steps, {ok_steps} identical plans, {create_mismatch} create mismatches, {cancel_mismatch} cancel mismatches");
    for e in examples {
        println!("  {e}");
    }
    if ok_steps == steps {
        Ok(())
    } else {
        std::process::exit(1)
    }
}
