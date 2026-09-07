//! P5.1: closed-loop parity check. The real runner (`LiveRunner` +
//! `Executor`, i.e. the `--live` path) drives the mock exchange
//! (`mock_exchange.rs`) through the scenario a Python fake-exchange run was
//! recorded on, and every step is compared with what the Python bot did.
//!
//! Inputs: a `tools/record_fake_v8.py` run directory:
//! - `config.json`, `scenario.json`: what the harness ran;
//! - `recordings/*.in.json`: one engine call per step (gives the wall-clock
//!   stem for the churn gate, D13, and the engine input for `--diff-inputs`);
//! - `artifacts/<stamp>_<name>/`: `remote_calls.json` (every request the fake
//!   saw), `step_summaries.json` (positions, fill and open-order counts per
//!   step), `fills.json`, `fake_exchange_state.json` (final state).
//!
//! Per step the runner plans and executes one wave, then the mock advances
//! one candle (`advance_time` in `run_fake_live._run_fake_bot`). Compared:
//! the create and cancel requests of the step (content, and order ids as a
//! secondary count), the open-order set, positions, balance and fill count
//! after the wave, and optionally the engine input against the recording.
//!
//!     cargo run -p pb-runner --bin pb-mockrun -- --run .local/fake_v8_public/grid_v7

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use pb_exchange_bybit::{PositionSide, Side};
use pb_runner::bot_params::ConfigView;
use pb_runner::config::LiveConfig;
use pb_runner::exchange_config::ExchangeConfigurator;
use pb_runner::execute::Executor;
use pb_runner::jsonexact::parse_exact;
use pb_runner::live::LiveRunner;
use pb_runner::mock_exchange::{MockExchange, MockOrder, Request, Scenario};
use pb_runner::reconcile::pb_order_type_from_custom_id;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Parser, Debug)]
struct Args {
    /// Run directory (`config.json`, `scenario.json`, `recordings/`, `artifacts/`).
    #[arg(long)]
    run: PathBuf,
    /// Overrides for the run directory layout.
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    scenario: Option<PathBuf>,
    #[arg(long)]
    recordings: Option<PathBuf>,
    /// Artifact directory (default: the single `artifacts/*` entry, or the
    /// newest when there are several).
    #[arg(long)]
    artifacts: Option<PathBuf>,
    #[arg(long, default_value_t = 0)]
    max_steps: usize,
    /// Churn gate clock: `wall` (recording stem, D13) or `cycle` (scenario time).
    #[arg(long, default_value = "wall")]
    gate_clock: String,
    /// Compare the runner's engine input with the recording of the same step.
    #[arg(long)]
    diff_inputs: bool,
    /// Do not reproduce the harness quirk that forager cache-only symbols are
    /// never refreshed (D17); expect the public forager runs to diverge.
    #[arg(long)]
    no_harness_compat: bool,
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

/// Comparable identity of an order (custom ids are random, exchange ids are
/// compared separately).
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

/// A Python-side open order replayed from `remote_calls.json`.
#[derive(Debug, Clone)]
struct PyOrder {
    id: String,
    key: String,
}

fn py_create_key(c: &Value) -> String {
    let t = pb_order_type_from_custom_id(c["client_order_id"].as_str());
    key(
        c["symbol"].as_str().unwrap_or(""),
        side_of(c["side"].as_str().unwrap_or("buy")),
        pside_of(c["position_side"].as_str().unwrap_or("long")),
        num(&c["amount"]),
        num(&c["price"]),
        c["reduce_only"].as_bool().unwrap_or(false),
        &t,
    )
}

fn mock_order_key(o: &MockOrder) -> String {
    key(
        &o.symbol,
        o.side,
        o.pside,
        o.remaining,
        o.price,
        o.reduce_only,
        &pb_order_type_from_custom_id(Some(&o.client_order_id)),
    )
}

/// Value diff restricted to paths (same shape as `pb-snapcheck`).
fn diff(path: &str, a: &Value, b: &Value, out: &mut Vec<(String, String)>) {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            let mut keys: Vec<&String> = x
                .keys()
                .chain(y.keys())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            keys.sort();
            for k in keys {
                match (x.get(k), y.get(k)) {
                    (Some(av), Some(bv)) => diff(&format!("{path}.{k}"), av, bv, out),
                    (Some(_), None) => {
                        out.push((format!("{path}.{k}"), "missing in recording".into()))
                    }
                    (None, Some(_)) => {
                        out.push((format!("{path}.{k}"), "missing in rebuilt".into()))
                    }
                    _ => {}
                }
            }
        }
        (Value::Array(x), Value::Array(y)) => {
            if x.len() != y.len() {
                out.push((
                    path.to_string(),
                    format!("len {} vs recorded {}", x.len(), y.len()),
                ));
                if path.ends_with("symbols") {
                    return;
                }
            }
            for (i, (av, bv)) in x.iter().zip(y.iter()).enumerate() {
                let p = if path.ends_with("symbols") {
                    format!("{path}[*]")
                } else {
                    format!("{path}[{i}]")
                };
                diff(&p, av, bv, out);
            }
        }
        _ => {
            if a != b {
                let short = |v: &Value| {
                    let s = v.to_string();
                    s.chars().take(60).collect::<String>()
                };
                out.push((
                    path.to_string(),
                    format!("{} vs recorded {}", short(a), short(b)),
                ));
            }
        }
    }
}

fn pick_artifacts(run: &Path) -> Result<PathBuf> {
    let dir = run.join("artifacts");
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&dir)
        .with_context(|| dir.display().to_string())?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.join("remote_calls.json").exists())
        .collect();
    entries.sort();
    entries.pop().ok_or_else(|| {
        anyhow!(
            "no artifacts with remote_calls.json under {}",
            dir.display()
        )
    })
}

fn load_json(path: &Path) -> Result<Value> {
    parse_exact(&std::fs::read_to_string(path).with_context(|| path.display().to_string())?)
        .with_context(|| path.display().to_string())
}

struct Examples {
    lines: Vec<String>,
    verbose: bool,
}

impl Examples {
    fn push(&mut self, s: String) {
        if self.verbose || self.lines.len() < 20 {
            self.lines.push(s);
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();
    let args = Args::parse();
    let config_path = args
        .config
        .clone()
        .unwrap_or_else(|| args.run.join("config.json"));
    let scenario_path = args
        .scenario
        .clone()
        .unwrap_or_else(|| args.run.join("scenario.json"));
    let recordings = args
        .recordings
        .clone()
        .unwrap_or_else(|| args.run.join("recordings"));
    let artifacts = match &args.artifacts {
        Some(a) => a.clone(),
        None => pick_artifacts(&args.run)?,
    };
    let wall_clock = match args.gate_clock.as_str() {
        "wall" => true,
        "cycle" => false,
        other => bail!("unknown --gate-clock {other:?} (wall|cycle)"),
    };

    // Config, exactly as `pb-runner` validates and loads it.
    let cfg_text =
        std::fs::read_to_string(&config_path).with_context(|| config_path.display().to_string())?;
    LiveConfig::parse(&cfg_text, 8)?;
    let raw: Value = parse_exact(&cfg_text)?;
    let cfg = ConfigView::new(raw)?;
    // `live.time_in_force` after template defaults (`good_till_cancelled`).
    let post_only = cfg
        .live("time_in_force")
        .and_then(Value::as_str)
        .map(|s| s == "post_only")
        .unwrap_or(false);
    let exchange_config = ExchangeConfigurator::from_config(&cfg);

    // Mock exchange from the scenario the Python harness ran.
    let scenario = Scenario::load(&scenario_path)?;
    let n_steps_total = scenario.timeline.len() - scenario.boot_index;
    let mock = Arc::new(MockExchange::new(scenario)?);
    eprintln!(
        "scenario {} ({} symbols, boot step {} of {}, balance {})",
        mock.scenario().name,
        mock.scenario().symbols.len(),
        mock.scenario().boot_index,
        mock.scenario().timeline.len(),
        mock.scenario().balance
    );

    // Python artifacts.
    let calls = load_json(&artifacts.join("remote_calls.json"))?
        .as_array()
        .cloned()
        .ok_or_else(|| anyhow!("remote_calls.json is not a list"))?;
    let summaries = load_json(&artifacts.join("step_summaries.json"))?
        .as_array()
        .cloned()
        .unwrap_or_default();
    let py_fills = load_json(&artifacts.join("fills.json"))
        .ok()
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default();
    let final_state = load_json(&artifacts.join("fake_exchange_state.json")).ok();
    let mut creates_at: BTreeMap<u64, Vec<Value>> = BTreeMap::new();
    let mut cancels_at: BTreeMap<u64, Vec<Value>> = BTreeMap::new();
    for c in &calls {
        let ts = c["timestamp"].as_u64().unwrap_or(0);
        match c["method"].as_str() {
            Some("create_order") => creates_at.entry(ts).or_default().push(c.clone()),
            Some("cancel_order") => cancels_at.entry(ts).or_default().push(c.clone()),
            _ => {}
        }
    }
    // Python fills: order id -> fill ts (an order fills entirely), balance
    // deltas in ledger order (`balance_total += pnl - fee`, fake.py:1112).
    let mut py_filled_at: HashMap<String, u64> = HashMap::new();
    let mut py_balance_deltas: Vec<(u64, f64)> = Vec::new();
    for f in &py_fills {
        let id = f["order"].as_str().unwrap_or_default();
        let ts = f["timestamp"].as_u64().unwrap_or(0);
        let historical = f["info"]["liquidity"].as_str() == Some("historical");
        if !id.is_empty() && !historical {
            let e = py_filled_at.entry(id.to_string()).or_insert(ts);
            *e = (*e).max(ts);
        }
        if !historical {
            py_balance_deltas.push((ts, num(&f["pnl"]) - num(&f["fee"]["cost"])));
        }
    }

    // Recordings: scenario ts -> (wall-clock stem ms, path).
    let mut rec_by_ts: BTreeMap<u64, (u64, PathBuf)> = BTreeMap::new();
    if recordings.is_dir() {
        for e in std::fs::read_dir(&recordings)? {
            let p = e?.path();
            if !p.to_string_lossy().ends_with(".in.json") {
                continue;
            }
            let stem_ms: Option<u64> = p
                .file_name()
                .and_then(|f| f.to_str())
                .and_then(|f| f.split('_').next())
                .and_then(|s| s.parse().ok());
            let v = load_json(&p)?;
            if let (Some(ts), Some(stem)) = (v["timestamp_ms"].as_u64(), stem_ms) {
                rec_by_ts.entry(ts).or_insert((stem, p));
            }
        }
    }
    if wall_clock && rec_by_ts.is_empty() {
        bail!(
            "--gate-clock wall needs recordings with wall-clock stems in {}",
            recordings.display()
        );
    }

    // Runner with scenario time and the recorder's wall clock (D13) injected.
    let mono_ms = Arc::new(AtomicU64::new(0));
    let wall = {
        let m = mock.clone();
        Arc::new(move || m.now_ms())
    };
    let mono = {
        let m = mono_ms.clone();
        Arc::new(move || m.load(Ordering::Relaxed) as f64 / 1000.0)
    };
    let mut runner = LiveRunner::with_clocks(cfg, mock.clone(), wall, mono)?;
    runner.set_harness_secondary_never_fetched(!args.no_harness_compat);
    let symbols = runner.warmup().await?;
    runner.configure_exchange().await?;
    let mut executor = Executor::new(mock.clone(), post_only, exchange_config);
    eprintln!("warmup done: {} symbols", symbols.len());

    let mut ex = Examples {
        lines: Vec::new(),
        verbose: args.verbose,
    };
    let mut steps = 0usize;
    let mut ok_requests = 0usize;
    let mut create_mismatch = 0usize;
    let mut cancel_mismatch = 0usize;
    let mut create_order_mismatch = 0usize;
    let mut cancel_id_mismatch = 0usize;
    let mut book_ok = 0usize;
    let mut book_id_ok = 0usize;
    let mut pos_ok = 0usize;
    let mut bal_ok = 0usize;
    let mut fills_ok = 0usize;
    let mut plan_errors = 0usize;
    let mut write_failures = 0usize;
    let mut input_identical = 0usize;
    let mut input_compared = 0usize;
    let mut input_paths: BTreeMap<String, usize> = BTreeMap::new();
    let mut py_book: BTreeMap<String, PyOrder> = BTreeMap::new();
    let mut py_applied: u64 = 0;
    let mut last_mono: u64 = 0;
    // The harness ran `--max-steps` cycles (one per `step_summaries` entry).
    let py_steps = if summaries.is_empty() {
        n_steps_total
    } else {
        summaries.len().min(n_steps_total)
    };
    let limit = if args.max_steps > 0 {
        args.max_steps.min(py_steps)
    } else {
        py_steps
    };

    loop {
        let ts = mock.now_ms();
        let step_index = mock.step_index();
        let mono_now = if wall_clock {
            match rec_by_ts.get(&ts) {
                Some((stem, _)) => *stem,
                None => {
                    eprintln!("step {step_index}: no recording for ts {ts}; monotonic clock advanced by one tick");
                    last_mono + mock.scenario().tick_interval_ms
                }
            }
        } else {
            ts
        };
        last_mono = mono_now;
        mono_ms.store(mono_now, Ordering::Relaxed);

        let req_from = mock.request_count();
        let recent = executor.recent_executions(ts).to_vec();
        let planned_input = match runner.plan(&recent).await {
            Ok(cycle) => {
                let book_before: HashMap<String, MockOrder> = mock
                    .snapshot()
                    .open_orders
                    .into_iter()
                    .map(|o| (o.id.clone(), o))
                    .collect();
                match executor.execute(&cycle.plan, ts).await {
                    Ok(r) => {
                        write_failures += r.failures;
                        runner.note_write_failures(&r.write_failures, ts);
                        for (symbol, e) in &r.write_failures {
                            ex.push(format!("{ts} write failure {symbol}: {e}"));
                        }
                    }
                    Err(e) => bail!("step {step_index}: {e}"),
                }
                Some((cycle.input, book_before))
            }
            Err(e) => {
                plan_errors += 1;
                ex.push(format!("{ts} planning failed: {e:#}"));
                None
            }
        };
        steps += 1;

        // --- requests of this step ---------------------------------------
        let mine = mock.requests_since(req_from);
        let mut got_creates: Vec<String> = Vec::new();
        let mut got_create_ids: Vec<(String, String)> = Vec::new();
        let mut got_cancels: Vec<String> = Vec::new();
        let mut got_cancel_ids: Vec<String> = Vec::new();
        let book_before = planned_input.as_ref().map(|(_, b)| b);
        for r in &mine {
            match &r.request {
                Request::Create {
                    symbol,
                    side,
                    pside,
                    amount,
                    price,
                    reduce_only,
                    client_order_id,
                    order_id,
                    ..
                } => {
                    let k = key(
                        symbol,
                        *side,
                        *pside,
                        *amount,
                        *price,
                        *reduce_only,
                        &pb_order_type_from_custom_id(Some(client_order_id)),
                    );
                    got_create_ids.push((order_id.clone(), k.clone()));
                    got_creates.push(k);
                }
                Request::Cancel { order_id, .. } => {
                    got_cancel_ids.push(order_id.clone());
                    let content = book_before
                        .and_then(|b| b.get(order_id))
                        .map(|o| {
                            format!(
                                "{} {:?} {:?} qty={} px={}",
                                o.symbol, o.side, o.pside, o.remaining, o.price
                            )
                        })
                        .unwrap_or_else(|| format!("unknown id {order_id}"));
                    got_cancels.push(content);
                }
                Request::Other { .. } => {}
            }
        }
        let py_creates = creates_at.get(&ts).cloned().unwrap_or_default();
        let py_cancels = cancels_at.get(&ts).cloned().unwrap_or_default();
        let mut exp_creates: Vec<String> = py_creates.iter().map(py_create_key).collect();
        let exp_create_ids: Vec<(String, String)> = py_creates
            .iter()
            .map(|c| {
                (
                    c["order_id"].as_str().unwrap_or_default().to_string(),
                    py_create_key(c),
                )
            })
            .collect();
        let mut exp_cancels: Vec<String> = py_cancels
            .iter()
            .map(|c| {
                let id = c["order_id"].as_str().unwrap_or("");
                py_book
                    .get(id)
                    .map(|o| {
                        // `key` minus reduce_only/type, matching the mock side above.
                        let parts: Vec<&str> = o.key.split(' ').collect();
                        parts[..5].join(" ")
                    })
                    .unwrap_or_else(|| format!("unknown id {id}"))
            })
            .collect();
        let mut exp_cancel_ids: Vec<String> = py_cancels
            .iter()
            .map(|c| c["order_id"].as_str().unwrap_or_default().to_string())
            .collect();
        exp_creates.sort();
        got_creates.sort();
        exp_cancels.sort();
        got_cancels.sort();
        exp_cancel_ids.sort();
        got_cancel_ids.sort();
        let mut bad = false;
        if exp_creates != got_creates {
            create_mismatch += 1;
            bad = true;
            ex.push(format!(
                "{ts} step {step_index} creates: expected {exp_creates:?}\n            got      {got_creates:?}"
            ));
        }
        if exp_cancels != got_cancels {
            cancel_mismatch += 1;
            bad = true;
            ex.push(format!(
                "{ts} step {step_index} cancels: expected {exp_cancels:?}\n            got      {got_cancels:?}"
            ));
        }
        if !bad {
            ok_requests += 1;
        }
        if exp_create_ids != got_create_ids {
            create_order_mismatch += 1;
            if args.verbose {
                ex.push(format!(
                    "{ts} step {step_index} create order/ids: expected {exp_create_ids:?}\n            got      {got_create_ids:?}"
                ));
            }
        }
        if exp_cancel_ids != got_cancel_ids {
            cancel_id_mismatch += 1;
            if args.verbose {
                ex.push(format!(
                    "{ts} step {step_index} cancel ids: expected {exp_cancel_ids:?} got {got_cancel_ids:?}"
                ));
            }
        }

        // --- Python book after this step's wave ---------------------------
        for (t, cs) in creates_at.range(py_applied..=ts) {
            for c in cs {
                let id = c["order_id"].as_str().unwrap_or_default().to_string();
                py_book.insert(
                    id.clone(),
                    PyOrder {
                        id,
                        key: py_create_key(c),
                    },
                );
            }
            let _ = t;
        }
        for (_, cs) in cancels_at.range(py_applied..=ts) {
            for c in cs {
                py_book.remove(c["order_id"].as_str().unwrap_or_default());
            }
        }
        py_book.retain(|id, _| py_filled_at.get(id).is_none_or(|t| *t > ts));
        py_applied = ts + 1;

        // --- account state after the wave ---------------------------------
        let snap = mock.snapshot();
        let mut exp_book: Vec<String> = py_book.values().map(|o| o.key.clone()).collect();
        let mut got_book: Vec<String> = snap.open_orders.iter().map(mock_order_key).collect();
        exp_book.sort();
        got_book.sort();
        if exp_book == got_book {
            book_ok += 1;
        } else {
            ex.push(format!(
                "{ts} step {step_index} open orders: expected {} {exp_book:?}\n            got      {} {got_book:?}",
                exp_book.len(),
                got_book.len()
            ));
        }
        let mut exp_ids: Vec<&String> = py_book.values().map(|o| &o.id).collect();
        let mut got_ids: Vec<&String> = snap.open_orders.iter().map(|o| &o.id).collect();
        exp_ids.sort();
        got_ids.sort();
        if exp_ids == got_ids {
            book_id_ok += 1;
        }
        let summary = summaries
            .iter()
            .find(|s| s["timestamp"].as_u64() == Some(ts));
        match summary {
            Some(s) => {
                if let Some(n) = s["open_orders"].as_u64() {
                    if n as usize != exp_book.len() {
                        ex.push(format!(
                            "{ts} step {step_index}: replayed Python book has {} orders, step_summaries says {n}",
                            exp_book.len()
                        ));
                    }
                }
                let mut exp_pos: Vec<String> = s["positions"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|p| {
                        format!(
                            "{} {:?} size={} entry={}",
                            p["symbol"].as_str().unwrap_or(""),
                            pside_of(p["position_side"].as_str().unwrap_or("long")),
                            num(&p["size"]),
                            num(&p["entry_price"])
                        )
                    })
                    .collect();
                let mut got_pos: Vec<String> = snap
                    .positions
                    .iter()
                    .map(|(s, ps, size, entry)| format!("{s} {ps:?} size={size} entry={entry}"))
                    .collect();
                exp_pos.sort();
                got_pos.sort();
                if exp_pos == got_pos {
                    pos_ok += 1;
                } else {
                    ex.push(format!(
                        "{ts} step {step_index} positions: expected {exp_pos:?}\n            got      {got_pos:?}"
                    ));
                }
                let exp_fills = s["fills"].as_u64().unwrap_or(0) as usize;
                if exp_fills == snap.fills.len() {
                    fills_ok += 1;
                } else {
                    ex.push(format!(
                        "{ts} step {step_index} fills: expected {exp_fills} got {}",
                        snap.fills.len()
                    ));
                }
            }
            None => ex.push(format!("{ts} step {step_index}: no step_summaries entry")),
        }
        let mut exp_balance = mock.scenario().balance;
        for (t, d) in &py_balance_deltas {
            if *t <= ts {
                exp_balance += d;
            }
        }
        if exp_balance == snap.balance_total {
            bal_ok += 1;
        } else {
            ex.push(format!(
                "{ts} step {step_index} balance: expected {exp_balance} got {}",
                snap.balance_total
            ));
        }

        // --- engine input vs recording ------------------------------------
        if args.diff_inputs {
            if let (Some((input, _)), Some((_, path))) = (&planned_input, rec_by_ts.get(&ts)) {
                let rec = load_json(path)?;
                let mut d = Vec::new();
                diff("", input, &rec, &mut d);
                input_compared += 1;
                if d.is_empty() {
                    input_identical += 1;
                } else {
                    for (p, msg) in d {
                        let n = input_paths.entry(p.clone()).or_default();
                        *n += 1;
                        if *n <= 2 {
                            ex.push(format!("{ts} step {step_index} input {p}: {msg}"));
                        }
                    }
                }
            }
        }

        if steps >= limit || !mock.advance() {
            break;
        }
    }

    // Final state vs `fake_exchange_state.json` (only when the whole run was replayed).
    let mut final_ok = true;
    let complete = steps == summaries.len();
    if let (Some(fs), true) = (&final_state, complete) {
        let snap = mock.snapshot();
        let exp_bal = num(&fs["balance_total"]);
        if exp_bal != snap.balance_total {
            final_ok = false;
            ex.push(format!(
                "final balance: expected {exp_bal} got {}",
                snap.balance_total
            ));
        }
        let exp_open = fs["open_orders"].as_array().map_or(0, Vec::len);
        if exp_open != snap.open_orders.len() {
            final_ok = false;
            ex.push(format!(
                "final open orders: expected {exp_open} got {}",
                snap.open_orders.len()
            ));
        }
        let exp_fills = fs["fills"].as_array().map_or(0, Vec::len);
        if exp_fills != snap.fills.len() {
            final_ok = false;
            ex.push(format!(
                "final fills: expected {exp_fills} got {}",
                snap.fills.len()
            ));
        }
    }

    println!(
        "{steps} steps: requests identical {ok_requests}/{steps} ({create_mismatch} create mismatches, {cancel_mismatch} cancel mismatches; create order/id sequence differs in {create_order_mismatch}, cancel id set differs in {cancel_id_mismatch}); \
account state: open-order set {book_ok}/{steps} (ids {book_id_ok}/{steps}), positions {pos_ok}/{steps}, balance {bal_ok}/{steps}, fill count {fills_ok}/{steps}; final state {}; planning errors {plan_errors}, write failures {write_failures}",
        if !complete {
            "not checked (partial run)"
        } else if final_ok {
            "identical"
        } else {
            "DIFFERS"
        }
    );
    if args.diff_inputs {
        println!(
            "engine inputs identical to the recordings: {input_identical}/{input_compared}; {} differing field paths",
            input_paths.len()
        );
        for (p, n) in &input_paths {
            println!("  {n:4}  {p}");
        }
    }
    for e in &ex.lines {
        println!("  {e}");
    }
    let all_ok = ok_requests == steps
        && book_ok == steps
        && pos_ok == steps
        && bal_ok == steps
        && fills_ok == steps
        && final_ok
        && plan_errors == 0;
    if all_ok {
        Ok(())
    } else {
        std::process::exit(1)
    }
}
