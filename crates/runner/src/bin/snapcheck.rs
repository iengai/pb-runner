//! P4.2 acceptance: rebuild each recorded `OrchestratorInput` from the market
//! state the fake-exchange run had at that time and diff it against the
//! recording (docs/PLAN.md P4.2, docs/SNAPSHOT_SPEC.md).
//!
//! State reconstruction per recording (all from committed fixtures plus the
//! dev box's candle cache):
//! - config: the public config the set was recorded with;
//! - candles: the replay `.npy` day files (`--candles <dir>`, `--dates a:b`),
//!   truncated to `ts <= timestamp_ms` like the harness does;
//! - positions, balance, order book, market specs, realized-pnl stats,
//!   trailing availability and fill timestamps: taken from the recording
//!   itself (they are exchange/fill-manager state, not derived data);
//! - open orders: the previous recording's output orders plus the recorded
//!   forager incumbents (the fake exchange keeps resting orders);
//! - candle availability: warm for every symbol on the first cycle, then
//!   only for symbols with a position or an open order (the harness never
//!   marks secondary symbols as fetched, SPEC 3.7).
//!
//! Derived fields are compared exactly (Value equality: int vs float kept);
//! `effective_min_cost` uses the 600 s-TTL cached price the bot had, which is
//! not recorded, so it is reported separately.
//!
//!     cargo run -p pb-runner --bin pb-snapcheck -- --config tests/fixtures/configs/fake_v8/grid_v7.json \
//!       --recordings tests/fixtures/recordings/fake_v8/grid_v7 \
//!       --candles E:/projects/passivbot/historical_data/ohlcvs_bybit --dates 2025-08-01:2025-10-28

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use passivbot_rust::types::TrailingPriceBundle;
use pb_runner::bot_params::ConfigView;
use pb_runner::emas::Candle;
use pb_runner::jsonexact::parse_exact;
use pb_runner::snapshot::{
    trailing_bundle, AccountState, MarketParams, SideState, SnapshotBuilder, SymbolState,
};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    recordings: PathBuf,
    #[arg(long)]
    candles: PathBuf,
    /// `YYYY-MM-DD:YYYY-MM-DD` inclusive day files to load per coin.
    #[arg(long)]
    dates: String,
    /// Scenario file of the run (for boot fills -> trailing anchors).
    #[arg(long)]
    scenario: Option<PathBuf>,
    #[arg(long, default_value_t = 0)]
    limit: usize,
    /// Print every mismatch instead of the first three per path.
    #[arg(long)]
    verbose: bool,
}

/// Minimal `.npy` reader for float64 C-order `(n, 6)` arrays.
fn read_npy_rows(path: &Path) -> Result<Vec<Candle>> {
    let bytes = std::fs::read(path).with_context(|| path.display().to_string())?;
    if bytes.len() < 10 || &bytes[..6] != b"\x93NUMPY" {
        bail!("{}: not a .npy file", path.display());
    }
    let major = bytes[6];
    let (header_len, offset) = if major == 1 {
        (u16::from_le_bytes([bytes[8], bytes[9]]) as usize, 10)
    } else {
        (
            u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize,
            12,
        )
    };
    let header = std::str::from_utf8(&bytes[offset..offset + header_len])?;
    if !header.contains("'<f8'") || header.contains("'fortran_order': True") {
        bail!("{}: unexpected npy header {header}", path.display());
    }
    let data = &bytes[offset + header_len..];
    if data.len() % 48 != 0 {
        bail!("{}: data length not a multiple of 6 f64", path.display());
    }
    let mut out = Vec::with_capacity(data.len() / 48);
    for row in data.chunks_exact(48) {
        let mut c = [0.0; 6];
        for (i, v) in c.iter_mut().enumerate() {
            *v = f64::from_le_bytes(row[i * 8..i * 8 + 8].try_into().unwrap());
        }
        out.push(c);
    }
    Ok(out)
}

fn date_range(spec: &str) -> Result<Vec<String>> {
    let (a, b) = spec
        .split_once(':')
        .ok_or_else(|| anyhow!("--dates must be a:b"))?;
    let parse = |s: &str| -> Result<(i32, u32, u32)> {
        let p: Vec<u32> = s
            .split('-')
            .map(|x| x.parse::<u32>())
            .collect::<std::result::Result<_, _>>()?;
        Ok((p[0] as i32, p[1], p[2]))
    };
    let (y0, m0, d0) = parse(a)?;
    let (y1, m1, d1) = parse(b)?;
    let days_in = |y: i32, m: u32| -> u32 {
        match m {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            _ => {
                if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                    29
                } else {
                    28
                }
            }
        }
    };
    let (mut y, mut m, mut d) = (y0, m0, d0);
    let mut out = Vec::new();
    loop {
        out.push(format!("{y:04}-{m:02}-{d:02}"));
        if (y, m, d) == (y1, m1, d1) {
            break;
        }
        d += 1;
        if d > days_in(y, m) {
            d = 1;
            m += 1;
            if m > 12 {
                m = 1;
                y += 1;
            }
        }
        if out.len() > 4000 {
            bail!("date range too long");
        }
    }
    Ok(out)
}

struct CandleStore {
    dir: PathBuf,
    dates: Vec<String>,
    cache: BTreeMap<String, Vec<Candle>>,
}

impl CandleStore {
    /// Load every coin up front and apply the fake exchange's replay rule:
    /// the timeline is the union of all coins' timestamps, and a coin without
    /// a candle at a timeline step gets a flat zero-volume candle at its
    /// previous close (`_build_replay_timeline`).
    fn load_all(&mut self, coins: &[String]) -> Result<()> {
        let mut raw: BTreeMap<String, Vec<Candle>> = BTreeMap::new();
        let mut union: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
        for coin in coins {
            let mut all = Vec::new();
            for d in &self.dates {
                all.extend(read_npy_rows(
                    &self.dir.join(coin).join(format!("{d}.npy")),
                )?);
            }
            all.sort_by(|a, b| a[0].partial_cmp(&b[0]).unwrap());
            all.dedup_by(|a, b| a[0] == b[0]);
            union.extend(all.iter().map(|c| c[0] as u64));
            raw.insert(coin.clone(), all);
        }
        for (coin, rows) in raw {
            let mut filled = Vec::with_capacity(union.len());
            let mut it = rows.iter().peekable();
            let mut last: Option<Candle> = None;
            for &t in &union {
                match it.peek() {
                    Some(c) if c[0] as u64 == t => {
                        last = Some(**c);
                        filled.push(**c);
                        it.next();
                    }
                    _ => {
                        let Some(prev) = last else {
                            bail!("{coin}: replay missing initial candle at {t}")
                        };
                        let c = [t as f64, prev[4], prev[4], prev[4], prev[4], 0.0];
                        last = Some(c);
                        filled.push(c);
                    }
                }
            }
            self.cache.insert(coin, filled);
        }
        Ok(())
    }

    fn coin(&mut self, coin: &str) -> Result<&Vec<Candle>> {
        if !self.cache.contains_key(coin) {
            let mut all = Vec::new();
            for d in &self.dates {
                all.extend(read_npy_rows(
                    &self.dir.join(coin).join(format!("{d}.npy")),
                )?);
            }
            all.sort_by(|a, b| a[0].partial_cmp(&b[0]).unwrap());
            all.dedup_by(|a, b| a[0] == b[0]);
            self.cache.insert(coin.to_string(), all);
        }
        Ok(&self.cache[coin])
    }
}

fn num(v: &Value) -> f64 {
    v.as_f64().unwrap_or(0.0)
}

/// Deep diff with paths; array indices under `symbols` are replaced by `*` in
/// the aggregated key so counts group per field.
fn diff(path: &str, a: &Value, b: &Value, out: &mut Vec<(String, String)>) {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            let keys: HashSet<&String> = x.keys().chain(y.keys()).collect();
            let mut keys: Vec<&String> = keys.into_iter().collect();
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
        (Value::Array(x), Value::Array(y))
            if path.ends_with("symbols")
                || path.contains(".emas.")
                || path.contains("forager_m1") =>
        {
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
                out.push((path.to_string(), format!("{a} vs recorded {b}")));
            }
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let cfg_text = std::fs::read_to_string(&args.config)?;
    let cfg = ConfigView::new(serde_json::from_str(&cfg_text)?)?;
    let builder = SnapshotBuilder::new(&cfg)?;
    let mut store = CandleStore {
        dir: args.candles.clone(),
        dates: date_range(&args.dates)?,
        cache: BTreeMap::new(),
    };

    // Boot fills (seeded scenarios) -> trailing anchors per (symbol, pside).
    let mut anchors: BTreeMap<(String, String), u64> = BTreeMap::new();
    if let Some(p) = &args.scenario {
        let sc: Value = serde_json::from_str(&std::fs::read_to_string(p)?)?;
        for f in sc
            .pointer("/account/fills")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let sym = f["symbol"].as_str().unwrap_or_default().to_string();
            let pside = f["position_side"].as_str().unwrap_or("long").to_string();
            let ts = f["timestamp"].as_u64().unwrap_or(0);
            let e = anchors.entry((sym, pside)).or_insert(0);
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

    let coins: Vec<String> = builder
        .universe(&[])
        .iter()
        .map(|s| s.split('/').next().unwrap().to_string())
        .collect();
    store.load_all(&coins)?;
    let mut totals: BTreeMap<String, usize> = BTreeMap::new();
    let mut examples: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut prev_out_symbols: HashSet<usize> = HashSet::new();
    let mut n = 0usize;
    let mut clean = 0usize;
    for (fi, file) in files.iter().enumerate() {
        let rec: Value = parse_exact(&std::fs::read_to_string(file)?)?;
        let out_path = PathBuf::from(file.to_string_lossy().replace(".in.json", ".out.json"));
        let rec_out: Value = parse_exact(&std::fs::read_to_string(&out_path)?)?;
        let ts = rec["timestamp_ms"].as_u64().unwrap();
        let symbols: Vec<String> = builder.universe(&[]);
        let incumbents: HashSet<(usize, &str)> = ["long", "short"]
            .iter()
            .flat_map(|p| {
                rec["forager_hysteresis"][format!("incumbent_{p}")]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|v| (v.as_u64().unwrap() as usize, *p))
                    .collect::<Vec<_>>()
            })
            .collect();
        let mut states = Vec::new();
        for (idx, symbol) in symbols.iter().enumerate() {
            let rs = &rec["symbols"][idx];
            let coin = symbol.split('/').next().unwrap();
            let all = store.coin(coin)?;
            let cut = all.partition_point(|c| c[0] as u64 <= ts);
            let candles = all[..cut].to_vec();
            let ex = &rs["exchange"];
            let market = MarketParams {
                qty_step: num(&ex["qty_step"]),
                price_step: num(&ex["price_step"]),
                min_qty: num(&ex["min_qty"]),
                min_cost: num(&ex["min_cost"]),
                c_mult: num(&ex["c_mult"]),
                maker_fee: num(&ex["maker_fee"]),
                taker_fee: num(&ex["taker_fee"]),
            };
            let side = |pside: &str| -> SideState {
                let r = &rs[pside];
                let size = num(&r["position"]["size"]);
                let has_entry = incumbents.contains(&(idx, pside));
                let anchor = anchors.get(&(symbol.clone(), pside.to_string())).copied();
                // `trailing_available == false` is fill-confirmation state the
                // runner cannot derive from REST (SPEC 4.3): take it as input.
                let recorded_avail = r["trailing_available"].as_bool().unwrap_or(true);
                let required = size != 0.0
                    && recorded_avail
                    && builder.is_trailing(symbol, pside).unwrap_or(false);
                let (trailing, avail) = match (required, anchor) {
                    (true, Some(a)) => match trailing_bundle(&candles, a, ts) {
                        Some(b) => (b, true),
                        None => (TrailingPriceBundle::default(), false),
                    },
                    _ => {
                        let t = &r["trailing"];
                        (
                            TrailingPriceBundle {
                                min_since_open: num(&t["min_since_open"]),
                                max_since_min: num(&t["max_since_min"]),
                                max_since_open: num(&t["max_since_open"]),
                                min_since_max: num(&t["min_since_max"]),
                            },
                            r["trailing_available"].as_bool().unwrap_or(true),
                        )
                    }
                };
                SideState {
                    position_size: size,
                    position_price: num(&r["position"]["price"]),
                    trailing,
                    trailing_available: avail,
                    last_increase_fill_ts: r["last_increase_fill_timestamp_ms"].as_u64(),
                    has_entry_order: has_entry,
                    has_open_order: has_entry || prev_out_symbols.contains(&idx),
                }
            };
            let long = side("long");
            let short = side("short");
            let has_pos = long.position_size != 0.0 || short.position_size != 0.0;
            let has_order = long.has_open_order || short.has_open_order;
            states.push(SymbolState {
                symbol: symbol.clone(),
                market,
                active: true,
                bid: num(&rs["order_book"]["bid"]),
                ask: num(&rs["order_book"]["ask"]),
                min_cost_price: num(&rs["order_book"]["bid"]),
                candles_1m: candles,
                candles_1h: None,
                candles_available: fi == 0 || has_pos || has_order,
                long,
                short,
            });
        }
        let account = AccountState {
            timestamp_ms: ts,
            balance: num(&rec["balance"]),
            balance_raw: num(&rec["balance_raw"]),
            realized_pnl_cumsum_max: num(&rec["global"]["realized_pnl_cumsum_max"]),
            realized_pnl_cumsum_last: num(&rec["global"]["realized_pnl_cumsum_last"]),
        };
        let snap = builder.build(&account, &states)?;
        let mut d = Vec::new();
        diff("", &snap.input, &rec, &mut d);
        if d.is_empty() {
            clean += 1;
        }
        for (p, msg) in d {
            *totals.entry(p.clone()).or_default() += 1;
            let ex = examples.entry(p).or_default();
            if args.verbose || ex.len() < 3 {
                ex.push(format!(
                    "{}: {}",
                    file.file_name().unwrap().to_string_lossy(),
                    msg
                ));
            }
        }
        prev_out_symbols = rec_out["orders"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|o| o["symbol_idx"].as_u64().map(|x| x as usize))
            .collect();
        n += 1;
    }
    println!(
        "{n} recordings, {clean} identical, {} mismatching field paths",
        totals.len()
    );
    for (p, c) in &totals {
        println!("  {c:4}  {p}");
        for e in &examples[p] {
            println!("          {}", e.chars().take(220).collect::<String>());
        }
    }
    if totals.is_empty() {
        Ok(())
    } else {
        std::process::exit(1)
    }
}
