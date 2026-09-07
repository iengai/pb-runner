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
//!   marks secondary symbols as fetched, SPEC 3.7);
//! - HSL (SPEC 2.3 step 1, `hsl.rs`): when the config enables the equity
//!   hard stop, the Python bot's HSL trace (`hsl_trace.jsonl`, written by
//!   `tools/fake_live_clock.py`) drives the Rust state machine with the same
//!   per-cycle inputs (balance, realized/unrealized pnl, positions, the
//!   supervisor's position/order counts). The Rust state is asserted against
//!   the traced Python state after every step, the start-up replay is
//!   recomputed from `fills.json` + candles, and the derived pnl inputs are
//!   cross-checked at every check. The mode overrides of each recording then
//!   come from the Rust machine, not from the trace. In coin mode (SPEC 2.3
//!   steps 2-3, `hsl_coin.rs`, D20) the trace's `coin_*` records drive the
//!   per-pair machine the same way (the traced per-pair realized peak/last,
//!   unrealized pnl and blocking-order counts are the inputs; the
//!   ledger-derived values are cross-checked), the start-up replay is
//!   recomputed from `fills.json` + candles through `hsl::coin_history`, and
//!   the protective-panic recordings of the coin RED supervisor are rebuilt
//!   with `SnapshotBuilder::build_protective` for the traced target pairs.
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
use pb_runner::hsl::{
    balance_equity_timeline, coin_history, hsl_pnl, latest_flatten_fill_timestamp, pside_index,
    realized_pnl_now, CycleInputs, FeePolicy, HslConfig, HslFill, HslPosition, HslState,
    RedObservation, ReplayInputs, SideState, Supervision, LONG, SHORT,
};
use pb_runner::hsl_coin::{coin_realized_pnl_peak_last, CoinEnv, CoinInputs, CoinState};
use pb_runner::jsonexact::parse_exact;
use pb_runner::snapshot::{
    trailing_bundle, AccountState, CycleState, MarketParams, SideState as SnapSide,
    SnapshotBuilder, SymbolState,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashSet};
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
    /// Scenario file of the run (boot fills -> trailing anchors; market steps
    /// for the HSL start-up replay).
    #[arg(long)]
    scenario: Option<PathBuf>,
    /// HSL trace of the run (default `<recordings>/hsl_trace.jsonl`; only
    /// read when the config enables the equity hard stop).
    #[arg(long)]
    hsl_trace: Option<PathBuf>,
    /// Fill ledger of the run (default `<recordings>/fills.json`).
    #[arg(long)]
    fills: Option<PathBuf>,
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
        let mut union: BTreeSet<u64> = BTreeSet::new();
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

    /// Close of the candle at exactly `ts`: the fake exchange's step price
    /// (ticker bid = ask = last) and the 1m close the HSL history replay
    /// reads. `None` when there is no such row.
    fn close_at(&self, coin: &str, ts: u64) -> Option<f64> {
        let rows = self.cache.get(coin)?;
        let i = rows.partition_point(|c| (c[0] as u64) < ts);
        rows.get(i).filter(|c| c[0] as u64 == ts).map(|c| c[4])
    }
}

fn num(v: &Value) -> f64 {
    v.as_f64().unwrap_or(0.0)
}

fn coin_of(symbol: &str) -> &str {
    symbol.split('/').next().unwrap_or(symbol)
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

/// `fills.json` (the fake exchange's fill ledger, ccxt-shaped) -> HSL fill
/// events in the fill manager's order (timestamp, then id as a string).
/// `fee_paid` follows the fill manager's fee normalisation (`FeePolicy`):
/// the seeded boot fills carry a zero fee and get the fallback percentage.
fn load_fills(path: &Path, fee: &FeePolicy) -> Result<Vec<HslFill>> {
    let v: Value = parse_exact(&std::fs::read_to_string(path)?)?;
    let mut rows: Vec<(u64, String, HslFill)> = Vec::new();
    for f in v.as_array().into_iter().flatten() {
        let pside = pside_index(f["position_side"].as_str().unwrap_or("long"));
        let side = f["side"].as_str().unwrap_or("").to_ascii_lowercase();
        let qty = num(&f["amount"]).abs();
        let price = num(&f["price"]);
        if qty <= 0.0 || price <= 0.0 {
            continue;
        }
        let c_mult = f["info"]["contractMultiplier"].as_f64().unwrap_or(1.0);
        let fee_paid = fee.signed_fee_paid(num(&f["fee"]["cost"]), qty * price * c_mult);
        let ts = f["timestamp"].as_u64().unwrap_or(0);
        let id = f["id"].as_str().unwrap_or("").to_string();
        rows.push((
            ts,
            id,
            HslFill {
                timestamp_ms: ts,
                symbol: f["symbol"].as_str().unwrap_or("").to_string(),
                pside,
                qty,
                price,
                increase: (pside == LONG) == (side == "buy"),
                pnl: num(&f["pnl"]),
                fee_paid,
                pb_order_type: pb_runner::reconcile::pb_order_type_from_custom_id(
                    f["clientOrderId"].as_str().filter(|c| !c.is_empty()),
                ),
            },
        ));
    }
    rows.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
    Ok(rows.into_iter().map(|r| r.2).collect())
}

fn trace_positions(v: &Value) -> Vec<HslPosition> {
    v.as_array()
        .into_iter()
        .flatten()
        .map(|p| HslPosition {
            symbol: p["symbol"].as_str().unwrap_or("").to_string(),
            pside: pside_index(p["pside"].as_str().unwrap_or("long")),
            size: num(&p["size"]),
            price: num(&p["price"]),
        })
        .collect()
}

/// Parity bookkeeping for the HSL trace assertions: booleans/ints/strings
/// must match exactly, floats bit-exact or within 1e-9 relative (the
/// Python timeline sums unrealized pnl in set-iteration order, so the
/// start-up replay is only reproducible up to summation order).
#[derive(Default)]
struct Parity {
    floats: usize,
    exact: usize,
    max_rel: f64,
    max_rel_at: String,
    mismatches: Vec<String>,
    /// Realized-pnl cross-checks where Python only knew the fills of its
    /// start-up ledger: the fake harness never refreshes the fill history
    /// after boot, the runner reads every fill (see RECORDER.md A).
    stale_ledger: usize,
}

impl Parity {
    fn float(&mut self, label: &str, py: Option<f64>, rs: f64) {
        let Some(py) = py else {
            self.mismatches
                .push(format!("{label}: python None vs rust {rs}"));
            return;
        };
        self.floats += 1;
        if py.to_bits() == rs.to_bits() {
            self.exact += 1;
            return;
        }
        let rel = (py - rs).abs() / py.abs().max(rs.abs()).max(1e-300);
        if rel > self.max_rel {
            self.max_rel = rel;
            self.max_rel_at = label.to_string();
        }
        if rel > 1e-9 {
            self.mismatches
                .push(format!("{label}: python {py} vs rust {rs}"));
        }
    }

    /// `float`, but a Python value that matches the boot-ledger figure
    /// instead of the full-ledger one counts as a stale-ledger deviation.
    fn float_or_stale(&mut self, label: &str, py: Option<f64>, rs: f64, rs_boot: f64) {
        if let Some(p) = py {
            if p.to_bits() != rs.to_bits() && p.to_bits() == rs_boot.to_bits() {
                self.stale_ledger += 1;
                return;
            }
        }
        self.float(label, py, rs);
    }

    fn eq<T: PartialEq + std::fmt::Debug>(&mut self, label: &str, py: T, rs: T) {
        if py != rs {
            self.mismatches
                .push(format!("{label}: python {py:?} vs rust {rs:?}"));
        }
    }
}

/// Assert one traced Python side state (`_hsl_state_summary`) against the
/// Rust side state.
fn compare_side(par: &mut Parity, label: &str, py: &Value, rs: &SideState) {
    let f = |k: &str| py.get(k).and_then(Value::as_f64);
    let b = |k: &str| py.get(k).and_then(Value::as_bool).unwrap_or(false);
    let u = |k: &str| py.get(k).and_then(Value::as_u64);
    let s = |k: &str| py.get(k).and_then(Value::as_str).unwrap_or("").to_string();
    par.eq(
        &format!("{label}.initialized"),
        b("initialized"),
        rs.runtime.initialized(),
    );
    par.eq(
        &format!("{label}.red_latched"),
        b("red_latched"),
        rs.runtime.red_latched(),
    );
    par.eq(
        &format!("{label}.red_seen_in_episode"),
        b("red_seen_in_episode"),
        rs.runtime.state.red_seen_in_episode,
    );
    par.eq(
        &format!("{label}.tier"),
        s("tier"),
        rs.runtime.tier().as_str().to_string(),
    );
    par.float(
        &format!("{label}.peak_strategy_equity"),
        f("peak_strategy_equity"),
        rs.runtime.state.peak_strategy_equity,
    );
    par.float(
        &format!("{label}.drawdown_ema"),
        f("drawdown_ema"),
        rs.runtime.state.drawdown_ema,
    );
    par.float(
        &format!("{label}.rolling_peak_strategy_equity"),
        f("rolling_peak_strategy_equity"),
        rs.runtime.last_rolling_peak,
    );
    par.eq(&format!("{label}.halted"), b("halted"), rs.halted);
    par.eq(
        &format!("{label}.no_restart_latched"),
        b("no_restart_latched"),
        rs.no_restart_latched,
    );
    par.float(
        &format!("{label}.no_restart_peak_strategy_equity"),
        f("no_restart_peak_strategy_equity"),
        rs.no_restart_peak_strategy_equity,
    );
    par.eq(
        &format!("{label}.cooldown_until_ms"),
        u("cooldown_until_ms"),
        rs.cooldown_until_ms,
    );
    par.eq(
        &format!("{label}.pending_red_since_ms"),
        u("pending_red_since_ms"),
        rs.pending_red_since_ms,
    );
    par.eq(
        &format!("{label}.red_flat_confirmations"),
        u("red_flat_confirmations").unwrap_or(0) as u32,
        rs.red_flat_confirmations,
    );
    par.eq(
        &format!("{label}.cooldown_intervention_active"),
        b("cooldown_intervention_active"),
        rs.cooldown_intervention_active,
    );
    par.eq(
        &format!("{label}.cooldown_repanic_reset_pending"),
        b("cooldown_repanic_reset_pending"),
        rs.cooldown_repanic_reset_pending,
    );
    par.eq(
        &format!("{label}.cooldown_repanic_since_ms"),
        u("cooldown_repanic_since_ms"),
        rs.cooldown_repanic_since_ms,
    );
    par.eq(
        &format!("{label}.cooldown_unresolved_residue"),
        b("cooldown_unresolved_residue"),
        rs.cooldown_unresolved_residue,
    );
    let pm = &py["last_metrics"];
    par.eq(
        &format!("{label}.last_metrics.some"),
        !pm.is_null(),
        rs.last_metrics.is_some(),
    );
    if let (false, Some(m)) = (pm.is_null(), &rs.last_metrics) {
        let l = format!("{label}.last_metrics");
        let g = |k: &str| pm.get(k).and_then(Value::as_f64);
        par.eq(
            &format!("{l}.timestamp_ms"),
            pm["timestamp_ms"].as_u64(),
            Some(m.timestamp_ms),
        );
        par.float(&format!("{l}.balance"), g("balance"), m.balance);
        par.float(
            &format!("{l}.realized_pnl_total"),
            g("realized_pnl_total"),
            m.realized_pnl_total,
        );
        par.float(
            &format!("{l}.realized_pnl"),
            g("realized_pnl"),
            m.realized_pnl,
        );
        par.float(
            &format!("{l}.unrealized_pnl"),
            g("unrealized_pnl"),
            m.unrealized_pnl,
        );
        par.float(
            &format!("{l}.strategy_pnl"),
            g("strategy_pnl"),
            m.strategy_pnl,
        );
        par.float(
            &format!("{l}.peak_strategy_pnl"),
            g("peak_strategy_pnl"),
            m.peak_strategy_pnl,
        );
        par.float(
            &format!("{l}.baseline_balance"),
            g("baseline_balance"),
            m.baseline_balance,
        );
        par.float(
            &format!("{l}.strategy_equity"),
            g("strategy_equity"),
            m.strategy_equity,
        );
        par.float(
            &format!("{l}.peak_strategy_equity"),
            g("peak_strategy_equity"),
            m.peak_strategy_equity,
        );
        par.float(
            &format!("{l}.rolling_peak_strategy_equity"),
            g("rolling_peak_strategy_equity"),
            m.rolling_peak_strategy_equity,
        );
        par.float(
            &format!("{l}.drawdown_raw"),
            g("drawdown_raw"),
            m.drawdown_raw,
        );
        par.float(
            &format!("{l}.drawdown_ema"),
            g("drawdown_ema"),
            m.drawdown_ema,
        );
        par.float(
            &format!("{l}.drawdown_score"),
            g("drawdown_score"),
            m.drawdown_score,
        );
        par.eq(
            &format!("{l}.tier"),
            pm["tier"].as_str().unwrap_or("").to_string(),
            m.tier.as_str().to_string(),
        );
        par.eq(
            &format!("{l}.red_active_now"),
            pm["red_active_now"].as_bool(),
            Some(m.red_active_now),
        );
        par.eq(
            &format!("{l}.red_seen_in_episode"),
            pm["red_seen_in_episode"].as_bool(),
            Some(m.red_seen_in_episode),
        );
        par.eq(
            &format!("{l}.changed"),
            pm["changed"].as_bool(),
            Some(m.changed),
        );
        par.eq(
            &format!("{l}.elapsed_minutes"),
            pm["elapsed_minutes"].as_u64(),
            Some(m.elapsed_minutes),
        );
    }
    let ps = &py["last_stop_event"];
    par.eq(
        &format!("{label}.last_stop_event.some"),
        !ps.is_null(),
        rs.last_stop_event.is_some(),
    );
    if let (false, Some(e)) = (ps.is_null(), &rs.last_stop_event) {
        let l = format!("{label}.last_stop_event");
        let g = |k: &str| ps.get(k).and_then(Value::as_f64);
        par.eq(
            &format!("{l}.stop_event_timestamp_ms"),
            ps["stop_event_timestamp_ms"].as_u64(),
            Some(e.stop_event_timestamp_ms),
        );
        par.eq(
            &format!("{l}.cooldown_until_ms"),
            ps["cooldown_until_ms"].as_u64(),
            e.cooldown_until_ms,
        );
        par.eq(
            &format!("{l}.no_restart_latched"),
            ps["no_restart_latched"].as_bool(),
            Some(e.no_restart_latched),
        );
        par.float(
            &format!("{l}.strategy_equity"),
            g("strategy_equity"),
            e.strategy_equity,
        );
        par.float(
            &format!("{l}.peak_strategy_equity"),
            g("peak_strategy_equity"),
            e.peak_strategy_equity,
        );
        par.float(
            &format!("{l}.trigger_peak_strategy_equity"),
            g("trigger_peak_strategy_equity"),
            e.trigger_peak_strategy_equity,
        );
        par.float(
            &format!("{l}.drawdown_raw"),
            g("drawdown_raw"),
            e.drawdown_raw,
        );
        par.float(
            &format!("{l}.drawdown_ema"),
            g("drawdown_ema"),
            e.drawdown_ema,
        );
        par.float(
            &format!("{l}.drawdown_score"),
            g("drawdown_score"),
            e.drawdown_score,
        );
        par.float(
            &format!("{l}.no_restart_peak_strategy_equity"),
            g("no_restart_peak_strategy_equity"),
            e.no_restart_peak_strategy_equity,
        );
        par.float(
            &format!("{l}.no_restart_drawdown_raw"),
            g("no_restart_drawdown_raw"),
            e.no_restart_drawdown_raw,
        );
    }
    let pp = &py["pending_stop_event"];
    par.eq(
        &format!("{label}.pending_stop_event.some"),
        !pp.is_null(),
        rs.pending_stop_event.is_some(),
    );
    if let (false, Some(e)) = (pp.is_null(), &rs.pending_stop_event) {
        let l = format!("{label}.pending_stop_event");
        par.eq(
            &format!("{l}.stop_event_timestamp_ms"),
            pp["stop_event_timestamp_ms"].as_u64(),
            Some(e.stop_event_timestamp_ms),
        );
        par.float(
            &format!("{l}.drawdown_raw"),
            pp["drawdown_raw"].as_f64(),
            e.drawdown_raw,
        );
        par.float(
            &format!("{l}.peak_strategy_equity"),
            pp["peak_strategy_equity"].as_f64(),
            e.peak_strategy_equity,
        );
    }
}

/// Assert one traced Python coin pair state (`_hsl_coin_state_summary`)
/// against the Rust pair state.
fn compare_coin_state(par: &mut Parity, label: &str, py: &Value, rs: &CoinState) {
    let f = |k: &str| py.get(k).and_then(Value::as_f64);
    let b = |k: &str| py.get(k).and_then(Value::as_bool).unwrap_or(false);
    let u = |k: &str| py.get(k).and_then(Value::as_u64);
    let s = |k: &str| py.get(k).and_then(Value::as_str).unwrap_or("").to_string();
    par.eq(
        &format!("{label}.initialized"),
        b("initialized"),
        rs.runtime.initialized(),
    );
    par.eq(
        &format!("{label}.red_latched"),
        b("red_latched"),
        rs.runtime.red_latched(),
    );
    par.eq(
        &format!("{label}.red_seen_in_episode"),
        b("red_seen_in_episode"),
        rs.runtime.state.red_seen_in_episode,
    );
    par.eq(
        &format!("{label}.tier"),
        s("tier"),
        rs.runtime.tier().as_str().to_string(),
    );
    par.float(
        &format!("{label}.peak_strategy_equity"),
        f("peak_strategy_equity"),
        rs.runtime.state.peak_strategy_equity,
    );
    par.float(
        &format!("{label}.drawdown_ema"),
        f("drawdown_ema"),
        rs.runtime.state.drawdown_ema,
    );
    par.float(
        &format!("{label}.rolling_peak_strategy_equity"),
        f("rolling_peak_strategy_equity"),
        rs.runtime.last_rolling_peak,
    );
    par.eq(&format!("{label}.halted"), b("halted"), rs.halted);
    par.eq(
        &format!("{label}.no_restart_latched"),
        b("no_restart_latched"),
        rs.no_restart_latched,
    );
    par.float(
        &format!("{label}.no_restart_peak_strategy_equity"),
        f("no_restart_peak_strategy_equity"),
        rs.no_restart_peak_strategy_equity,
    );
    par.eq(
        &format!("{label}.cooldown_until_ms"),
        u("cooldown_until_ms"),
        rs.cooldown_until_ms,
    );
    par.eq(
        &format!("{label}.pending_red_since_ms"),
        u("pending_red_since_ms"),
        rs.pending_red_since_ms,
    );
    par.eq(
        &format!("{label}.red_flat_confirmations"),
        u("red_flat_confirmations").unwrap_or(0) as u32,
        rs.red_flat_confirmations,
    );
    par.eq(
        &format!("{label}.cooldown_intervention_active"),
        b("cooldown_intervention_active"),
        rs.cooldown_intervention_active,
    );
    par.eq(
        &format!("{label}.cooldown_repanic_reset_pending"),
        b("cooldown_repanic_reset_pending"),
        rs.cooldown_repanic_reset_pending,
    );
    par.eq(
        &format!("{label}.cooldown_repanic_since_ms"),
        u("cooldown_repanic_since_ms"),
        rs.cooldown_repanic_since_ms,
    );
    par.eq(
        &format!("{label}.cooldown_unresolved_residue"),
        b("cooldown_unresolved_residue"),
        rs.cooldown_unresolved_residue,
    );
    par.eq(
        &format!("{label}.pnl_reset_timestamp_ms"),
        u("pnl_reset_timestamp_ms"),
        rs.pnl_reset_timestamp_ms,
    );
    let pm = &py["last_metrics"];
    par.eq(
        &format!("{label}.last_metrics.some"),
        !pm.is_null(),
        rs.last_metrics.is_some(),
    );
    if let (false, Some(m)) = (pm.is_null(), &rs.last_metrics) {
        let l = format!("{label}.last_metrics");
        let g = |k: &str| pm.get(k).and_then(Value::as_f64);
        par.eq(
            &format!("{l}.timestamp_ms"),
            pm["timestamp_ms"].as_u64(),
            Some(m.timestamp_ms),
        );
        par.float(&format!("{l}.balance"), g("balance"), m.balance);
        par.float(&format!("{l}.slot_budget"), g("slot_budget"), m.slot_budget);
        par.float(
            &format!("{l}.peak_realized_pnl"),
            g("peak_realized_pnl"),
            m.peak_realized_pnl,
        );
        par.float(
            &format!("{l}.realized_pnl"),
            g("realized_pnl"),
            m.realized_pnl,
        );
        par.float(
            &format!("{l}.unrealized_pnl"),
            g("unrealized_pnl"),
            m.unrealized_pnl,
        );
        par.float(
            &format!("{l}.strategy_pnl"),
            g("strategy_pnl"),
            m.strategy_pnl,
        );
        par.float(
            &format!("{l}.peak_strategy_pnl"),
            g("peak_strategy_pnl"),
            m.peak_strategy_pnl,
        );
        par.float(
            &format!("{l}.strategy_equity"),
            g("strategy_equity"),
            m.strategy_equity,
        );
        par.float(&format!("{l}.equity"), g("equity"), m.strategy_equity);
        par.float(
            &format!("{l}.drawdown_usd"),
            g("drawdown_usd"),
            m.drawdown_usd,
        );
        par.float(
            &format!("{l}.drawdown_raw"),
            g("drawdown_raw"),
            m.drawdown_raw,
        );
        par.float(
            &format!("{l}.drawdown_ema"),
            g("drawdown_ema"),
            m.drawdown_ema,
        );
        par.float(
            &format!("{l}.drawdown_score"),
            g("drawdown_score"),
            m.drawdown_score,
        );
        par.float(
            &format!("{l}.red_threshold"),
            g("red_threshold"),
            m.red_threshold,
        );
        par.eq(
            &format!("{l}.tier"),
            pm["tier"].as_str().unwrap_or("").to_string(),
            m.tier.as_str().to_string(),
        );
        par.eq(
            &format!("{l}.red_active_now"),
            pm["red_active_now"].as_bool(),
            Some(m.red_active_now),
        );
        par.eq(
            &format!("{l}.red_seen_in_episode"),
            pm["red_seen_in_episode"].as_bool(),
            Some(m.red_seen_in_episode),
        );
        par.eq(
            &format!("{l}.changed"),
            pm["changed"].as_bool(),
            Some(m.changed),
        );
        par.eq(
            &format!("{l}.elapsed_minutes"),
            pm["elapsed_minutes"].as_u64(),
            Some(m.elapsed_minutes),
        );
    }
    let ps = &py["last_stop_event"];
    par.eq(
        &format!("{label}.last_stop_event.some"),
        !ps.is_null(),
        rs.last_stop_event.is_some(),
    );
    if let (false, Some(e)) = (ps.is_null(), &rs.last_stop_event) {
        let l = format!("{label}.last_stop_event");
        let g = |k: &str| ps.get(k).and_then(Value::as_f64);
        par.eq(
            &format!("{l}.stop_event_timestamp_ms"),
            ps["stop_event_timestamp_ms"].as_u64(),
            Some(e.stop_event_timestamp_ms),
        );
        par.eq(
            &format!("{l}.cooldown_until_ms"),
            ps["cooldown_until_ms"].as_u64(),
            e.cooldown_until_ms,
        );
        par.eq(
            &format!("{l}.no_restart_latched"),
            ps["no_restart_latched"].as_bool(),
            Some(e.no_restart_latched),
        );
        // The contract-reconstructed payload of a halted pair only carries
        // the fields above (`complete = false`).
        par.eq(
            &format!("{l}.complete"),
            ps.get("strategy_equity").is_some_and(|v| !v.is_null()),
            e.complete,
        );
        if e.complete {
            par.float(
                &format!("{l}.strategy_equity"),
                g("strategy_equity"),
                e.strategy_equity,
            );
            par.float(
                &format!("{l}.peak_strategy_equity"),
                g("peak_strategy_equity"),
                e.peak_strategy_equity,
            );
            par.float(
                &format!("{l}.trigger_peak_strategy_equity"),
                g("trigger_peak_strategy_equity"),
                e.trigger_peak_strategy_equity,
            );
            par.float(
                &format!("{l}.drawdown_raw"),
                g("drawdown_raw"),
                e.drawdown_raw,
            );
            par.float(
                &format!("{l}.drawdown_ema"),
                g("drawdown_ema"),
                e.drawdown_ema,
            );
            par.float(
                &format!("{l}.drawdown_score"),
                g("drawdown_score"),
                e.drawdown_score,
            );
            par.float(
                &format!("{l}.no_restart_peak_strategy_equity"),
                g("no_restart_peak_strategy_equity"),
                e.no_restart_peak_strategy_equity,
            );
            par.float(
                &format!("{l}.no_restart_drawdown_raw"),
                g("no_restart_drawdown_raw"),
                e.no_restart_drawdown_raw,
            );
        }
    }
    let pp = &py["pending_stop_event"];
    par.eq(
        &format!("{label}.pending_stop_event.some"),
        !pp.is_null(),
        rs.pending_stop_event.is_some(),
    );
    if let (false, Some(e)) = (pp.is_null(), &rs.pending_stop_event) {
        let l = format!("{label}.pending_stop_event");
        par.eq(
            &format!("{l}.stop_event_timestamp_ms"),
            pp["stop_event_timestamp_ms"].as_u64(),
            Some(e.stop_event_timestamp_ms),
        );
        par.float(
            &format!("{l}.drawdown_raw"),
            pp["drawdown_raw"].as_f64(),
            e.drawdown_raw,
        );
        par.float(
            &format!("{l}.drawdown_ema"),
            pp["drawdown_ema"].as_f64(),
            e.drawdown_ema,
        );
        par.float(
            &format!("{l}.strategy_equity"),
            pp["strategy_equity"].as_f64(),
            e.strategy_equity,
        );
        par.float(
            &format!("{l}.slot_budget"),
            pp["slot_budget"].as_f64(),
            e.slot_budget,
        );
    }
}

/// Every traced pair state (`_hsl_coin_states`) against the Rust pair
/// states, both ways.
fn compare_coin_states(par: &mut Parity, label: &str, py: &Value, hsl: &HslState) {
    for (pside, idx) in [("long", LONG), ("short", SHORT)] {
        let traced = py.get(pside).and_then(Value::as_object);
        for (symbol, st) in traced.into_iter().flatten() {
            match hsl.coin[idx].get(symbol) {
                Some(rs) => compare_coin_state(par, &format!("{label}.{pside}:{symbol}"), st, rs),
                None => par.mismatches.push(format!(
                    "{label}.{pside}:{symbol}: python has a state, rust none"
                )),
            }
        }
        for symbol in hsl.coin[idx].keys() {
            if !traced.is_some_and(|t| t.contains_key(symbol)) {
                par.mismatches.push(format!(
                    "{label}.{pside}:{symbol}: rust has a state, python none"
                ));
            }
        }
    }
}

/// `_hsl_coin_modes`: the runtime forced modes and the replay-pending pairs.
fn compare_coin_modes(par: &mut Parity, label: &str, py: &Value, hsl: &HslState) {
    for (pside, idx) in [("long", LONG), ("short", SHORT)] {
        let traced: BTreeMap<String, String> = py["forced"][pside]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
            .collect();
        par.eq(
            &format!("{label}.forced.{pside}"),
            traced,
            hsl.runtime_forced[idx].clone(),
        );
    }
    let pending: BTreeSet<(usize, String)> = py["replay_pending"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|p| {
            Some((
                pside_index(p.get(0)?.as_str()?),
                p.get(1)?.as_str()?.to_string(),
            ))
        })
        .collect();
    par.eq(
        &format!("{label}.replay_pending"),
        pending,
        hsl.replay_pending.clone(),
    );
}

/// The traced per-pair inputs of a coin record (`_hsl_coin_inputs`):
/// `(pside, symbol) -> (peak_realized, last_realized, unrealized_pnl,
/// entry_orders, nonpanic_close_orders, reset_ts)`.
type TracedPairs = BTreeMap<(usize, String), (f64, f64, f64, usize, usize, Option<u64>)>;

fn traced_pairs(ev: &Value) -> TracedPairs {
    let mut out = TracedPairs::new();
    for (key, v) in ev["pairs"].as_object().into_iter().flatten() {
        let Some((pside, symbol)) = key.split_once(':') else {
            continue;
        };
        out.insert(
            (pside_index(pside), symbol.to_string()),
            (
                num(&v["peak_realized"]),
                num(&v["last_realized"]),
                num(&v["unrealized_pnl"]),
                v["entry_orders"].as_u64().unwrap_or(0) as usize,
                v["nonpanic_close_orders"].as_u64().unwrap_or(0) as usize,
                v["reset_ts"].as_u64(),
            ),
        );
    }
    out
}

fn traced_symbols(ev: &Value) -> BTreeSet<String> {
    ev["symbols"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|s| s.as_str().map(str::to_string))
        .collect()
}

fn traced_pairs_list(v: &Value) -> Vec<(usize, String)> {
    v.as_array()
        .into_iter()
        .flatten()
        .filter_map(|p| {
            Some((
                pside_index(p.get(0)?.as_str()?),
                p.get(1)?.as_str()?.to_string(),
            ))
        })
        .collect()
}

fn compare_states(par: &mut Parity, label: &str, py: &Value, hsl: &HslState) {
    for (pside, idx) in [("long", LONG), ("short", SHORT)] {
        if !hsl.enabled(idx) {
            continue;
        }
        compare_side(
            par,
            &format!("{label}.{pside}"),
            &py[pside],
            &hsl.sides[idx],
        );
    }
}

/// The trace record's HSL inputs (`_hsl_inputs` in `fake_live_clock.py`).
fn trace_inputs<'a>(
    ev: &Value,
    positions: &'a [HslPosition],
    fills: &'a [HslFill],
) -> CycleInputs<'a> {
    CycleInputs {
        now_ms: ev["ts"].as_u64().unwrap_or(0),
        balance: num(&ev["balance"]),
        realized_pnl_total: num(&ev["realized_pnl_total"]),
        realized_pnl: [
            num(&ev["realized_pnl_long"]),
            num(&ev["realized_pnl_short"]),
        ],
        unrealized_pnl: [
            num(&ev["unrealized_pnl_long"]),
            num(&ev["unrealized_pnl_short"]),
        ],
        positions,
        fills,
    }
}

fn observation(v: &Value) -> RedObservation {
    RedObservation {
        n_positions: v["n_positions"].as_u64().unwrap_or(0) as usize,
        entry_orders: v["entry_orders"].as_u64().unwrap_or(0) as usize,
        nonpanic_close_orders: v["nonpanic_close_orders"].as_u64().unwrap_or(0) as usize,
    }
}

fn fills_until(fills: &[HslFill], ts: u64) -> Vec<HslFill> {
    fills
        .iter()
        .filter(|f| f.timestamp_ms <= ts)
        .cloned()
        .collect()
}

/// Everything the per-recording rebuild carries across recordings.
struct Checker<'a> {
    cfg: &'a ConfigView,
    store: &'a mut CandleStore,
    anchors: BTreeMap<(String, String), u64>,
    all_symbols: Vec<String>,
    markets: BTreeMap<String, MarketParams>,
    totals: BTreeMap<String, usize>,
    examples: BTreeMap<String, Vec<String>>,
    prev_out_symbols: HashSet<String>,
    cycle: CycleState,
    n: usize,
    clean: usize,
    verbose: bool,
}

impl Checker<'_> {
    /// Cross-check the derived HSL pnl inputs of one trace record against
    /// what the runner computes from the fill ledger and the step prices.
    #[allow(clippy::too_many_arguments)]
    fn check_hsl_inputs(
        &self,
        par: &mut Parity,
        label: &str,
        ev: &Value,
        positions: &[HslPosition],
        fills_now: &[HslFill],
        boot_fills: &[HslFill],
        start: Option<u64>,
        c_mults: &BTreeMap<String, f64>,
    ) -> Result<()> {
        let ts = ev["ts"].as_u64().unwrap_or(0);
        par.float_or_stale(
            &format!("{label}.realized_pnl_total"),
            ev["realized_pnl_total"].as_f64(),
            realized_pnl_now(fills_now, start, None),
            realized_pnl_now(boot_fills, start, None),
        );
        for (pside, idx) in [("long", LONG), ("short", SHORT)] {
            par.float_or_stale(
                &format!("{label}.realized_pnl_{pside}"),
                ev[format!("realized_pnl_{pside}")].as_f64(),
                realized_pnl_now(fills_now, start, Some(idx)),
                realized_pnl_now(boot_fills, start, Some(idx)),
            );
            let mut upnl = 0.0;
            for p in positions.iter().filter(|p| p.pside == idx) {
                let price = self
                    .store
                    .close_at(coin_of(&p.symbol), ts)
                    .ok_or_else(|| anyhow!("no step price for {} at {ts}", p.symbol))?;
                upnl += hsl_pnl(
                    idx,
                    p.price,
                    price,
                    p.size,
                    c_mults.get(&p.symbol).copied().unwrap_or(1.0),
                );
            }
            par.float(
                &format!("{label}.unrealized_pnl_{pside}"),
                ev[format!("unrealized_pnl_{pside}")].as_f64(),
                upnl,
            );
        }
        Ok(())
    }

    /// Cross-check the traced per-pair coin inputs against the ledger-derived
    /// realized peak/last (stale-ledger aware) and the step-price unrealized
    /// pnl.
    #[allow(clippy::too_many_arguments)]
    fn check_coin_inputs(
        &self,
        par: &mut Parity,
        label: &str,
        ts: u64,
        pairs: &TracedPairs,
        positions: &[HslPosition],
        fills_now: &[HslFill],
        boot_fills: &[HslFill],
        lookback_ms: Option<u64>,
        c_mults: &BTreeMap<String, f64>,
    ) -> Result<()> {
        for ((pside, symbol), (peak, last, upnl, _, _, reset_ts)) in pairs {
            let l = format!("{label}.{}:{symbol}", ["long", "short"][*pside]);
            let (rs_peak, rs_last) =
                coin_realized_pnl_peak_last(fills_now, *pside, symbol, ts, lookback_ms, *reset_ts);
            let (boot_peak, boot_last) =
                coin_realized_pnl_peak_last(boot_fills, *pside, symbol, ts, lookback_ms, *reset_ts);
            par.float_or_stale(
                &format!("{l}.peak_realized"),
                Some(*peak),
                rs_peak,
                boot_peak,
            );
            par.float_or_stale(
                &format!("{l}.last_realized"),
                Some(*last),
                rs_last,
                boot_last,
            );
            let mut rs_upnl = 0.0;
            for p in positions
                .iter()
                .filter(|p| p.pside == *pside && p.symbol == *symbol)
            {
                let price = self
                    .store
                    .close_at(coin_of(&p.symbol), ts)
                    .ok_or_else(|| anyhow!("no step price for {} at {ts}", p.symbol))?;
                rs_upnl += hsl_pnl(
                    *pside,
                    p.price,
                    price,
                    p.size,
                    c_mults.get(&p.symbol).copied().unwrap_or(1.0),
                );
            }
            par.float(&format!("{l}.unrealized_pnl"), Some(*upnl), rs_upnl);
        }
        Ok(())
    }

    /// Rebuild one protective-panic recording of the coin RED supervisor
    /// (`calc_protective_panic_ideal_orders_orchestrator`): the recorded
    /// symbols are the target symbols holding a position, `targets` the
    /// traced `_protective_panic_target_psides_by_symbol`, cross-checked
    /// against the Rust mode overrides of the recorded position psides.
    fn run_protective(
        &mut self,
        file: &Path,
        names: Vec<String>,
        targets: &BTreeMap<String, BTreeSet<String>>,
        par: &mut Parity,
    ) -> Result<()> {
        let rec: Value = parse_exact(&std::fs::read_to_string(file)?)?;
        let ts = rec["timestamp_ms"].as_u64().unwrap();
        let n_rec = rec["symbols"].as_array().map_or(0, Vec::len);
        if names.len() != n_rec {
            bail!(
                "{}: {} recorded symbols but the trace names {}",
                file.display(),
                n_rec,
                names.len()
            );
        }
        let rec_idx: BTreeMap<&str, usize> = names
            .iter()
            .enumerate()
            .map(|(i, s)| (s.as_str(), i))
            .collect();
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
        let builder = SnapshotBuilder::new(self.cfg)?.with_hsl(&self.cycle.hsl);
        let mut states = Vec::new();
        let mut derived: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for symbol in &names {
            let idx = rec_idx[symbol.as_str()];
            let rs = &rec["symbols"][idx];
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
            let side = |pside: &str| -> SnapSide {
                let r = &rs[pside];
                let has_entry = incumbents.contains(&(idx, pside));
                SnapSide {
                    position_size: num(&r["position"]["size"]),
                    position_price: num(&r["position"]["price"]),
                    has_entry_order: has_entry,
                    has_open_order: has_entry,
                    ..SnapSide::default()
                }
            };
            let state = SymbolState {
                symbol: symbol.clone(),
                market,
                active: true,
                bid: num(&rs["order_book"]["bid"]),
                ask: num(&rs["order_book"]["ask"]),
                min_cost_price: num(&rs["order_book"]["bid"]),
                candles_1m: Vec::new(),
                candles_1h: None,
                candles_available: false,
                long: side("long"),
                short: side("short"),
            };
            for pside in ["long", "short"] {
                let size = if pside == "long" {
                    state.long.position_size
                } else {
                    state.short.position_size
                };
                if size != 0.0 && builder.mode_override(pside, &state)?.as_deref() == Some("panic")
                {
                    derived
                        .entry(symbol.clone())
                        .or_default()
                        .insert(pside.to_string());
                }
            }
            states.push(state);
        }
        // The traced targets restricted to the recorded position psides.
        let traced: BTreeMap<String, BTreeSet<String>> = names
            .iter()
            .filter_map(|symbol| {
                let psides: BTreeSet<String> = targets
                    .get(symbol)?
                    .iter()
                    .filter(|p| {
                        num(
                            &rec["symbols"][rec_idx[symbol.as_str()]][p.as_str()]["position"]
                                ["size"],
                        ) != 0.0
                    })
                    .cloned()
                    .collect();
                (!psides.is_empty()).then(|| (symbol.clone(), psides))
            })
            .collect();
        par.eq(&format!("protective@{ts}.targets"), traced, derived);
        let account = AccountState {
            timestamp_ms: ts,
            balance: num(&rec["balance"]),
            balance_raw: num(&rec["balance_raw"]),
            realized_pnl_cumsum_max: 0.0,
            realized_pnl_cumsum_last: 0.0,
        };
        let Some(snap) = builder.build_protective(&account, &states, targets)? else {
            bail!("{}: no target symbol holds a position", file.display());
        };
        let mut d = Vec::new();
        diff("", &snap.input, &rec, &mut d);
        if d.is_empty() {
            self.clean += 1;
        }
        for (p, msg) in d {
            *self.totals.entry(p.clone()).or_default() += 1;
            let ex = self.examples.entry(p).or_default();
            if self.verbose || ex.len() < 3 {
                ex.push(format!(
                    "{}: {}",
                    file.file_name().unwrap().to_string_lossy(),
                    msg
                ));
            }
        }
        self.n += 1;
        Ok(())
    }

    /// Rebuild one recording. `names` is the recorded `symbol_idx` order when
    /// the trace named it (the harness drops symbols without candles from the
    /// planning universe); otherwise the recording must cover the full
    /// candidate list.
    fn run(&mut self, fi: usize, file: &Path, names: Option<Vec<String>>) -> Result<()> {
        let rec: Value = parse_exact(&std::fs::read_to_string(file)?)?;
        let out_path = PathBuf::from(file.to_string_lossy().replace(".in.json", ".out.json"));
        let rec_out: Value = parse_exact(&std::fs::read_to_string(&out_path)?)?;
        let ts = rec["timestamp_ms"].as_u64().unwrap();
        let n_rec = rec["symbols"].as_array().map_or(0, Vec::len);
        let names = match names {
            Some(n) if n.len() == n_rec => n,
            None if n_rec == self.all_symbols.len() => self.all_symbols.clone(),
            other => bail!(
                "{}: {} recorded symbols but {} candidates{}",
                file.display(),
                n_rec,
                self.all_symbols.len(),
                other.map_or(String::new(), |n| format!(" (trace names {})", n.len()))
            ),
        };
        let rec_idx: BTreeMap<&str, usize> = names
            .iter()
            .enumerate()
            .map(|(i, s)| (s.as_str(), i))
            .collect();
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
        for symbol in &names {
            let ex = &rec["symbols"][rec_idx[symbol.as_str()]]["exchange"];
            self.markets.insert(
                symbol.clone(),
                MarketParams {
                    qty_step: num(&ex["qty_step"]),
                    price_step: num(&ex["price_step"]),
                    min_qty: num(&ex["min_qty"]),
                    min_cost: num(&ex["min_cost"]),
                    c_mult: num(&ex["c_mult"]),
                    maker_fee: num(&ex["maker_fee"]),
                    taker_fee: num(&ex["taker_fee"]),
                },
            );
        }
        let builder = SnapshotBuilder::new(self.cfg)?.with_hsl(&self.cycle.hsl);
        let mut universe: BTreeSet<String> = self.all_symbols.iter().cloned().collect();
        universe.extend(names.iter().cloned());
        let mut states = Vec::new();
        for symbol in &universe {
            let coin = coin_of(symbol);
            let all = self.store.coin(coin)?;
            let cut = all.partition_point(|c| c[0] as u64 <= ts);
            let candles = all[..cut].to_vec();
            let market = self
                .markets
                .get(symbol)
                .cloned()
                .ok_or_else(|| anyhow!("{symbol}: no market params recorded yet"))?;
            let rs = rec_idx.get(symbol.as_str()).map(|i| &rec["symbols"][*i]);
            let side = |pside: &str| -> SnapSide {
                let Some(rs) = rs else {
                    return SnapSide {
                        has_open_order: self.prev_out_symbols.contains(symbol),
                        ..SnapSide::default()
                    };
                };
                let idx = rec_idx[symbol.as_str()];
                let r = &rs[pside];
                let size = num(&r["position"]["size"]);
                let has_entry = incumbents.contains(&(idx, pside));
                let anchor = self
                    .anchors
                    .get(&(symbol.clone(), pside.to_string()))
                    .copied();
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
                SnapSide {
                    position_size: size,
                    position_price: num(&r["position"]["price"]),
                    trailing,
                    trailing_available: avail,
                    last_increase_fill_ts: r["last_increase_fill_timestamp_ms"].as_u64(),
                    has_entry_order: has_entry,
                    has_open_order: has_entry || self.prev_out_symbols.contains(symbol),
                }
            };
            let long = side("long");
            let short = side("short");
            let has_pos = long.position_size != 0.0 || short.position_size != 0.0;
            let has_order = long.has_open_order || short.has_open_order;
            let (bid, ask) = match rs {
                Some(rs) => (num(&rs["order_book"]["bid"]), num(&rs["order_book"]["ask"])),
                None => {
                    let p = self.store.close_at(coin, ts).unwrap_or(0.0);
                    (p, p)
                }
            };
            states.push(SymbolState {
                symbol: symbol.clone(),
                market,
                active: true,
                bid,
                ask,
                min_cost_price: bid,
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
        let snap = builder.build(&account, &states, &mut self.cycle)?;
        let active: Vec<(usize, bool, bool)> = rec_out["diagnostics"]["symbol_states"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|st| {
                (
                    st["symbol_idx"].as_u64().unwrap_or(u64::MAX) as usize,
                    st["long"]["active"].as_bool().unwrap_or(false),
                    st["short"]["active"].as_bool().unwrap_or(false),
                )
            })
            .collect();
        self.cycle.pb_modes = builder.pb_modes_after_cycle(&snap, &active);
        let mut d = Vec::new();
        diff("", &snap.input, &rec, &mut d);
        if d.is_empty() {
            self.clean += 1;
        }
        for (p, msg) in d {
            *self.totals.entry(p.clone()).or_default() += 1;
            let ex = self.examples.entry(p).or_default();
            if self.verbose || ex.len() < 3 {
                ex.push(format!(
                    "{}: {}",
                    file.file_name().unwrap().to_string_lossy(),
                    msg
                ));
            }
        }
        self.prev_out_symbols = rec_out["orders"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|o| o["symbol_idx"].as_u64().map(|x| x as usize))
            .filter_map(|i| snap.symbols.get(i).cloned())
            .collect();
        self.n += 1;
        Ok(())
    }
}

/// Recording stem `<timestamp>_<hash>.in.json` -> the input hash.
fn stem_hash(file: &Path) -> String {
    let stem = file.file_name().unwrap().to_string_lossy().to_string();
    stem.split('_')
        .nth(1)
        .unwrap_or("")
        .split('.')
        .next()
        .unwrap_or("")
        .to_string()
}

/// Replay the HSL trace through the Rust state machine, rebuilding each
/// recording at its `compute` event with the machine's mode overrides.
fn replay_with_trace(
    checker: &mut Checker<'_>,
    files: &[PathBuf],
    trace: &[Value],
    hsl: &mut HslState,
    fills: &[HslFill],
    qty_steps: &BTreeMap<String, f64>,
    c_mults: &BTreeMap<String, f64>,
) -> Result<(Parity, BTreeMap<String, String>)> {
    let mut par = Parity::default();
    let mut modes_by_hash: BTreeMap<String, String> = BTreeMap::new();
    let mut rec_i = 0usize;
    // (supervisor_begin record, its positions, first `counts` consumed)
    let mut supervisor: Option<(Value, Vec<HslPosition>, bool)> = None;
    let (mut checks, mut inits, mut supervisions, mut finalizes, mut resets) = (0, 0, 0, 0, 0);
    let (mut iterations, mut protectives, mut flattens, mut stale_flattens) = (0, 0, 0, 0);
    let lookback = hsl.cfg.lookback;
    let coin = hsl.cfg.coin_mode();
    let mut boot_fills: Vec<HslFill> = Vec::new();
    // Coin mode: the traced targets of the current supervisor iteration and
    // the pairs the Rust supervisor left active after it.
    let mut targets: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut active_after: Option<Vec<(usize, String)>> = None;
    let c_mult_of = |s: &str| c_mults.get(s).copied().unwrap_or(1.0);
    let qty_step_of = |s: &str| qty_steps.get(s).copied().unwrap_or(0.0);
    let known_market = |s: &str| c_mults.contains_key(s);
    for ev in trace {
        let kind = ev["kind"].as_str().unwrap_or("");
        let ts = ev["ts"].as_u64().unwrap_or(0);
        if coin && !kind.starts_with("coin_") && kind != "compute" {
            continue;
        }
        match kind {
            "coin_init" | "coin_check_begin" | "coin_iter_begin" => {
                if kind == "coin_iter_begin" && ev["ok"].as_bool() != Some(true) {
                    continue;
                }
                let positions = trace_positions(&ev["positions"]);
                let fills_now = fills_until(fills, ts);
                if kind == "coin_init" {
                    inits += 1;
                    boot_fills = fills_now.clone();
                }
                let pairs = traced_pairs(ev);
                let known = traced_symbols(ev);
                checker.check_coin_inputs(
                    &mut par,
                    &format!("{kind}@{ts}"),
                    ts,
                    &pairs,
                    &positions,
                    &fills_now,
                    &boot_fills,
                    lookback.hsl_window_ms(),
                    c_mults,
                )?;
                let upnl = |pside: usize, symbol: &str| -> Result<f64> {
                    match pairs.get(&(pside, symbol.to_string())) {
                        Some(p) => Ok(p.2),
                        None if positions
                            .iter()
                            .any(|q| q.pside == pside && q.symbol == symbol) =>
                        {
                            bail!("no traced unrealized pnl for {pside}:{symbol} at {ts}")
                        }
                        None => Ok(0.0),
                    }
                };
                // The traced realized peak/last are Python's ledger values
                // when the record was written. Inside a supervisor iteration
                // a flat pair's flatten lookup refreshes that ledger
                // (`_flatten_fill_timestamp_with_refresh` -> `update_pnls`)
                // before the sample refresh, so for flat pairs the sample
                // must read the full ledger like the runner does.
                let supervisor_iteration = kind == "coin_iter_begin";
                let realized = |pside: usize, symbol: &str, _ts: u64, _reset: Option<u64>| {
                    let held = positions
                        .iter()
                        .any(|p| p.pside == pside && p.symbol == symbol && p.size != 0.0);
                    if supervisor_iteration && !held {
                        return None;
                    }
                    pairs.get(&(pside, symbol.to_string())).map(|p| (p.0, p.1))
                };
                let blocking = |pside: usize, symbol: &str| {
                    pairs
                        .get(&(pside, symbol.to_string()))
                        .map_or((0, 0), |p| (p.3, p.4))
                };
                let env = CoinEnv {
                    upnl: &upnl,
                    realized: Some(&realized),
                    blocking_orders: &blocking,
                };
                let inp = CoinInputs {
                    now_ms: ts,
                    balance: num(&ev["balance"]),
                    positions: &positions,
                    fills: &fills_now,
                    known_symbols: &known,
                };
                match kind {
                    "coin_init" => {
                        let history = {
                            let store = &*checker.store;
                            let close_at =
                                |symbol: &str, minute: u64| store.close_at(coin_of(symbol), minute);
                            coin_history(&ReplayInputs {
                                now_ms: ts,
                                balance_now: num(&ev["balance"]),
                                lookback,
                                fills: &fills_now,
                                positions: &positions,
                                known_positions: &known,
                                close_at: &close_at,
                                c_mult: &c_mult_of,
                                qty_step: &qty_step_of,
                                known_market: &known_market,
                            })
                        };
                        hsl.initialize_coin_from_history(
                            &inp,
                            &env,
                            &history,
                            &qty_step_of,
                            &known_market,
                        )?;
                        compare_coin_states(&mut par, &format!("init@{ts}"), &ev["after"], hsl);
                        compare_coin_modes(&mut par, &format!("init@{ts}"), &ev["modes"], hsl);
                    }
                    "coin_check_begin" => {
                        checks += 1;
                        hsl.check_coin(&inp, &env)?;
                    }
                    _ => {
                        iterations += 1;
                        let step = hsl.supervise_coin_red(&inp, &env)?;
                        par.eq(
                            &format!("iter@{ts}#{}.active", ev["iteration"]),
                            traced_pairs_list(&ev["active"]),
                            step.active_before,
                        );
                        active_after = Some(step.active_after);
                    }
                }
                checker.cycle.hsl = hsl.modes();
            }
            "coin_check_end" => {
                // A compacted fixture trace keeps the pair states of the
                // kept and transition cycles only (`select_fixtures.py`).
                if ev["after"].is_object() {
                    compare_coin_states(&mut par, &format!("check@{ts}"), &ev["after"], hsl);
                }
                compare_coin_modes(&mut par, &format!("check@{ts}"), &ev["modes"], hsl);
            }
            "coin_supervisor_begin" => {
                supervisions += 1;
                par.eq(
                    &format!("supervisor@{ts}.active"),
                    traced_pairs_list(&ev["active"]),
                    hsl.coin_panic_pairs(),
                );
            }
            "coin_iter_end" | "coin_supervisor_end" => {
                let label = if kind == "coin_iter_end" {
                    format!("iter@{ts}#{}", ev["iteration"])
                } else {
                    format!("supervisor@{ts}")
                };
                if let Some(after) = active_after.take() {
                    par.eq(
                        &format!("{label}.active_after"),
                        traced_pairs_list(&ev["active"]),
                        after,
                    );
                }
                compare_coin_states(&mut par, &label, &ev["after"], hsl);
                compare_coin_modes(&mut par, &label, &ev["modes"], hsl);
                if kind == "coin_iter_end" {
                    targets = ev["targets"]
                        .as_object()
                        .into_iter()
                        .flatten()
                        .map(|(k, v)| {
                            (
                                k.clone(),
                                v.as_array()
                                    .into_iter()
                                    .flatten()
                                    .filter_map(|p| p.as_str().map(str::to_string))
                                    .collect(),
                            )
                        })
                        .collect();
                }
                checker.cycle.hsl = hsl.modes();
            }
            "coin_flatten" => {
                // The flatten-fill lookup Python made (on its own ledger)
                // against the runner's lookup on the full ledger.
                flattens += 1;
                let pside = pside_index(ev["pside"].as_str().unwrap_or("long"));
                let symbol = ev["symbol"].as_str().unwrap_or("");
                let since = ev["since_ms"].as_u64();
                let sizes: Option<BTreeMap<String, f64>> = ev["replay_start_sizes"]
                    .as_object()
                    .map(|o| o.iter().map(|(k, v)| (k.clone(), num(v))).collect());
                let rs = since.and_then(|s| {
                    latest_flatten_fill_timestamp(
                        &fills_until(fills, ts),
                        pside,
                        Some(symbol),
                        Some(s),
                        sizes.as_ref(),
                    )
                });
                let py = ev["result"].as_u64();
                if py != rs {
                    let boot = since.and_then(|s| {
                        latest_flatten_fill_timestamp(
                            &boot_fills,
                            pside,
                            Some(symbol),
                            Some(s),
                            sizes.as_ref(),
                        )
                    });
                    if py == boot {
                        stale_flattens += 1;
                    } else {
                        par.mismatches.push(format!(
                            "flatten@{ts}.{}:{symbol}: python {py:?} vs rust {rs:?}",
                            ["long", "short"][pside]
                        ));
                    }
                }
            }
            "coin_finalize" => finalizes += 1,
            "coin_reset" => resets += 1,
            "compute" if coin => {
                let Some(file) = files.get(rec_i) else {
                    continue;
                };
                if ev["hash"].as_str() != Some(stem_hash(file).as_str()) {
                    continue;
                }
                let names: Option<Vec<String>> = ev["symbols"].as_array().map(|a| {
                    a.iter()
                        .filter_map(|s| s.as_str().map(str::to_string))
                        .collect()
                });
                if ev["protective"].as_bool() == Some(true) {
                    protectives += 1;
                    checker.run_protective(file, names.unwrap_or_default(), &targets, &mut par)?;
                } else {
                    checker.run(rec_i, file, names)?;
                }
                modes_by_hash.insert(
                    stem_hash(file),
                    format!(
                        "forced_long={:?} forced_short={:?}",
                        checker.cycle.hsl.runtime_forced[LONG],
                        checker.cycle.hsl.runtime_forced[SHORT]
                    ),
                );
                rec_i += 1;
            }
            "init" => {
                inits += 1;
                let positions = trace_positions(&ev["positions"]);
                let fills_now = fills_until(fills, ts);
                boot_fills = fills_now.clone();
                let start = lookback.event_history_start_ms(ts);
                checker.check_hsl_inputs(
                    &mut par,
                    &format!("init@{ts}"),
                    ev,
                    &positions,
                    &fills_now,
                    &boot_fills,
                    start,
                    c_mults,
                )?;
                let timeline = {
                    let store = &*checker.store;
                    let close_at =
                        |symbol: &str, minute: u64| store.close_at(coin_of(symbol), minute);
                    let c_mult = |s: &str| c_mults.get(s).copied().unwrap_or(1.0);
                    let qty_step = |s: &str| qty_steps.get(s).copied().unwrap_or(0.0);
                    let known = |s: &str| c_mults.contains_key(s);
                    balance_equity_timeline(
                        ts,
                        num(&ev["balance"]),
                        lookback,
                        &fills_now,
                        &positions,
                        &close_at,
                        &c_mult,
                        &qty_step,
                        &known,
                    )
                };
                hsl.initialize_from_history(
                    ts,
                    num(&ev["balance"]),
                    &fills_now,
                    &timeline,
                    num(&ev["realized_pnl_total"]),
                    [
                        num(&ev["realized_pnl_long"]),
                        num(&ev["realized_pnl_short"]),
                    ],
                    [
                        num(&ev["unrealized_pnl_long"]),
                        num(&ev["unrealized_pnl_short"]),
                    ],
                )?;
                compare_states(&mut par, &format!("init@{ts}"), &ev["after"], hsl);
                checker.cycle.hsl = hsl.modes();
            }
            "check_begin" => {
                checks += 1;
                let positions = trace_positions(&ev["positions"]);
                let fills_now = fills_until(fills, ts);
                let start = lookback.event_history_start_ms(ts);
                checker.check_hsl_inputs(
                    &mut par,
                    &format!("check@{ts}"),
                    ev,
                    &positions,
                    &fills_now,
                    &boot_fills,
                    start,
                    c_mults,
                )?;
                let inp = trace_inputs(ev, &positions, &fills_now);
                hsl.check(&inp)?;
                checker.cycle.hsl = hsl.modes();
            }
            "check_end" => {
                compare_states(&mut par, &format!("check@{ts}"), &ev["after"], hsl);
            }
            "supervisor_begin" => {
                supervisions += 1;
                supervisor = Some((ev.clone(), trace_positions(&ev["positions"]), false));
            }
            "counts" => {
                let Some((begin, positions, seen)) = supervisor.as_mut() else {
                    continue;
                };
                if *seen {
                    continue;
                }
                *seen = true;
                let fills_now = fills_until(fills, ts);
                let inp = trace_inputs(begin, positions, &fills_now);
                for pside in begin["psides"].as_array().into_iter().flatten() {
                    let name = pside.as_str().unwrap_or("long");
                    hsl.supervise_red(
                        pside_index(name),
                        observation(&ev["counts"][name]),
                        &inp,
                        Supervision::FakeHarness,
                    )?;
                }
                checker.cycle.hsl = hsl.modes();
            }
            "sync_flat" => {
                let positions = trace_positions(&ev["positions"]);
                let fills_now = fills_until(fills, ts);
                let inp = trace_inputs(ev, &positions, &fills_now);
                for pside in ev["psides"].as_array().into_iter().flatten() {
                    let name = pside.as_str().unwrap_or("long");
                    hsl.sync_flat_finalize(
                        pside_index(name),
                        observation(&ev["counts"][name]),
                        &inp,
                    )?;
                }
                compare_states(&mut par, &format!("sync_flat@{ts}"), &ev["after"], hsl);
                checker.cycle.hsl = hsl.modes();
            }
            "supervisor_end" => {
                compare_states(&mut par, &format!("supervisor@{ts}"), &ev["after"], hsl);
                supervisor = None;
            }
            "finalize" => {
                finalizes += 1;
                let name = ev["pside"].as_str().unwrap_or("long");
                compare_side(
                    &mut par,
                    &format!("finalize@{ts}.{name}"),
                    &ev["after"],
                    &hsl.sides[pside_index(name)],
                );
            }
            "reset" => resets += 1,
            "compute" => {
                let Some(file) = files.get(rec_i) else {
                    continue;
                };
                if ev["hash"].as_str() != Some(stem_hash(file).as_str()) {
                    continue;
                }
                let names = ev["symbols"].as_array().map(|a| {
                    a.iter()
                        .filter_map(|s| s.as_str().map(str::to_string))
                        .collect()
                });
                checker.run(rec_i, file, names)?;
                modes_by_hash.insert(
                    stem_hash(file),
                    format!(
                        "long={:?} short={:?}",
                        checker.cycle.hsl.side("long"),
                        checker.cycle.hsl.side("short")
                    ),
                );
                rec_i += 1;
            }
            _ => {}
        }
    }
    if rec_i != files.len() {
        bail!(
            "HSL trace matched {rec_i} of {} recordings (compute events by input hash)",
            files.len()
        );
    }
    if coin {
        println!(
            "hsl coin trace: {inits} init, {checks} checks, {supervisions} supervisor runs ({iterations} iterations, {protectives} protective recordings), {finalizes} finalizations, {resets} resets, {flattens} flatten lookups ({stale_flattens} explained by the stale ledger); {} floats compared, {} bit-exact, max rel dev {:.3e} ({}); {} realized-pnl inputs explained by the harness's stale fill ledger",
            par.floats, par.exact, par.max_rel, par.max_rel_at, par.stale_ledger
        );
    } else {
        println!(
            "hsl trace: {inits} init, {checks} checks, {supervisions} supervisor steps, {finalizes} finalizations, {resets} resets; {} floats compared, {} bit-exact, max rel dev {:.3e} ({}); {} realized-pnl inputs explained by the harness's stale fill ledger",
            par.floats, par.exact, par.max_rel, par.max_rel_at, par.stale_ledger
        );
    }
    Ok((par, modes_by_hash))
}

fn main() -> Result<()> {
    let args = Args::parse();
    let cfg_text = std::fs::read_to_string(&args.config)?;
    let cfg = ConfigView::new(serde_json::from_str(&cfg_text)?)?;
    let hsl_cfg = HslConfig::from_config(&cfg)?;
    let hsl_on = hsl_cfg.any_enabled();
    let mut store = CandleStore {
        dir: args.candles.clone(),
        dates: date_range(&args.dates)?,
        cache: BTreeMap::new(),
    };

    // Boot fills (seeded scenarios) -> trailing anchors per (symbol, pside);
    // market steps for the HSL start-up replay.
    let mut anchors: BTreeMap<(String, String), u64> = BTreeMap::new();
    let mut qty_steps: BTreeMap<String, f64> = BTreeMap::new();
    let mut c_mults: BTreeMap<String, f64> = BTreeMap::new();
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
        for (sym, m) in sc["symbols"].as_object().into_iter().flatten() {
            qty_steps.insert(sym.clone(), num(&m["qty_step"]));
            c_mults.insert(sym.clone(), m.get("contractSize").map_or(1.0, num));
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

    let all_symbols: Vec<String> = SnapshotBuilder::new(&cfg)?.universe(&[], &Default::default());
    let coins: Vec<String> = all_symbols.iter().map(|s| coin_of(s).to_string()).collect();
    store.load_all(&coins)?;
    let mut checker = Checker {
        cfg: &cfg,
        store: &mut store,
        anchors,
        all_symbols,
        markets: BTreeMap::new(),
        totals: BTreeMap::new(),
        examples: BTreeMap::new(),
        prev_out_symbols: HashSet::new(),
        // Cross-cycle state (SPEC 8): `PB_modes` from the previous recording's
        // output (the previous *cycle* when the set is not subsampled), the
        // dynamic forager eligibility, the close-EMA carry-forward cache, the
        // symbols ever seen and the HSL side modes.
        cycle: CycleState::default(),
        n: 0,
        clean: 0,
        verbose: args.verbose,
    };

    let mut parity: Option<(Parity, BTreeMap<String, String>)> = None;
    if hsl_on {
        let trace_path = args
            .hsl_trace
            .clone()
            .unwrap_or_else(|| args.recordings.join("hsl_trace.jsonl"));
        let fills_path = args
            .fills
            .clone()
            .unwrap_or_else(|| args.recordings.join("fills.json"));
        let fills = load_fills(&fills_path, &hsl_cfg.fee)
            .with_context(|| fills_path.display().to_string())?;
        let text = std::fs::read_to_string(&trace_path).with_context(|| {
            format!(
                "HSL enabled: need the run's trace at {}",
                trace_path.display()
            )
        })?;
        let mut trace: Vec<Value> = text
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(parse_exact)
            .collect::<Result<_>>()?;
        trace.sort_by_key(|v| v["seq"].as_u64().unwrap_or(0));
        if qty_steps.is_empty() {
            bail!("HSL enabled: pass --scenario for the market steps of the start-up replay");
        }
        let mut hsl = HslState::new(hsl_cfg);
        parity = Some(replay_with_trace(
            &mut checker,
            &files,
            &trace,
            &mut hsl,
            &fills,
            &qty_steps,
            &c_mults,
        )?);
    } else {
        for (fi, file) in files.iter().enumerate() {
            checker.run(fi, file, None)?;
        }
    }
    let Checker {
        totals,
        examples,
        n,
        clean,
        ..
    } = checker;
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
    let mut ok = totals.is_empty();
    if let Some((par, modes)) = parity {
        if args.verbose {
            for (h, m) in &modes {
                println!("  hsl modes {h}: {m}");
            }
        }
        if par.mismatches.is_empty() {
            println!("hsl trace: every traced state matches");
        } else {
            ok = false;
            println!("hsl trace: {} state mismatches", par.mismatches.len());
            let show = if args.verbose { usize::MAX } else { 30 };
            for m in par.mismatches.iter().take(show) {
                println!("          {m}");
            }
        }
    }
    if ok {
        Ok(())
    } else {
        std::process::exit(1)
    }
}
