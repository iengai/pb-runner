//! HSL `live.hsl_signal_mode = "coin"` (docs/SNAPSHOT_SPEC.md 2.3 steps
//! 2-3, docs/DECISIONS.md D20): one `EquityHardStopRuntime` per
//! `(pside, symbol)` fed with the engine's slot-budget drawdown signal
//! (`hsl_coin_drawdown_signal`: `drawdown_usd = peak_realized -
//! (last_realized + upnl)`, `slot_budget = balance / n_positions`), the
//! per-coin episode bookkeeping of `src/passivbot_hsl.py` (`hsl:`), the
//! runtime forced modes the coin machine writes for
//! `_orchestrator_mode_override` step 3, the production coin RED supervisor
//! (`_equity_hard_stop_run_coin_red_supervisor`, one iteration per call) and
//! the start-up reconstruction from the fill history
//! (`_equity_hard_stop_initialize_coin_from_history`, hsl:5569) over the
//! compact replay of `hsl::coin_history`.
//!
//! Not ported: the replay-matrix cache reuse (`_equity_hard_stop_try_reuse_replay_cache`
//! returns a history "equivalent to `get_balance_equity_history()`", an
//! accelerator that never decides; the replay then walks every row instead
//! of the change points), the background/partial replay (`mark_protective_ready`:
//! the runner replays synchronously, so `replay_pending` is empty once
//! initialized), latch files and events.

use crate::hsl::{
    flat_epsilon, latest_flatten_fill_timestamp, py_max2, CoinHistory, CooldownPositionPolicy,
    HslFill, HslPosition, HslState, LatchPayload, Runtime, SideConfig, Tier, LONG, ONE_MIN_MS,
    SHORT,
};
use anyhow::{anyhow, bail, Result};
use passivbot_rust::equity_hard_stop_loss as ehsl;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

const PSIDES: [&str; 2] = ["long", "short"];

/// `_equity_hard_stop_apply_coin_metrics_sample` result (hsl:3770).
#[derive(Debug, Clone, PartialEq)]
pub struct CoinMetrics {
    pub timestamp_ms: u64,
    pub balance: f64,
    pub slot_budget: f64,
    pub peak_realized_pnl: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub strategy_pnl: f64,
    pub peak_strategy_pnl: f64,
    pub baseline_balance: f64,
    /// `strategy_equity` = `equity` = `max(1 - drawdown_raw, 1e-12)`.
    pub strategy_equity: f64,
    pub drawdown_usd: f64,
    pub drawdown_raw: f64,
    pub drawdown_ema: f64,
    pub drawdown_score: f64,
    pub red_threshold: f64,
    pub tier: Tier,
    pub red_active_now: bool,
    pub red_seen_in_episode: bool,
    pub changed: bool,
    pub alpha: f64,
    pub elapsed_minutes: u64,
}

/// `_equity_hard_stop_compute_coin_stop_event` (hsl:4622).
#[derive(Debug, Clone, PartialEq)]
pub struct CoinStopEvent {
    pub stop_event_timestamp_ms: u64,
    pub balance: f64,
    pub slot_budget: f64,
    pub realized_pnl: f64,
    pub peak_realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub strategy_pnl: f64,
    pub peak_strategy_pnl: f64,
    pub strategy_equity: f64,
    pub peak_strategy_equity: f64,
    pub trigger_peak_strategy_equity: f64,
    pub drawdown_raw: f64,
    pub drawdown_ema: f64,
    pub drawdown_score: f64,
}

/// `_hsl_coin_state(pside, symbol)` (`_equity_hard_stop_make_state` plus
/// `pnl_reset_timestamp_ms`) minus logging throttles.
#[derive(Debug, Clone, Default)]
pub struct CoinState {
    pub runtime: Runtime,
    pub no_restart_peak_strategy_equity: f64,
    pub halted: bool,
    pub no_restart_latched: bool,
    pub last_metrics: Option<CoinMetrics>,
    pub red_flat_confirmations: u32,
    pub pending_red_since_ms: Option<u64>,
    pub cooldown_until_ms: Option<u64>,
    pub pending_stop_event: Option<CoinStopEvent>,
    pub last_stop_event: Option<LatchPayload>,
    pub cooldown_intervention_active: bool,
    pub cooldown_repanic_reset_pending: bool,
    pub cooldown_repanic_since_ms: Option<u64>,
    pub cooldown_repanic_start_sizes: Option<BTreeMap<String, f64>>,
    pub cooldown_unresolved_residue: bool,
    /// Realized-pnl window start after a finalized episode (`stop_ts + 1`).
    pub pnl_reset_timestamp_ms: Option<u64>,
}

/// Per-cycle account inputs of the coin machine.
pub struct CoinInputs<'a> {
    pub now_ms: u64,
    /// `get_raw_balance()`.
    pub balance: f64,
    /// Non-flat positions (`fetched_positions`).
    pub positions: &'a [HslPosition],
    /// The fill ledger (`_pnls_manager.get_events()`).
    pub fills: &'a [HslFill],
    /// `self.positions.keys()`: every symbol of the planning universe
    /// (zero-size entries included, SPEC 2.1).
    pub known_symbols: &'a BTreeSet<String>,
}

/// `(pside, symbol, timestamp_ms, reset_ts) -> Some((peak, last))`.
pub type RealizedOverride<'a> = dyn Fn(usize, &str, u64, Option<u64>) -> Option<(f64, f64)> + 'a;

/// What the coin machine asks its owner per pair.
pub struct CoinEnv<'a> {
    /// `_calc_upnl_sum_strict(pside, symbol)`: the pair's unrealized pnl at
    /// the live last price (errors on a missing price like Python).
    pub upnl: &'a dyn Fn(usize, &str) -> Result<f64>,
    /// Optional override of `_equity_hard_stop_coin_realized_pnl_peak_last`
    /// `(peak, last)` for `(pside, symbol, timestamp_ms, reset_ts)`
    /// (`pb-snapcheck` feeds the traced Python values); `None` derives them
    /// from `CoinInputs.fills`.
    pub realized: Option<&'a RealizedOverride<'a>>,
    /// `_equity_hard_stop_count_blocking_open_orders_symbol(pside, symbol)`
    /// -> `(entry_orders, nonpanic_close_orders)`.
    pub blocking_orders: &'a dyn Fn(usize, &str) -> (usize, usize),
}

/// Result of one coin RED supervisor iteration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CoinRedStep {
    /// Pairs that needed supervision when the iteration started.
    pub active_before: Vec<(usize, String)>,
    /// Pairs still needing panic supervision after the iteration: the
    /// protective planning targets (`_protective_panic_target_psides_by_symbol`
    /// = symbols whose mode override is `panic`).
    pub active_after: Vec<(usize, String)>,
}

/// `_equity_hard_stop_coin_replay_events` (hsl:3993): `(ts, increase, qty,
/// realized_delta)` per fill of the pair in timestamp order and whether the
/// replay is ambiguous (unusable fill, or a decrease larger than the size).
pub fn coin_replay_events(
    fills: &[HslFill],
    pside: usize,
    symbol: &str,
    qty_step: f64,
) -> (Vec<(u64, bool, f64, f64)>, bool) {
    let mut out = Vec::new();
    let mut ambiguous = false;
    let eps = flat_epsilon(qty_step);
    for f in fills
        .iter()
        .filter(|f| f.pside == pside && f.symbol == symbol)
    {
        if f.qty <= 0.0 || !f.qty.is_finite() {
            ambiguous = true;
            continue;
        }
        let delta = f.pnl + f.fee_paid;
        if !delta.is_finite() {
            ambiguous = true;
            continue;
        }
        out.push((f.timestamp_ms, f.increase, f.qty, delta));
    }
    out.sort_by_key(|e| e.0);
    let mut size = 0.0f64;
    for (_, increase, qty, _) in &out {
        if *increase {
            size += qty;
        } else {
            if *qty > size + eps {
                ambiguous = true;
            }
            size = (size - qty).max(0.0);
        }
    }
    (out, ambiguous)
}

/// `_equity_hard_stop_coin_bounded_required_replay_start_ts` (hsl:4039):
/// with `restart_after_red_policy = always`, the start of the current
/// episode (walking earlier episodes back while a cooldown could chain);
/// `None` when the boundary cannot be proven.
pub fn coin_bounded_required_replay_start_ts(
    fills: &[HslFill],
    pside: usize,
    symbol: &str,
    current_size: f64,
    qty_step: f64,
    cooldown_minutes: f64,
) -> Option<u64> {
    let eps = flat_epsilon(qty_step);
    let current_size = current_size.abs();
    if current_size <= eps {
        return None;
    }
    let (events, ambiguous) = coin_replay_events(fills, pside, symbol, qty_step);
    if ambiguous {
        return None;
    }
    let mut episodes: Vec<(u64, u64)> = Vec::new();
    let mut size = 0.0f64;
    let mut start: Option<u64> = None;
    for (ts, increase, qty, _) in &events {
        let was_flat = size <= eps;
        if *increase {
            size += qty;
            if was_flat && size > eps {
                start = Some(*ts);
            }
        } else {
            size = (size - qty).max(0.0);
            if !was_flat && size <= eps {
                let s = start?;
                episodes.push((s, *ts));
                start = None;
            }
        }
    }
    let tolerance = eps.max(current_size * 1e-12).max(1e-12);
    if (size - current_size).abs() > tolerance {
        return None;
    }
    let mut required = start?;
    let cooldown_ms = if cooldown_minutes > 0.0 {
        ((cooldown_minutes * 60_000.0).round() as i64).max(0) as u64
    } else {
        0
    };
    if cooldown_ms == 0 {
        return Some(required);
    }
    let mut next_start = required;
    for (prev_start, prev_flat) in episodes.iter().rev() {
        if prev_flat + cooldown_ms <= next_start {
            break;
        }
        required = *prev_start;
        next_start = *prev_start;
    }
    Some(required)
}

/// `_equity_hard_stop_infer_coin_replay_contract` (hsl:2862).
#[derive(Debug, Clone, PartialEq)]
pub struct CoinReplayContract {
    pub policy: CooldownPositionPolicy,
    pub latest_panic_ts: Option<u64>,
    pub cooldown_until_ms: Option<u64>,
    pub intervention_entry_ts: Option<u64>,
    pub active_cooldown_now: bool,
    pub intervention_active: bool,
    pub unresolved_residue: bool,
}

pub fn infer_coin_replay_contract(
    fills: &[HslFill],
    pside: usize,
    symbol: &str,
    pos_now: bool,
    policy: CooldownPositionPolicy,
    cooldown_minutes: f64,
    now_ms: u64,
) -> CoinReplayContract {
    let cooldown_ms = if cooldown_minutes > 0.0 {
        (cooldown_minutes * 60_000.0).round() as i64
    } else {
        0
    };
    let latest_panic_ts = fills
        .iter()
        .rfind(|f| f.pside == pside && f.symbol == symbol && f.is_panic())
        .map(|f| f.timestamp_ms);
    let cooldown_until_ms = match latest_panic_ts {
        Some(t) if cooldown_ms > 0 => Some(t + cooldown_ms as u64),
        _ => None,
    };
    let mut intervention_entry_ts = None;
    if let Some(latest) = latest_panic_ts {
        for f in fills
            .iter()
            .filter(|f| f.pside == pside && f.symbol == symbol)
        {
            if f.timestamp_ms <= latest {
                continue;
            }
            if cooldown_until_ms.is_some_and(|c| f.timestamp_ms >= c) {
                break;
            }
            if f.increase && !f.is_panic() {
                intervention_entry_ts = Some(f.timestamp_ms);
                break;
            }
        }
    }
    let active_cooldown_now = cooldown_until_ms.is_some_and(|c| now_ms < c);
    CoinReplayContract {
        policy,
        latest_panic_ts,
        cooldown_until_ms,
        intervention_entry_ts,
        active_cooldown_now,
        intervention_active: active_cooldown_now && pos_now && intervention_entry_ts.is_some(),
        unresolved_residue: active_cooldown_now && pos_now && intervention_entry_ts.is_none(),
    }
}

/// `_equity_hard_stop_replay_marker_confirms_red`.
fn replay_marker_confirms_red(m: &CoinMetrics) -> bool {
    m.tier == Tier::Red || m.drawdown_score >= m.red_threshold - 1e-12
}

/// `_hsl_compact_sparse_replay_indices` (hsl:3912): the change-point rows of
/// a pair's compact series (run starts/ends of balance / realized /
/// unrealized, the lookback expiry of every realized run end, every
/// boundary timestamp and its predecessor, the last row).
pub fn compact_sparse_replay_indices(
    timestamps: &[u64],
    balances: &[f64],
    realized: Option<&[f64]>,
    unrealized: Option<&[f64]>,
    lookback_ms: Option<u64>,
    boundary_timestamps: &[u64],
) -> Vec<usize> {
    let n = timestamps.len();
    if n == 0 {
        return Vec::new();
    }
    let mut selected = vec![false; n];
    let mark = |values: Option<&[f64]>, selected: &mut Vec<bool>| -> Vec<usize> {
        let Some(arr) = values else {
            return Vec::new();
        };
        let mut starts = vec![0usize];
        for i in 1..n {
            let (a, b) = (arr[i - 1], arr[i]);
            let same = (a.is_finite() == b.is_finite()) && (!b.is_finite() || a == b);
            if !same {
                starts.push(i);
            }
        }
        let mut ends: Vec<usize> = starts.iter().skip(1).map(|s| s - 1).collect();
        ends.push(n - 1);
        for s in &starts {
            selected[*s] = true;
        }
        for e in &ends {
            selected[*e] = true;
        }
        ends
    };
    mark(Some(balances), &mut selected);
    let realized_run_ends = mark(realized, &mut selected);
    mark(unrealized, &mut selected);
    if let Some(lb) = lookback_ms {
        for run_end in realized_run_ends {
            let target = timestamps[run_end] + lb;
            let expiry = timestamps.partition_point(|t| *t <= target);
            if expiry < n {
                selected[expiry] = true;
                if expiry > 0 {
                    selected[expiry - 1] = true;
                }
            }
        }
    }
    for b in boundary_timestamps {
        let idx = timestamps.partition_point(|t| *t < *b);
        if idx < n {
            selected[idx] = true;
        }
        if idx > 0 {
            selected[idx - 1] = true;
        }
    }
    selected[n - 1] = true;
    selected
        .iter()
        .enumerate()
        .filter(|(_, s)| **s)
        .map(|(i, _)| i)
        .collect()
}

/// `_equity_hard_stop_coin_realized_pnl_peak_last` (hsl:3485): `(peak,
/// current)` of the pair's running realized pnl over the events at or after
/// `max(timestamp - lookback, reset_ts)`; no upper bound on the timestamp.
pub fn coin_realized_pnl_peak_last(
    fills: &[HslFill],
    pside: usize,
    symbol: &str,
    timestamp_ms: u64,
    lookback_ms: Option<u64>,
    reset_ts: Option<u64>,
) -> (f64, f64) {
    let mut start_ms: Option<i64> = lookback_ms.map(|l| timestamp_ms as i64 - l as i64);
    if let Some(r) = reset_ts {
        start_ms = Some(start_ms.map_or(r as i64, |s| s.max(r as i64)));
    }
    let mut events: Vec<&HslFill> = fills
        .iter()
        .filter(|f| f.pside == pside && f.symbol == symbol)
        .filter(|f| start_ms.is_none_or(|s| f.timestamp_ms as i64 >= s))
        .collect();
    events.sort_by_key(|f| f.timestamp_ms);
    let mut current = 0.0;
    let mut peak = 0.0;
    for e in events {
        current += e.pnl;
        current += e.fee_paid;
        peak = py_max2(peak, current);
    }
    (peak, current)
}

impl HslState {
    /// `_hsl_coin_state(pside, symbol)`: creates the state on first use.
    pub fn coin_state(&mut self, pside: usize, symbol: &str) -> &mut CoinState {
        self.coin[pside].entry(symbol.to_string()).or_default()
    }

    fn coin_cfg(&self, pside: usize, symbol: &str) -> &SideConfig {
        self.cfg.side_config(pside, Some(symbol))
    }

    /// `_equity_hard_stop_set_coin_runtime_forced_mode`.
    pub fn set_coin_runtime_forced_mode(&mut self, pside: usize, symbol: &str, mode: &str) {
        self.runtime_forced[pside].insert(symbol.to_string(), mode.to_string());
    }

    /// `_equity_hard_stop_clear_coin_runtime_forced_mode`.
    pub fn clear_coin_runtime_forced_mode(&mut self, pside: usize, symbol: &str) {
        self.runtime_forced[pside].remove(symbol);
    }

    /// `_equity_hard_stop_reset_coin_after_restart` (hsl:7273): everything
    /// but the pnl reset timestamp and the no-restart peak; the pair's
    /// runtime forced mode is cleared.
    pub fn reset_coin_after_restart(&mut self, pside: usize, symbol: &str) {
        let s = self.coin_state(pside, symbol);
        let reset_ts = s.pnl_reset_timestamp_ms;
        let keep = s.no_restart_peak_strategy_equity;
        *s = CoinState {
            pnl_reset_timestamp_ms: reset_ts,
            no_restart_peak_strategy_equity: keep,
            ..CoinState::default()
        };
        self.clear_coin_runtime_forced_mode(pside, symbol);
    }

    /// `_equity_hard_stop_prime_coin_runtime_for_replay` (hsl:4325): a
    /// green baseline sample one minute before the first replayed sample.
    fn prime_coin_runtime(
        &mut self,
        pside: usize,
        symbol: &str,
        first_sample_ts: u64,
    ) -> Result<()> {
        let cfg = self.coin_cfg(pside, symbol).clone();
        let s = self.coin_state(pside, symbol);
        if s.runtime.initialized() {
            return Ok(());
        }
        let baseline_ts = first_sample_ts.saturating_sub(ONE_MIN_MS);
        s.runtime.last_rolling_peak = 1.0;
        ehsl::step_with_peak_strategy_equity_latch(
            &mut s.runtime.state,
            engine_cfg(&cfg),
            1.0,
            1.0,
            baseline_ts,
            true,
        )
        .map_err(|e| anyhow!("HSL coin runtime prime: {e}"))?;
        Ok(())
    }

    /// `_equity_hard_stop_apply_coin_metrics_sample` (hsl:3676).
    #[allow(clippy::too_many_arguments)]
    pub fn apply_coin_metrics_sample(
        &mut self,
        pside: usize,
        symbol: &str,
        timestamp_ms: u64,
        balance: f64,
        peak_realized: f64,
        last_realized: f64,
        current_upnl: f64,
        latch_red: bool,
    ) -> Result<CoinMetrics> {
        if !balance.is_finite() || balance <= 0.0 {
            bail!("balance must be finite and > 0, got {balance}");
        }
        if !peak_realized.is_finite() {
            bail!("peak_realized must be finite, got {peak_realized}");
        }
        if !last_realized.is_finite() {
            bail!("last_realized must be finite, got {last_realized}");
        }
        if !current_upnl.is_finite() {
            bail!("current_upnl must be finite, got {current_upnl}");
        }
        let cfg = self.coin_cfg(pside, symbol).clone();
        let n_raw = self.cfg.n_positions[pside];
        if !n_raw.is_finite() || n_raw <= 0.0 {
            bail!(
                "coin HSL n_positions must be finite and > 0 for {symbol} {}, got {n_raw}",
                PSIDES[pside]
            );
        }
        let n = n_raw.round_ties_even() as i64;
        if n <= 0 {
            bail!(
                "coin HSL n_positions must round to > 0 for {symbol} {}, got {n_raw}",
                PSIDES[pside]
            );
        }
        let signal = ehsl::coin_drawdown_signal(
            balance,
            n as usize,
            peak_realized,
            last_realized,
            current_upnl,
        )
        .map_err(|e| anyhow!("hsl_coin_drawdown_signal: {e}"))?;
        let current_minute = timestamp_ms / ONE_MIN_MS;
        let state = self.coin_state(pside, symbol);
        if let Some(last) = &state.last_metrics {
            if last.timestamp_ms / ONE_MIN_MS == current_minute {
                let same_inputs = last.balance == balance
                    && last.peak_realized_pnl == peak_realized
                    && last.realized_pnl == last_realized
                    && last.unrealized_pnl == current_upnl;
                let needs_latching_replay_red =
                    latch_red && last.tier == Tier::Red && !state.runtime.red_latched();
                if same_inputs && !needs_latching_replay_red {
                    let mut cached = last.clone();
                    cached.changed = false;
                    cached.elapsed_minutes = 0;
                    state.last_metrics = Some(cached.clone());
                    return Ok(cached);
                }
            }
        }
        let prev_tier = state.runtime.tier();
        let synthetic_equity = py_max2(1.0 - signal.drawdown_raw, 1e-12);
        state.runtime.last_rolling_peak = 1.0;
        let step = ehsl::step_with_peak_strategy_equity_latch(
            &mut state.runtime.state,
            engine_cfg(&cfg),
            synthetic_equity,
            1.0,
            timestamp_ms,
            latch_red,
        )
        .map_err(|e| anyhow!("HSL coin runtime: {e}"))?;
        let tier = Tier::from_engine(state.runtime.state.tier);
        let metrics = CoinMetrics {
            timestamp_ms,
            balance,
            slot_budget: signal.slot_budget,
            peak_realized_pnl: peak_realized,
            realized_pnl: last_realized,
            unrealized_pnl: current_upnl,
            strategy_pnl: last_realized + current_upnl,
            peak_strategy_pnl: peak_realized,
            baseline_balance: balance,
            strategy_equity: synthetic_equity,
            drawdown_usd: signal.drawdown_usd,
            drawdown_raw: step.drawdown_raw,
            drawdown_ema: state.runtime.state.drawdown_ema,
            drawdown_score: step.drawdown_score,
            red_threshold: cfg.red_threshold,
            tier,
            red_active_now: step.red_active_now,
            red_seen_in_episode: state.runtime.state.red_seen_in_episode,
            changed: step.changed || tier != prev_tier,
            alpha: step.alpha,
            elapsed_minutes: step.elapsed_minutes,
        };
        state.last_metrics = Some(metrics.clone());
        Ok(metrics)
    }

    /// `(peak, last)` realized pnl for a sample: the owner's override or the
    /// ledger derivation.
    fn coin_realized(
        &self,
        pside: usize,
        symbol: &str,
        timestamp_ms: u64,
        fills: &[HslFill],
        env: &CoinEnv,
    ) -> (f64, f64) {
        let reset_ts = self.coin[pside]
            .get(symbol)
            .and_then(|s| s.pnl_reset_timestamp_ms);
        if let Some(f) = env.realized {
            if let Some(v) = f(pside, symbol, timestamp_ms, reset_ts) {
                return v;
            }
        }
        coin_realized_pnl_peak_last(
            fills,
            pside,
            symbol,
            timestamp_ms,
            self.cfg.lookback.hsl_window_ms(),
            reset_ts,
        )
    }

    /// `_equity_hard_stop_apply_coin_sample` (hsl:3648): the ledger's
    /// `(peak, last)` plus the pair's live unrealized pnl.
    #[allow(clippy::too_many_arguments)]
    pub fn apply_coin_sample(
        &mut self,
        pside: usize,
        symbol: &str,
        timestamp_ms: u64,
        balance: f64,
        fills: &[HslFill],
        env: &CoinEnv,
        latch_red: bool,
    ) -> Result<CoinMetrics> {
        let (peak, last) = self.coin_realized(pside, symbol, timestamp_ms, fills, env);
        let upnl = (env.upnl)(pside, symbol)?;
        self.apply_coin_metrics_sample(
            pside,
            symbol,
            timestamp_ms,
            balance,
            peak,
            last,
            upnl,
            latch_red,
        )
    }

    /// `_equity_hard_stop_compute_coin_stop_event` (hsl:4622): the latest
    /// metrics when they are at or after the stop timestamp, else a sample
    /// taken at the stop timestamp.
    pub fn compute_coin_stop_event(
        &mut self,
        pside: usize,
        symbol: &str,
        stop_ts: u64,
        inp: &CoinInputs,
        env: &CoinEnv,
    ) -> Result<CoinStopEvent> {
        let current = self.coin[pside]
            .get(symbol)
            .and_then(|s| s.last_metrics.clone());
        let m = match current {
            Some(m) if m.timestamp_ms >= stop_ts => m,
            _ => {
                self.apply_coin_sample(pside, symbol, stop_ts, inp.balance, inp.fills, env, true)?
            }
        };
        Ok(CoinStopEvent {
            stop_event_timestamp_ms: stop_ts,
            balance: m.balance,
            slot_budget: m.slot_budget,
            realized_pnl: m.realized_pnl,
            peak_realized_pnl: m.peak_realized_pnl,
            unrealized_pnl: m.unrealized_pnl,
            strategy_pnl: m.strategy_pnl,
            peak_strategy_pnl: m.peak_strategy_pnl,
            strategy_equity: m.strategy_equity,
            peak_strategy_equity: 1.0,
            trigger_peak_strategy_equity: 1.0,
            drawdown_raw: m.drawdown_raw,
            drawdown_ema: m.drawdown_ema,
            drawdown_score: m.drawdown_score,
        })
    }

    /// `_equity_hard_stop_red_episode_finalization` for a coin scope.
    fn coin_red_episode_finalization(
        &mut self,
        pside: usize,
        symbol: &str,
        stop_equity: f64,
        stop_peak: f64,
        drawdown_ema: f64,
        stop_ts: u64,
    ) -> Result<ehsl::RedEpisodeFinalization> {
        let cfg = self.coin_cfg(pside, symbol).clone();
        let prev_peak = self
            .coin_state(pside, symbol)
            .no_restart_peak_strategy_equity;
        let r = ehsl::evaluate_red_episode_finalization(
            &cfg.restart_after_red_policy,
            stop_ts,
            stop_equity,
            stop_peak,
            prev_peak,
            drawdown_ema,
            cfg.red_threshold,
            cfg.no_restart_drawdown_threshold,
            cfg.cooldown_minutes_after_red,
        )
        .map_err(|e| anyhow!("HSL coin red episode finalization: {e}"))?;
        self.coin_state(pside, symbol)
            .no_restart_peak_strategy_equity = r.no_restart_peak_strategy_equity;
        Ok(r)
    }

    /// `_equity_hard_stop_finalize_coin_red_stop` (hsl:7940).
    pub fn finalize_coin_red_stop(
        &mut self,
        pside: usize,
        symbol: &str,
        ev: &CoinStopEvent,
    ) -> Result<()> {
        let stop_ts = ev.stop_event_timestamp_ms;
        let fin = self.coin_red_episode_finalization(
            pside,
            symbol,
            ev.strategy_equity,
            ev.peak_strategy_equity,
            ev.drawdown_ema,
            stop_ts,
        )?;
        let s = self.coin_state(pside, symbol);
        s.last_stop_event = Some(LatchPayload {
            stop_event_timestamp_ms: stop_ts,
            strategy_equity: ev.strategy_equity,
            peak_strategy_equity: ev.peak_strategy_equity,
            trigger_peak_strategy_equity: ev.trigger_peak_strategy_equity,
            drawdown_raw: ev.drawdown_raw,
            drawdown_ema: ev.drawdown_ema,
            drawdown_score: ev.drawdown_score,
            no_restart_latched: fin.no_restart_latched,
            cooldown_until_ms: fin.cooldown_until_ms,
            no_restart_peak_strategy_equity: fin.no_restart_peak_strategy_equity,
            no_restart_drawdown_raw: fin.no_restart_drawdown_raw,
            complete: true,
        });
        s.halted = true;
        s.no_restart_latched = fin.no_restart_latched;
        s.cooldown_until_ms = fin.cooldown_until_ms;
        s.pending_stop_event = None;
        s.red_flat_confirmations = 0;
        s.pending_red_since_ms = None;
        s.pnl_reset_timestamp_ms = Some(stop_ts + 1);
        self.clear_coin_runtime_forced_mode(pside, symbol);
        if fin.cooldown_until_ms.is_some() {
            self.set_coin_runtime_forced_mode(pside, symbol, "graceful_stop");
        }
        Ok(())
    }

    /// `_equity_hard_stop_flatten_fill_timestamp_with_refresh` without the
    /// ledger refresh (the runner's ledger is complete): `since = None` and
    /// a missing anchor both defer (`_defer_missing_flatten_fill`).
    fn coin_flatten_fill_timestamp(
        &mut self,
        pside: usize,
        symbol: &str,
        fills: &[HslFill],
        since_ms: Option<u64>,
        replay_start_sizes: Option<&BTreeMap<String, f64>>,
    ) -> Option<u64> {
        let found = since_ms.and_then(|since| {
            latest_flatten_fill_timestamp(
                fills,
                pside,
                Some(symbol),
                Some(since),
                replay_start_sizes,
            )
        });
        if found.is_none() {
            let s = self.coin_state(pside, symbol);
            s.pending_stop_event = None;
            s.red_flat_confirmations = 0;
        }
        found
    }

    /// `_equity_hard_stop_refresh_coin_cooldown_after_repanic` (hsl:4776).
    fn refresh_coin_cooldown_after_repanic(
        &mut self,
        pside: usize,
        symbol: &str,
        inp: &CoinInputs,
        env: &CoinEnv,
    ) -> Result<bool> {
        let cooldown_minutes = self.coin_cfg(pside, symbol).cooldown_minutes_after_red;
        let cooldown_ms = if cooldown_minutes > 0.0 {
            ((cooldown_minutes * 60_000.0).round() as i64).max(0) as u64
        } else {
            0
        };
        let (since, sizes) = {
            let s = self.coin_state(pside, symbol);
            (
                s.cooldown_repanic_since_ms,
                s.cooldown_repanic_start_sizes.clone().unwrap_or_default(),
            )
        };
        let Some(stop_ts) =
            self.coin_flatten_fill_timestamp(pside, symbol, inp.fills, since, Some(&sizes))
        else {
            return Ok(false);
        };
        let cooldown_until_ms = (cooldown_ms > 0).then(|| stop_ts + cooldown_ms);
        let ev = self.compute_coin_stop_event(pside, symbol, stop_ts, inp, env)?;
        let s = self.coin_state(pside, symbol);
        s.last_stop_event = Some(LatchPayload {
            stop_event_timestamp_ms: stop_ts,
            strategy_equity: ev.strategy_equity,
            peak_strategy_equity: ev.peak_strategy_equity,
            trigger_peak_strategy_equity: ev.trigger_peak_strategy_equity,
            drawdown_raw: ev.drawdown_raw,
            drawdown_ema: ev.drawdown_ema,
            drawdown_score: ev.drawdown_score,
            no_restart_latched: false,
            cooldown_until_ms,
            no_restart_peak_strategy_equity: ev.peak_strategy_equity,
            no_restart_drawdown_raw: ev.drawdown_raw,
            complete: true,
        });
        s.cooldown_until_ms = cooldown_until_ms;
        s.cooldown_intervention_active = false;
        s.cooldown_repanic_reset_pending = false;
        s.cooldown_repanic_since_ms = None;
        s.cooldown_repanic_start_sizes = None;
        s.cooldown_unresolved_residue = false;
        s.pending_stop_event = None;
        s.red_flat_confirmations = 0;
        s.pnl_reset_timestamp_ms = Some(stop_ts + 1);
        if cooldown_until_ms.is_some() {
            self.set_coin_runtime_forced_mode(pside, symbol, "graceful_stop");
        }
        Ok(true)
    }

    fn has_open_position_symbol(inp: &CoinInputs, pside: usize, symbol: &str) -> bool {
        inp.positions
            .iter()
            .any(|p| p.pside == pside && p.symbol == symbol && p.size != 0.0)
    }

    /// `_equity_hard_stop_handle_coin_position_during_cooldown` (hsl:4941).
    pub fn handle_coin_position_during_cooldown(
        &mut self,
        pside: usize,
        symbol: &str,
        inp: &CoinInputs,
        env: &CoinEnv,
    ) -> Result<bool> {
        let now_ms = inp.now_ms;
        {
            let s = self.coin_state(pside, symbol);
            if !s.halted || s.no_restart_latched {
                return Ok(false);
            }
            let repanic_pending = s.cooldown_repanic_reset_pending;
            if (s.cooldown_until_ms.is_none() || s.cooldown_until_ms.is_some_and(|c| now_ms >= c))
                && !repanic_pending
            {
                return Ok(false);
            }
        }
        let has_position = Self::has_open_position_symbol(inp, pside, symbol);
        let policy = self.cfg.cooldown_position_policy;
        if !has_position {
            if self
                .coin_state(pside, symbol)
                .cooldown_repanic_reset_pending
            {
                self.refresh_coin_cooldown_after_repanic(pside, symbol, inp, env)?;
                return Ok(true);
            }
            let s = self.coin_state(pside, symbol);
            s.cooldown_intervention_active = false;
            s.cooldown_repanic_reset_pending = false;
            s.cooldown_repanic_since_ms = None;
            s.cooldown_repanic_start_sizes = None;
            s.cooldown_unresolved_residue = false;
            return Ok(false);
        }
        if self.coin_state(pside, symbol).cooldown_unresolved_residue {
            return Ok(false);
        }
        self.coin_state(pside, symbol).cooldown_intervention_active = true;
        match policy {
            CooldownPositionPolicy::Normal => {
                self.reset_coin_after_restart(pside, symbol);
                Ok(true)
            }
            CooldownPositionPolicy::Panic => {
                let size = inp
                    .positions
                    .iter()
                    .find(|p| p.pside == pside && p.symbol == symbol)
                    .map_or(0.0, |p| p.size.abs());
                let s = self.coin_state(pside, symbol);
                if !s.cooldown_repanic_reset_pending {
                    s.cooldown_repanic_since_ms = Some(now_ms);
                    s.cooldown_repanic_start_sizes =
                        Some(BTreeMap::from([(symbol.to_string(), size)]));
                }
                s.cooldown_repanic_reset_pending = true;
                self.set_coin_runtime_forced_mode(pside, symbol, "panic");
                Ok(false)
            }
            CooldownPositionPolicy::Manual => {
                self.set_coin_runtime_forced_mode(pside, symbol, "manual");
                Ok(false)
            }
            CooldownPositionPolicy::TpOnly => {
                self.set_coin_runtime_forced_mode(
                    pside,
                    symbol,
                    "tp_only_with_active_entry_cancellation",
                );
                Ok(false)
            }
            CooldownPositionPolicy::GracefulStop => {
                self.set_coin_runtime_forced_mode(pside, symbol, "graceful_stop");
                Ok(false)
            }
        }
    }

    /// `_equity_hard_stop_coin_symbols` (hsl:7255): `self.positions` keys,
    /// every pair state and every fill symbol inside the HSL lookback.
    pub fn coin_symbols(&self, inp: &CoinInputs) -> Vec<String> {
        let mut out: BTreeSet<String> = inp.known_symbols.clone();
        out.extend(inp.positions.iter().map(|p| p.symbol.clone()));
        for m in &self.coin {
            out.extend(m.keys().cloned());
        }
        let start = self
            .cfg
            .lookback
            .hsl_window_ms()
            .map(|l| inp.now_ms as i64 - l as i64);
        for f in inp.fills {
            if start.is_some_and(|s| (f.timestamp_ms as i64) < s) {
                continue;
            }
            if !f.symbol.is_empty() {
                out.insert(f.symbol.clone());
            }
        }
        out.retain(|s| !s.is_empty());
        out.into_iter().collect()
    }

    /// `_equity_hard_stop_check_coin` (hsl:7409): cooldown handling, the
    /// per-pair sample, the runtime forced modes and the check-path
    /// finalization of a red-seen episode whose sample recovered.
    pub fn check_coin(&mut self, inp: &CoinInputs, env: &CoinEnv) -> Result<()> {
        if !self.cfg.coin_mode() {
            bail!("HslState::check_coin needs hsl_signal_mode = coin");
        }
        if !self.coin_initialized {
            bail!("HSL coin machine is not initialized (initialize_coin_from_history)");
        }
        let ts_ms = inp.now_ms;
        let balance = inp.balance;
        let symbols = self.coin_symbols(inp);
        for pside in [LONG, SHORT] {
            if !self.cfg.coin_active_pside(pside, None)? {
                continue;
            }
            for symbol in &symbols {
                if !self.cfg.coin_active_pside(pside, Some(symbol))? {
                    continue;
                }
                if self.coin_state(pside, symbol).halted {
                    self.handle_coin_position_during_cooldown(pside, symbol, inp, env)?;
                    let s = self.coin_state(pside, symbol);
                    if s.halted {
                        if !s.no_restart_latched
                            && !s.cooldown_repanic_reset_pending
                            && s.cooldown_until_ms.is_some_and(|c| ts_ms >= c)
                        {
                            self.reset_coin_after_restart(pside, symbol);
                        } else {
                            if !s.cooldown_repanic_reset_pending
                                && !self.runtime_forced[pside].contains_key(symbol)
                            {
                                self.set_coin_runtime_forced_mode(pside, symbol, "graceful_stop");
                            }
                            continue;
                        }
                    }
                }
                let prev_latched = self.coin_state(pside, symbol).runtime.red_latched();
                let metrics =
                    self.apply_coin_sample(pside, symbol, ts_ms, balance, inp.fills, env, true)?;
                let latched_now = self.coin_state(pside, symbol).runtime.red_latched();
                if metrics.tier == Tier::Red && !prev_latched {
                    let s = self.coin_state(pside, symbol);
                    s.pending_red_since_ms = Some(metrics.timestamp_ms);
                    s.pending_stop_event = None;
                    self.set_coin_runtime_forced_mode(pside, symbol, "panic");
                } else if metrics.tier != Tier::Red && !latched_now {
                    let s = self.coin_state(pside, symbol);
                    s.pending_red_since_ms = None;
                    if !s.halted {
                        self.clear_coin_runtime_forced_mode(pside, symbol);
                    }
                }
                if metrics.tier == Tier::Orange {
                    let target = self.coin_cfg(pside, symbol).orange_tier_mode.clone();
                    if target == "graceful_stop"
                        || target == "tp_only_with_active_entry_cancellation"
                    {
                        self.set_coin_runtime_forced_mode(pside, symbol, &target);
                    }
                }
                let (latched, halted) = {
                    let s = self.coin_state(pside, symbol);
                    (s.runtime.red_latched(), s.halted)
                };
                if latched && !halted && !metrics.red_active_now {
                    // B2.1 red split: the episode saw RED but the sample
                    // recovered; entries stay blocked and the check path
                    // performs the flat confirmations / finalization.
                    self.set_coin_runtime_forced_mode(
                        pside,
                        symbol,
                        "tp_only_with_active_entry_cancellation",
                    );
                    let has_position = Self::has_open_position_symbol(inp, pside, symbol);
                    let (entry_orders, nonpanic) = (env.blocking_orders)(pside, symbol);
                    if !has_position && entry_orders == 0 && nonpanic == 0 {
                        let since = self.coin_state(pside, symbol).pending_red_since_ms;
                        if let Some(stop_ts) =
                            self.coin_flatten_fill_timestamp(pside, symbol, inp.fills, since, None)
                        {
                            let ev =
                                self.compute_coin_stop_event(pside, symbol, stop_ts, inp, env)?;
                            let s = self.coin_state(pside, symbol);
                            s.pending_stop_event = Some(ev);
                            s.red_flat_confirmations += 1;
                        }
                    } else {
                        let s = self.coin_state(pside, symbol);
                        s.red_flat_confirmations = 0;
                        s.pending_stop_event = None;
                    }
                    let s = self.coin_state(pside, symbol);
                    if s.red_flat_confirmations >= 2 {
                        let ev = s.pending_stop_event.clone().ok_or_else(|| {
                            anyhow!("HSL coin flat confirmations without a pending stop event")
                        })?;
                        self.finalize_coin_red_stop(pside, symbol, &ev)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// `_equity_hard_stop_coin_needs_panic_supervision` (hsl:7597).
    pub fn coin_needs_panic_supervision(&self, pside: usize, symbol: &str) -> bool {
        if !self.cfg.enabled_symbol(pside, symbol) {
            return false;
        }
        let Some(s) = self.coin[pside].get(symbol) else {
            return false;
        };
        if s.runtime.red_latched() && !s.halted {
            return s.last_metrics.as_ref().is_none_or(|m| m.red_active_now);
        }
        s.halted && s.cooldown_repanic_reset_pending
    }

    /// Pairs needing panic supervision, in state order.
    pub fn coin_panic_pairs(&self) -> Vec<(usize, String)> {
        let mut out = Vec::new();
        for pside in [LONG, SHORT] {
            for symbol in self.coin[pside].keys() {
                if self.coin_needs_panic_supervision(pside, symbol) {
                    out.push((pside, symbol.clone()));
                }
            }
        }
        out
    }

    /// `_equity_hard_stop_coin_red_active`.
    pub fn coin_red_active(&self) -> bool {
        !self.coin_panic_pairs().is_empty()
    }

    /// One iteration of `_equity_hard_stop_run_coin_red_supervisor`
    /// (hsl:8205) after the protective refresh: flat confirmations or the
    /// panic-mode refresh per active pair, finalization / repanic cooldown
    /// refresh after two confirmations. The caller runs the protective
    /// panic planning for `active_after` when it is not empty.
    pub fn supervise_coin_red(&mut self, inp: &CoinInputs, env: &CoinEnv) -> Result<CoinRedStep> {
        let active_before = self.coin_panic_pairs();
        for (pside, symbol) in &active_before {
            let (pside, symbol) = (*pside, symbol.as_str());
            let has_position = Self::has_open_position_symbol(inp, pside, symbol);
            let (entry_orders, nonpanic) = (env.blocking_orders)(pside, symbol);
            if !has_position && entry_orders == 0 && nonpanic == 0 {
                let (halted_repanic, since, sizes) = {
                    let s = self.coin_state(pside, symbol);
                    let hr = s.halted && s.cooldown_repanic_reset_pending;
                    (
                        hr,
                        if hr {
                            s.cooldown_repanic_since_ms
                        } else {
                            s.pending_red_since_ms
                        },
                        hr.then(|| s.cooldown_repanic_start_sizes.clone().unwrap_or_default()),
                    )
                };
                if let Some(stop_ts) = self.coin_flatten_fill_timestamp(
                    pside,
                    symbol,
                    inp.fills,
                    since,
                    sizes.as_ref(),
                ) {
                    if !halted_repanic {
                        let ev = self.compute_coin_stop_event(pside, symbol, stop_ts, inp, env)?;
                        self.coin_state(pside, symbol).pending_stop_event = Some(ev);
                    }
                    self.coin_state(pside, symbol).red_flat_confirmations += 1;
                }
            } else {
                let s = self.coin_state(pside, symbol);
                s.red_flat_confirmations = 0;
                s.pending_stop_event = None;
            }
            let (confirmations, halted_repanic, pending) = {
                let s = self.coin_state(pside, symbol);
                (
                    s.red_flat_confirmations,
                    s.halted && s.cooldown_repanic_reset_pending,
                    s.pending_stop_event.clone(),
                )
            };
            if confirmations >= 2 {
                if halted_repanic {
                    self.refresh_coin_cooldown_after_repanic(pside, symbol, inp, env)?;
                } else {
                    let ev = pending.ok_or_else(|| {
                        anyhow!("HSL coin flat confirmations without a pending stop event")
                    })?;
                    self.finalize_coin_red_stop(pside, symbol, &ev)?;
                }
            } else {
                // B2.1: refresh the sample so recovery is observable; any
                // failure keeps panic (recovery must be proven).
                let recovered = match self.apply_coin_sample(
                    pside,
                    symbol,
                    inp.now_ms,
                    inp.balance,
                    inp.fills,
                    env,
                    true,
                ) {
                    Ok(m) => !m.red_active_now,
                    Err(e) => {
                        tracing::warn!(
                            symbol,
                            pside = PSIDES[pside],
                            error = %e,
                            "[risk] HSL coin RED supervisor sample refresh failed; keeping panic mode"
                        );
                        false
                    }
                };
                let mode = if recovered {
                    "tp_only_with_active_entry_cancellation"
                } else {
                    "panic"
                };
                self.set_coin_runtime_forced_mode(pside, symbol, mode);
            }
        }
        Ok(CoinRedStep {
            active_before,
            active_after: self.coin_panic_pairs(),
        })
    }

    /// `_equity_hard_stop_activate_coin_red_from_metrics`.
    fn activate_coin_red(&mut self, pside: usize, symbol: &str, ts: u64) {
        let s = self.coin_state(pside, symbol);
        if s.pending_red_since_ms.is_none() {
            s.pending_red_since_ms = Some(ts);
        }
        s.pending_stop_event = None;
        self.set_coin_runtime_forced_mode(pside, symbol, "panic");
    }

    /// `_equity_hard_stop_initialize_coin_from_history` (hsl:5569) over the
    /// compact history of `hsl::coin_history` (`qty_step(symbol)` and
    /// `known_market(symbol)` = `symbol in self.c_mults` as there).
    #[allow(clippy::too_many_arguments)]
    pub fn initialize_coin_from_history(
        &mut self,
        inp: &CoinInputs,
        env: &CoinEnv,
        history: &CoinHistory,
        qty_step: &dyn Fn(&str) -> f64,
        known_market: &dyn Fn(&str) -> bool,
    ) -> Result<()> {
        if !self.cfg.any_enabled() || !self.cfg.coin_mode() {
            return Ok(());
        }
        for m in &mut self.coin {
            m.clear();
        }
        for m in &mut self.runtime_forced {
            m.clear();
        }
        self.replay_pending.clear();
        self.coin_initialized = false;
        let now_ms = inp.now_ms;
        let lookback_ms = self.cfg.lookback.hsl_window_ms();
        let lookback_start_ms: Option<i64> = lookback_ms.map(|l| now_ms as i64 - l as i64);
        let position_symbols: BTreeSet<&str> =
            inp.positions.iter().map(|p| p.symbol.as_str()).collect();
        let supported = |symbol: &str| position_symbols.contains(symbol) || known_market(symbol);
        let fill_events = &history.fill_events;
        let mut by_pair: BTreeMap<(usize, String), Vec<&HslFill>> = BTreeMap::new();
        for f in fill_events {
            by_pair
                .entry((f.pside, f.symbol.clone()))
                .or_default()
                .push(f);
        }
        let mut symbols: BTreeSet<String> = inp.known_symbols.clone();
        let mut current_position_pairs: BTreeSet<(usize, String)> = BTreeSet::new();
        let mut required_replay_pairs: BTreeSet<(usize, String)> = BTreeSet::new();
        let mut required_replay_start_ts: BTreeMap<(usize, String), u64> = BTreeMap::new();
        let mut panic_replay_pairs: BTreeSet<(usize, String)> = BTreeSet::new();
        let remember =
            |map: &mut BTreeMap<(usize, String), u64>, pside: usize, symbol: &str, ts: u64| {
                let replay_ts = ts / ONE_MIN_MS * ONE_MIN_MS;
                let e = map.entry((pside, symbol.to_string())).or_insert(replay_ts);
                if replay_ts < *e {
                    *e = replay_ts;
                }
            };
        for p in inp.positions {
            if p.size != 0.0 {
                symbols.insert(p.symbol.clone());
                current_position_pairs.insert((p.pside, p.symbol.clone()));
                required_replay_pairs.insert((p.pside, p.symbol.clone()));
            }
        }
        for f in fill_events {
            if lookback_start_ms.is_some_and(|s| (f.timestamp_ms as i64) < s) {
                continue;
            }
            if f.symbol.is_empty() || !supported(&f.symbol) {
                continue;
            }
            symbols.insert(f.symbol.clone());
        }
        let mut bounded_held_replay_starts: BTreeMap<(usize, String), u64> = BTreeMap::new();
        for (pside, symbol) in &current_position_pairs {
            let pair_fills: Vec<HslFill> = by_pair
                .get(&(*pside, symbol.clone()))
                .map(|v| v.iter().map(|f| (*f).clone()).collect())
                .unwrap_or_default();
            let cfg = self.coin_cfg(*pside, symbol);
            let mut bounded = None;
            if cfg.restart_after_red_policy == "always" {
                let size = inp
                    .positions
                    .iter()
                    .find(|p| p.pside == *pside && p.symbol == *symbol)
                    .map_or(0.0, |p| p.size);
                bounded = coin_bounded_required_replay_start_ts(
                    &pair_fills,
                    *pside,
                    symbol,
                    size,
                    qty_step(symbol),
                    cfg.cooldown_minutes_after_red,
                );
            }
            if let Some(b) = bounded {
                let replay_ts = b / ONE_MIN_MS * ONE_MIN_MS;
                required_replay_start_ts.insert((*pside, symbol.clone()), replay_ts);
                bounded_held_replay_starts.insert((*pside, symbol.clone()), replay_ts);
            } else {
                for f in &pair_fills {
                    remember(
                        &mut required_replay_start_ts,
                        *pside,
                        symbol,
                        f.timestamp_ms,
                    );
                }
            }
        }
        for (_, symbol) in history.pair_values.keys() {
            if supported(symbol) {
                symbols.insert(symbol.clone());
            }
        }
        // Latest panic marker per (pside, symbol, minute).
        let mut latest_panic: BTreeMap<(usize, String, u64), u64> = BTreeMap::new();
        for m in &history.panic_flatten_events {
            if m.symbol.is_empty() {
                continue;
            }
            if lookback_start_ms.is_some_and(|s| (m.timestamp as i64) < s) {
                continue;
            }
            if !supported(&m.symbol) {
                continue;
            }
            symbols.insert(m.symbol.clone());
            panic_replay_pairs.insert((m.pside, m.symbol.clone()));
            required_replay_pairs.insert((m.pside, m.symbol.clone()));
            let bounded = bounded_held_replay_starts.get(&(m.pside, m.symbol.clone()));
            if bounded.is_none_or(|b| m.minute_timestamp >= *b) {
                remember(
                    &mut required_replay_start_ts,
                    m.pside,
                    &m.symbol,
                    m.minute_timestamp,
                );
            }
            let key = (m.pside, m.symbol.clone(), m.minute_timestamp);
            let e = latest_panic.entry(key).or_insert(m.timestamp);
            if m.timestamp >= *e {
                *e = m.timestamp;
            }
        }
        let replay_row_count = history.timestamps.partition_point(|t| *t <= now_ms);
        let balance = inp.balance;
        let mut active_pairs: Vec<(usize, String)> = Vec::new();
        for pside in [LONG, SHORT] {
            for symbol in &symbols {
                if self.cfg.coin_active_pside(pside, Some(symbol))? {
                    active_pairs.push((pside, symbol.clone()));
                }
            }
        }
        let active_pair_set: BTreeSet<(usize, String)> = active_pairs.iter().cloned().collect();
        let held: BTreeSet<(usize, String)> = active_pair_set
            .intersection(&current_position_pairs)
            .cloned()
            .collect();
        let cooldown: BTreeSet<(usize, String)> = active_pair_set
            .intersection(&panic_replay_pairs)
            .filter(|p| !held.contains(p))
            .cloned()
            .collect();
        // `_hsl_coin_replay_candidate_batches`: held, cooldown, rest.
        let mut ordered: Vec<(usize, String)> = Vec::new();
        ordered.extend(active_pairs.iter().filter(|p| held.contains(p)).cloned());
        ordered.extend(
            active_pairs
                .iter()
                .filter(|p| cooldown.contains(p))
                .cloned(),
        );
        ordered.extend(
            active_pairs
                .iter()
                .filter(|p| !held.contains(p) && !cooldown.contains(p))
                .cloned(),
        );
        self.replay_pending = active_pair_set.clone();

        for (pside, symbol) in ordered {
            let symbol = symbol.as_str();
            self.coin_state(pside, symbol);
            let cfg = self.coin_cfg(pside, symbol).clone();
            let cooldown_ms: u64 = if cfg.cooldown_minutes_after_red > 0.0 {
                (cfg.cooldown_minutes_after_red * 60_000.0).round() as u64
            } else {
                0
            };
            let pair_fills: Vec<HslFill> = by_pair
                .get(&(pside, symbol.to_string()))
                .map(|v| v.iter().map(|f| (*f).clone()).collect())
                .unwrap_or_default();
            let pos_now = Self::has_open_position_symbol(inp, pside, symbol);
            let contract = infer_coin_replay_contract(
                &pair_fills,
                pside,
                symbol,
                pos_now,
                self.cfg.cooldown_position_policy,
                cfg.cooldown_minutes_after_red,
                now_ms,
            );
            let mut replay_start_boundary_ts = bounded_held_replay_starts
                .get(&(pside, symbol.to_string()))
                .copied();
            if let (Some(entry_ts), CooldownPositionPolicy::Normal) =
                (contract.intervention_entry_ts, contract.policy)
            {
                replay_start_boundary_ts =
                    Some(replay_start_boundary_ts.map_or(entry_ts, |b| b.max(entry_ts)));
                self.reset_coin_after_restart(pside, symbol);
            }
            let mut window = RealizedWindow::default();
            let mut reset_baseline_realized = 0.0f64;
            let mut applied_rows = 0usize;
            let require_coin_timeline_fields =
                required_replay_pairs.contains(&(pside, symbol.to_string()));
            let required_start_ts = required_replay_start_ts
                .get(&(pside, symbol.to_string()))
                .copied();
            let mut seen_coin_timeline_fields = false;
            let step = qty_step(symbol);
            let (replay_events, replay_ambiguous) =
                coin_replay_events(&pair_fills, pside, symbol, step);
            if let Some(b) = replay_start_boundary_ts
                .filter(|_| bounded_held_replay_starts.contains_key(&(pside, symbol.to_string())))
            {
                reset_baseline_realized =
                    replay_events.iter().filter(|e| e.0 < b).map(|e| e.3).sum();
            }
            let pair_uses_dense_replay =
                replay_ambiguous || held.contains(&(pside, symbol.to_string()));
            let mut sparse_boundaries: Vec<u64> = Vec::new();
            if let Some(t) = required_start_ts {
                sparse_boundaries.push(t);
            }
            if let Some(t) = replay_start_boundary_ts {
                sparse_boundaries.push(t);
            }
            if let Some(t) = contract.cooldown_until_ms {
                sparse_boundaries.push(t);
            }
            for (ts, _, _, _) in &replay_events {
                sparse_boundaries.push(ts / ONE_MIN_MS * ONE_MIN_MS);
                if cooldown_ms > 0 {
                    sparse_boundaries.push(ts + cooldown_ms);
                }
            }
            for ((mp, ms, minute_ts), stop_ts) in &latest_panic {
                if *mp != pside || ms != symbol {
                    continue;
                }
                sparse_boundaries.push(*minute_ts);
                if cooldown_ms > 0 {
                    sparse_boundaries.push(stop_ts + cooldown_ms);
                }
            }
            let series = history.pair_values.get(&(pside, symbol.to_string()));
            let indices: Vec<usize> = if pair_uses_dense_replay {
                (0..replay_row_count).collect()
            } else {
                compact_sparse_replay_indices(
                    &history.timestamps[..replay_row_count],
                    &history.balances[..replay_row_count],
                    series.map(|s| &s.realized[..replay_row_count]),
                    series.map(|s| &s.unrealized[..replay_row_count]),
                    lookback_ms,
                    &sparse_boundaries,
                )
            };

            let mut replay_event_idx = 0usize;
            let mut replay_size = 0.0f64;
            let eps = flat_epsilon(step);
            let mut ignored_markers: BTreeSet<u64> = BTreeSet::new();
            // `replay_transitions_at`: advance the fill-derived size through
            // the row's minute, collecting every zero crossing with the
            // cumulative realized delta at that point.
            let mut transitions_at = |row_ts: u64| -> (f64, Vec<(u64, f64)>, f64) {
                let boundary = row_ts + ONE_MIN_MS;
                let mut realized_delta = 0.0;
                let mut flatten: Vec<(u64, f64)> = Vec::new();
                while replay_event_idx < replay_events.len() {
                    let (ts, increase, qty, delta) = replay_events[replay_event_idx];
                    if ts >= boundary {
                        break;
                    }
                    let was_nonflat = replay_size > eps;
                    realized_delta += delta;
                    if increase {
                        replay_size += qty;
                    } else {
                        replay_size = (replay_size - qty).max(0.0);
                        if was_nonflat && replay_size <= eps {
                            flatten.push((ts, realized_delta));
                        }
                    }
                    replay_event_idx += 1;
                }
                (replay_size, flatten, realized_delta)
            };

            let mut halted_break = false;
            for idx in indices {
                let ts = history.timestamps[idx];
                let row_balance = history.balances[idx];
                let row_realized_pnl = history.realized_pnl[idx];
                let (has_realized, realized_value, has_unrealized, unrealized_value) = match series
                {
                    Some(s) => (
                        !s.realized[idx].is_nan(),
                        s.realized[idx],
                        !s.unrealized[idx].is_nan(),
                        s.unrealized[idx],
                    ),
                    None => (false, f64::NAN, false, f64::NAN),
                };
                if replay_start_boundary_ts.is_some_and(|b| ts < b) {
                    transitions_at(ts);
                    continue;
                }
                {
                    let s = self.coin_state(pside, symbol);
                    if s.halted {
                        if !s.no_restart_latched && s.cooldown_until_ms.is_some_and(|c| ts >= c) {
                            self.reset_coin_after_restart(pside, symbol);
                            window.clear();
                        } else {
                            continue;
                        }
                    }
                }
                let row_has_coin_fields = has_realized || has_unrealized;
                let require_value = (require_coin_timeline_fields
                    && required_start_ts.is_some_and(|r| ts >= r))
                    || seen_coin_timeline_fields
                    || row_has_coin_fields;
                if !require_value {
                    continue;
                }
                let (replay_position_size, flatten_boundaries, row_realized_delta) =
                    transitions_at(ts);
                if !has_realized {
                    bail!(
                        "coin HSL replay missing required realized_pnl_by_coin_pside value for {}:{symbol} at {ts}",
                        PSIDES[pside]
                    );
                }
                let abs_realized = realized_value;
                seen_coin_timeline_fields = true;
                let marker = latest_panic.get(&(pside, symbol.to_string(), ts)).copied();
                let mut metrics: Option<CoinMetrics> = None;
                let mut stop_ts: Option<u64> = None;
                let mut stop_abs_realized = abs_realized;
                if !flatten_boundaries.is_empty() && !replay_ambiguous {
                    let row_start_abs_realized = abs_realized - row_realized_delta;
                    for (flatten_ts, delta_at_flatten) in &flatten_boundaries {
                        let boundary_abs_realized = row_start_abs_realized + delta_at_flatten;
                        let boundary_balance = row_balance - row_realized_delta + delta_at_flatten;
                        let reset_ts = self.coin_state(pside, symbol).pnl_reset_timestamp_ms;
                        let (peak, window_last) = window.sample(
                            lookback_ms,
                            *flatten_ts,
                            boundary_abs_realized,
                            reset_ts,
                            reset_baseline_realized,
                        );
                        self.prime_coin_runtime(pside, symbol, *flatten_ts)?;
                        let m = self.apply_coin_metrics_sample(
                            pside,
                            symbol,
                            *flatten_ts,
                            boundary_balance,
                            peak,
                            window_last,
                            0.0,
                            false,
                        )?;
                        applied_rows += 1;
                        let mut boundary_marker = marker.filter(|m| *m == *flatten_ts);
                        if boundary_marker.is_some() && !replay_marker_confirms_red(&m) {
                            ignored_markers.insert(*flatten_ts);
                            boundary_marker = None;
                        }
                        if boundary_marker.is_some() || m.red_seen_in_episode {
                            stop_ts = Some(*flatten_ts);
                            stop_abs_realized = boundary_abs_realized;
                            metrics = Some(m);
                            break;
                        }
                        // RED-free episodes end at every zero crossing.
                        self.coin_state(pside, symbol).pnl_reset_timestamp_ms =
                            Some(flatten_ts + 1);
                        reset_baseline_realized = boundary_abs_realized;
                        self.reset_coin_after_restart(pside, symbol);
                        window.clear();
                    }
                    if stop_ts.is_none() {
                        continue;
                    }
                } else {
                    let reset_ts = self.coin_state(pside, symbol).pnl_reset_timestamp_ms;
                    let (peak, window_last) = window.sample(
                        lookback_ms,
                        ts,
                        abs_realized,
                        reset_ts,
                        reset_baseline_realized,
                    );
                    let current_upnl = if !has_unrealized {
                        if replay_ambiguous || replay_position_size > eps {
                            if !require_coin_timeline_fields {
                                continue;
                            }
                            bail!(
                                "coin HSL replay missing required unrealized_pnl_by_coin_pside value for {}:{symbol} at {ts}",
                                PSIDES[pside]
                            );
                        }
                        0.0
                    } else {
                        unrealized_value
                    };
                    self.prime_coin_runtime(pside, symbol, ts)?;
                    let m = self.apply_coin_metrics_sample(
                        pside,
                        symbol,
                        ts,
                        row_balance,
                        peak,
                        window_last,
                        current_upnl,
                        false,
                    )?;
                    applied_rows += 1;
                    let Some(marker_ts) = marker else {
                        continue;
                    };
                    if !replay_marker_confirms_red(&m) {
                        ignored_markers.insert(marker_ts);
                        continue;
                    }
                    stop_ts = Some(marker_ts);
                    metrics = Some(m);
                }
                let (stop_ts, m) = (stop_ts.unwrap(), metrics.unwrap());
                let stop_drawdown_raw = m.drawdown_raw;
                let fin = self.coin_red_episode_finalization(
                    pside,
                    symbol,
                    m.strategy_equity,
                    1.0,
                    m.drawdown_ema,
                    stop_ts,
                )?;
                let s = self.coin_state(pside, symbol);
                s.last_stop_event = Some(LatchPayload {
                    stop_event_timestamp_ms: stop_ts,
                    strategy_equity: m.strategy_equity,
                    peak_strategy_equity: 1.0,
                    trigger_peak_strategy_equity: 1.0,
                    drawdown_raw: stop_drawdown_raw,
                    drawdown_ema: m.drawdown_ema,
                    drawdown_score: m.drawdown_score,
                    no_restart_latched: fin.no_restart_latched,
                    cooldown_until_ms: fin.cooldown_until_ms,
                    no_restart_peak_strategy_equity: fin.no_restart_peak_strategy_equity,
                    no_restart_drawdown_raw: fin.no_restart_drawdown_raw,
                    complete: true,
                });
                s.pnl_reset_timestamp_ms = Some(stop_ts + 1);
                s.pending_red_since_ms = None;
                reset_baseline_realized = stop_abs_realized;
                window.clear();
                let _ = row_realized_pnl;
                if fin.no_restart_latched {
                    s.halted = true;
                    s.no_restart_latched = true;
                    s.cooldown_until_ms = None;
                    self.set_coin_runtime_forced_mode(pside, symbol, "graceful_stop");
                    halted_break = true;
                    break;
                }
                s.halted = true;
                s.no_restart_latched = false;
                s.cooldown_until_ms = fin.cooldown_until_ms;
                if fin.cooldown_until_ms.is_some_and(|c| now_ms < c) {
                    self.set_coin_runtime_forced_mode(pside, symbol, "graceful_stop");
                    continue;
                }
                self.reset_coin_after_restart(pside, symbol);
            }
            let _ = halted_break;
            // A panic contract without a reconstructed stop: the cooldown is
            // active by the fills alone.
            {
                let s = self.coin_state(pside, symbol);
                if !s.halted
                    && contract
                        .latest_panic_ts
                        .is_some_and(|t| !ignored_markers.contains(&t))
                    && contract.active_cooldown_now
                    && !s.no_restart_latched
                    && !(contract.policy == CooldownPositionPolicy::Normal
                        && contract.intervention_entry_ts.is_some())
                {
                    s.halted = true;
                    s.cooldown_until_ms = contract.cooldown_until_ms;
                    s.cooldown_intervention_active = contract.intervention_active;
                    s.cooldown_unresolved_residue = contract.unresolved_residue;
                    if s.last_stop_event.is_none() {
                        s.last_stop_event = Some(LatchPayload {
                            stop_event_timestamp_ms: contract.latest_panic_ts.unwrap(),
                            strategy_equity: 0.0,
                            peak_strategy_equity: 0.0,
                            trigger_peak_strategy_equity: 0.0,
                            drawdown_raw: 0.0,
                            drawdown_ema: 0.0,
                            drawdown_score: 0.0,
                            no_restart_latched: false,
                            cooldown_until_ms: contract.cooldown_until_ms,
                            no_restart_peak_strategy_equity: 0.0,
                            no_restart_drawdown_raw: 0.0,
                            complete: false,
                        });
                    }
                }
            }
            let (halted, no_restart, cooldown_until) = {
                let s = self.coin_state(pside, symbol);
                (s.halted, s.no_restart_latched, s.cooldown_until_ms)
            };
            if halted && !no_restart {
                if let Some(c) = cooldown_until {
                    if now_ms >= c {
                        self.reset_coin_after_restart(pside, symbol);
                    } else {
                        let s = self.coin_state(pside, symbol);
                        s.cooldown_intervention_active = contract.intervention_active;
                        s.cooldown_unresolved_residue = contract.unresolved_residue;
                        let residue = s.cooldown_unresolved_residue;
                        let held_now = pos_now;
                        let mode = if residue {
                            if held_now {
                                "panic"
                            } else {
                                "graceful_stop"
                            }
                        } else if contract.intervention_active && held_now {
                            match contract.policy {
                                CooldownPositionPolicy::Panic => "panic",
                                CooldownPositionPolicy::Manual => "manual",
                                CooldownPositionPolicy::TpOnly => {
                                    "tp_only_with_active_entry_cancellation"
                                }
                                _ => "graceful_stop",
                            }
                        } else {
                            "graceful_stop"
                        };
                        self.set_coin_runtime_forced_mode(pside, symbol, mode);
                    }
                }
            }
            if self.coin_state(pside, symbol).halted {
                self.replay_pending.remove(&(pside, symbol.to_string()));
                continue;
            }
            if applied_rows == 0 {
                self.prime_coin_runtime(pside, symbol, now_ms)?;
            }
            let current =
                self.apply_coin_sample(pside, symbol, now_ms, balance, inp.fills, env, true)?;
            if current.tier == Tier::Red {
                self.activate_coin_red(pside, symbol, current.timestamp_ms);
            }
            self.replay_pending.remove(&(pside, symbol.to_string()));
        }
        self.coin_initialized = true;
        Ok(())
    }
}

/// `rolling_realized_at` of the coin replay (hsl:6237): a sliding window
/// of `(ts, realized)` samples with a monotonic max deque; the value of
/// the last expired sample is the window base.
#[derive(Default)]
struct RealizedWindow {
    points: VecDeque<(u64, f64)>,
    max_points: VecDeque<(u64, f64)>,
    base: f64,
}

impl RealizedWindow {
    fn clear(&mut self) {
        self.points.clear();
        self.max_points.clear();
        self.base = 0.0;
    }

    /// `(peak_realized, window_last_realized)` for a sample.
    fn sample(
        &mut self,
        lookback_ms: Option<u64>,
        sample_ts: u64,
        sample_abs_realized: f64,
        reset_ts: Option<u64>,
        reset_baseline: f64,
    ) -> (f64, f64) {
        let last_realized = sample_abs_realized - reset_baseline;
        let mut start_ms: Option<i64> = lookback_ms.map(|l| sample_ts as i64 - l as i64);
        if let Some(r) = reset_ts {
            start_ms = Some(start_ms.map_or(r as i64, |s| s.max(r as i64)));
        }
        self.points.push_back((sample_ts, last_realized));
        while self
            .max_points
            .back()
            .is_some_and(|(_, v)| *v <= last_realized)
        {
            self.max_points.pop_back();
        }
        self.max_points.push_back((sample_ts, last_realized));
        if let Some(start) = start_ms {
            while self
                .points
                .front()
                .is_some_and(|(t, _)| (*t as i64) < start)
            {
                let (_, old) = self.points.pop_front().unwrap();
                self.base = old;
            }
            while self
                .max_points
                .front()
                .is_some_and(|(t, _)| (*t as i64) < start)
            {
                self.max_points.pop_front();
            }
        }
        let window_last = if self.points.is_empty() {
            0.0
        } else {
            last_realized - self.base
        };
        let peak = py_max2(
            0.0,
            self.max_points.front().map_or(0.0, |(_, v)| v - self.base),
        );
        (peak, window_last)
    }
}

fn engine_cfg(cfg: &SideConfig) -> ehsl::HardStopConfig {
    ehsl::HardStopConfig {
        red_threshold: cfg.red_threshold,
        ema_span_minutes: cfg.ema_span_minutes,
        tier_ratios: ehsl::HardStopTierRatios {
            yellow: cfg.ratio_yellow,
            orange: cfg.ratio_orange,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bot_params::ConfigView;
    use crate::hsl::HslConfig;
    use serde_json::Value;
    use std::path::PathBuf;

    /// `make_coin_bot()` of `tests/test_hsl_coin_mode.py`: coin mode, long
    /// enabled with red 0.5 / ema 1 min / cooldown 5 min / no-restart 0.9,
    /// `n_positions = 2` (slot budget 50 on a 100 balance), repanic policy
    /// `panic`.
    fn config(edit: impl FnOnce(&mut Value)) -> HslConfig {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/configs/fake_v8/grid_v7.json");
        let mut v: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        v["live"]["hsl_signal_mode"] = Value::from("coin");
        v["live"]["hsl_position_during_cooldown_policy"] = Value::from("panic");
        v["live"]["pnls_max_lookback_days"] = Value::from(30.0);
        v["bot"]["long"]["risk"]["n_positions"] = Value::from(2);
        v["bot"]["long"]["risk"]["total_wallet_exposure_limit"] = Value::from(2.0);
        v["bot"]["long"]["hsl"]["enabled"] = Value::Bool(true);
        v["bot"]["long"]["hsl"]["red_threshold"] = Value::from(0.5);
        v["bot"]["long"]["hsl"]["ema_span_minutes"] = Value::from(1.0);
        v["bot"]["long"]["hsl"]["cooldown_minutes_after_red"] = Value::from(5.0);
        v["bot"]["long"]["hsl"]["no_restart_drawdown_threshold"] = Value::from(0.9);
        v["bot"]["long"]["hsl"]["restart_after_red_policy"] = Value::from("threshold");
        v["bot"]["long"]["hsl"]["orange_tier_mode"] =
            Value::from("tp_only_with_active_entry_cancellation");
        edit(&mut v);
        HslConfig::from_config(&ConfigView::new(v).unwrap()).unwrap()
    }

    fn state() -> HslState {
        let mut st = HslState::new(config(|_| {}));
        st.coin_initialized = true;
        st
    }

    const A: &str = "A/USDT:USDT";

    fn pos(symbol: &str, size: f64) -> HslPosition {
        HslPosition {
            symbol: symbol.into(),
            pside: LONG,
            size,
            price: 100.0,
        }
    }

    fn fill(ts: u64, symbol: &str, qty: f64, increase: bool, pb_order_type: &str) -> HslFill {
        HslFill {
            timestamp_ms: ts,
            symbol: symbol.into(),
            pside: LONG,
            qty,
            price: 100.0,
            increase,
            pnl: 0.0,
            fee_paid: 0.0,
            pb_order_type: pb_order_type.into(),
        }
    }

    /// The pair's unrealized pnl and blocking-order counts of a step.
    struct Env {
        upnl: f64,
        blocking: (usize, usize),
    }

    fn run_check(
        st: &mut HslState,
        ts: u64,
        positions: &[HslPosition],
        fills: &[HslFill],
        env: &Env,
    ) {
        let known: BTreeSet<String> = [A.to_string()].into_iter().collect();
        let upnl = |_: usize, _: &str| Ok(env.upnl);
        let blocking = |_: usize, _: &str| env.blocking;
        let e = CoinEnv {
            upnl: &upnl,
            realized: None,
            blocking_orders: &blocking,
        };
        st.check_coin(
            &CoinInputs {
                now_ms: ts,
                balance: 100.0,
                positions,
                fills,
                known_symbols: &known,
            },
            &e,
        )
        .unwrap();
    }

    fn run_supervisor(
        st: &mut HslState,
        ts: u64,
        positions: &[HslPosition],
        fills: &[HslFill],
        env: &Env,
    ) -> CoinRedStep {
        let known: BTreeSet<String> = [A.to_string()].into_iter().collect();
        let upnl = |_: usize, _: &str| Ok(env.upnl);
        let blocking = |_: usize, _: &str| env.blocking;
        let e = CoinEnv {
            upnl: &upnl,
            realized: None,
            blocking_orders: &blocking,
        };
        st.supervise_coin_red(
            &CoinInputs {
                now_ms: ts,
                balance: 100.0,
                positions,
                fills,
                known_symbols: &known,
            },
            &e,
        )
        .unwrap()
    }

    /// `test_compact_sparse_replay_indices_keep_run_and_explicit_boundaries`.
    #[test]
    fn sparse_indices_keep_runs_and_explicit_boundaries() {
        let ts: Vec<u64> = (0..20).map(|i| i * 60_000).collect();
        let constant = vec![0.0; 20];
        let idx = compact_sparse_replay_indices(
            &ts,
            &[100.0; 20],
            Some(&constant),
            Some(&constant),
            Some(30 * 24 * 60 * 60 * 1_000),
            &[10 * 60_000],
        );
        assert_eq!(idx, vec![0, 9, 10, 19]);
    }

    /// `test_compact_sparse_replay_indices_keep_rolling_window_expiry_boundary`.
    #[test]
    fn sparse_indices_keep_rolling_window_expiry() {
        let ts: Vec<u64> = (0..10).map(|i| i * 60_000).collect();
        let mut realized = vec![0.0; 3];
        realized.extend([1.0; 7]);
        let idx = compact_sparse_replay_indices(
            &ts,
            &[100.0; 10],
            Some(&realized),
            Some(&[0.0; 10]),
            Some(2 * 60_000),
            &[],
        );
        assert_eq!(idx, vec![0, 2, 3, 4, 5, 9]);
    }

    /// `test_coin_panic_supervision_requires_red_active_now` (B2.1 red
    /// split).
    #[test]
    fn panic_supervision_requires_red_active_now() {
        let mut st = state();
        let held = [pos(A, 1.0)];
        let env = Env {
            upnl: 0.0,
            blocking: (0, 0),
        };
        run_check(&mut st, 60_000, &held, &[], &env);
        run_check(
            &mut st,
            120_000,
            &held,
            &[],
            &Env {
                upnl: -30.0,
                blocking: (0, 0),
            },
        );
        assert!(st.coin[LONG][A].runtime.red_latched());
        // No metrics against the latched state: stay protective.
        st.coin_state(LONG, A).last_metrics = None;
        assert!(st.coin_needs_panic_supervision(LONG, A));
        // Current sample in RED: panic authorized.
        let mut m = st
            .apply_coin_metrics_sample(LONG, A, 180_000, 100.0, 0.0, 0.0, -30.0, true)
            .unwrap();
        assert!(m.red_active_now);
        assert!(st.coin_needs_panic_supervision(LONG, A));
        // Current sample recovered: no new panic orders for this episode.
        m.red_active_now = false;
        st.coin_state(LONG, A).last_metrics = Some(m);
        assert!(!st.coin_needs_panic_supervision(LONG, A));
        // Halted repanic-reset supervision is unaffected by the split.
        let s = st.coin_state(LONG, A);
        s.halted = true;
        s.cooldown_repanic_reset_pending = true;
        assert!(st.coin_needs_panic_supervision(LONG, A));
        assert_eq!(st.coin_panic_pairs(), vec![(LONG, A.to_string())]);
    }

    /// `test_recovered_red_episode_finalizes_from_check_path`: RED latches,
    /// the sample recovers while the position is open (panic pauses,
    /// tp-only holds), the position flattens normally and the episode is
    /// finalized by the check path at the flattening fill.
    #[test]
    fn recovered_red_episode_finalizes_from_check_path() {
        let mut st = state();
        let held = [pos(A, 1.0)];
        let flat: [HslPosition; 0] = [];
        let quiet = Env {
            upnl: 0.0,
            blocking: (0, 0),
        };
        run_check(&mut st, 60_000, &held, &[], &quiet);
        run_check(
            &mut st,
            120_000,
            &held,
            &[],
            &Env {
                upnl: -30.0,
                blocking: (0, 0),
            },
        );
        {
            let s = &st.coin[LONG][A];
            assert_eq!(s.last_metrics.as_ref().unwrap().tier, Tier::Red);
            assert!(s.runtime.red_latched());
        }
        assert_eq!(st.runtime_forced[LONG][A], "panic");
        assert_eq!(st.modes().runtime_forced("long", A), Some("panic"));
        // Recovery while the position is open.
        run_check(&mut st, 180_000, &held, &[], &quiet);
        {
            let s = &st.coin[LONG][A];
            assert!(!s.last_metrics.as_ref().unwrap().red_active_now);
            assert!(!s.halted);
            assert_eq!(s.pending_red_since_ms, Some(120_000));
        }
        assert_eq!(
            st.runtime_forced[LONG][A],
            "tp_only_with_active_entry_cancellation"
        );
        assert!(!st.coin_needs_panic_supervision(LONG, A));
        assert!(!st.coin_red_active());
        // Flat exchange state without its close fill: no cooldown anchor.
        run_check(&mut st, 240_000, &flat, &[], &quiet);
        {
            let s = &st.coin[LONG][A];
            assert_eq!(s.red_flat_confirmations, 0);
            assert!(!s.halted);
            assert_eq!(s.pending_red_since_ms, Some(120_000));
        }
        let fills = [fill(230_000, A, 1.0, false, "close_grid_long")];
        run_check(&mut st, 300_000, &flat, &fills, &quiet);
        {
            let s = &st.coin[LONG][A];
            assert_eq!(s.red_flat_confirmations, 1);
            assert!(!s.halted);
        }
        run_check(&mut st, 360_000, &flat, &fills, &quiet);
        let s = &st.coin[LONG][A];
        assert!(s.halted);
        assert_eq!(s.cooldown_until_ms, Some(230_000 + 5 * ONE_MIN_MS));
        assert_eq!(
            s.last_stop_event.as_ref().unwrap().stop_event_timestamp_ms,
            230_000
        );
        assert_eq!(s.pnl_reset_timestamp_ms, Some(230_001));
        assert_eq!(st.runtime_forced[LONG][A], "graceful_stop");
    }

    /// RED -> production coin supervisor: a non-flat iteration keeps
    /// `panic`, the recovered flat sample pauses panic, the check path sees
    /// the panic fill and finalizes at it after two confirmations, the
    /// cooldown forces `graceful_stop`, its expiry resets the pair
    /// (`_equity_hard_stop_run_coin_red_supervisor`, `_reset_coin_after_restart`).
    #[test]
    fn red_supervisor_then_finalization_and_reset() {
        let mut st = state();
        let held = [pos(A, 1.0)];
        let flat: [HslPosition; 0] = [];
        let quiet = Env {
            upnl: 0.0,
            blocking: (0, 0),
        };
        let red = Env {
            upnl: -30.0,
            blocking: (0, 0),
        };
        run_check(&mut st, 60_000, &held, &[], &quiet);
        run_check(&mut st, 120_000, &held, &[], &red);
        assert!(st.coin_red_active());
        let step = run_supervisor(&mut st, 120_000, &held, &[], &red);
        assert_eq!(step.active_before, vec![(LONG, A.to_string())]);
        assert_eq!(step.active_after, vec![(LONG, A.to_string())]);
        assert_eq!(st.runtime_forced[LONG][A], "panic");
        // Flat, but the panic fill is not in the ledger yet: deferred; the
        // recovered sample pauses panic emission.
        let step = run_supervisor(&mut st, 180_000, &flat, &[], &quiet);
        assert_eq!(st.coin[LONG][A].red_flat_confirmations, 0);
        assert!(step.active_after.is_empty());
        assert_eq!(
            st.runtime_forced[LONG][A],
            "tp_only_with_active_entry_cancellation"
        );
        // The next checks see the fill: two confirmations finalize at it.
        let fills = [fill(150_000, A, 1.0, false, "close_panic_long")];
        run_check(&mut st, 240_000, &flat, &fills, &quiet);
        assert_eq!(st.coin[LONG][A].red_flat_confirmations, 1);
        run_check(&mut st, 300_000, &flat, &fills, &quiet);
        {
            let s = &st.coin[LONG][A];
            assert!(s.halted && !s.no_restart_latched);
            assert_eq!(s.cooldown_until_ms, Some(150_000 + 5 * ONE_MIN_MS));
            assert_eq!(
                s.last_stop_event.as_ref().unwrap().stop_event_timestamp_ms,
                150_000
            );
        }
        assert_eq!(st.runtime_forced[LONG][A], "graceful_stop");
        // Cooling down: no sample, the forced mode stays.
        run_check(&mut st, 360_000, &flat, &fills, &quiet);
        assert!(st.coin[LONG][A].halted);
        // Cooldown elapsed: reset (keeps the pnl reset timestamp) and a
        // fresh green sample; the forced mode is gone.
        run_check(&mut st, 460_000, &flat, &fills, &quiet);
        let s = &st.coin[LONG][A];
        assert!(!s.halted && !s.runtime.red_latched());
        assert_eq!(s.pnl_reset_timestamp_ms, Some(150_001));
        assert_eq!(s.last_metrics.as_ref().unwrap().tier, Tier::Green);
        assert!(!st.runtime_forced[LONG].contains_key(A));
        assert!(st.modes().runtime_forced("long", A).is_none());
    }

    /// Two flat supervisor iterations with the panic fill in the ledger
    /// finalize inside the supervisor (the production loop's own path).
    #[test]
    fn red_supervisor_finalizes_with_the_fill_visible() {
        let mut st = state();
        let held = [pos(A, 1.0)];
        let flat: [HslPosition; 0] = [];
        let quiet = Env {
            upnl: 0.0,
            blocking: (0, 0),
        };
        let red = Env {
            upnl: -30.0,
            blocking: (0, 0),
        };
        run_check(&mut st, 60_000, &held, &[], &quiet);
        run_check(&mut st, 120_000, &held, &[], &red);
        let fills = [fill(150_000, A, 1.0, false, "close_panic_long")];
        // Still red on the first flat iteration (the sample is cached for
        // the minute at 120_000 -> use 180_000 with a red upnl).
        let step = run_supervisor(&mut st, 180_000, &flat, &fills, &red);
        assert_eq!(st.coin[LONG][A].red_flat_confirmations, 1);
        assert_eq!(step.active_after, vec![(LONG, A.to_string())]);
        let step = run_supervisor(&mut st, 180_000, &flat, &fills, &red);
        assert!(step.active_after.is_empty());
        let s = &st.coin[LONG][A];
        assert!(s.halted);
        assert_eq!(
            s.last_stop_event.as_ref().unwrap().stop_event_timestamp_ms,
            150_000
        );
        assert_eq!(st.runtime_forced[LONG][A], "graceful_stop");
    }

    /// `test_coin_hsl_restart_reset_preserves_persistent_no_restart_peak`.
    #[test]
    fn reset_after_restart_keeps_no_restart_peak_and_pnl_reset() {
        let mut st = state();
        let s = st.coin_state(LONG, A);
        s.no_restart_peak_strategy_equity = 1.25;
        s.pnl_reset_timestamp_ms = Some(123_456);
        s.halted = true;
        st.set_coin_runtime_forced_mode(LONG, A, "graceful_stop");
        st.reset_coin_after_restart(LONG, A);
        let s = &st.coin[LONG][A];
        assert_eq!(s.no_restart_peak_strategy_equity, 1.25);
        assert_eq!(s.pnl_reset_timestamp_ms, Some(123_456));
        assert!(!s.halted);
        assert!(!st.runtime_forced[LONG].contains_key(A));
    }

    /// `test_cooldown_anchor_uses_scope_flattening_fill`: the anchor is the
    /// pair's latest fill since the episode start, whatever closed it;
    /// other pairs never leak in.
    #[test]
    fn flatten_anchor_is_the_pairs_latest_fill_since() {
        let fills = [
            fill(120_500, "A", 1.0, false, "close_panic_long"),
            fill(150_000, "A", 1.0, false, "close_manual_long"),
        ];
        assert_eq!(
            latest_flatten_fill_timestamp(&fills, LONG, Some("A"), Some(60_000), None),
            Some(150_000)
        );
        assert_eq!(
            latest_flatten_fill_timestamp(&fills, LONG, Some("A"), Some(200_000), None),
            None
        );
        assert_eq!(
            latest_flatten_fill_timestamp(&fills, LONG, Some("B"), Some(60_000), None),
            None
        );
    }

    /// `_equity_hard_stop_coin_realized_pnl_peak_last`: running sum of
    /// `pnl + fee_paid` over the pair's events at or after
    /// `max(ts - lookback, reset_ts)`, peak floored at zero.
    #[test]
    fn realized_peak_last_window_and_reset() {
        let mut f1 = fill(100, "A", 1.0, false, "close_grid_long");
        f1.pnl = 5.0;
        f1.fee_paid = -0.5;
        let mut f2 = fill(200, "A", 1.0, false, "close_grid_long");
        f2.pnl = -8.0;
        let mut f3 = fill(300, "B", 1.0, false, "close_grid_long");
        f3.pnl = 100.0;
        let fills = [f1, f2, f3];
        assert_eq!(
            coin_realized_pnl_peak_last(&fills, LONG, "A", 1_000, None, None),
            (4.5, -3.5)
        );
        assert_eq!(
            coin_realized_pnl_peak_last(&fills, LONG, "A", 1_000, Some(850), None),
            (0.0, -8.0)
        );
        assert_eq!(
            coin_realized_pnl_peak_last(&fills, LONG, "A", 1_000, None, Some(101)),
            (0.0, -8.0)
        );
        assert_eq!(
            coin_realized_pnl_peak_last(&fills, LONG, "C", 1_000, None, None),
            (0.0, 0.0)
        );
    }

    /// `_equity_hard_stop_infer_coin_replay_contract`: the latest panic fill
    /// owns a cooldown; an ordinary entry inside it is an intervention, a
    /// held position without one is unresolved residue.
    #[test]
    fn replay_contract_from_panic_fills() {
        let fills = [
            fill(100_000, A, 1.0, true, "entry_grid_normal_long"),
            fill(200_000, A, 1.0, false, "close_panic_long"),
            fill(260_000, A, 1.0, true, "entry_grid_normal_long"),
        ];
        let c = infer_coin_replay_contract(
            &fills,
            LONG,
            A,
            true,
            CooldownPositionPolicy::Panic,
            5.0,
            400_000,
        );
        assert_eq!(c.latest_panic_ts, Some(200_000));
        assert_eq!(c.cooldown_until_ms, Some(500_000));
        assert_eq!(c.intervention_entry_ts, Some(260_000));
        assert!(c.active_cooldown_now && c.intervention_active && !c.unresolved_residue);
        let c = infer_coin_replay_contract(
            &fills[..2],
            LONG,
            A,
            true,
            CooldownPositionPolicy::Panic,
            5.0,
            400_000,
        );
        assert!(c.unresolved_residue && !c.intervention_active);
        let c = infer_coin_replay_contract(
            &fills[..2],
            LONG,
            A,
            false,
            CooldownPositionPolicy::Panic,
            5.0,
            600_000,
        );
        assert!(!c.active_cooldown_now && !c.unresolved_residue);
    }

    /// The pair's per-coin config: an override coin gets its own red
    /// threshold, the rest the global block; disabled sides are inactive.
    #[test]
    fn per_coin_config_and_activity() {
        let cfg = config(|v| {
            v["coin_overrides"]["BTC"] = serde_json::json!({
                "bot": {"long": {"hsl": {"red_threshold": 0.3}}}
            });
        });
        assert!(cfg.coin_mode());
        assert_eq!(
            cfg.side_config(LONG, Some("BTC/USDT:USDT")).red_threshold,
            0.3
        );
        assert_eq!(cfg.side_config(LONG, Some(A)).red_threshold, 0.5);
        assert!(cfg.coin_active_pside(LONG, Some(A)).unwrap());
        assert!(!cfg.coin_active_pside(SHORT, Some(A)).unwrap());
        assert_eq!(cfg.n_positions[LONG], 2.0);
    }
}
