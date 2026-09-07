//! Order churn gate (docs/RECONCILE_SPEC.md section 2.9): a port of
//! `live/order_churn_gate.py` (`OrderChurnGateState`), of the evidence step
//! `prepare_order_churn_evidence` (`live/reconciler.py`) and of the admission
//! step `_apply_order_churn_admission` (`live/executor.py`).
//!
//! Two halves:
//!
//! - **Evidence** (`ChurnGate::evaluate`, once per cycle, right after the
//!   engine's executable orders are known and before reconciliation): keeps a
//!   per-symbol history of ideal-order snapshots bounded by the window and
//!   marks every ideal order `churn_evidenced` when its recent history shows
//!   sustained drift or repeated exclusive cohort switching.
//! - **Admission** (`ChurnGate::admit`, applied to the create list after the
//!   cancel-first barrier and the recent-execution guard, before the creation
//!   batch capacity): once more than `activation_count` creates were sent in
//!   the last window, far (`order_market_diff > market_dist_pct`), non
//!   risk-critical, limit, churn-evidenced creates are deferred. Every
//!   submitted create records an attempt (`record_attempts`), exempt ones too.
//!
//! All timestamps are seconds on one monotonic clock (Python uses
//! `time.monotonic()`); only differences matter.

use crate::bot_params::ConfigView;
use crate::reconcile::OrderRec;
use pb_exchange_bybit::{PositionSide, Side};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::OnceLock;
use std::time::Instant;

/// Seconds since the first call in this process (a `time.monotonic()` stand-in).
pub fn monotonic_seconds() -> f64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_secs_f64()
}

/// `EXECUTION_SCHEDULED_WAIT_SECONDS` (`passivbot.py`).
const EXECUTION_SCHEDULED_WAIT_SECONDS: f64 = 30.0;

#[derive(Debug, Clone, PartialEq)]
pub struct ChurnParams {
    /// `live.order_replacement_churn_gate_activation_count` (<= 0 disables).
    pub activation_count: i64,
    /// `live.order_replacement_churn_gate_window_minutes * 60`.
    pub window_seconds: f64,
    /// `live.order_replacement_churn_gate_stability_minutes * 60`.
    pub stability_seconds: f64,
    /// `live.order_replacement_churn_gate_market_dist_pct` (fraction).
    pub market_dist_pct: f64,
    /// `live.order_match_tolerance_pct` (fraction).
    pub tolerance: f64,
    /// `max(10, 3 * (execution_delay_seconds + 30))` (`reconciler.py:48-52`).
    pub max_sample_gap_seconds: f64,
}

impl ChurnParams {
    pub fn from_config(cfg: &ConfigView) -> Self {
        let f = |key: &str, default: f64| cfg.live(key).and_then(Value::as_f64).unwrap_or(default);
        Self {
            activation_count: cfg
                .live("order_replacement_churn_gate_activation_count")
                .and_then(Value::as_i64)
                .unwrap_or(10),
            window_seconds: f("order_replacement_churn_gate_window_minutes", 10.0) * 60.0,
            stability_seconds: f("order_replacement_churn_gate_stability_minutes", 2.0) * 60.0,
            market_dist_pct: f("order_replacement_churn_gate_market_dist_pct", 0.005),
            tolerance: f("order_match_tolerance_pct", 0.0002),
            max_sample_gap_seconds: Self::max_sample_gap(f("execution_delay_seconds", 2.0)),
        }
    }

    /// `_order_churn_max_generation_gap_seconds`.
    pub fn max_sample_gap(execution_delay_seconds: f64) -> f64 {
        (3.0 * (execution_delay_seconds + EXECUTION_SCHEDULED_WAIT_SECONDS)).max(10.0)
    }
}

/// `OrderCohort`: orders that are the same "kind" across snapshots.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Cohort {
    symbol: String,
    pside: PositionSide,
    side: Side,
    reduce_only: bool,
    limit: bool,
    pb_order_type: String,
}

impl Cohort {
    fn of(o: &OrderRec) -> Self {
        Self {
            symbol: o.symbol.clone(),
            pside: o.pside,
            side: o.side,
            reduce_only: o.reduce_only,
            limit: o.limit,
            pb_order_type: o.pb_order_type.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Obs {
    price: f64,
    qty: f64,
}

fn obs_order(a: &Obs, b: &Obs) -> std::cmp::Ordering {
    (a.price, a.qty)
        .partial_cmp(&(b.price, b.qty))
        .unwrap_or(std::cmp::Ordering::Equal)
}

/// `IdealSnapshot`, stored already grouped by cohort (each group sorted by
/// `(price, qty)` as `_group_by_cohort` does).
#[derive(Debug, Clone)]
struct Snapshot {
    seconds: f64,
    groups: BTreeMap<Cohort, Vec<Obs>>,
}

/// `ChurnDecision`.
#[derive(Debug, Clone, PartialEq)]
pub struct ChurnDecision {
    pub churn_evidenced: bool,
    pub reason: &'static str,
    pub tight_prefix_count: usize,
    pub tight_prefix_seconds: f64,
}

impl ChurnDecision {
    fn new(churn_evidenced: bool, reason: &'static str) -> Self {
        Self {
            churn_evidenced,
            reason,
            tight_prefix_count: 0,
            tight_prefix_seconds: 0.0,
        }
    }

    fn with_prefix(mut self, count: usize, seconds: f64) -> Self {
        self.tight_prefix_count = count;
        self.tight_prefix_seconds = seconds;
        self
    }
}

/// `_relative_diff`.
fn relative_diff(current: f64, previous: f64) -> f64 {
    if !current.is_finite() || current <= 0.0 {
        return f64::INFINITY;
    }
    (previous - current).abs() / current
}

/// `_continuous_drift_start_index`: index of the first changed observation of
/// the current monotonic run (>= 2 moves, total move beyond `tolerance`).
fn continuous_drift_start_index(values: &[f64], tolerance: f64) -> Option<usize> {
    if values.len() < 3 {
        return None;
    }
    let changed: Vec<(usize, f64)> = values
        .windows(2)
        .enumerate()
        .filter(|(_, w)| w[1] != w[0])
        .map(|(i, w)| (i + 1, w[1] - w[0]))
        .collect();
    if changed.len() < 2 {
        return None;
    }
    let (last_index, last_delta) = changed[changed.len() - 1];
    let upward = last_delta > 0.0;
    let mut run_start = last_index;
    let mut run_moves = 0usize;
    for &(index, delta) in changed.iter().rev() {
        if (delta > 0.0) != upward {
            break;
        }
        run_start = index;
        run_moves += 1;
    }
    if run_moves < 2 {
        return None;
    }
    if relative_diff(values[values.len() - 1], values[run_start - 1]) <= tolerance {
        return None;
    }
    Some(run_start)
}

/// `_exclusive_cohort_reappearance_seconds`: the span between the last two
/// runs of `cohort` when every snapshot of the contiguous history holds
/// exactly one cohort with unchanged cardinality and the cohort appeared in
/// >= 3 distinct runs.
fn exclusive_cohort_reappearance_seconds<'a>(
    cohort: &'a Cohort,
    current_len: usize,
    current_group_count: usize,
    historical: &[(f64, &'a BTreeMap<Cohort, Vec<Obs>>)],
    now: f64,
    max_sample_gap_seconds: f64,
) -> Option<f64> {
    if current_group_count != 1 {
        return None;
    }
    let mut cardinality: BTreeMap<&'a Cohort, usize> = BTreeMap::new();
    cardinality.insert(cohort, current_len);
    let mut samples_newest_first: Vec<(f64, &'a Cohort)> = vec![(now, cohort)];
    let mut previous_time = now;
    for (snapshot_time, groups) in historical {
        let gap = previous_time - snapshot_time;
        if gap < 0.0 || gap > max_sample_gap_seconds || groups.len() != 1 {
            break;
        }
        let (historical_cohort, historical_group) = groups.iter().next().expect("one cohort");
        let expected = *cardinality
            .entry(historical_cohort)
            .or_insert(historical_group.len());
        if historical_group.len() != expected {
            break;
        }
        samples_newest_first.push((*snapshot_time, historical_cohort));
        previous_time = *snapshot_time;
    }
    let mut runs: Vec<(&Cohort, f64)> = Vec::new();
    for (time, sample_cohort) in samples_newest_first.iter().rev() {
        if runs.last().is_none_or(|r| r.0 != *sample_cohort) {
            runs.push((sample_cohort, *time));
        }
    }
    let appearances: Vec<f64> = runs
        .iter()
        .filter(|(c, _)| *c == cohort)
        .map(|(_, t)| *t)
        .collect();
    if appearances.len() < 3 {
        return None;
    }
    Some((appearances[appearances.len() - 1] - appearances[appearances.len() - 2]).max(0.0))
}

/// The per-order decision of `evaluate_and_record` for one track (current
/// observation first, then the same-rank observation of each preceding
/// contiguous snapshot of the same cohort and cardinality).
fn decide(
    track: &[Obs],
    track_times: &[f64],
    now: f64,
    reappearance_seconds: Option<f64>,
    params: &ChurnParams,
) -> ChurnDecision {
    let current = track[0];
    let mut tight_count = 0usize;
    let mut oldest_tight_time = now;
    for (index, historical) in track.iter().enumerate().skip(1) {
        if relative_diff(current.price, historical.price) > params.tolerance
            || relative_diff(current.qty, historical.qty) > params.tolerance
        {
            break;
        }
        tight_count += 1;
        oldest_tight_time = track_times[index];
    }
    let tight_seconds = (now - oldest_tight_time).max(0.0);
    let intermittent = reappearance_seconds.is_some();
    if tight_count >= 2 && tight_seconds >= params.stability_seconds {
        return ChurnDecision::new(false, "stable_tight_prefix")
            .with_prefix(tight_count, tight_seconds);
    }
    if reappearance_seconds.is_some_and(|s| s >= params.stability_seconds) {
        return ChurnDecision::new(true, "intermittent_cohort_reappearance")
            .with_prefix(tight_count, tight_seconds);
    }
    if track.len() == 1 {
        return ChurnDecision::new(
            false,
            if intermittent {
                "intermittent_run_short"
            } else {
                "no_history"
            },
        );
    }
    if now - track_times[track_times.len() - 1] < params.stability_seconds {
        return ChurnDecision::new(
            false,
            if intermittent {
                "intermittent_run_short"
            } else {
                "history_short"
            },
        )
        .with_prefix(tight_count, tight_seconds);
    }
    let prices: Vec<f64> = track.iter().rev().map(|o| o.price).collect();
    let qtys: Vec<f64> = track.iter().rev().map(|o| o.qty).collect();
    let times: Vec<f64> = track_times.iter().rev().copied().collect();
    let price_start = continuous_drift_start_index(&prices, params.tolerance);
    let qty_start = continuous_drift_start_index(&qtys, params.tolerance);
    let lasted =
        |start: Option<usize>| start.is_some_and(|i| now - times[i] >= params.stability_seconds);
    let price_drift = lasted(price_start);
    let qty_drift = lasted(qty_start);
    let reason = if price_drift && qty_drift {
        "continuous_price_qty_drift"
    } else if price_drift {
        "continuous_price_drift"
    } else if qty_drift {
        "continuous_qty_drift"
    } else if price_start.is_some() || qty_start.is_some() {
        "drift_run_short"
    } else if intermittent {
        "intermittent_run_short"
    } else {
        "no_continuous_drift"
    };
    ChurnDecision::new(price_drift || qty_drift, reason).with_prefix(tight_count, tight_seconds)
}

fn prune_snapshots(snapshots: &mut VecDeque<Snapshot>, now: f64, window_seconds: f64) {
    let cutoff = now - window_seconds;
    while snapshots.front().is_some_and(|s| s.seconds < cutoff) {
        snapshots.pop_front();
    }
}

/// `OrderChurnGateState` plus the evidence / admission / accounting steps.
#[derive(Debug, Clone)]
pub struct ChurnGate {
    params: ChurnParams,
    history: BTreeMap<String, VecDeque<Snapshot>>,
    attempts: VecDeque<f64>,
    /// Number of history resets (departed symbols, completed stable runs).
    pub reset_count: u64,
}

impl ChurnGate {
    pub fn new(params: ChurnParams) -> Self {
        Self {
            params,
            history: BTreeMap::new(),
            attempts: VecDeque::new(),
            reset_count: 0,
        }
    }

    pub fn params(&self) -> &ChurnParams {
        &self.params
    }

    pub fn enabled(&self) -> bool {
        self.params.activation_count > 0
    }

    pub fn symbols_with_history(&self) -> Vec<&str> {
        self.history.keys().map(String::as_str).collect()
    }

    /// `prepare_order_churn_evidence` + `evaluate_and_record`: record this
    /// cycle's ideal orders and set `churn_evidenced` on each of them.
    /// `universe` is every symbol the bot currently tracks (active symbols,
    /// symbols with open orders or a position); symbols with history that
    /// are not in it (nor in `ideal`) have their history cleared, and
    /// universe symbols without ideal orders record an empty snapshot.
    /// `risk_pairs` are the `(symbol, pside)` pairs with a risk-critical
    /// engine order or a `loss_gate_blocks` entry this cycle; their orders
    /// are never churn-evidenced. Returns one decision per ideal order.
    pub fn evaluate(
        &mut self,
        universe: &[String],
        ideal: &mut [OrderRec],
        risk_pairs: &HashSet<(String, PositionSide)>,
        now: f64,
    ) -> Vec<ChurnDecision> {
        if !self.enabled() {
            if !self.history.is_empty() {
                self.history.clear();
                self.reset_count += 1;
            }
            for o in ideal.iter_mut() {
                o.churn_evidenced = false;
            }
            return vec![ChurnDecision::new(false, "disabled"); ideal.len()];
        }
        let mut current: BTreeMap<String, Vec<(usize, Cohort, Obs)>> = BTreeMap::new();
        for s in universe {
            current.entry(s.clone()).or_default();
        }
        for (i, o) in ideal.iter().enumerate() {
            current.entry(o.symbol.clone()).or_default().push((
                i,
                Cohort::of(o),
                Obs {
                    price: o.price,
                    qty: o.qty.abs(),
                },
            ));
        }
        let departed: Vec<String> = self
            .history
            .keys()
            .filter(|s| !current.contains_key(*s))
            .cloned()
            .collect();
        for s in departed {
            self.history.remove(&s);
            self.reset_count += 1;
        }
        let params = self.params.clone();
        let mut decisions = vec![ChurnDecision::new(false, "unavailable"); ideal.len()];
        for (symbol, rows) in &current {
            let snapshots = self.history.entry(symbol.clone()).or_default();
            prune_snapshots(snapshots, now, params.window_seconds);
            let mut groups: BTreeMap<Cohort, Vec<(usize, Obs)>> = BTreeMap::new();
            for (i, cohort, obs) in rows {
                groups.entry(cohort.clone()).or_default().push((*i, *obs));
            }
            for g in groups.values_mut() {
                g.sort_by(|a, b| obs_order(&a.1, &b.1));
            }
            let historical: Vec<(f64, &BTreeMap<Cohort, Vec<Obs>>)> = snapshots
                .iter()
                .rev()
                .map(|s| (s.seconds, &s.groups))
                .collect();
            let mut symbol_decisions: Vec<(usize, ChurnDecision)> = Vec::new();
            for (cohort, group) in &groups {
                let reappearance = exclusive_cohort_reappearance_seconds(
                    cohort,
                    group.len(),
                    groups.len(),
                    &historical,
                    now,
                    params.max_sample_gap_seconds,
                );
                let mut tracks: Vec<Vec<Obs>> = group.iter().map(|(_, o)| vec![*o]).collect();
                let mut track_times = vec![now];
                let mut previous_time = now;
                for (snapshot_time, hist_groups) in &historical {
                    let gap = previous_time - snapshot_time;
                    let Some(previous_group) = hist_groups.get(cohort) else {
                        break;
                    };
                    if gap < 0.0
                        || gap > params.max_sample_gap_seconds
                        || previous_group.len() != group.len()
                    {
                        break;
                    }
                    for (track, o) in tracks.iter_mut().zip(previous_group) {
                        track.push(*o);
                    }
                    track_times.push(*snapshot_time);
                    previous_time = *snapshot_time;
                }
                for ((source_index, _), track) in group.iter().zip(&tracks) {
                    symbol_decisions.push((
                        *source_index,
                        decide(track, &track_times, now, reappearance, &params),
                    ));
                }
            }
            drop(historical);
            // A completed stable exclusive run ends the preceding switching
            // episode: keep only the proven stable prefix as history.
            if groups.len() == 1
                && !symbol_decisions.is_empty()
                && symbol_decisions
                    .iter()
                    .all(|(_, d)| d.reason == "stable_tight_prefix")
            {
                let stable_prefix_seconds = symbol_decisions
                    .iter()
                    .map(|(_, d)| d.tight_prefix_seconds)
                    .fold(f64::INFINITY, f64::min);
                let stable_prefix_start = now - stable_prefix_seconds;
                let mut discarded = false;
                while snapshots
                    .front()
                    .is_some_and(|s| s.seconds < stable_prefix_start)
                {
                    snapshots.pop_front();
                    discarded = true;
                }
                if discarded {
                    self.reset_count += 1;
                }
            }
            for (i, d) in symbol_decisions {
                decisions[i] = d;
            }
            snapshots.push_back(Snapshot {
                seconds: now,
                groups: groups
                    .into_iter()
                    .map(|(c, g)| (c, g.into_iter().map(|(_, o)| o).collect()))
                    .collect(),
            });
        }
        self.history.retain(|symbol, snapshots| {
            prune_snapshots(snapshots, now, params.window_seconds);
            !snapshots.is_empty() || current.contains_key(symbol)
        });
        for (i, o) in ideal.iter_mut().enumerate() {
            if risk_pairs.contains(&(o.symbol.clone(), o.pside)) {
                decisions[i] = ChurnDecision::new(false, "rust_risk_phase_active");
            }
            o.churn_evidenced = decisions[i].churn_evidenced;
        }
        decisions
    }

    fn prune_attempts(&mut self, now: f64) {
        let cutoff = now - self.params.window_seconds;
        while self.attempts.front().is_some_and(|t| *t < cutoff) {
            self.attempts.pop_front();
        }
    }

    /// Create attempts recorded in the last window.
    pub fn attempt_count(&mut self, now: f64) -> usize {
        self.prune_attempts(now);
        self.attempts.len()
    }

    /// `_record_order_churn_allowance_attempts`: call with the number of
    /// creates actually submitted this cycle (exempt ones included).
    pub fn record_attempts(&mut self, count: usize, now: f64) {
        if count == 0 || !self.enabled() {
            return;
        }
        self.attempts.extend(std::iter::repeat_n(now, count));
    }

    /// `_apply_order_churn_admission` over the create list in list order.
    /// `market_dist` returns the signed `order_market_diff` of a create, or
    /// `None` when no market price is available (the create is then deferred,
    /// `market_distance_unavailable`). Returns the admitted creates and the
    /// number deferred.
    pub fn admit(
        &mut self,
        creates: Vec<OrderRec>,
        market_dist: &dyn Fn(&OrderRec) -> Option<f64>,
        now: f64,
    ) -> (Vec<OrderRec>, usize) {
        if creates.is_empty() || !self.enabled() {
            return (creates, 0);
        }
        let mut projected_usage = self.attempt_count(now) as i64;
        let mut selected = Vec::with_capacity(creates.len());
        let mut deferred = 0usize;
        for o in creates {
            let always_allowed = !o.limit || o.risk_critical || !o.churn_evidenced;
            let exempt = if always_allowed {
                true
            } else {
                match market_dist(&o) {
                    Some(d) if d.is_finite() => d <= self.params.market_dist_pct,
                    _ => {
                        deferred += 1;
                        continue;
                    }
                }
            };
            if o.churn_evidenced && !exempt && projected_usage + 1 > self.params.activation_count {
                deferred += 1;
                continue;
            }
            selected.push(o);
            projected_usage += 1;
        }
        (selected, deferred)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SYMBOL: &str = "BTC/USDT:USDT";

    fn params() -> ChurnParams {
        ChurnParams {
            activation_count: 10,
            window_seconds: 600.0,
            stability_seconds: 120.0,
            market_dist_pct: 0.005,
            tolerance: 0.0002,
            max_sample_gap_seconds: ChurnParams::max_sample_gap(2.0),
        }
    }

    fn order(price: f64, qty: f64) -> OrderRec {
        OrderRec {
            symbol: SYMBOL.into(),
            side: Side::Buy,
            pside: PositionSide::Long,
            qty,
            price,
            reduce_only: false,
            limit: true,
            pb_order_type: "entry_grid_normal_long".into(),
            risk_critical: false,
            churn_evidenced: false,
            id: None,
            custom_id: None,
        }
    }

    fn short_order(price: f64) -> OrderRec {
        OrderRec {
            side: Side::Sell,
            pside: PositionSide::Short,
            pb_order_type: "entry_grid_normal_short".into(),
            ..order(price, 1.0)
        }
    }

    fn eval(gate: &mut ChurnGate, orders: &mut [OrderRec], now: f64) -> Vec<ChurnDecision> {
        gate.evaluate(&[SYMBOL.to_string()], orders, &HashSet::new(), now)
    }

    fn eval_one(gate: &mut ChurnGate, o: OrderRec, now: f64) -> ChurnDecision {
        let mut v = vec![o];
        eval(gate, &mut v, now).remove(0)
    }

    #[test]
    fn max_sample_gap() {
        assert_eq!(ChurnParams::max_sample_gap(2.0), 96.0);
        assert_eq!(ChurnParams::max_sample_gap(-29.0), 10.0);
    }

    #[test]
    fn no_history_and_single_move_fail_open() {
        let mut g = ChurnGate::new(params());
        assert_eq!(
            eval_one(&mut g, order(100.0, 1.0), 0.0).reason,
            "no_history"
        );
        let d = eval_one(&mut g, order(100.1, 1.0), 60.0);
        assert!(!d.churn_evidenced);
        assert_eq!(d.reason, "history_short");
    }

    #[test]
    fn sustained_monotonic_price_drift_is_evidence() {
        let mut g = ChurnGate::new(params());
        eval_one(&mut g, order(100.0, 1.0), 0.0);
        eval_one(&mut g, order(100.1, 1.0), 60.0);
        eval_one(&mut g, order(100.2, 1.0), 120.0);
        let d = eval_one(&mut g, order(100.3, 1.0), 180.0);
        assert!(d.churn_evidenced);
        assert_eq!(d.reason, "continuous_price_drift");
    }

    #[test]
    fn sustained_monotonic_qty_drift_is_evidence() {
        let mut g = ChurnGate::new(params());
        eval_one(&mut g, order(100.0, 1.0), 0.0);
        eval_one(&mut g, order(100.0, 1.001), 60.0);
        eval_one(&mut g, order(100.0, 1.002), 120.0);
        let d = eval_one(&mut g, order(100.0, 1.003), 180.0);
        assert!(d.churn_evidenced);
        assert_eq!(d.reason, "continuous_qty_drift");
    }

    #[test]
    fn old_stable_history_does_not_count_toward_drift_duration() {
        let mut g = ChurnGate::new(params());
        eval_one(&mut g, order(100.0, 1.0), 0.0);
        eval_one(&mut g, order(100.0, 1.0), 60.0);
        eval_one(&mut g, order(100.0, 1.0), 120.0);
        eval_one(&mut g, order(100.1, 1.0), 180.0);
        let d = eval_one(&mut g, order(100.2, 1.0), 250.0);
        assert!(!d.churn_evidenced);
        assert_eq!(d.reason, "drift_run_short");
    }

    #[test]
    fn oscillation_and_one_time_jump_are_not_continuous_drift() {
        let mut g = ChurnGate::new(params());
        eval_one(&mut g, order(100.0, 1.0), 0.0);
        eval_one(&mut g, order(100.1, 1.0), 60.0);
        let d = eval_one(&mut g, order(100.0, 1.0), 120.0);
        assert!(!d.churn_evidenced);
        assert_eq!(d.reason, "no_continuous_drift");

        let mut g = ChurnGate::new(params());
        eval_one(&mut g, order(100.0, 1.0), 0.0);
        eval_one(&mut g, order(100.0, 1.0), 60.0);
        let d = eval_one(&mut g, order(101.0, 1.0), 120.0);
        assert!(!d.churn_evidenced);
        assert_eq!(d.reason, "no_continuous_drift");
    }

    #[test]
    fn repeated_exclusive_long_short_switching_is_evidence() {
        let mut g = ChurnGate::new(params());
        eval_one(&mut g, order(99.0, 1.0), 0.0);
        eval_one(&mut g, short_order(101.0), 60.0);
        let d = eval_one(&mut g, order(99.0, 1.0), 120.0);
        assert!(!d.churn_evidenced);
        eval_one(&mut g, short_order(101.0), 180.0);
        let d = eval_one(&mut g, order(99.0, 1.0), 240.0);
        assert!(d.churn_evidenced);
        assert_eq!(d.reason, "intermittent_cohort_reappearance");
    }

    #[test]
    fn exclusive_switching_needs_stability_duration() {
        let mut g = ChurnGate::new(params());
        eval_one(&mut g, order(99.0, 1.0), 0.0);
        eval_one(&mut g, short_order(101.0), 10.0);
        eval_one(&mut g, order(99.0, 1.0), 20.0);
        eval_one(&mut g, short_order(101.0), 30.0);
        let d = eval_one(&mut g, order(99.0, 1.0), 40.0);
        assert!(!d.churn_evidenced);
        assert_eq!(d.reason, "intermittent_run_short");
        eval_one(&mut g, order(99.0, 1.0), 80.0);
        eval_one(&mut g, order(99.0, 1.0), 120.0);
        let d = eval_one(&mut g, order(99.0, 1.0), 140.0);
        assert!(!d.churn_evidenced);
        assert_eq!(d.reason, "intermittent_run_short");
    }

    #[test]
    fn sustained_drift_overrides_short_switching_interval() {
        let mut g = ChurnGate::new(params());
        for (now, o) in [
            (0.0, order(99.0, 1.0)),
            (10.0, short_order(101.0)),
            (20.0, order(99.0, 1.0)),
            (30.0, short_order(101.0)),
            (40.0, order(99.0, 1.0)),
            (80.0, order(99.1, 1.0)),
            (120.0, order(99.2, 1.0)),
            (160.0, order(99.3, 1.0)),
        ] {
            eval_one(&mut g, o, now);
        }
        let d = eval_one(&mut g, order(99.4, 1.0), 200.0);
        assert!(d.churn_evidenced);
        assert_eq!(d.reason, "continuous_price_drift");
    }

    #[test]
    fn continuous_stability_clears_exclusive_switching_evidence() {
        let mut g = ChurnGate::new(params());
        for (now, o) in [
            (0.0, order(99.0, 1.0)),
            (60.0, short_order(101.0)),
            (120.0, order(99.0, 1.0)),
            (180.0, short_order(101.0)),
            (240.0, order(99.0, 1.0)),
            (300.0, order(99.0, 1.0)),
        ] {
            eval_one(&mut g, o, now);
        }
        let d = eval_one(&mut g, order(99.0, 1.0), 360.0);
        assert!(!d.churn_evidenced);
        assert_eq!(d.reason, "stable_tight_prefix");
        assert_eq!(g.reset_count, 1);
        assert!(!eval_one(&mut g, short_order(101.0), 420.0).churn_evidenced);
        assert!(!eval_one(&mut g, order(99.0, 1.0), 480.0).churn_evidenced);
    }

    #[test]
    fn uncertain_exclusive_switching_fails_open() {
        // An empty snapshot breaks provenance.
        let mut g = ChurnGate::new(params());
        eval_one(&mut g, order(99.0, 1.0), 0.0);
        eval_one(&mut g, short_order(101.0), 60.0);
        eval(&mut g, &mut [], 120.0);
        eval_one(&mut g, order(99.0, 1.0), 180.0);
        eval_one(&mut g, short_order(101.0), 240.0);
        assert!(!eval_one(&mut g, order(99.0, 1.0), 300.0).churn_evidenced);
        // Coexisting cohorts are not exclusive switching.
        let mut g = ChurnGate::new(params());
        eval_one(&mut g, order(99.0, 1.0), 0.0);
        eval_one(&mut g, short_order(101.0), 60.0);
        eval(&mut g, &mut [order(99.0, 1.0), short_order(101.0)], 120.0);
        eval_one(&mut g, short_order(101.0), 180.0);
        assert!(!eval_one(&mut g, order(99.0, 1.0), 240.0).churn_evidenced);
        // A changed ladder cardinality breaks cohort continuity.
        let mut g = ChurnGate::new(params());
        eval(&mut g, &mut [order(98.0, 1.0), order(99.0, 1.0)], 0.0);
        eval_one(&mut g, short_order(101.0), 60.0);
        eval(&mut g, &mut [order(98.0, 1.0), order(99.0, 1.0)], 120.0);
        eval_one(&mut g, short_order(101.0), 180.0);
        assert!(!eval_one(&mut g, order(99.0, 1.0), 240.0).churn_evidenced);
        // The alternate cohort changing cardinality breaks the proof too.
        let mut g = ChurnGate::new(params());
        eval_one(&mut g, order(99.0, 1.0), 0.0);
        eval_one(&mut g, short_order(101.0), 60.0);
        eval_one(&mut g, order(99.0, 1.0), 120.0);
        eval(&mut g, &mut [short_order(101.0), short_order(102.0)], 180.0);
        assert!(!eval_one(&mut g, order(99.0, 1.0), 240.0).churn_evidenced);
    }

    #[test]
    fn recent_stability_clears_older_drift() {
        let mut g = ChurnGate::new(params());
        eval_one(&mut g, order(100.0, 1.0), 0.0);
        eval_one(&mut g, order(100.1, 1.0), 60.0);
        eval_one(&mut g, order(100.2, 1.0), 120.0);
        eval_one(&mut g, order(100.2, 1.0), 180.0);
        let d = eval_one(&mut g, order(100.2, 1.0), 240.0);
        assert!(!d.churn_evidenced);
        assert_eq!(d.reason, "stable_tight_prefix");
        assert!(d.tight_prefix_seconds >= 120.0);
    }

    #[test]
    fn time_gap_and_cohort_or_cardinality_change_fail_open() {
        let mut g = ChurnGate::new(params());
        eval_one(&mut g, order(100.0, 1.0), 0.0);
        eval_one(&mut g, order(100.1, 1.0), 60.0);
        let d = eval_one(&mut g, order(100.2, 1.0), 140.0); // 80 s gap (limit 96 s) is fine ...
        assert_eq!(d.reason, "drift_run_short");
        let d = eval_one(&mut g, order(100.3, 1.0), 240.0); // ... a 100 s gap is not
        assert_eq!(d.reason, "no_history");
        let mut changed = order(100.4, 1.0);
        changed.pb_order_type = "entry_grid_inflated_long".into();
        assert_eq!(eval_one(&mut g, changed, 300.0).reason, "no_history");

        let mut g = ChurnGate::new(params());
        eval_one(&mut g, order(100.0, 1.0), 0.0);
        let ds = eval(&mut g, &mut [order(100.1, 1.0), order(101.0, 1.0)], 60.0);
        assert!(ds.iter().all(|d| !d.churn_evidenced));
    }

    #[test]
    fn ladder_reordering_preserves_rank_based_decisions() {
        let mut a = ChurnGate::new(params());
        let mut b = ChurnGate::new(params());
        for (now, prices) in [(0.0, [100.0, 101.0]), (60.0, [100.1, 101.1])] {
            eval(
                &mut a,
                &mut [order(prices[0], 1.0), order(prices[1], 1.0)],
                now,
            );
            eval(
                &mut b,
                &mut [order(prices[1], 1.0), order(prices[0], 1.0)],
                now,
            );
        }
        let da = eval(&mut a, &mut [order(100.2, 1.0), order(101.2, 1.0)], 120.0);
        let db = eval(&mut b, &mut [order(101.2, 1.0), order(100.2, 1.0)], 120.0);
        assert_eq!(da[0], db[1]);
        assert_eq!(da[1], db[0]);
    }

    #[test]
    fn snapshot_and_attempt_windows_prune() {
        let mut g = ChurnGate::new(ChurnParams {
            window_seconds: 10.0,
            ..params()
        });
        eval_one(&mut g, order(100.0, 1.0), 0.0);
        g.record_attempts(2, 0.0);
        g.record_attempts(1, 5.0);
        assert_eq!(g.attempt_count(9.0), 3);
        assert_eq!(g.attempt_count(11.0), 1);
        eval_one(&mut g, order(100.0, 1.0), 601.0);
        assert_eq!(g.history[SYMBOL].len(), 1);
        assert_eq!(g.history[SYMBOL][0].seconds, 601.0);
    }

    #[test]
    fn departed_symbol_clears_history_and_positions_keep_it() {
        let mut g = ChurnGate::new(params());
        eval_one(&mut g, order(100.0, 1.0), 0.0);
        assert_eq!(g.symbols_with_history(), vec![SYMBOL]);
        // Still in the universe (position / open order): an empty snapshot is recorded.
        g.evaluate(&[SYMBOL.to_string()], &mut [], &HashSet::new(), 1.0);
        assert_eq!(g.symbols_with_history(), vec![SYMBOL]);
        assert_eq!(g.history[SYMBOL].len(), 2);
        // Left the universe: history cleared.
        g.evaluate(&[], &mut [], &HashSet::new(), 2.0);
        assert!(g.symbols_with_history().is_empty());
        assert_eq!(g.reset_count, 1);
    }

    #[test]
    fn active_risk_pair_bypasses_observed_churn() {
        let mut g = ChurnGate::new(params());
        let risk: HashSet<(String, PositionSide)> = [(SYMBOL.to_string(), PositionSide::Long)]
            .into_iter()
            .collect();
        let mut d = ChurnDecision::new(false, "unavailable");
        let mut last = order(100.0, 1.0);
        for (now, price) in [(0.0, 100.0), (60.0, 100.1), (120.0, 100.2), (180.0, 100.3)] {
            let mut v = vec![order(price, 1.0)];
            d = g
                .evaluate(&[SYMBOL.to_string()], &mut v, &risk, now)
                .remove(0);
            last = v.remove(0);
        }
        assert!(!d.churn_evidenced);
        assert_eq!(d.reason, "rust_risk_phase_active");
        assert!(!last.churn_evidenced);
        // The same history without the risk override is drift evidence.
        let mut g2 = ChurnGate::new(params());
        for (now, price) in [(0.0, 100.0), (60.0, 100.1), (120.0, 100.2)] {
            eval_one(&mut g2, order(price, 1.0), now);
        }
        assert!(eval_one(&mut g2, order(100.3, 1.0), 180.0).churn_evidenced);
    }

    #[test]
    fn disabled_gate_records_nothing() {
        let mut g = ChurnGate::new(ChurnParams {
            activation_count: 0,
            ..params()
        });
        let d = eval_one(&mut g, order(100.0, 1.0), 0.0);
        assert_eq!(d.reason, "disabled");
        assert!(g.symbols_with_history().is_empty());
        g.record_attempts(3, 0.0);
        assert_eq!(g.attempt_count(0.0), 0);
        let mut far = order(90.0, 1.0);
        far.churn_evidenced = true;
        let (kept, deferred) = g.admit(vec![far], &|_| Some(0.1), 0.0);
        assert_eq!((kept.len(), deferred), (1, 0));
    }

    fn create(price: f64, evidenced: bool) -> OrderRec {
        let mut o = order(price, 1.0);
        o.churn_evidenced = evidenced;
        o
    }

    fn dist(o: &OrderRec) -> Option<f64> {
        Some(1.0 - o.price / 100.0)
    }

    #[test]
    fn admission_arithmetic() {
        let mut g = ChurnGate::new(ChurnParams {
            activation_count: 3,
            ..params()
        });
        // Two attempts already in the window; the third create (exempt: near
        // market) consumes the last slot, so far evidenced creates are deferred.
        g.record_attempts(2, 0.0);
        let creates = vec![
            create(99.9, true),  // 0.1 % from market: exempt, admitted, usage 3
            create(98.0, true),  // far + evidenced: 3 + 1 > 3 -> deferred
            create(97.0, false), // no evidence: admitted, usage 4
            create(96.0, true),  // deferred
        ];
        let (kept, deferred) = g.admit(creates, &dist, 10.0);
        assert_eq!(deferred, 2);
        assert_eq!(
            kept.iter().map(|o| o.price).collect::<Vec<_>>(),
            vec![99.9, 97.0]
        );
        // Nothing recorded yet by admission itself.
        assert_eq!(g.attempt_count(10.0), 2);
        g.record_attempts(kept.len(), 10.0);
        assert_eq!(g.attempt_count(10.0), 4);
        // Once the first two attempts leave the window one far create fits again.
        let (kept, deferred) = g.admit(vec![create(98.0, true), create(96.0, true)], &dist, 601.0);
        assert_eq!((kept.len(), deferred), (1, 1));
        assert_eq!(kept[0].price, 98.0);
    }

    #[test]
    fn admission_exemptions_and_unavailable_distance() {
        let mut g = ChurnGate::new(ChurnParams {
            activation_count: 1,
            ..params()
        });
        g.record_attempts(5, 0.0);
        let mut market = create(90.0, true);
        market.limit = false;
        let mut risk = create(90.0, true);
        risk.risk_critical = true;
        let (kept, deferred) = g.admit(
            vec![market, risk, create(90.0, false), create(90.0, true)],
            &dist,
            1.0,
        );
        assert_eq!((kept.len(), deferred), (3, 1));
        let (kept, deferred) = g.admit(vec![create(99.9, true)], &|_| None, 1.0);
        assert_eq!((kept.len(), deferred), (0, 1));
        let (kept, deferred) = g.admit(vec![create(99.9, true)], &|_| Some(f64::NAN), 1.0);
        assert_eq!((kept.len(), deferred), (0, 1));
    }
}
