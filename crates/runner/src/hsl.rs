//! HSL equity hard-stop-loss state machine (docs/SNAPSHOT_SPEC.md 2.3 step 1,
//! section 8; docs/DECISIONS.md D16). Port of the account-level
//! (`live.hsl_signal_mode` = `unified` | `pside`) machine in
//! `src/passivbot_hsl.py` (`hsl:`): the per-side runtime is the engine's own
//! `equity_hard_stop_loss::HardStopState` fed exactly like
//! `_equity_hard_stop_apply_sample` (hsl:3530); around it the Python bot keeps
//! the episode state (`_equity_hard_stop_make_state`, hsl:2430): halted /
//! cooldown / no-restart latch, pending stop event and flat confirmations of
//! the RED supervisor, the cooldown position policy and the repanic flow.
//!
//! Inputs per cycle (`_equity_hard_stop_check`, hsl:7150): raw balance,
//! realized pnl (`pnl + fee_paid` over the fill events of the lookback,
//! hsl:3457), unrealized pnl per side from positions at the live last price
//! (hsl:3320), the positions and the fill events. Start-up
//! (`_equity_hard_stop_initialize_from_history`, hsl:5161) replays the
//! minute-by-minute equity timeline the bot rebuilds from fills and candle
//! closes (`get_balance_equity_history`, pb:14661) with `latch_red = false`,
//! then samples the present.
//!
//! Not ported (D16): `hsl_signal_mode = coin` (per-coin runtimes, slot-budget
//! signal, coin history replay; the runner refuses such configs while any
//! side has HSL enabled), panic-marker reconstruction from fills
//! (`pb_order_type` is `unknown` on the fake exchange and the runner does not
//! decode custom ids), the replay-matrix caches, the operator runtime forced
//! modes (they never change `_orchestrator_mode_override` for HSL: step 1
//! already answers) and the latch files (write-only diagnostics in Python).

use crate::bot_params::{ConfigView, PSIDES};
use anyhow::{anyhow, bail, Result};
use passivbot_rust::equity_hard_stop_loss as ehsl;
use passivbot_rust::utils::{calc_pnl_long, calc_pnl_short};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub const ONE_MIN_MS: u64 = 60_000;
const ONE_DAY_MS: f64 = 86_400_000.0;

pub const LONG: usize = 0;
pub const SHORT: usize = 1;

pub fn pside_index(pside: &str) -> usize {
    if pside == "short" {
        SHORT
    } else {
        LONG
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalMode {
    Unified,
    Pside,
    Coin,
}

impl SignalMode {
    fn parse(v: &str) -> Result<Self> {
        Ok(match v {
            "unified" => Self::Unified,
            "pside" => Self::Pside,
            "coin" => Self::Coin,
            other => {
                bail!("live.hsl_signal_mode must be one of coin, pside, unified, got {other:?}")
            }
        })
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unified => "unified",
            Self::Pside => "pside",
            Self::Coin => "coin",
        }
    }
}

/// `live.hsl_position_during_cooldown_policy` (`config/coerce.py:9`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CooldownPositionPolicy {
    Normal,
    #[default]
    Panic,
    TpOnly,
    Manual,
    GracefulStop,
}

impl CooldownPositionPolicy {
    fn parse(v: &str) -> Result<Self> {
        Ok(match v {
            "normal" => Self::Normal,
            "panic" => Self::Panic,
            "tp_only" => Self::TpOnly,
            "manual" => Self::Manual,
            "graceful_stop" => Self::GracefulStop,
            other => bail!(
                "live.hsl_position_during_cooldown_policy must be one of normal, panic, tp_only, manual, graceful_stop, got {other:?}"
            ),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Tier {
    #[default]
    Green,
    Yellow,
    Orange,
    Red,
}

impl Tier {
    fn from_engine(t: ehsl::HardStopTier) -> Self {
        match t {
            ehsl::HardStopTier::Green => Self::Green,
            ehsl::HardStopTier::Yellow => Self::Yellow,
            ehsl::HardStopTier::Orange => Self::Orange,
            ehsl::HardStopTier::Red => Self::Red,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Green => "green",
            Self::Yellow => "yellow",
            Self::Orange => "orange",
            Self::Red => "red",
        }
    }
}

/// `_parse_hsl_config()[pside]` (hsl:2521) after validation and the
/// `no_restart_drawdown_threshold >= red_threshold` clamp.
#[derive(Debug, Clone, PartialEq)]
pub struct SideConfig {
    pub enabled: bool,
    pub red_threshold: f64,
    pub ema_span_minutes: f64,
    pub cooldown_minutes_after_red: f64,
    pub no_restart_drawdown_threshold: f64,
    pub ratio_yellow: f64,
    pub ratio_orange: f64,
    pub orange_tier_mode: String,
    pub panic_close_order_type: String,
    pub restart_after_red_policy: String,
}

/// `live.pnls_max_lookback_days` (`config/pnl_lookback.py`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PnlsLookback {
    /// `None` = `"all"`.
    pub days: Option<f64>,
}

impl PnlsLookback {
    pub fn parse(v: Option<&Value>) -> Result<Self> {
        let days = match v {
            None => bail!("live.pnls_max_lookback_days missing"),
            Some(Value::String(s)) => {
                let s = s.trim().to_ascii_lowercase();
                if s == "all" {
                    return Ok(Self { days: None });
                }
                s.parse::<f64>()
                    .map_err(|_| anyhow!("live.pnls_max_lookback_days must be >= 0 or 'all'"))?
            }
            Some(Value::Number(n)) => n.as_f64().unwrap_or(f64::NAN),
            Some(Value::Bool(b)) => f64::from(*b as u8),
            Some(_) => bail!("live.pnls_max_lookback_days must be >= 0 or 'all'"),
        };
        if !days.is_finite() || days < 0.0 {
            bail!("live.pnls_max_lookback_days must be >= 0 or 'all', got {days}");
        }
        Ok(Self { days: Some(days) })
    }
    fn window_ms(&self, minimum_ms: u64) -> Option<u64> {
        let days = self.days?;
        Some(((days * ONE_DAY_MS).round() as u64).max(minimum_ms))
    }
    fn start_ms(&self, now_ms: u64, minimum_ms: u64) -> Option<u64> {
        // Python: `int(now_ms) - lookback_ms` (may go negative for tiny clocks).
        self.window_ms(minimum_ms)
            .map(|w| (now_ms as i64 - w as i64).max(0) as u64)
    }
    /// `event_history_start_ms` (minimum 1 ms).
    pub fn event_history_start_ms(&self, now_ms: u64) -> Option<u64> {
        self.start_ms(now_ms, 1)
    }
    /// `hsl_window_ms` (minimum one minute).
    pub fn hsl_window_ms(&self) -> Option<u64> {
        self.window_ms(ONE_MIN_MS)
    }
    /// `balance_history_start_ms` (minimum one minute).
    pub fn balance_history_start_ms(&self, now_ms: u64) -> Option<u64> {
        self.start_ms(now_ms, ONE_MIN_MS)
    }
}

/// `fill_events_manager._normalize_fee_paid_from_payload`: how a fill's
/// reported fee becomes the signed quote cashflow `fee_paid` the HSL (and
/// the realized-pnl history) reads. Missing/zero fees fall back to
/// `live.fee_pct_fallback` of the notional; fee ratios beyond
/// `live.fee_pct_sanity_abs_max` are replaced by the fallback too.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FeePolicy {
    pub fallback_pct: f64,
    pub sanity_abs_max: f64,
}

impl Default for FeePolicy {
    fn default() -> Self {
        Self {
            fallback_pct: 0.0002,
            sanity_abs_max: 0.001,
        }
    }
}

impl FeePolicy {
    pub fn from_config(cfg: &ConfigView) -> Self {
        let d = Self::default();
        Self {
            fallback_pct: cfg
                .live("fee_pct_fallback")
                .and_then(Value::as_f64)
                .unwrap_or(d.fallback_pct),
            sanity_abs_max: cfg
                .live("fee_pct_sanity_abs_max")
                .and_then(Value::as_f64)
                .unwrap_or(d.sanity_abs_max),
        }
    }

    /// Signed `fee_paid` of one fill: `fee_cost` is the exchange's reported
    /// quote fee (positive = paid, negative = rebate), `notional` is
    /// `qty * price * c_mult`.
    pub fn signed_fee_paid(&self, fee_cost: f64, notional: f64) -> f64 {
        let fallback = || {
            if notional <= 0.0 {
                0.0
            } else {
                -(notional * self.fallback_pct).abs()
            }
        };
        let mut fee_paid = if fee_cost == 0.0 {
            fallback()
        } else if fee_cost < 0.0 {
            fee_cost.abs()
        } else {
            -fee_cost.abs()
        };
        let sanity_max = self.sanity_abs_max.max(0.0);
        if notional > 0.0 && sanity_max > 0.0 && (fee_paid / notional).abs() > sanity_max {
            fee_paid = fallback();
        }
        fee_paid
    }
}

#[derive(Debug, Clone)]
pub struct HslConfig {
    pub signal_mode: SignalMode,
    pub cooldown_position_policy: CooldownPositionPolicy,
    pub lookback: PnlsLookback,
    pub fee: FeePolicy,
    pub sides: [SideConfig; 2],
}

fn str_of(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => "None".to_string(),
        other => other.to_string(),
    }
}

impl HslConfig {
    /// `_parse_hsl_config` + the live-level HSL settings. Fails like Python
    /// on invalid values; additionally refuses `hsl_signal_mode = coin`
    /// while any side is enabled (coin mode is not modelled, D16).
    pub fn from_config(cfg: &ConfigView) -> Result<Self> {
        let signal_mode = SignalMode::parse(
            cfg.live("hsl_signal_mode")
                .map(str_of)
                .ok_or_else(|| anyhow!("live.hsl_signal_mode missing"))?
                .as_str(),
        )?;
        let cooldown_position_policy = match cfg.live("hsl_position_during_cooldown_policy") {
            None | Some(Value::Null) => CooldownPositionPolicy::Panic,
            Some(v) => CooldownPositionPolicy::parse(&str_of(v))?,
        };
        let lookback = PnlsLookback::parse(cfg.live("pnls_max_lookback_days"))?;
        let mut sides = Vec::with_capacity(2);
        for pside in PSIDES {
            let m = cfg.hsl_side(pside)?;
            let f = |k: &str| -> Result<f64> {
                m.get(k)
                    .and_then(Value::as_f64)
                    .ok_or_else(|| anyhow!("bot.{pside}.hsl.{k} not numeric"))
            };
            let enabled = m.get("enabled").and_then(Value::as_bool).unwrap_or(false);
            let red_threshold = f("red_threshold")?;
            let ema_span_minutes = f("ema_span_minutes")?;
            let cooldown_minutes_after_red = f("cooldown_minutes_after_red")?;
            let no_restart_drawdown_threshold = f("no_restart_drawdown_threshold")?;
            let ratio_yellow = m["tier_ratios"]["yellow"].as_f64().unwrap_or(f64::NAN);
            let ratio_orange = m["tier_ratios"]["orange"].as_f64().unwrap_or(f64::NAN);
            let orange_tier_mode = str_of(&m["orange_tier_mode"]);
            let panic_close_order_type = str_of(&m["panic_close_order_type"]);
            let restart_after_red_policy = str_of(&m["restart_after_red_policy"]);
            if enabled && red_threshold <= 0.0 {
                bail!("bot.{pside}.hsl_red_threshold must be > 0.0 when enabled");
            }
            if enabled && ema_span_minutes <= 0.0 {
                bail!("bot.{pside}.hsl_ema_span_minutes must be > 0.0 when enabled");
            }
            if cooldown_minutes_after_red < 0.0 {
                bail!("bot.{pside}.hsl_cooldown_minutes_after_red must be >= 0.0");
            }
            if !(red_threshold <= no_restart_drawdown_threshold
                && no_restart_drawdown_threshold <= 1.0)
            {
                bail!(
                    "bot.{pside}.hsl_no_restart_drawdown_threshold must satisfy hsl_red_threshold <= hsl_no_restart_drawdown_threshold <= 1.0"
                );
            }
            if !(0.0 < ratio_yellow && ratio_yellow < ratio_orange && ratio_orange < 1.0) {
                bail!("bot.{pside}.hsl_tier_ratios must satisfy 0 < yellow < orange < 1");
            }
            if !["graceful_stop", "tp_only_with_active_entry_cancellation"]
                .contains(&orange_tier_mode.as_str())
            {
                bail!(
                    "bot.{pside}.hsl_orange_tier_mode must be one of {{graceful_stop, tp_only_with_active_entry_cancellation}}"
                );
            }
            if !["market", "limit"].contains(&panic_close_order_type.as_str()) {
                bail!("bot.{pside}.hsl_panic_close_order_type must be one of {{market, limit}}");
            }
            if !["always", "threshold", "never"].contains(&restart_after_red_policy.as_str()) {
                bail!("bot.{pside}.hsl_restart_after_red_policy must be one of always, threshold, never");
            }
            sides.push(SideConfig {
                enabled,
                red_threshold,
                ema_span_minutes,
                cooldown_minutes_after_red,
                no_restart_drawdown_threshold,
                ratio_yellow,
                ratio_orange,
                orange_tier_mode,
                panic_close_order_type,
                restart_after_red_policy,
            });
        }
        let sides: [SideConfig; 2] = [sides.remove(0), sides.remove(0)];
        if signal_mode == SignalMode::Coin && sides.iter().any(|s| s.enabled) {
            bail!(
                "live.hsl_signal_mode = \"coin\" with HSL enabled is not supported by pb-runner (D16): use \"unified\" or \"pside\", or disable bot.<pside>.hsl.enabled"
            );
        }
        Ok(Self {
            signal_mode,
            cooldown_position_policy,
            lookback,
            fee: FeePolicy::from_config(cfg),
            sides,
        })
    }

    /// `_equity_hard_stop_enabled(pside)` for the non-coin modes.
    pub fn enabled(&self, pside: usize) -> bool {
        self.sides[pside].enabled
    }
    pub fn any_enabled(&self) -> bool {
        self.sides.iter().any(|s| s.enabled)
    }
}

/// One normalised fill event (`_hsl_extract_fill_events`, pb:14580, and the
/// `FillEvent` fields `_equity_hard_stop_realized_pnl_now` reads).
#[derive(Debug, Clone, PartialEq)]
pub struct HslFill {
    pub timestamp_ms: u64,
    pub symbol: String,
    pub pside: usize,
    pub qty: f64,
    pub price: f64,
    /// `true` = increase, `false` = decrease.
    pub increase: bool,
    pub pnl: f64,
    /// Signed cashflow: paid fees are negative.
    pub fee_paid: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HslPosition {
    pub symbol: String,
    pub pside: usize,
    pub size: f64,
    pub price: f64,
}

/// `_calc_hsl_pnl`.
pub fn hsl_pnl(pside: usize, entry: f64, close: f64, qty: f64, c_mult: f64) -> f64 {
    if pside == LONG {
        calc_pnl_long(entry, close, qty, c_mult)
    } else {
        calc_pnl_short(entry, close, qty, c_mult)
    }
}

/// `_calc_upnl_sum_strict(pside)`: positions of the side at the live last
/// price; errors on a missing price like Python.
pub fn upnl_sum(
    positions: &[HslPosition],
    pside: usize,
    last_price: &dyn Fn(&str) -> Option<f64>,
    c_mult: &dyn Fn(&str) -> f64,
) -> Result<f64> {
    let mut sum = 0.0;
    for p in positions.iter().filter(|p| p.pside == pside) {
        let price = last_price(&p.symbol).ok_or_else(|| {
            anyhow!(
                "missing last price for {} while evaluating hard stop",
                p.symbol
            )
        })?;
        let upnl = hsl_pnl(pside, p.price, price, p.size, c_mult(&p.symbol));
        if !upnl.is_finite() {
            bail!(
                "non-finite upnl for {} while evaluating hard stop",
                p.symbol
            );
        }
        sum += upnl;
    }
    Ok(sum)
}

/// `_equity_hard_stop_realized_pnl_now(pside)`: `pnl` then `fee_paid` added
/// per event in chronological order over events at or after `start_ms`.
pub fn realized_pnl_now(fills: &[HslFill], start_ms: Option<u64>, pside: Option<usize>) -> f64 {
    let mut realized = 0.0;
    for f in fills {
        if start_ms.is_some_and(|s| f.timestamp_ms < s) {
            continue;
        }
        if pside.is_some_and(|p| f.pside != p) {
            continue;
        }
        realized += f.pnl;
        realized += f.fee_paid;
    }
    realized
}

/// `_hsl_flat_epsilon`.
pub fn flat_epsilon(qty_step: f64) -> f64 {
    let step = if qty_step.is_finite() {
        qty_step.abs()
    } else {
        0.0
    };
    (step * 0.5).max(1e-12)
}

/// `_equity_hard_stop_latest_flatten_fill_timestamp_optional_ms` (hsl:3122).
pub fn latest_flatten_fill_timestamp(
    fills: &[HslFill],
    pside: usize,
    since_ms: Option<u64>,
    replay_start_sizes: Option<&BTreeMap<String, f64>>,
) -> Option<u64> {
    let candidates: Vec<&HslFill> = fills
        .iter()
        .filter(|f| f.pside == pside && since_ms.is_none_or(|s| f.timestamp_ms >= s))
        .collect();
    let Some(start) = replay_start_sizes else {
        return candidates.iter().map(|f| f.timestamp_ms).max();
    };
    let eps = flat_epsilon(0.0);
    let mut running: BTreeMap<String, f64> = start
        .iter()
        .filter(|(_, s)| s.is_finite() && s.abs() > eps)
        .map(|(k, s)| (k.clone(), s.abs()))
        .collect();
    if running.is_empty() {
        return None;
    }
    let mut sorted = candidates;
    sorted.sort_by_key(|f| f.timestamp_ms);
    for f in sorted {
        // Python `not (qty > 0)`: NaN counts as invalid too.
        if f.symbol.is_empty() || f.qty.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater) {
            return None;
        }
        let e = running.entry(f.symbol.clone()).or_insert(0.0);
        if f.increase {
            *e += f.qty;
        } else {
            *e = (*e - f.qty).max(0.0);
        }
        if !running.values().any(|s| *s > eps) {
            return Some(f.timestamp_ms);
        }
    }
    None
}

/// The engine runtime with the `EquityHardStopRuntime` wrapper semantics
/// (python.rs:171): `rolling_peak_strategy_equity` is the last peak handed in.
#[derive(Debug, Clone, Default)]
pub struct Runtime {
    pub state: ehsl::HardStopState,
    pub last_rolling_peak: f64,
}

impl Runtime {
    pub fn tier(&self) -> Tier {
        Tier::from_engine(self.state.tier)
    }
    pub fn red_latched(&self) -> bool {
        self.state.red_latched
    }
    pub fn initialized(&self) -> bool {
        self.state.initialized
    }
}

/// `_equity_hard_stop_apply_sample` result (hsl:3603).
#[derive(Debug, Clone, PartialEq)]
pub struct Metrics {
    pub timestamp_ms: u64,
    pub balance: f64,
    pub realized_pnl_total: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub strategy_pnl: f64,
    pub peak_strategy_pnl: f64,
    pub baseline_balance: f64,
    pub strategy_equity: f64,
    pub peak_strategy_equity: f64,
    pub rolling_peak_strategy_equity: f64,
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

/// `_equity_hard_stop_compute_stop_event` (hsl:4569).
#[derive(Debug, Clone, PartialEq)]
pub struct StopEvent {
    pub stop_event_timestamp_ms: u64,
    pub balance: f64,
    pub realized_pnl_total: f64,
    pub realized_pnl: f64,
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

/// The numeric part of `_equity_hard_stop_build_latch_payload` (hsl:4458)
/// kept as `last_stop_event`.
#[derive(Debug, Clone, PartialEq)]
pub struct LatchPayload {
    pub stop_event_timestamp_ms: u64,
    pub strategy_equity: f64,
    pub peak_strategy_equity: f64,
    pub trigger_peak_strategy_equity: f64,
    pub drawdown_raw: f64,
    pub drawdown_ema: f64,
    pub drawdown_score: f64,
    pub no_restart_latched: bool,
    pub cooldown_until_ms: Option<u64>,
    pub no_restart_peak_strategy_equity: f64,
    pub no_restart_drawdown_raw: f64,
}

/// `_equity_hard_stop_make_state` (hsl:2430) minus logging throttles.
#[derive(Debug, Clone, Default)]
pub struct SideState {
    pub runtime: Runtime,
    pub strategy_pnl_peak: ehsl::RollingPeakTracker,
    pub no_restart_peak_strategy_equity: f64,
    pub halted: bool,
    pub no_restart_latched: bool,
    pub last_metrics: Option<Metrics>,
    pub red_flat_confirmations: u32,
    pub pending_red_since_ms: Option<u64>,
    pub cooldown_until_ms: Option<u64>,
    pub pending_stop_event: Option<StopEvent>,
    pub last_stop_event: Option<LatchPayload>,
    pub cooldown_intervention_active: bool,
    pub cooldown_repanic_reset_pending: bool,
    pub cooldown_repanic_since_ms: Option<u64>,
    pub cooldown_repanic_start_sizes: Option<BTreeMap<String, f64>>,
    pub cooldown_unresolved_residue: bool,
}

impl SideState {
    /// `_equity_hard_stop_reset_after_restart` (hsl:5035) / `_reset_state`:
    /// everything but the persistent no-restart peak.
    /// `_equity_hard_stop_reset_after_restart` (hsl:5035): everything but
    /// the no-restart peak and the last latch payload is cleared.
    fn reset_after_restart(&mut self) {
        let keep = self.no_restart_peak_strategy_equity;
        let last_stop_event = self.last_stop_event.take();
        *self = Self::default();
        self.no_restart_peak_strategy_equity = keep;
        self.last_stop_event = last_stop_event;
    }
}

/// What the mode override needs from a side's HSL state
/// (`_orchestrator_mode_override` step 1, pb:17091, and `get_forced_PB_mode`).
#[derive(Debug, Clone, PartialEq, Default)]
pub enum HslSideMode {
    #[default]
    None,
    /// Red latched and not halted.
    Panic,
    /// Halted (RED stop finalized, cooldown or terminal).
    Halted {
        policy: CooldownPositionPolicy,
        unresolved_residue: bool,
    },
    /// Orange tier: the configured `hsl_orange_tier_mode`.
    Orange(String),
}

impl HslSideMode {
    /// Step 1 of `_orchestrator_mode_override` for one symbol.
    pub fn symbol_override(&self, has_position: bool) -> Option<String> {
        match self {
            Self::None => None,
            Self::Panic => Some("panic".to_string()),
            Self::Halted {
                policy,
                unresolved_residue,
            } => Some(halted_mode(*policy, *unresolved_residue, has_position).to_string()),
            Self::Orange(m) => Some(m.clone()),
        }
    }
    /// `get_forced_PB_mode(pside)` with `symbol = None`: the side-level forced
    /// mode that blocks new entries and forager mode (pb:10895-10906).
    pub fn side_forced_mode(&self) -> Option<&'static str> {
        match self {
            Self::Panic => Some("panic"),
            Self::Halted { .. } => Some("graceful_stop"),
            _ => None,
        }
    }
}

/// `_equity_hard_stop_halted_mode` (hsl:2924).
pub fn halted_mode(
    policy: CooldownPositionPolicy,
    unresolved_residue: bool,
    has_position: bool,
) -> &'static str {
    if !has_position {
        return "graceful_stop";
    }
    if unresolved_residue {
        return "panic";
    }
    match policy {
        CooldownPositionPolicy::Panic => "panic",
        CooldownPositionPolicy::Manual => "manual",
        CooldownPositionPolicy::TpOnly => "tp_only",
        CooldownPositionPolicy::Normal | CooldownPositionPolicy::GracefulStop => "graceful_stop",
    }
}

/// Per-side HSL modes carried into the snapshot builder (`CycleState.hsl`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct HslModes {
    pub sides: [HslSideMode; 2],
}

impl HslModes {
    pub fn side(&self, pside: &str) -> &HslSideMode {
        &self.sides[pside_index(pside)]
    }
}

/// Per-cycle inputs of `_equity_hard_stop_check`.
pub struct CycleInputs<'a> {
    pub now_ms: u64,
    pub balance: f64,
    pub realized_pnl_total: f64,
    pub realized_pnl: [f64; 2],
    pub unrealized_pnl: [f64; 2],
    pub positions: &'a [HslPosition],
    pub fills: &'a [HslFill],
}

/// What the RED supervisor observes per iteration
/// (`_equity_hard_stop_count_open_positions`, `_count_blocking_open_orders`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RedObservation {
    pub n_positions: usize,
    pub entry_orders: usize,
    pub nonpanic_close_orders: usize,
}

impl RedObservation {
    pub fn is_flat(&self) -> bool {
        self.n_positions == 0 && self.entry_orders == 0 && self.nonpanic_close_orders == 0
    }
}

/// Which supervisor flavour drives the flat confirmations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Supervision {
    /// `_equity_hard_stop_run_red_supervisor` (hsl:8068): the stop event is
    /// anchored at the fill that flattened the scope; a non-flat iteration
    /// drops the pending stop event.
    Production,
    /// `run_fake_live._run_fake_red_supervisor_step`: the stop event is
    /// computed once at the first flat step's exchange time and kept.
    FakeHarness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RedStep {
    pub finalized: bool,
    pub needs_panic_execution: bool,
}

/// One row of `get_balance_equity_history()["timeline"]` (pb:15600).
#[derive(Debug, Clone, PartialEq)]
pub struct TimelineRow {
    pub timestamp: u64,
    pub balance: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: [f64; 2],
    pub realized_pnl_by_pside: [f64; 2],
    pub is_flat: bool,
    pub is_flat_by_pside: [bool; 2],
}

#[derive(Debug, Clone)]
pub struct HslState {
    pub cfg: HslConfig,
    pub sides: [SideState; 2],
}

impl HslState {
    pub fn new(cfg: HslConfig) -> Self {
        Self {
            cfg,
            sides: [SideState::default(), SideState::default()],
        }
    }

    pub fn enabled(&self, pside: usize) -> bool {
        self.cfg.enabled(pside)
    }

    /// `_equity_hard_stop_reset_state`.
    pub fn reset_state(&mut self) {
        for s in &mut self.sides {
            *s = SideState::default();
        }
    }

    fn lookback_ms(&self) -> Option<u64> {
        self.cfg.lookback.hsl_window_ms()
    }

    /// `_equity_hard_stop_signal_values`.
    fn signal_values(
        &self,
        realized_pnl_total: f64,
        realized_pnl_pside: f64,
        unrealized_pnl_pside: f64,
        unrealized_pnl_total: Option<f64>,
    ) -> Result<(f64, f64)> {
        if self.cfg.signal_mode == SignalMode::Pside {
            return Ok((realized_pnl_pside, unrealized_pnl_pside));
        }
        let Some(t) = unrealized_pnl_total else {
            bail!("HSL unified signal mode requires unrealized_pnl_total sample input");
        };
        if !t.is_finite() {
            bail!("unrealized_pnl_total must be finite, got {t}");
        }
        Ok((realized_pnl_total, t))
    }

    /// `_equity_hard_stop_apply_sample` (hsl:3530).
    #[allow(clippy::too_many_arguments)]
    pub fn apply_sample(
        &mut self,
        pside: usize,
        timestamp_ms: u64,
        balance: f64,
        realized_pnl_total: f64,
        realized_pnl_pside: f64,
        unrealized_pnl_pside: f64,
        unrealized_pnl_total: Option<f64>,
        latch_red: bool,
    ) -> Result<Metrics> {
        if !balance.is_finite() || balance <= 0.0 {
            bail!("balance must be finite and > 0, got {balance}");
        }
        if !realized_pnl_total.is_finite() {
            bail!("realized_pnl_total must be finite, got {realized_pnl_total}");
        }
        if !realized_pnl_pside.is_finite() {
            bail!("realized_pnl_pside must be finite, got {realized_pnl_pside}");
        }
        if !unrealized_pnl_pside.is_finite() {
            bail!("unrealized_pnl_pside must be finite, got {unrealized_pnl_pside}");
        }
        let (realized_signal, unrealized_signal) = self.signal_values(
            realized_pnl_total,
            realized_pnl_pside,
            unrealized_pnl_pside,
            unrealized_pnl_total,
        )?;
        let current_minute = timestamp_ms / ONE_MIN_MS;
        let lookback_ms = self.lookback_ms();
        let cfg = self.cfg.sides[pside].clone();
        let state = &mut self.sides[pside];
        if let Some(last) = &state.last_metrics {
            if last.timestamp_ms / ONE_MIN_MS == current_minute {
                let same_inputs = last.balance == balance
                    && last.realized_pnl_total == realized_pnl_total
                    && last.realized_pnl == realized_signal
                    && last.unrealized_pnl == unrealized_signal;
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
        let strategy_pnl = realized_signal + unrealized_signal;
        let peak_strategy_pnl = state
            .strategy_pnl_peak
            .update(timestamp_ms, strategy_pnl, lookback_ms.unwrap_or(u64::MAX))
            .map_err(|e| anyhow!("HSL rolling peak: {e}"))?;
        let baseline_balance = balance - realized_pnl_total;
        let strategy_equity = (baseline_balance + strategy_pnl).max(1e-12);
        let peak_strategy_equity =
            strategy_equity.max((baseline_balance + peak_strategy_pnl).max(1e-12));
        state.runtime.last_rolling_peak = peak_strategy_equity;
        let engine_cfg = ehsl::HardStopConfig {
            red_threshold: cfg.red_threshold,
            ema_span_minutes: cfg.ema_span_minutes,
            tier_ratios: ehsl::HardStopTierRatios {
                yellow: cfg.ratio_yellow,
                orange: cfg.ratio_orange,
            },
        };
        let step = ehsl::step_with_peak_strategy_equity_latch(
            &mut state.runtime.state,
            engine_cfg,
            strategy_equity,
            peak_strategy_equity,
            timestamp_ms,
            latch_red,
        )
        .map_err(|e| anyhow!("HSL runtime: {e}"))?;
        let tier = Tier::from_engine(state.runtime.state.tier);
        let metrics = Metrics {
            timestamp_ms,
            balance,
            realized_pnl_total,
            realized_pnl: realized_signal,
            unrealized_pnl: unrealized_signal,
            strategy_pnl,
            peak_strategy_pnl,
            baseline_balance,
            strategy_equity,
            peak_strategy_equity: state.runtime.state.peak_strategy_equity,
            rolling_peak_strategy_equity: state.runtime.last_rolling_peak,
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

    fn unrealized_total(&self, unrealized: &[f64; 2]) -> Option<f64> {
        (self.cfg.signal_mode == SignalMode::Unified).then(|| unrealized[LONG] + unrealized[SHORT])
    }

    /// `_equity_hard_stop_compute_stop_event` (hsl:4569) from the current inputs.
    pub fn compute_stop_event(
        &self,
        pside: usize,
        stop_event_ts_ms: u64,
        inp: &CycleInputs,
    ) -> Result<StopEvent> {
        let state = &self.sides[pside];
        let (realized_pnl, unrealized_pnl) = self.signal_values(
            inp.realized_pnl_total,
            inp.realized_pnl[pside],
            inp.unrealized_pnl[pside],
            self.unrealized_total(&inp.unrealized_pnl),
        )?;
        let strategy_pnl = realized_pnl + unrealized_pnl;
        let last_peak = state
            .last_metrics
            .as_ref()
            .map_or(strategy_pnl, |m| m.peak_strategy_pnl);
        let peak_strategy_pnl = py_max2(strategy_pnl, last_peak);
        let baseline_balance = inp.balance - inp.realized_pnl_total;
        let strategy_equity = py_max2(baseline_balance + strategy_pnl, 1e-12);
        let trigger_peak_strategy_equity = state.runtime.state.peak_strategy_equity;
        let peak_strategy_equity = py_max2(
            py_max2(strategy_equity, baseline_balance + peak_strategy_pnl),
            1e-12,
        );
        if !trigger_peak_strategy_equity.is_finite() || trigger_peak_strategy_equity <= 0.0 {
            bail!("invalid HSL trigger_peak_strategy_equity at stop finalization: {trigger_peak_strategy_equity}");
        }
        if !peak_strategy_equity.is_finite() || peak_strategy_equity <= 0.0 {
            bail!("invalid HSL rolling peak_strategy_equity at stop finalization: {peak_strategy_equity}");
        }
        let drawdown_ema = state.runtime.state.drawdown_ema;
        let drawdown_raw = py_max2(
            0.0,
            1.0 - strategy_equity / py_max2(peak_strategy_equity, 1e-12),
        );
        Ok(StopEvent {
            stop_event_timestamp_ms: stop_event_ts_ms,
            balance: inp.balance,
            realized_pnl_total: inp.realized_pnl_total,
            realized_pnl,
            unrealized_pnl,
            strategy_pnl,
            peak_strategy_pnl,
            strategy_equity,
            peak_strategy_equity,
            trigger_peak_strategy_equity,
            drawdown_raw,
            drawdown_ema,
            drawdown_score: py_min2(drawdown_raw, drawdown_ema),
        })
    }

    /// `_equity_hard_stop_red_episode_finalization` (hsl:4530): engine-owned
    /// policy math; updates the persistent no-restart peak.
    fn red_episode_finalization(
        &mut self,
        pside: usize,
        stop_equity: f64,
        stop_peak_strategy_equity: f64,
        drawdown_ema: f64,
        stop_ts: u64,
    ) -> Result<ehsl::RedEpisodeFinalization> {
        let cfg = &self.cfg.sides[pside];
        let r = ehsl::evaluate_red_episode_finalization(
            &cfg.restart_after_red_policy,
            stop_ts,
            stop_equity,
            stop_peak_strategy_equity,
            self.sides[pside].no_restart_peak_strategy_equity,
            drawdown_ema,
            cfg.red_threshold,
            cfg.no_restart_drawdown_threshold,
            cfg.cooldown_minutes_after_red,
        )
        .map_err(|e| anyhow!("HSL red episode finalization: {e}"))?;
        self.sides[pside].no_restart_peak_strategy_equity = r.no_restart_peak_strategy_equity;
        Ok(r)
    }

    /// `_equity_hard_stop_finalize_red_stop` (hsl:7808).
    pub fn finalize_red_stop(&mut self, pside: usize, stop_event: &StopEvent) -> Result<()> {
        let stop_ts = stop_event.stop_event_timestamp_ms;
        let fin = self.red_episode_finalization(
            pside,
            stop_event.strategy_equity,
            stop_event.peak_strategy_equity,
            stop_event.drawdown_ema,
            stop_ts,
        )?;
        let state = &mut self.sides[pside];
        state.last_stop_event = Some(LatchPayload {
            stop_event_timestamp_ms: stop_ts,
            strategy_equity: stop_event.strategy_equity,
            peak_strategy_equity: stop_event.peak_strategy_equity,
            trigger_peak_strategy_equity: stop_event.trigger_peak_strategy_equity,
            drawdown_raw: stop_event.drawdown_raw,
            drawdown_ema: stop_event.drawdown_ema,
            drawdown_score: stop_event.drawdown_score,
            no_restart_latched: fin.no_restart_latched,
            cooldown_until_ms: fin.cooldown_until_ms,
            no_restart_peak_strategy_equity: fin.no_restart_peak_strategy_equity,
            no_restart_drawdown_raw: fin.no_restart_drawdown_raw,
        });
        state.halted = true;
        state.no_restart_latched = fin.no_restart_latched;
        state.cooldown_until_ms = fin.cooldown_until_ms;
        state.pending_stop_event = None;
        state.red_flat_confirmations = 0;
        state.pending_red_since_ms = None;
        Ok(())
    }

    /// `_equity_hard_stop_position_symbols`.
    fn position_symbols(positions: &[HslPosition], pside: usize) -> Vec<String> {
        positions
            .iter()
            .filter(|p| p.pside == pside && p.size != 0.0)
            .map(|p| p.symbol.clone())
            .collect()
    }

    /// `_equity_hard_stop_refresh_cooldown_after_repanic` (hsl:4704).
    fn refresh_cooldown_after_repanic(&mut self, pside: usize, inp: &CycleInputs) -> Result<bool> {
        let cooldown_minutes = self.cfg.sides[pside].cooldown_minutes_after_red;
        let cooldown_ms = if cooldown_minutes > 0.0 {
            ((cooldown_minutes * 60_000.0).round() as i64).max(0) as u64
        } else {
            0
        };
        let (since, start_sizes) = {
            let s = &self.sides[pside];
            (
                s.cooldown_repanic_since_ms,
                s.cooldown_repanic_start_sizes.clone().unwrap_or_default(),
            )
        };
        // `_flatten_fill_timestamp_with_refresh`: no `since` -> deferred.
        let Some(since) = since else { return Ok(false) };
        let Some(stop_ts) =
            latest_flatten_fill_timestamp(inp.fills, pside, Some(since), Some(&start_sizes))
        else {
            return Ok(false);
        };
        let cooldown_until_ms = (cooldown_ms > 0).then(|| stop_ts + cooldown_ms);
        let ev = self.compute_stop_event(pside, stop_ts, inp)?;
        let state = &mut self.sides[pside];
        state.last_stop_event = Some(LatchPayload {
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
        });
        state.cooldown_until_ms = cooldown_until_ms;
        state.cooldown_intervention_active = false;
        state.cooldown_repanic_reset_pending = false;
        state.cooldown_repanic_since_ms = None;
        state.cooldown_repanic_start_sizes = None;
        state.cooldown_unresolved_residue = false;
        Ok(true)
    }

    /// `_equity_hard_stop_handle_position_during_cooldown` (hsl:4860).
    /// Returns `true` when the side state was replaced (reset or repanic
    /// refresh), like Python.
    pub fn handle_position_during_cooldown(
        &mut self,
        pside: usize,
        inp: &CycleInputs,
    ) -> Result<bool> {
        let now_ms = inp.now_ms;
        {
            let s = &self.sides[pside];
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
        let symbols = Self::position_symbols(inp.positions, pside);
        let policy = self.cfg.cooldown_position_policy;
        if symbols.is_empty() {
            if self.sides[pside].cooldown_repanic_reset_pending {
                self.refresh_cooldown_after_repanic(pside, inp)?;
                return Ok(true);
            }
            let s = &mut self.sides[pside];
            s.cooldown_intervention_active = false;
            s.cooldown_repanic_reset_pending = false;
            s.cooldown_repanic_since_ms = None;
            s.cooldown_repanic_start_sizes = None;
            s.cooldown_unresolved_residue = false;
            return Ok(false);
        }
        if self.sides[pside].cooldown_unresolved_residue {
            return Ok(false);
        }
        self.sides[pside].cooldown_intervention_active = true;
        match policy {
            CooldownPositionPolicy::Normal => {
                self.sides[pside].reset_after_restart();
                Ok(true)
            }
            CooldownPositionPolicy::Panic => {
                let s = &mut self.sides[pside];
                if !s.cooldown_repanic_reset_pending {
                    s.cooldown_repanic_since_ms = Some(now_ms);
                    s.cooldown_repanic_start_sizes = Some(
                        inp.positions
                            .iter()
                            .filter(|p| p.pside == pside && p.size != 0.0)
                            .map(|p| (p.symbol.clone(), p.size.abs()))
                            .collect(),
                    );
                }
                s.cooldown_repanic_reset_pending = true;
                Ok(false)
            }
            _ => {
                let s = &mut self.sides[pside];
                s.cooldown_repanic_reset_pending = false;
                s.cooldown_repanic_since_ms = None;
                s.cooldown_repanic_start_sizes = None;
                Ok(false)
            }
        }
    }

    /// `_equity_hard_stop_check` (hsl:7150) for the non-coin modes. The
    /// runtime forced modes it refreshes at the end are not carried: step 1
    /// of `_orchestrator_mode_override` decides before they are consulted.
    pub fn check(&mut self, inp: &CycleInputs) -> Result<()> {
        if !self.cfg.any_enabled() {
            return Ok(());
        }
        if self.cfg.signal_mode == SignalMode::Coin {
            bail!("HSL coin signal mode is not modelled");
        }
        let ts_ms = inp.now_ms;
        let unrealized_total = self.unrealized_total(&inp.unrealized_pnl);
        for pside in [LONG, SHORT] {
            if !self.cfg.enabled(pside) {
                continue;
            }
            if self.sides[pside].halted {
                self.handle_position_during_cooldown(pside, inp)?;
                let s = &self.sides[pside];
                if s.halted {
                    if !s.no_restart_latched
                        && !s.cooldown_repanic_reset_pending
                        && s.cooldown_until_ms.is_some_and(|c| ts_ms >= c)
                    {
                        self.sides[pside].reset_after_restart();
                    } else {
                        continue;
                    }
                }
            }
            let prev_latched = self.sides[pside].runtime.red_latched();
            let metrics = self.apply_sample(
                pside,
                ts_ms,
                inp.balance,
                inp.realized_pnl_total,
                inp.realized_pnl[pside],
                inp.unrealized_pnl[pside],
                unrealized_total,
                true,
            )?;
            let s = &mut self.sides[pside];
            if metrics.tier == Tier::Red && !prev_latched {
                s.pending_red_since_ms = Some(metrics.timestamp_ms);
                s.pending_stop_event = None;
            } else if metrics.tier != Tier::Red && !s.runtime.red_latched() {
                s.pending_red_since_ms = None;
            }
        }
        Ok(())
    }

    /// Red latched and not halted: the supervisor owns the side.
    pub fn red_active(&self, pside: usize) -> bool {
        self.cfg.enabled(pside)
            && self.sides[pside].runtime.red_latched()
            && !self.sides[pside].halted
    }

    /// One RED supervisor iteration for one side (hsl:8068 /
    /// `_run_fake_red_supervisor_step`): flat confirmations, finalization
    /// after two, otherwise panic execution is due.
    pub fn supervise_red(
        &mut self,
        pside: usize,
        obs: RedObservation,
        inp: &CycleInputs,
        mode: Supervision,
    ) -> Result<RedStep> {
        if !self.red_active(pside) {
            return Ok(RedStep {
                finalized: true,
                needs_panic_execution: false,
            });
        }
        let mut needs_panic_execution = false;
        if obs.is_flat() {
            match mode {
                Supervision::FakeHarness => {
                    let s = &self.sides[pside];
                    if s.red_flat_confirmations == 0 && s.pending_stop_event.is_none() {
                        let ev = self.compute_stop_event(pside, inp.now_ms, inp)?;
                        self.sides[pside].pending_stop_event = Some(ev);
                    }
                    self.sides[pside].red_flat_confirmations += 1;
                }
                Supervision::Production => {
                    let since = self.sides[pside].pending_red_since_ms;
                    let stop_ts = since.and_then(|since| {
                        latest_flatten_fill_timestamp(inp.fills, pside, Some(since), None)
                    });
                    match stop_ts {
                        Some(stop_ts) => {
                            let ev = self.compute_stop_event(pside, stop_ts, inp)?;
                            let s = &mut self.sides[pside];
                            s.pending_stop_event = Some(ev);
                            s.red_flat_confirmations += 1;
                        }
                        None => {
                            // `_defer_missing_flatten_fill`: stay protective.
                            let s = &mut self.sides[pside];
                            s.pending_stop_event = None;
                            s.red_flat_confirmations = 0;
                        }
                    }
                }
            }
        } else {
            needs_panic_execution = true;
            let s = &mut self.sides[pside];
            s.red_flat_confirmations = 0;
            if mode == Supervision::Production {
                s.pending_stop_event = None;
            }
        }
        if self.sides[pside].red_flat_confirmations >= 2 {
            let ev = self.sides[pside]
                .pending_stop_event
                .clone()
                .ok_or_else(|| anyhow!("HSL flat confirmations without a pending stop event"))?;
            self.finalize_red_stop(pside, &ev)?;
            return Ok(RedStep {
                finalized: true,
                needs_panic_execution: false,
            });
        }
        Ok(RedStep {
            finalized: false,
            needs_panic_execution,
        })
    }

    /// `run_fake_live._finalize_fake_terminal_red_if_sync_flat`: after a
    /// synchronous panic execution flattened the side, finalize at once when
    /// the stop event's raw drawdown reaches the no-restart threshold.
    pub fn sync_flat_finalize(
        &mut self,
        pside: usize,
        obs: RedObservation,
        inp: &CycleInputs,
    ) -> Result<bool> {
        if !self.red_active(pside) || !obs.is_flat() {
            return Ok(false);
        }
        let ev = match &self.sides[pside].pending_stop_event {
            Some(ev) => ev.clone(),
            None => {
                let ev = self.compute_stop_event(pside, inp.now_ms, inp)?;
                self.sides[pside].pending_stop_event = Some(ev.clone());
                ev
            }
        };
        if ev.drawdown_raw < self.cfg.sides[pside].no_restart_drawdown_threshold {
            return Ok(false);
        }
        self.sides[pside].red_flat_confirmations = 2;
        self.finalize_red_stop(pside, &ev)?;
        Ok(true)
    }

    /// The per-side modes for the snapshot builder.
    pub fn modes(&self) -> HslModes {
        let mut out = HslModes::default();
        for pside in [LONG, SHORT] {
            if !self.cfg.enabled(pside) {
                continue;
            }
            let s = &self.sides[pside];
            out.sides[pside] = if s.runtime.red_latched() && !s.halted {
                HslSideMode::Panic
            } else if s.halted {
                HslSideMode::Halted {
                    policy: self.cfg.cooldown_position_policy,
                    unresolved_residue: s.cooldown_unresolved_residue,
                }
            } else if s.runtime.tier() == Tier::Orange {
                HslSideMode::Orange(self.cfg.sides[pside].orange_tier_mode.clone())
            } else {
                HslSideMode::None
            };
        }
        out
    }

    /// `_equity_hard_stop_initialize_from_history` (hsl:5161) for the
    /// non-coin modes without panic markers: replay the timeline with
    /// `latch_red = false`, finalize a red-seen episode that flattened by an
    /// ordinary fill, reset red-free flattened episodes, then sample now.
    #[allow(clippy::too_many_arguments)]
    pub fn initialize_from_history(
        &mut self,
        now_ms: u64,
        balance: f64,
        fills: &[HslFill],
        timeline: &[TimelineRow],
        current_realized_total: f64,
        current_realized: [f64; 2],
        current_unrealized: [f64; 2],
    ) -> Result<()> {
        if !self.cfg.any_enabled() {
            return Ok(());
        }
        self.reset_state();
        if self.cfg.signal_mode == SignalMode::Coin {
            bail!("HSL initialize_from_history requires signal_mode unified or pside");
        }
        let unified = self.cfg.signal_mode == SignalMode::Unified;
        let current_upnl_total = current_unrealized[LONG] + current_unrealized[SHORT];
        for pside in [LONG, SHORT] {
            if !self.cfg.enabled(pside) {
                continue;
            }
            let scope_fill_ts: Vec<u64> = {
                let mut v: Vec<u64> = fills
                    .iter()
                    .filter(|f| unified || f.pside == pside)
                    .map(|f| f.timestamp_ms)
                    .collect();
                v.sort_unstable();
                v
            };
            let mut scope_was_nonflat = false;
            let mut prev_recorded_ts: Option<u64> = None;
            for row in timeline {
                let ts = row.timestamp;
                if ts > now_ms {
                    break;
                }
                {
                    let s = &self.sides[pside];
                    if s.halted && !s.no_restart_latched {
                        match s.cooldown_until_ms {
                            Some(c) if ts >= c => self.sides[pside].reset_after_restart(),
                            Some(_) => continue,
                            None => {}
                        }
                    }
                }
                let row_upnl_total =
                    unified.then(|| row.unrealized_pnl[LONG] + row.unrealized_pnl[SHORT]);
                let (row_realized_pside, row_unrealized_pside) = if unified {
                    (0.0, 0.0)
                } else {
                    (row.realized_pnl_by_pside[pside], row.unrealized_pnl[pside])
                };
                let m = self.apply_sample(
                    pside,
                    ts,
                    row.balance,
                    row.realized_pnl,
                    row_realized_pside,
                    row_unrealized_pside,
                    row_upnl_total,
                    false,
                )?;
                let row_flat = if unified {
                    row.is_flat
                } else {
                    row.is_flat_by_pside[pside]
                };
                let scope_flattened_this_row = row_flat && scope_was_nonflat;
                scope_was_nonflat = !row_flat;
                let row_prev_recorded_ts = prev_recorded_ts;
                prev_recorded_ts = Some(ts);
                if !scope_flattened_this_row {
                    continue;
                }
                if !m.red_seen_in_episode {
                    // Ordinary flatten of a RED-free episode: plain reset.
                    self.sides[pside].reset_after_restart();
                    continue;
                }
                // B2.1: red episode ended by an ordinary flattening fill;
                // anchor at the latest scope fill inside the flatten window.
                let boundary = ts + ONE_MIN_MS;
                let window_start = row_prev_recorded_ts.unwrap_or(ts.saturating_sub(ONE_MIN_MS));
                let idx = scope_fill_ts.partition_point(|t| *t < boundary);
                let anchor = (idx > 0 && scope_fill_ts[idx - 1] > window_start)
                    .then(|| scope_fill_ts[idx - 1]);
                let stop_ts = anchor.unwrap_or(ts);
                self.sides[pside].pending_red_since_ms = Some(ts);
                let fin = self.red_episode_finalization(
                    pside,
                    m.strategy_equity,
                    m.peak_strategy_equity,
                    m.drawdown_ema,
                    stop_ts,
                )?;
                let s = &mut self.sides[pside];
                s.last_stop_event = Some(LatchPayload {
                    stop_event_timestamp_ms: stop_ts,
                    strategy_equity: m.strategy_equity,
                    peak_strategy_equity: m.peak_strategy_equity,
                    trigger_peak_strategy_equity: m.peak_strategy_equity,
                    drawdown_raw: m.drawdown_raw,
                    drawdown_ema: m.drawdown_ema,
                    drawdown_score: m.drawdown_score,
                    no_restart_latched: fin.no_restart_latched,
                    cooldown_until_ms: fin.cooldown_until_ms,
                    no_restart_peak_strategy_equity: fin.no_restart_peak_strategy_equity,
                    no_restart_drawdown_raw: fin.no_restart_drawdown_raw,
                });
                s.halted = true;
                s.no_restart_latched = fin.no_restart_latched;
                s.cooldown_until_ms = fin.cooldown_until_ms;
                s.pending_red_since_ms = None;
                if s.no_restart_latched {
                    break;
                }
            }
            {
                let s = &self.sides[pside];
                if s.halted && !s.no_restart_latched {
                    if let Some(c) = s.cooldown_until_ms {
                        if now_ms >= c {
                            self.sides[pside].reset_after_restart();
                        }
                    }
                }
            }
            if self.sides[pside].halted {
                continue;
            }
            let m = self.apply_sample(
                pside,
                now_ms,
                balance,
                current_realized_total,
                current_realized[pside],
                current_unrealized[pside],
                unified.then_some(current_upnl_total),
                true,
            )?;
            if m.tier == Tier::Red {
                self.sides[pside].pending_red_since_ms = Some(m.timestamp_ms);
            }
        }
        Ok(())
    }
}

/// Python `max(a, b)`: the first argument wins ties (relevant for NaN/-0.0 only).
fn py_max2(a: f64, b: f64) -> f64 {
    if b > a {
        b
    } else {
        a
    }
}
fn py_min2(a: f64, b: f64) -> f64 {
    if b < a {
        b
    } else {
        a
    }
}

/// `get_balance_equity_history` timeline (pb:14661-15700) for the account
/// level modes: replay the fill events into positions and balance, one row
/// per minute from the lookback start to `now`, unrealized pnl from the
/// minute's candle close (carried forward when a minute has no candle).
/// `close_at(symbol, minute_ts)` must return the 1m close of that minute
/// (`> 0`) or `None`; it is only consulted for finalized minutes
/// (`< floor(now)`) and rounded to f32 like the candle manager's storage.
/// `known_market` mirrors `symbol in self.c_mults`.
#[allow(clippy::too_many_arguments)]
pub fn balance_equity_timeline(
    now_ms: u64,
    balance_now: f64,
    lookback: PnlsLookback,
    fills: &[HslFill],
    positions: &[HslPosition],
    close_at: &dyn Fn(&str, u64) -> Option<f64>,
    c_mult: &dyn Fn(&str) -> f64,
    qty_step: &dyn Fn(&str) -> f64,
    known_market: &dyn Fn(&str) -> bool,
) -> Vec<TimelineRow> {
    let is_flat = |symbol: &str, size: f64| size.abs() <= flat_epsilon(qty_step(symbol));
    let mut events: Vec<&HslFill> = fills.iter().collect();
    events.sort_by_key(|f| f.timestamp_ms);
    let single_point = |b: f64| TimelineRow {
        timestamp: now_ms,
        balance: b,
        realized_pnl: 0.0,
        unrealized_pnl: [0.0, 0.0],
        realized_pnl_by_pside: [0.0, 0.0],
        is_flat: true,
        is_flat_by_pside: [true, true],
    };
    if events.is_empty() {
        return vec![single_point(balance_now)];
    }
    let lookback_start = lookback.balance_history_start_ms(now_ms);
    let balance_now = balance_now.max(0.0);
    let mut total_realised = 0.0;
    for e in &events {
        if e.timestamp_ms <= now_ms {
            total_realised += e.pnl + e.fee_paid;
        }
    }
    let baseline_balance = balance_now - total_realised;
    let floor_min = |t: u64| t / ONE_MIN_MS * ONE_MIN_MS;
    let (start_ts, record_start_ts, record_start_minute) = match lookback_start {
        None => {
            let t = events[0].timestamp_ms;
            (t, floor_min(t), floor_min(t))
        }
        Some(l) => (l, l, floor_min(l)),
    };
    let start_minute = floor_min(start_ts);
    let mut end_minute = floor_min(now_ms);
    if end_minute < record_start_minute {
        end_minute = record_start_minute;
    }
    let current_position_symbols: BTreeSet<String> = positions
        .iter()
        .filter(|p| !is_flat(&p.symbol, p.size))
        .map(|p| p.symbol.clone())
        .collect();
    let mut symbols: BTreeSet<String> = events
        .iter()
        .filter(|e| {
            lookback_start.is_none()
                || e.timestamp_ms >= record_start_ts
                || current_position_symbols.contains(&e.symbol)
        })
        .map(|e| e.symbol.clone())
        .collect();
    symbols.extend(current_position_symbols.iter().cloned());
    let price_replay_symbols: BTreeSet<String> = symbols
        .iter()
        .filter(|s| known_market(s) || current_position_symbols.contains(*s))
        .cloned()
        .collect();

    #[derive(Default, Clone, Copy)]
    struct Slot {
        size: f64,
        price: f64,
    }
    let mut slots: BTreeMap<String, [Slot; 2]> = BTreeMap::new();
    let mut active: BTreeSet<String> = BTreeSet::new();
    let mut realized_running = [0.0f64; 2];
    let mut balance = baseline_balance;
    let apply = |e: &HslFill,
                 slots: &mut BTreeMap<String, [Slot; 2]>,
                 active: &mut BTreeSet<String>,
                 balance: &mut f64,
                 realized_running: &mut [f64; 2]| {
        let sides = slots.entry(e.symbol.clone()).or_default();
        let slot = &mut sides[e.pside];
        if e.increase {
            let old_size = slot.size;
            let new_size = old_size + e.qty;
            if new_size <= 0.0 {
                slot.size = 0.0;
                slot.price = 0.0;
            } else if old_size <= 0.0 {
                slot.size = new_size;
                slot.price = e.price;
            } else {
                slot.price = ((old_size * slot.price + e.qty * e.price) / new_size).max(0.0);
                slot.size = new_size;
            }
        } else {
            slot.size = (slot.size - e.qty).max(0.0);
            if slot.size <= 0.0 {
                slot.price = 0.0;
            }
        }
        let has_pos = !is_flat(&e.symbol, sides[e.pside].size);
        if has_pos {
            active.insert(e.symbol.clone());
        } else if sides.iter().all(|s| is_flat(&e.symbol, s.size)) {
            active.remove(&e.symbol);
        }
        let realized_delta = e.pnl + e.fee_paid;
        *balance += realized_delta;
        realized_running[e.pside] += realized_delta;
    };

    let mut idx = 0usize;
    while idx < events.len() && events[idx].timestamp_ms < record_start_ts {
        apply(
            events[idx],
            &mut slots,
            &mut active,
            &mut balance,
            &mut realized_running,
        );
        idx += 1;
    }
    let record_start_balance = balance;
    let record_start_realized = realized_running;

    let mut last_price: BTreeMap<String, f64> = BTreeMap::new();
    let mut timeline = Vec::new();
    let mut minute = start_minute;
    while minute <= end_minute {
        let boundary = minute + ONE_MIN_MS;
        while idx < events.len() && events[idx].timestamp_ms < boundary {
            apply(
                events[idx],
                &mut slots,
                &mut active,
                &mut balance,
                &mut realized_running,
            );
            idx += 1;
        }
        let mut upnl_by_pside = [0.0f64; 2];
        for symbol in active.iter() {
            if !price_replay_symbols.contains(symbol) {
                continue;
            }
            // `CandlestickManager.get_candles` only serves finalized minutes
            // (`latest_finalized = floor(now) - 1m`): the row of the current
            // minute carries the previous close forward.
            let close = if minute >= floor_min(now_ms) {
                None
            } else {
                close_at(symbol, minute)
            };
            // The manager stores candles as float32 (`CandlestickManager`
            // dtype): the replay sees the close rounded to f32.
            let price = match close.map(|p| (p as f32) as f64) {
                Some(p) if p > 0.0 => {
                    last_price.insert(symbol.clone(), p);
                    Some(p)
                }
                _ => last_price.get(symbol).copied(),
            };
            let Some(price) = price else { continue };
            if price <= 0.0 {
                continue;
            }
            let Some(sides) = slots.get(symbol) else {
                continue;
            };
            for pside in [LONG, SHORT] {
                let slot = sides[pside];
                if slot.size <= 0.0 || slot.price <= 0.0 {
                    continue;
                }
                upnl_by_pside[pside] +=
                    hsl_pnl(pside, slot.price, price, slot.size, c_mult(symbol));
            }
        }
        if minute >= record_start_minute {
            let flat_side = |pside: usize| {
                !slots
                    .iter()
                    .any(|(sym, sides)| !is_flat(sym, sides[pside].size))
            };
            timeline.push(TimelineRow {
                timestamp: minute,
                balance,
                realized_pnl: balance - record_start_balance,
                unrealized_pnl: upnl_by_pside,
                realized_pnl_by_pside: [
                    realized_running[LONG] - record_start_realized[LONG],
                    realized_running[SHORT] - record_start_realized[SHORT],
                ],
                is_flat: active.is_empty(),
                is_flat_by_pside: [flat_side(LONG), flat_side(SHORT)],
            });
        }
        minute += ONE_MIN_MS;
    }
    if timeline.is_empty() {
        timeline.push(single_point(balance_now));
    }
    timeline
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn config(edit: impl FnOnce(&mut Value)) -> HslConfig {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/configs/fake_v8/grid_v7.json");
        let mut v: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        v["live"]["hsl_signal_mode"] = Value::from("unified");
        v["bot"]["long"]["hsl"]["enabled"] = Value::Bool(true);
        v["bot"]["long"]["hsl"]["red_threshold"] = Value::from(0.2);
        v["bot"]["long"]["hsl"]["ema_span_minutes"] = Value::from(1.0);
        v["bot"]["long"]["hsl"]["cooldown_minutes_after_red"] = Value::from(5.0);
        v["bot"]["long"]["hsl"]["no_restart_drawdown_threshold"] = Value::from(0.9);
        v["bot"]["long"]["hsl"]["orange_tier_mode"] =
            Value::from("tp_only_with_active_entry_cancellation");
        v["live"]["pnls_max_lookback_days"] = Value::from(30.0);
        edit(&mut v);
        HslConfig::from_config(&ConfigView::new(v).unwrap()).unwrap()
    }

    fn state(edit: impl FnOnce(&mut Value)) -> HslState {
        HslState::new(config(edit))
    }

    fn sample(
        st: &mut HslState,
        ts: u64,
        balance: f64,
        realized: f64,
        upnl: f64,
        latch: bool,
    ) -> Metrics {
        st.apply_sample(LONG, ts, balance, realized, 0.0, 0.0, Some(upnl), latch)
            .unwrap()
    }

    fn approx(a: f64, b: f64) -> bool {
        (a - b).abs() <= 1e-12 * a.abs().max(b.abs()).max(1.0)
    }

    /// `_normalize_fee_paid_from_payload`: reported fees are signed
    /// cashflows, zero/missing fees use the fallback percentage, outliers
    /// beyond the sanity ratio are replaced by the fallback.
    #[test]
    fn fee_policy_signs_falls_back_and_sanity_replaces() {
        let fee = FeePolicy::default();
        assert_eq!(fee.signed_fee_paid(0.05, 100.0), -0.05);
        assert_eq!(fee.signed_fee_paid(-0.02, 100.0), 0.02);
        assert!(approx(fee.signed_fee_paid(0.0, 849.6985), -0.1699397));
        assert_eq!(fee.signed_fee_paid(0.0, 0.0), 0.0);
        // 0.5 / 100 = 0.5 % > 0.1 % sanity max -> fallback
        assert!(approx(fee.signed_fee_paid(0.5, 100.0), -0.02));
        let zero = FeePolicy {
            fallback_pct: 0.0,
            sanity_abs_max: 0.0,
        };
        assert_eq!(zero.signed_fee_paid(0.0, 100.0), 0.0);
        assert_eq!(zero.signed_fee_paid(0.5, 100.0), -0.5);
        let cfg = config(|v| {
            v["live"]["fee_pct_fallback"] = Value::from(0.0005);
            v["live"]["fee_pct_sanity_abs_max"] = Value::from(0.01);
        });
        assert_eq!(cfg.fee.fallback_pct, 0.0005);
        assert_eq!(cfg.fee.sanity_abs_max, 0.01);
        assert_eq!(config(|_| {}).fee, FeePolicy::default());
    }

    /// `test_unified_metrics_pin_drawdown_and_tier_ladder`.
    #[test]
    fn unified_metrics_pin_drawdown_and_tier_ladder() {
        let mut st = state(|_| {});
        let m = sample(&mut st, 60_000, 100.0, 0.0, 0.0, false);
        assert!(approx(m.baseline_balance, 100.0));
        assert!(approx(m.strategy_equity, 100.0));
        assert_eq!(m.tier, Tier::Green);
        let m = sample(&mut st, 120_000, 100.0, 0.0, 10.0, false);
        assert!(approx(m.strategy_equity, 110.0));
        assert_eq!(m.tier, Tier::Green);
        let m = sample(&mut st, 180_000, 100.0, 0.0, -6.0, false);
        assert!(approx(m.strategy_equity, 94.0));
        assert!(approx(m.drawdown_raw, 16.0 / 110.0));
        assert!(approx(m.drawdown_ema, m.drawdown_raw));
        assert_eq!(m.tier, Tier::Yellow);
        // Realized losses move balance but keep the baseline anchored.
        let m = sample(&mut st, 240_000, 92.0, -8.0, 0.0, false);
        assert!(approx(m.baseline_balance, 100.0));
        assert!(approx(m.strategy_equity, 92.0));
        assert!(approx(m.drawdown_raw, 18.0 / 110.0));
        assert_eq!(m.tier, Tier::Orange);
        assert_eq!(
            st.modes().sides[LONG],
            HslSideMode::Orange("tp_only_with_active_entry_cancellation".into())
        );
        let m = sample(&mut st, 300_000, 92.0, -8.0, -14.0, false);
        assert!(approx(m.strategy_equity, 78.0));
        assert!(approx(m.drawdown_raw, 32.0 / 110.0));
        assert_eq!(m.tier, Tier::Red);
        // latch_red = false: red is reported, not latched.
        assert!(!st.sides[LONG].runtime.red_latched());
        assert_eq!(st.modes().sides[LONG], HslSideMode::None);
    }

    /// `test_pside_metrics_use_scoped_signal_and_ignore_unified_upnl` (subset).
    #[test]
    fn pside_mode_uses_scoped_signal_with_total_baseline() {
        let mut st = state(|v| v["live"]["hsl_signal_mode"] = Value::from("pside"));
        let m = st
            .apply_sample(LONG, 60_000, 100.0, 0.0, 0.0, 0.0, None, false)
            .unwrap();
        assert!(approx(m.strategy_equity, 100.0));
        // realized total -8 (short lost), long side realized +2, long upnl -3:
        // baseline = 92 + 8 = 100; equity = 100 + (2 - 3) = 99.
        let m = st
            .apply_sample(LONG, 120_000, 92.0, -8.0, 2.0, -3.0, None, false)
            .unwrap();
        assert!(approx(m.baseline_balance, 100.0));
        assert!(approx(m.strategy_equity, 99.0));
        assert!(approx(m.realized_pnl, 2.0));
        assert!(approx(m.unrealized_pnl, -3.0));
        // Unified mode requires the total.
        let mut u = state(|_| {});
        assert!(u
            .apply_sample(LONG, 60_000, 100.0, 0.0, 0.0, 0.0, None, false)
            .is_err());
    }

    /// `test_red_latching_holds_tier_after_recovery`.
    #[test]
    fn red_latching_holds_tier_after_recovery() {
        let mut st = state(|_| {});
        sample(&mut st, 60_000, 100.0, 0.0, 10.0, true);
        let m = sample(&mut st, 120_000, 100.0, 0.0, -15.0, true);
        assert_eq!(m.tier, Tier::Red);
        assert!(m.red_active_now && m.red_seen_in_episode);
        assert!(st.sides[LONG].runtime.red_latched());
        let m = sample(&mut st, 180_000, 100.0, 0.0, 12.0, true);
        assert!(approx(m.drawdown_raw, 0.0));
        assert_eq!(m.tier, Tier::Red);
        assert!(!m.red_active_now && m.red_seen_in_episode);
        assert_eq!(st.modes().sides[LONG], HslSideMode::Panic);
    }

    /// `test_red_tier_score_is_min_of_raw_and_ema`.
    #[test]
    fn red_tier_score_is_min_of_raw_and_ema() {
        let mut st = state(|v| v["bot"]["long"]["hsl"]["ema_span_minutes"] = Value::from(60.0));
        sample(&mut st, 60_000, 100.0, 0.0, 10.0, false);
        let m = sample(&mut st, 120_000, 100.0, 0.0, -20.0, false);
        assert!(m.drawdown_raw > m.red_threshold);
        assert!(m.drawdown_ema < m.red_threshold);
        assert!(approx(m.drawdown_score, m.drawdown_raw.min(m.drawdown_ema)));
        assert_ne!(m.tier, Tier::Red);
    }

    /// `test_hard_stop_apply_sample_same_minute_returns_cached_metrics_when_inputs_match`
    /// and `..._recomputes_when_inputs_change`.
    #[test]
    fn same_minute_cache_and_recompute() {
        let mut st = state(|_| {});
        let first = sample(&mut st, 60_000, 100.0, 0.0, 0.0, true);
        let second = sample(&mut st, 60_500, 100.0, 0.0, 0.0, true);
        assert_eq!(second.timestamp_ms, 60_000);
        assert!(!second.changed && second.elapsed_minutes == 0);
        assert_eq!(second.peak_strategy_equity, first.peak_strategy_equity);
        let third = sample(&mut st, 60_900, 90.0, 0.0, 0.0, true);
        assert_eq!(third.timestamp_ms, 60_900);
        assert!(approx(third.strategy_equity, 90.0));
        // Same minute: the engine keeps the cached step (drawdown 0), only
        // the metrics envelope changes.
        assert_eq!(third.drawdown_raw, first.drawdown_raw);
    }

    /// `test_hard_stop_apply_sample_rolling_peak_prunes_by_lookback`.
    #[test]
    fn rolling_peak_prunes_by_lookback() {
        let mut st = state(|v| v["live"]["pnls_max_lookback_days"] = Value::from(0.0));
        let m0 = sample(&mut st, 1_000, 100.0, 0.0, 0.0, true);
        let m1 = sample(&mut st, 61_000, 95.0, 0.0, 0.0, true);
        assert!(approx(m0.peak_strategy_equity, 100.0));
        assert!(approx(m1.peak_strategy_equity, 95.0));
        assert!(approx(m1.rolling_peak_strategy_equity, 95.0));
        assert!(approx(m1.drawdown_raw, 0.0));
    }

    fn inputs<'a>(
        now: u64,
        balance: f64,
        upnl: f64,
        positions: &'a [HslPosition],
        fills: &'a [HslFill],
    ) -> CycleInputs<'a> {
        CycleInputs {
            now_ms: now,
            balance,
            realized_pnl_total: 0.0,
            realized_pnl: [0.0, 0.0],
            unrealized_pnl: [upnl, 0.0],
            positions,
            fills,
        }
    }

    fn pos(symbol: &str, size: f64) -> HslPosition {
        HslPosition {
            symbol: symbol.into(),
            pside: LONG,
            size,
            price: 1.0,
        }
    }

    fn fill(ts: u64, symbol: &str, qty: f64, increase: bool) -> HslFill {
        HslFill {
            timestamp_ms: ts,
            symbol: symbol.into(),
            pside: LONG,
            qty,
            price: 1.0,
            increase,
            pnl: 0.0,
            fee_paid: 0.0,
        }
    }

    /// Red -> panic supervision -> two flat confirmations -> halted with a
    /// cooldown -> cooldown elapsed -> resumed (`test_hard_stop_finalize_red_stop_autorestarts_after_cooldown`,
    /// `test_hard_stop_check_defers_stop_event_until_flat_confirmation`).
    #[test]
    fn red_episode_finalizes_after_two_flat_confirmations_and_resumes() {
        let mut st = state(|_| {});
        let held = [pos("A/USDT:USDT", 5.0)];
        let flat: [HslPosition; 0] = [];
        st.check(&inputs(60_000, 100.0, 10.0, &held, &[])).unwrap();
        st.check(&inputs(120_000, 100.0, -30.0, &held, &[]))
            .unwrap();
        assert!(st.red_active(LONG));
        assert_eq!(st.sides[LONG].pending_red_since_ms, Some(120_000));
        assert_eq!(st.modes().sides[LONG], HslSideMode::Panic);
        // Non-flat: panic execution due.
        let obs = RedObservation {
            n_positions: 1,
            ..Default::default()
        };
        let step = st
            .supervise_red(
                LONG,
                obs,
                &inputs(120_000, 100.0, -30.0, &held, &[]),
                Supervision::Production,
            )
            .unwrap();
        assert!(step.needs_panic_execution && !step.finalized);
        // Flat but the flattening fill is not visible yet: deferred.
        let flat_obs = RedObservation::default();
        let step = st
            .supervise_red(
                LONG,
                flat_obs,
                &inputs(180_000, 70.0, 0.0, &flat, &[]),
                Supervision::Production,
            )
            .unwrap();
        assert!(!step.finalized && !step.needs_panic_execution);
        assert_eq!(st.sides[LONG].red_flat_confirmations, 0);
        // The fill arrives: two confirmations finalize at the fill timestamp.
        let fills = [fill(150_000, "A/USDT:USDT", 5.0, false)];
        for _ in 0..2 {
            st.supervise_red(
                LONG,
                flat_obs,
                &inputs(180_000, 70.0, 0.0, &flat, &fills),
                Supervision::Production,
            )
            .unwrap();
        }
        let s = &st.sides[LONG];
        assert!(s.halted && !s.no_restart_latched);
        assert_eq!(s.cooldown_until_ms, Some(150_000 + 5 * ONE_MIN_MS));
        assert_eq!(
            s.last_stop_event.as_ref().unwrap().stop_event_timestamp_ms,
            150_000
        );
        assert_eq!(
            st.modes().sides[LONG],
            HslSideMode::Halted {
                policy: CooldownPositionPolicy::Panic,
                unresolved_residue: false
            }
        );
        assert_eq!(
            st.modes().sides[LONG].symbol_override(false).as_deref(),
            Some("graceful_stop")
        );
        assert_eq!(
            st.modes().sides[LONG].side_forced_mode(),
            Some("graceful_stop")
        );
        // Still cooling down: no sample is taken.
        st.check(&inputs(300_000, 70.0, 0.0, &flat, &fills))
            .unwrap();
        assert!(st.sides[LONG].halted);
        // Cooldown elapsed: reset and a fresh green sample.
        st.check(&inputs(450_000, 70.0, 0.0, &flat, &fills))
            .unwrap();
        let s = &st.sides[LONG];
        assert!(!s.halted && !s.runtime.red_latched());
        assert_eq!(s.last_metrics.as_ref().unwrap().tier, Tier::Green);
        assert_eq!(st.modes().sides[LONG], HslSideMode::None);
    }

    /// `test_hard_stop_finalize_red_stop_terminal_latches_and_stops` /
    /// `..._equal_threshold_latches_terminal`: no-restart at the threshold.
    #[test]
    fn terminal_no_restart_at_threshold_and_never_policy() {
        let mut st = state(|v| {
            v["bot"]["long"]["hsl"]["no_restart_drawdown_threshold"] = Value::from(0.25);
        });
        let flat: [HslPosition; 0] = [];
        sample(&mut st, 60_000, 100.0, 0.0, 0.0, true);
        sample(&mut st, 120_000, 100.0, 0.0, -30.0, true);
        // Flattened: the loss is realized, the baseline stays at 100.
        let closed = CycleInputs {
            realized_pnl_total: -30.0,
            realized_pnl: [-30.0, 0.0],
            ..inputs(130_000, 70.0, 0.0, &flat, &[])
        };
        let ev = st.compute_stop_event(LONG, 130_000, &closed).unwrap();
        assert!(approx(ev.drawdown_raw, 0.3));
        assert!(approx(ev.peak_strategy_equity, 100.0));
        st.finalize_red_stop(LONG, &ev).unwrap();
        let s = &st.sides[LONG];
        assert!(s.halted && s.no_restart_latched && s.cooldown_until_ms.is_none());
        // Terminal halt never resets, whatever the clock says.
        st.check(&inputs(10_000_000, 70.0, 0.0, &flat, &[]))
            .unwrap();
        assert!(st.sides[LONG].halted);

        let mut never = state(|v| {
            v["bot"]["long"]["hsl"]["restart_after_red_policy"] = Value::from("never");
        });
        sample(&mut never, 60_000, 100.0, 0.0, 0.0, true);
        sample(&mut never, 120_000, 100.0, 0.0, -30.0, true);
        let ev = never.compute_stop_event(LONG, 130_000, &closed).unwrap();
        never.finalize_red_stop(LONG, &ev).unwrap();
        assert!(never.sides[LONG].no_restart_latched);
    }

    /// `test_hsl_cooldown_{tp_only,manual,graceful_stop,normal}_*` and
    /// `test_hsl_cooldown_normal_blocks_fresh_initials_while_flat`.
    #[test]
    fn cooldown_position_policies() {
        for (policy, expect, changed) in [
            ("tp_only", "tp_only", false),
            ("manual", "manual", false),
            ("graceful_stop", "graceful_stop", false),
            ("normal", "graceful_stop", true),
        ] {
            let mut st =
                state(|v| v["live"]["hsl_position_during_cooldown_policy"] = Value::from(policy));
            st.sides[LONG].halted = true;
            st.sides[LONG].cooldown_until_ms = Some(200_000);
            let held = [pos("A/USDT:USDT", 1.0)];
            let inp = inputs(150_000, 100.0, 0.0, &held, &[]);
            let modes_before = st.modes();
            assert_eq!(
                modes_before.sides[LONG].symbol_override(true).as_deref(),
                Some(expect),
                "{policy}"
            );
            assert_eq!(
                modes_before.sides[LONG].symbol_override(false).as_deref(),
                Some("graceful_stop")
            );
            let got = st.handle_position_during_cooldown(LONG, &inp).unwrap();
            assert_eq!(got, changed, "{policy}");
            if changed {
                assert!(!st.sides[LONG].halted && st.sides[LONG].cooldown_until_ms.is_none());
            } else {
                assert!(st.sides[LONG].cooldown_intervention_active);
                assert_eq!(st.sides[LONG].cooldown_until_ms, Some(200_000));
            }
        }
    }

    /// `test_hsl_cooldown_panic_refreshes_anchor`: a position during the
    /// cooldown under the panic policy re-panics and, once the replayed
    /// fills prove the scope flat, the cooldown restarts from that fill.
    #[test]
    fn cooldown_panic_policy_repanics_and_refreshes_anchor() {
        let mut st = state(|_| {});
        sample(&mut st, 60_000, 100.0, 0.0, 0.0, true);
        st.sides[LONG].halted = true;
        st.sides[LONG].cooldown_until_ms = Some(600_000);
        let held = [pos("A/USDT:USDT", 2.0)];
        let flat: [HslPosition; 0] = [];
        st.check(&inputs(150_000, 100.0, -1.0, &held, &[])).unwrap();
        let s = &st.sides[LONG];
        assert!(s.cooldown_repanic_reset_pending && s.cooldown_intervention_active);
        assert_eq!(s.cooldown_repanic_since_ms, Some(150_000));
        assert_eq!(
            st.modes().sides[LONG].symbol_override(true).as_deref(),
            Some("panic")
        );
        // A partial close does not flatten the scope; the full close does.
        let fills = [
            fill(160_000, "A/USDT:USDT", 1.0, false),
            fill(170_000, "A/USDT:USDT", 1.0, false),
        ];
        st.check(&inputs(180_000, 99.0, 0.0, &flat, &fills))
            .unwrap();
        let s = &st.sides[LONG];
        assert!(s.halted && !s.cooldown_repanic_reset_pending);
        assert_eq!(s.cooldown_until_ms, Some(170_000 + 5 * ONE_MIN_MS));
        assert_eq!(
            s.last_stop_event.as_ref().unwrap().stop_event_timestamp_ms,
            170_000
        );
    }

    /// `test_orange_mode_override_blocks_flat_initial_entries` and the
    /// graceful_stop variant; `test_hsl_halted_universe_...` side mode.
    #[test]
    fn orange_and_halted_modes() {
        let mut st =
            state(|v| v["bot"]["long"]["hsl"]["orange_tier_mode"] = Value::from("graceful_stop"));
        sample(&mut st, 60_000, 100.0, 0.0, 10.0, true);
        let m = sample(&mut st, 120_000, 100.0, 0.0, -8.0, true);
        assert_eq!(m.tier, Tier::Orange);
        let modes = st.modes();
        assert_eq!(
            modes.sides[LONG],
            HslSideMode::Orange("graceful_stop".into())
        );
        assert_eq!(
            modes.sides[LONG].symbol_override(false).as_deref(),
            Some("graceful_stop")
        );
        assert_eq!(modes.sides[LONG].side_forced_mode(), None);
        assert_eq!(modes.sides[SHORT], HslSideMode::None);
        let m = sample(&mut st, 180_000, 100.0, 0.0, -30.0, true);
        assert_eq!(m.tier, Tier::Red);
        assert_eq!(st.modes().sides[LONG].side_forced_mode(), Some("panic"));
    }

    #[test]
    fn coin_mode_with_hsl_enabled_is_refused_and_disabled_is_fine() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/configs/fake_v8/grid_v7.json");
        let v: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let cfg = HslConfig::from_config(&ConfigView::new(v.clone()).unwrap()).unwrap();
        assert_eq!(cfg.signal_mode, SignalMode::Coin);
        assert!(!cfg.any_enabled());
        let mut on = v;
        on["bot"]["long"]["hsl"]["enabled"] = Value::Bool(true);
        assert!(HslConfig::from_config(&ConfigView::new(on).unwrap()).is_err());
    }

    #[test]
    fn flatten_fill_replay_and_realized_pnl() {
        let fills = [
            HslFill {
                pnl: -2.0,
                fee_paid: -0.1,
                ..fill(100, "A/USDT:USDT", 1.0, false)
            },
            fill(200, "A/USDT:USDT", 1.0, true),
            fill(300, "A/USDT:USDT", 2.0, false),
        ];
        assert_eq!(
            latest_flatten_fill_timestamp(&fills, LONG, None, None),
            Some(300)
        );
        assert_eq!(
            latest_flatten_fill_timestamp(&fills, LONG, Some(400), None),
            None
        );
        let start: BTreeMap<String, f64> = [("A/USDT:USDT".to_string(), 1.0)].into();
        // 1 -> 0 at 100 already flattens the replayed scope.
        assert_eq!(
            latest_flatten_fill_timestamp(&fills, LONG, Some(0), Some(&start)),
            Some(100)
        );
        assert_eq!(
            latest_flatten_fill_timestamp(&fills, LONG, Some(150), Some(&start)),
            Some(300)
        );
        assert!(approx(realized_pnl_now(&fills, None, None), -2.1));
        assert!(approx(realized_pnl_now(&fills, Some(150), Some(LONG)), 0.0));
        assert_eq!(realized_pnl_now(&fills, None, Some(SHORT)), 0.0);
    }

    /// Timeline replay: rows every minute from the lookback start, balance
    /// moves by `pnl + fee`, unrealized pnl from candle closes with
    /// carry-forward, flat flags per scope.
    #[test]
    fn balance_equity_timeline_rows() {
        let lookback = PnlsLookback { days: Some(0.0) }; // window = one minute
        let now = 10 * ONE_MIN_MS + 30_000;
        let fills = [
            HslFill {
                price: 10.0,
                ..fill(8 * ONE_MIN_MS + 1000, "A/USDT:USDT", 2.0, true)
            },
            HslFill {
                price: 11.0,
                pnl: 2.0,
                fee_paid: -0.05,
                ..fill(9 * ONE_MIN_MS + 5000, "A/USDT:USDT", 2.0, false)
            },
        ];
        let close = |_: &str, m: u64| Some(if m == 8 * ONE_MIN_MS { 10.5 } else { 12.0 });
        let rows = balance_equity_timeline(
            now,
            101.95,
            lookback,
            &fills,
            &[],
            &close,
            &|_| 1.0,
            &|_| 0.001,
            &|_| true,
        );
        // Lookback start = now - 60 s = 9:30 -> record start minute 9; both
        // fills are pre-window events, so the window has rows 9 and 10 with
        // the realized pnl already in the start balance.
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].timestamp, 9 * ONE_MIN_MS);
        // Baseline = 101.95 - (2 - 0.05) = 100; balance after both fills 101.95.
        assert!(approx(rows[0].balance, 101.95));
        assert!(approx(rows[0].realized_pnl, 0.0));
        assert!(rows[0].is_flat && rows[0].is_flat_by_pside[LONG]);
        assert!(approx(rows[1].unrealized_pnl[LONG], 0.0));
        // Without a lookback the replay starts at the first fill: minute 8
        // holds 2 @ 10 with close 10.5 -> upnl 1.0, not flat.
        let rows = balance_equity_timeline(
            now,
            101.95,
            PnlsLookback { days: None },
            &fills,
            &[],
            &close,
            &|_| 1.0,
            &|_| 0.001,
            &|_| true,
        );
        assert_eq!(rows.len(), 3);
        assert!(approx(rows[0].balance, 100.0));
        assert!(approx(rows[0].unrealized_pnl[LONG], 1.0));
        assert!(!rows[0].is_flat);
        assert!(rows[1].is_flat);
    }

    /// `test_pside_replay_red_episode_ordinary_flatten_latches_cooldown` and
    /// `test_pside_replay_red_free_flatten_resets_without_stop`: the start-up
    /// replay finalizes a red-seen episode ended by an ordinary fill and
    /// resets a red-free one.
    #[test]
    fn initialize_from_history_finalizes_red_episode_and_resets_red_free() {
        let row = |minute: u64, balance: f64, upnl: f64, flat: bool| TimelineRow {
            timestamp: minute * ONE_MIN_MS,
            balance,
            realized_pnl: balance - 100.0,
            unrealized_pnl: [upnl, 0.0],
            realized_pnl_by_pside: [balance - 100.0, 0.0],
            is_flat: flat,
            is_flat_by_pside: [flat, true],
        };
        let fills = [fill(3 * ONE_MIN_MS + 10_000, "A/USDT:USDT", 1.0, false)];
        // Peak 100 -> equity 70 (red seen) -> flat by an ordinary close at
        // minute 3 (fill at 3:10) -> cooldown from that fill.
        let timeline = [
            row(1, 100.0, 0.0, true),
            row(2, 100.0, -30.0, false),
            row(3, 70.0, 0.0, true),
            row(4, 70.0, 0.0, true),
        ];
        let mut st = state(|_| {});
        st.initialize_from_history(
            4 * ONE_MIN_MS + 30_000,
            70.0,
            &fills,
            &timeline,
            -30.0,
            [-30.0, 0.0],
            [0.0, 0.0],
        )
        .unwrap();
        let s = &st.sides[LONG];
        assert!(s.halted && !s.no_restart_latched);
        assert_eq!(
            s.cooldown_until_ms,
            Some(3 * ONE_MIN_MS + 10_000 + 5 * ONE_MIN_MS)
        );
        assert_eq!(
            st.modes().sides[LONG].side_forced_mode(),
            Some("graceful_stop")
        );
        // With the cooldown already elapsed at start-up the side resumes and
        // samples the present.
        let mut st = state(|_| {});
        st.initialize_from_history(
            20 * ONE_MIN_MS,
            70.0,
            &fills,
            &timeline,
            -30.0,
            [-30.0, 0.0],
            [0.0, 0.0],
        )
        .unwrap();
        let s = &st.sides[LONG];
        assert!(!s.halted && s.runtime.initialized());
        assert_eq!(
            s.last_metrics.as_ref().unwrap().timestamp_ms,
            20 * ONE_MIN_MS
        );
        // A red-free episode that flattened resets the tracker: the later
        // sample (realized -5, unrealized -5 -> equity 90) starts a fresh
        // peak at 90 instead of measuring a 10 % drawdown from 100.
        let timeline = [
            row(1, 100.0, 0.0, true),
            row(2, 100.0, -5.0, false),
            row(3, 95.0, 0.0, true),
            row(4, 95.0, -5.0, false),
        ];
        let mut st = state(|_| {});
        st.initialize_from_history(
            4 * ONE_MIN_MS + 30_000,
            95.0,
            &[],
            &timeline,
            -5.0,
            [-5.0, 0.0],
            [-5.0, 0.0],
        )
        .unwrap();
        let m = st.sides[LONG].last_metrics.as_ref().unwrap();
        assert!(!st.sides[LONG].halted);
        assert!(approx(m.strategy_equity, 90.0));
        assert!(approx(m.peak_strategy_equity, 90.0));
        assert!(approx(m.drawdown_raw, 0.0));
    }
}
