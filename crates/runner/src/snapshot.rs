//! Orchestrator snapshot builder (docs/SNAPSHOT_SPEC.md, PLAN P4.2).
//!
//! Turns the runner's market/account state plus a [`ConfigView`] into the
//! JSON `OrchestratorInput` the Python bot would build for the same state.
//! The result is a `serde_json::Value` with Python's int/float typing so it
//! can be fed to the engine through the same text round trip (D8) and diffed
//! against recorded inputs (`pb-snapcheck`).
//!
//! Cross-cycle state the Python bot keeps privately and the builder needs is
//! carried in [`CycleState`] (SPEC section 8): the previous engine output's
//! `PB_modes`, the dynamic forager eligibility of the previous cycle, the
//! close-EMA carry-forward cache and the exchange-unavailable cooldown set.
//!
//! Not reproduced (documented in SPEC section 8): HSL-driven modes, operator
//! runtime forced modes, the EMA-entry-cancellation order keys, the cached
//! forager-metric fallback and the forager stale-tail projection.

use crate::bot_params::{ConfigView, PSIDES};
use crate::emas::{
    self, open_tail_gap, open_tail_rows, projected_ema, Candle, GapPolicy, Metric, ONE_HOUR_MS,
    ONE_MIN_MS,
};
use anyhow::{anyhow, bail, Result};
use passivbot_rust::entries::calc_min_entry_qty;
use passivbot_rust::types::{ExchangeParams, TrailingPriceBundle};
use passivbot_rust::utils::qty_to_cost;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq)]
pub struct MarketParams {
    pub qty_step: f64,
    pub price_step: f64,
    pub min_qty: f64,
    pub min_cost: f64,
    pub c_mult: f64,
    pub maker_fee: f64,
    pub taker_fee: f64,
}

impl MarketParams {
    fn exchange_params(&self) -> ExchangeParams {
        ExchangeParams {
            qty_step: self.qty_step,
            price_step: self.price_step,
            min_qty: self.min_qty,
            min_cost: self.min_cost,
            c_mult: self.c_mult,
            maker_fee: self.maker_fee,
            taker_fee: self.taker_fee,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SideState {
    pub position_size: f64,
    pub position_price: f64,
    pub trailing: TrailingPriceBundle,
    pub trailing_available: bool,
    pub last_increase_fill_ts: Option<u64>,
    /// A resting non-reduce-only entry order placed by the bot exists.
    pub has_entry_order: bool,
    /// Any open order on this side exists.
    pub has_open_order: bool,
}

impl Default for SideState {
    fn default() -> Self {
        Self {
            position_size: 0.0,
            position_price: 0.0,
            trailing: TrailingPriceBundle::default(),
            trailing_available: true,
            last_increase_fill_ts: None,
            has_entry_order: false,
            has_open_order: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SymbolState {
    pub symbol: String,
    pub market: MarketParams,
    /// ccxt `markets[symbol]["active"]`.
    pub active: bool,
    pub bid: f64,
    pub ask: f64,
    /// Price used for `effective_min_cost` (the bot's 600 s-TTL cached last price).
    pub min_cost_price: f64,
    /// Ascending 1m candles `[ts, o, h, l, c, v]`; the current (open) minute may be included.
    pub candles_1m: Vec<Candle>,
    /// Hourly candles from the exchange (the Python bot fetches `1h` klines
    /// for hourly windows); `None` aggregates them from `candles_1m`.
    pub candles_1h: Option<Vec<Candle>>,
    /// False when the candle manager never fetched this symbol (forager
    /// secondary symbol without a warm cache -> unavailable, SPEC 3.7).
    pub candles_available: bool,
    pub long: SideState,
    pub short: SideState,
}

impl SymbolState {
    fn side(&self, pside: &str) -> &SideState {
        if pside == "long" {
            &self.long
        } else {
            &self.short
        }
    }
    fn has_position(&self) -> bool {
        self.long.position_size != 0.0 || self.short.position_size != 0.0
    }
    fn has_open_order(&self) -> bool {
        self.long.has_open_order || self.short.has_open_order
    }
}

#[derive(Debug, Clone)]
pub struct AccountState {
    pub timestamp_ms: u64,
    pub balance: f64,
    pub balance_raw: f64,
    pub realized_pnl_cumsum_max: f64,
    pub realized_pnl_cumsum_last: f64,
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub input: Value,
    /// `symbol_idx -> symbol`.
    pub symbols: Vec<String>,
    /// Final `mode_overrides` per `symbol_idx` (long, short), after the
    /// exchange-cooldown policy: what Python hands to
    /// `_apply_orchestrator_symbol_states` and the EMA bundle.
    pub mode_overrides: Vec<(Option<String>, Option<String>)>,
}

/// State the Python bot keeps across planning cycles and the builder reads
/// (SPEC section 8, item 1). `build` updates the carried parts in place;
/// the caller sets `pb_modes` from the engine output ([`SnapshotBuilder::pb_modes_after_cycle`])
/// and `exchange_unavailable` from its cooldown state (`cooldown.rs`).
#[derive(Debug, Default, Clone)]
pub struct CycleState {
    /// `_orchestrator_exchange_unavailable_symbols`: symbols under an
    /// exchange-unavailable cooldown this cycle (SPEC 2.2/2.3).
    pub exchange_unavailable: BTreeSet<String>,
    /// `PB_modes[pside][symbol]` from the previous cycle (SPEC 1.6, 3.6).
    pub pb_modes: BTreeMap<(String, String), String>,
    /// `_orchestrator_dynamic_forager_eligibility_psides_by_symbol` of the
    /// previous cycle (SPEC 3.6); rewritten by `build`.
    pub dynamic_forager_eligibility: BTreeMap<String, BTreeSet<String>>,
    /// `_orchestrator_prev_close_ema[symbol][span] = (value, read_ts_ms)`
    /// (SPEC 3.3 carry-forward); spans keyed by bit pattern.
    pub prev_close_ema: BTreeMap<String, BTreeMap<u64, (f64, u64)>>,
}

type Modes = BTreeMap<&'static str, Option<String>>;

const STOP_MODES: [&str; 5] = [
    "panic",
    "graceful_stop",
    "tp_only",
    "tp_only_with_active_entry_cancellation",
    "manual",
];

/// `config/overrides.py::expand_PB_mode`.
fn expand_pb_mode(raw: &str) -> Result<Option<String>> {
    let m = raw.trim().to_ascii_lowercase();
    Ok(Some(
        match m.as_str() {
            "" => return Ok(None),
            "gs" | "graceful_stop" | "graceful-stop" => "graceful_stop",
            "m" | "manual" => "manual",
            "n" | "normal" => "normal",
            "p" | "panic" => "panic",
            "t" | "tp" | "tp_only" | "tp-only" => "tp_only",
            other => bail!("unknown forced mode {other:?}"),
        }
        .to_string(),
    ))
}

/// `_mode_override_to_orchestrator_mode`.
fn orchestrator_mode(mode: Option<&str>) -> Value {
    match mode {
        None => Value::Null,
        Some("tp_only_with_active_entry_cancellation") => Value::from("tp_only"),
        Some(m) if ["normal", "panic", "graceful_stop", "tp_only", "manual"].contains(&m) => {
            Value::from(m)
        }
        Some(_) => Value::from("manual"),
    }
}

fn positive_finite(v: Option<&Value>) -> Result<f64> {
    let Some(v) = v else { return Ok(0.0) };
    let x = match v {
        Value::Number(n) => n.as_f64().unwrap_or(f64::NAN),
        Value::Bool(b) => f64::from(*b as u8),
        Value::String(s) => s.trim().parse().unwrap_or(f64::NAN),
        _ => bail!("warmup value {v} is not numeric"),
    };
    if !x.is_finite() {
        bail!("warmup value {v} is not finite");
    }
    Ok(if x > 0.0 { x } else { 0.0 })
}

fn path_get<'a>(v: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut cur = v;
    for p in path {
        cur = cur.as_object()?.get(*p)?;
    }
    Some(cur)
}

/// `strategy_warmup_value`: max positive value over the probe paths.
fn warmup_max(sp: &Value, paths: &[&[&str]]) -> Result<f64> {
    let mut m = 0.0f64;
    for p in paths {
        m = m.max(positive_finite(path_get(sp, p))?);
    }
    Ok(m)
}

/// `strategy_abs_max_weight`.
fn abs_max_weight(sp: &Value, paths: &[&[&str]]) -> Result<f64> {
    let mut m = 0.0f64;
    for p in paths {
        if let Some(v) = path_get(sp, p) {
            let x = v
                .as_f64()
                .ok_or_else(|| anyhow!("weight {v} not numeric"))?;
            if !x.is_finite() {
                bail!("weight {v} not finite");
            }
            m = m.max(x.abs());
        }
    }
    Ok(m)
}

const M1_LR_SPAN_PATHS: &[&[&str]] = &[
    &["volatility_ema_span_1m"],
    &["entry_volatility_ema_span_1m"],
    &["offset_volatility_ema_span_1m"],
];
const H1_SPAN_PATHS: &[&[&str]] = &[
    &["volatility_ema_span_1h"],
    &["offset_volatility_ema_span_1h"],
    &["entry_volatility_ema_span_1h"],
    &["entry", "volatility_ema_span_hours"],
];
const M1_ENTRY_WEIGHTS: &[&[&str]] = &[
    &["entry", "threshold_volatility_1m_weight"],
    &["entry", "retracement_volatility_1m_weight"],
    &["entry_weight_volatility_1m"],
    &["offset_volatility_1m_weight"],
];
const M1_CLOSE_WEIGHTS: &[&[&str]] = &[
    &["close", "threshold_volatility_1m_weight"],
    &["close", "retracement_volatility_1m_weight"],
    &["close_weight_volatility_1m"],
    &["offset_volatility_1m_weight"],
];
const H1_ENTRY_WEIGHTS: &[&[&str]] = &[
    &["entry", "threshold_volatility_1h_weight"],
    &["entry", "retracement_volatility_1h_weight"],
    &["entry", "grid_spacing_volatility_weight"],
    &["entry", "trailing_threshold_volatility_weight"],
    &["entry", "trailing_retracement_volatility_weight"],
    &["entry_weight_volatility_1h"],
    &["offset_volatility_1h_weight"],
];
const H1_CLOSE_WEIGHTS: &[&[&str]] = &[
    &["close", "threshold_volatility_1h_weight"],
    &["close", "retracement_volatility_1h_weight"],
    &["close_weight_volatility_1h"],
    &["offset_volatility_1h_weight"],
];

/// Spans one symbol needs (SPEC 3.1), unioned over both sides.
#[derive(Debug, Default, Clone)]
struct SymbolSpans {
    close: BTreeSet<u64>,
    m1_lr_required: BTreeSet<u64>,
    h1_lr: BTreeSet<u64>,
}

/// f64 spans are stored by bit pattern so sets stay exact (`sqrt` spans are irrational).
fn key(x: f64) -> u64 {
    x.to_bits()
}
fn unkey(k: u64) -> f64 {
    f64::from_bits(k)
}

pub struct SnapshotBuilder<'a> {
    cfg: &'a ConfigView,
    approved: BTreeMap<&'static str, BTreeSet<String>>,
    auto_gs: bool,
    forced_mode: BTreeMap<&'static str, Option<String>>,
    /// `_close_ema_fallback_max_age_ms()`.
    close_ema_fallback_max_age_ms: u64,
    /// `_active_candle_tail_gap_max_ms()`.
    tail_gap_max_ms: u64,
}

/// `live.<key>` as Python's `float(value)` with the "10 minutes" fallback
/// used by the candle-freshness helpers: non-numeric, non-finite or `<= 0`
/// values fall back to `default`.
fn live_minutes(cfg: &ConfigView, key: &str, default: f64) -> f64 {
    let m = match cfg.live(key) {
        Some(Value::Number(n)) => n.as_f64().unwrap_or(f64::NAN),
        Some(Value::Bool(b)) => f64::from(*b as u8),
        Some(Value::String(s)) => s.trim().parse().unwrap_or(f64::NAN),
        Some(_) => f64::NAN,
        None => default,
    };
    if !m.is_finite() || m <= 0.0 {
        default
    } else {
        m
    }
}

/// `_close_ema_fallback_max_age_ms` (pb:20250): `max_forager_candle_staleness_minutes`,
/// else `inactive_coin_candle_ttl_minutes` (default 10), floor 60 s.
pub fn close_ema_fallback_max_age_ms(cfg: &ConfigView) -> u64 {
    let default_minutes = match cfg.live("inactive_coin_candle_ttl_minutes") {
        None => 10.0,
        Some(v) => v.as_f64().unwrap_or(10.0),
    };
    let minutes = match cfg.live("max_forager_candle_staleness_minutes") {
        // key present (even `null`): Python's `float(raw)` path, 10 on failure
        Some(_) => live_minutes(cfg, "max_forager_candle_staleness_minutes", 10.0),
        None => {
            if default_minutes.is_finite() && default_minutes > 0.0 {
                default_minutes
            } else {
                10.0
            }
        }
    };
    ((minutes * 60_000.0) as u64).max(60_000)
}

/// `_active_candle_tail_gap_max_ms` (pb:11749): `max_active_candle_tail_gap_minutes`
/// (default 10), floor 60 s.
pub fn active_candle_tail_gap_max_ms(cfg: &ConfigView) -> u64 {
    let minutes = live_minutes(cfg, "max_active_candle_tail_gap_minutes", 10.0);
    ((minutes * 60_000.0) as u64).max(60_000)
}

/// `_apply_exchange_symbol_unavailable_planning_policy` (pb:10113) for one
/// side of a cooled-down symbol: entry-capable modes become entry-blocking.
pub fn cooldown_mode(mode: Option<&str>, has_position: bool) -> Option<String> {
    let normalized = orchestrator_mode(mode);
    let replace = matches!(&normalized, Value::Null)
        || normalized == "normal"
        || (has_position && normalized == "graceful_stop");
    if replace {
        Some(
            if has_position {
                "tp_only"
            } else {
                "graceful_stop"
            }
            .to_string(),
        )
    } else {
        mode.map(str::to_string)
    }
}

/// `is_bot_managed_entry_override` (pb:17578): modes the bot itself may have
/// set on a forager-managed side (`graceful_stop`, `panic`,
/// `tp_only_with_active_entry_cancellation`).
fn is_bot_managed_entry_override(mode: &str) -> bool {
    let raw = mode.trim().to_ascii_lowercase();
    raw == "tp_only_with_active_entry_cancellation"
        || matches!(
            orchestrator_mode(Some(&raw)).as_str(),
            Some("graceful_stop") | Some("panic")
        )
}

impl<'a> SnapshotBuilder<'a> {
    pub fn new(cfg: &'a ConfigView) -> Result<Self> {
        let mut approved = BTreeMap::new();
        for pside in PSIDES {
            let mut set = BTreeSet::new();
            if cfg.is_pside_enabled(pside)? {
                let listed = coin_list(cfg.live("approved_coins"), pside);
                let ignored = coin_list(cfg.live("ignored_coins"), pside);
                for coin in listed {
                    if !ignored.contains(&coin) {
                        set.insert(format!("{coin}/USDT:USDT"));
                    }
                }
            }
            approved.insert(pside, set);
        }
        let auto_gs = cfg.live("auto_gs").map(truthy).unwrap_or(true);
        let mut forced_mode = BTreeMap::new();
        for pside in PSIDES {
            let raw = cfg
                .live(&format!("forced_mode_{pside}"))
                .and_then(Value::as_str)
                .unwrap_or("");
            forced_mode.insert(pside, expand_pb_mode(raw)?);
        }
        Ok(Self {
            cfg,
            approved,
            auto_gs,
            forced_mode,
            close_ema_fallback_max_age_ms: close_ema_fallback_max_age_ms(cfg),
            tail_gap_max_ms: active_candle_tail_gap_max_ms(cfg),
        })
    }

    /// `get_max_n_positions(pside)`: `max(0, round(min(n_positions, len(approved))))`.
    fn max_n_positions(&self, pside: &str) -> i64 {
        let n = self
            .cfg
            .bot_value(pside, "n_positions")
            .ok()
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let m = n.min(self.approved[pside].len() as f64);
        (m.round_ties_even() as i64).max(0)
    }

    /// `has_default_entry_capacity(pside)` (pb:18023).
    fn has_default_entry_capacity(&self, pside: &str) -> bool {
        self.max_n_positions(pside) > 0
    }

    /// `normal_planning_psides(symbol)` (pb:18031): sides whose payload may
    /// place entries this cycle. Explicit override first; else the previous
    /// cycle's `PB_modes`; else the default (capacity and side not blocked;
    /// every input symbol is in `active_symbols`).
    fn normal_planning_psides(
        &self,
        symbol: &str,
        modes: &Modes,
        cycle: &CycleState,
    ) -> BTreeSet<&'static str> {
        let mut out = BTreeSet::new();
        for pside in PSIDES {
            if let Some(explicit) = modes[pside].as_deref() {
                if orchestrator_mode(Some(explicit)) == "normal" {
                    out.insert(pside);
                }
                continue;
            }
            if let Some(pb) = cycle.pb_modes.get(&(symbol.to_string(), pside.to_string())) {
                if self.has_default_entry_capacity(pside) && orchestrator_mode(Some(pb)) == "normal"
                {
                    out.insert(pside);
                }
                continue;
            }
            if self.has_default_entry_capacity(pside) && !self.pside_blocks_new_entries(pside) {
                out.insert(pside);
            }
        }
        out
    }

    /// `dynamic_forager_normal_psides(symbol)` (pb:18106): forager sides that
    /// are normal now or were dynamically normal last cycle. The
    /// "previously authorised resting entry" branch (EMA-entry-cancellation
    /// order keys) is not modelled.
    fn dynamic_forager_normal_psides(
        &self,
        modes: &Modes,
        normal: &BTreeSet<&'static str>,
        prev_dynamic: Option<&BTreeSet<String>>,
    ) -> Result<BTreeSet<&'static str>> {
        let mut out = BTreeSet::new();
        for pside in PSIDES {
            let explicit = modes[pside].as_deref();
            let retained_previous = prev_dynamic.is_some_and(|p| p.contains(pside))
                && explicit.is_none_or(is_bot_managed_entry_override);
            if explicit.is_some() && !retained_previous {
                continue;
            }
            if self.has_default_entry_capacity(pside)
                && self.is_forager_mode(pside)?
                && (normal.contains(pside) || retained_previous)
            {
                out.insert(pside);
            }
        }
        Ok(out)
    }

    /// `dynamic_forager_managed_entry_psides(symbol)` (pb:18142).
    fn dynamic_forager_managed_entry_psides(
        &self,
        modes: &Modes,
        dynamic_normal: &BTreeSet<&'static str>,
    ) -> Result<BTreeSet<&'static str>> {
        let mut out = dynamic_normal.clone();
        for pside in PSIDES {
            if let Some(explicit) = modes[pside].as_deref() {
                if self.is_forager_mode(pside)? && is_bot_managed_entry_override(explicit) {
                    out.insert(pside);
                }
            }
        }
        Ok(out)
    }

    /// `_apply_orchestrator_symbol_states` -> `PB_modes` (SPEC 1.6): the
    /// explicit override when there is one, else `normal` when the engine
    /// reports the side active, else `PB_mode_stop`. `active` is
    /// `(symbol_idx, long_active, short_active)` from the output's
    /// `symbol_states`; a symbol without a row counts as inactive.
    pub fn pb_modes_after_cycle(
        &self,
        snap: &Snapshot,
        active: &[(usize, bool, bool)],
    ) -> BTreeMap<(String, String), String> {
        let mut out = BTreeMap::new();
        for (idx, symbol) in snap.symbols.iter().enumerate() {
            let row = active.iter().find(|(i, _, _)| *i == idx);
            let (long, short) = &snap.mode_overrides[idx];
            for (pside, explicit, is_active) in [
                ("long", long, row.is_some_and(|r| r.1)),
                ("short", short, row.is_some_and(|r| r.2)),
            ] {
                let mode = match explicit {
                    Some(m) if !m.is_empty() => m.clone(),
                    _ if is_active => "normal".to_string(),
                    _ => self.stop_mode().to_string(),
                };
                out.insert((symbol.clone(), pside.to_string()), mode);
            }
        }
        out
    }

    pub fn approved(&self, pside: &str) -> &BTreeSet<String> {
        &self.approved[pside]
    }

    fn is_approved(&self, pside: &str, symbol: &str) -> bool {
        self.approved[pside].contains(symbol)
    }

    fn pside_blocks_new_entries(&self, pside: &str) -> bool {
        self.forced_mode[pside]
            .as_deref()
            .is_some_and(|m| STOP_MODES.contains(&m))
    }

    /// `is_forager_mode(pside)`.
    pub fn is_forager_mode(&self, pside: &str) -> Result<bool> {
        if !self.cfg.is_pside_enabled(pside)? || self.forced_mode[pside].is_some() {
            return Ok(false);
        }
        let n = self.cfg.n_positions(pside)?;
        Ok(n > 0 && (n as usize) < self.approved[pside].len())
    }

    /// `_build_live_symbol_universe`: sorted union of positions, open orders,
    /// override coins and the approved set of every non-blocked side.
    pub fn universe(&self, states: &[SymbolState]) -> Vec<String> {
        let mut set: BTreeSet<String> = BTreeSet::new();
        for s in states {
            if s.has_position() || s.has_open_order() {
                set.insert(s.symbol.clone());
            }
        }
        for coin in self.cfg.override_coins() {
            set.insert(format!("{coin}/USDT:USDT"));
        }
        for pside in PSIDES {
            if self.pside_blocks_new_entries(pside) {
                continue;
            }
            set.extend(self.approved[pside].iter().cloned());
        }
        set.into_iter().collect()
    }

    /// `_apply_entry_eligibility_mode`.
    fn apply_entry_eligibility(
        &self,
        pside: &str,
        symbol: &str,
        mode: Option<String>,
    ) -> Option<String> {
        if self.is_approved(pside, symbol)
            || mode.as_deref().is_some_and(|m| STOP_MODES.contains(&m))
        {
            return mode;
        }
        Some(
            if self.auto_gs {
                "graceful_stop"
            } else {
                "manual"
            }
            .to_string(),
        )
    }

    /// `PB_mode_stop`: what a side falls back to when the engine reports it inactive.
    pub fn stop_mode(&self) -> &'static str {
        if self.auto_gs {
            "graceful_stop"
        } else {
            "manual"
        }
    }

    /// `_orchestrator_mode_override` steps 4-7 (HSL, runtime overrides and
    /// exchange cooldowns are not modelled).
    pub fn mode_override(&self, pside: &str, s: &SymbolState) -> Result<Option<String>> {
        let coin = s.symbol.split('/').next().unwrap_or(&s.symbol);
        let per_symbol = self
            .cfg
            .config()
            .pointer(&format!("/coin_overrides/{coin}/live/forced_mode_{pside}"))
            .and_then(Value::as_str);
        let raw = per_symbol.unwrap_or_else(|| {
            self.cfg
                .live(&format!("forced_mode_{pside}"))
                .and_then(Value::as_str)
                .unwrap_or("")
        });
        if let Some(mode) = expand_pb_mode(raw)? {
            return Ok(self.apply_entry_eligibility(pside, &s.symbol, Some(mode)));
        }
        if !s.active {
            return Ok(Some("tp_only".to_string()));
        }
        Ok(self.apply_entry_eligibility(pside, &s.symbol, None))
    }

    fn spans_for(&self, symbol: &str) -> Result<SymbolSpans> {
        let mut out = SymbolSpans::default();
        for pside in PSIDES {
            let sp = self.cfg.strategy_params(pside, Some(symbol))?;
            let span0 = positive_finite(sp.get("ema_span_0"))?;
            let span1 = positive_finite(sp.get("ema_span_1"))?;
            let span2 = if span0 > 0.0 && span1 > 0.0 {
                (span0 * span1).powf(0.5)
            } else {
                0.0
            };
            for sp_ in [span0, span1, span2] {
                if sp_ > 0.0 && sp_.is_finite() {
                    out.close.insert(key(sp_));
                }
            }
            let m1_lr_span = warmup_max(&sp, M1_LR_SPAN_PATHS)?;
            if m1_lr_span > 0.0 {
                let w = abs_max_weight(&sp, M1_ENTRY_WEIGHTS)?
                    .max(abs_max_weight(&sp, M1_CLOSE_WEIGHTS)?);
                if w > 0.0 && m1_lr_span.is_finite() {
                    out.m1_lr_required.insert(key(m1_lr_span));
                }
            }
            let h1_span = warmup_max(&sp, H1_SPAN_PATHS)?;
            if h1_span > 0.0 {
                let w = abs_max_weight(&sp, H1_ENTRY_WEIGHTS)?
                    .max(abs_max_weight(&sp, H1_CLOSE_WEIGHTS)?);
                if w > 0.0 && h1_span.is_finite() {
                    out.h1_lr.insert(key(h1_span));
                }
            }
        }
        Ok(out)
    }

    /// `Passivbot.is_trailing(symbol, pside)`: whether the side's strategy
    /// uses trailing prices at all (SPEC 4.1). Sides without it keep the
    /// default bundle and stay `trailing_available = true`.
    pub fn is_trailing(&self, symbol: &str, pside: &str) -> Result<bool> {
        let sp = self.cfg.strategy_params(pside, Some(symbol))?;
        let f = |path: &[&str]| path_get(&sp, path).and_then(Value::as_f64).unwrap_or(0.0);
        Ok(match self.cfg.strategy_kind_name.as_str() {
            "trailing_grid_v7" => {
                f(&["entry", "trailing_grid_ratio"]) != 0.0
                    || f(&["close", "trailing_grid_ratio"]) != 0.0
            }
            _ => {
                f(&["entry", "retracement_base_pct"]) > 0.0
                    || f(&["close", "retracement_base_pct"]) > 0.0
            }
        })
    }

    /// Largest 1m span (close / strategy log-range) and 1h span a symbol
    /// needs; used to size the candle warmup.
    pub fn max_spans(&self, symbol: &str) -> Result<(f64, f64)> {
        let sp = self.spans_for(symbol)?;
        let m1 = sp
            .close
            .iter()
            .chain(sp.m1_lr_required.iter())
            .map(|k| unkey(*k))
            .fold(0.0, f64::max);
        let h1 = sp.h1_lr.iter().map(|k| unkey(*k)).fold(0.0, f64::max);
        Ok((m1, h1))
    }

    fn forager_spans(&self, flat_key: &str) -> Result<BTreeSet<u64>> {
        let mut set = BTreeSet::new();
        for pside in PSIDES {
            let v = self.cfg.bot_value(pside, flat_key)?;
            let x = positive_finite(Some(&v))?;
            if x > 0.0 {
                set.insert(key(x));
            }
        }
        Ok(set)
    }

    /// `_orchestrator_uses_realized_pnl`: `max_realized_loss_pct < 1` or unstuck uses it.
    pub fn uses_realized_pnl(&self) -> Result<bool> {
        let mrl = match self.cfg.live("max_realized_loss_pct") {
            None | Some(Value::Null) => 1.0,
            Some(v) => v.as_f64().unwrap_or(1.0),
        };
        Ok(mrl < 1.0 || self.auto_unstuck_allowed()?)
    }

    /// `_unstuck_uses_realized_pnl`.
    fn auto_unstuck_allowed(&self) -> Result<bool> {
        for pside in PSIDES {
            let twel = self
                .cfg
                .bot_value(pside, "total_wallet_exposure_limit")?
                .as_f64()
                .unwrap_or(0.0);
            if twel <= 0.0 {
                continue;
            }
            let mut targets: Vec<Option<String>> = vec![None];
            targets.extend(
                self.cfg
                    .override_coins()
                    .map(|c| Some(format!("{c}/USDT:USDT"))),
            );
            for sym in targets {
                let enabled = truthy(&self.cfg.bp(pside, "unstuck_enabled", sym.as_deref())?);
                let loss = self
                    .cfg
                    .bp(pside, "unstuck_loss_allowance_pct", sym.as_deref())?
                    .as_f64()
                    .unwrap_or(0.0);
                let close = self
                    .cfg
                    .bp(pside, "unstuck_close_pct", sym.as_deref())?
                    .as_f64()
                    .unwrap_or(0.0);
                if enabled && loss > 0.0 && close > 0.0 {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn global(&self, account: &AccountState) -> Result<Value> {
        let live_f = |k: &str, d: f64| self.cfg.live(k).and_then(Value::as_f64).unwrap_or(d);
        let live_b = |k: &str, d: bool| self.cfg.live(k).map(truthy).unwrap_or(d);
        let max_realized_loss_pct = match self.cfg.live("max_realized_loss_pct") {
            None | Some(Value::Null) => 1.0,
            Some(v) => v
                .as_f64()
                .ok_or_else(|| anyhow!("live.max_realized_loss_pct not numeric"))?,
        };
        Ok(json!({
            "filter_by_min_effective_cost": live_b("filter_by_min_effective_cost", true),
            "market_orders_allowed": live_b("market_orders_allowed", true),
            "market_order_near_touch_threshold": live_f("market_order_near_touch_threshold", 0.0),
            "panic_close_market": false,
            "auto_unstuck_allowed": self.auto_unstuck_allowed()?,
            "max_realized_loss_pct": max_realized_loss_pct,
            "realized_pnl_cumsum_max": account.realized_pnl_cumsum_max,
            "realized_pnl_cumsum_last": account.realized_pnl_cumsum_last,
            "sort_global": true,
            "global_bot_params": {
                "long": self.cfg.bot_params("long", None)?,
                "short": self.cfg.bot_params("short", None)?,
            },
            "hedge_mode": live_b("hedge_mode", true),
            "strategy_kind": self.cfg.strategy_kind_name,
        }))
    }

    /// `_calc_effective_min_cost_at_price`.
    pub fn effective_min_cost(market: &MarketParams, price: f64) -> f64 {
        let mut ep = market.exchange_params();
        if ep.min_qty <= 0.0 && ep.qty_step > 0.0 {
            ep.min_qty = ep.qty_step;
        }
        let min_entry_qty = calc_min_entry_qty(price, &ep);
        qty_to_cost(min_entry_qty, price, ep.c_mult)
    }

    pub fn build(
        &self,
        account: &AccountState,
        states: &[SymbolState],
        cycle: &mut CycleState,
    ) -> Result<Snapshot> {
        let now_ms = account.timestamp_ms;
        let mut next_dynamic: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut mode_overrides: Vec<(Option<String>, Option<String>)> = Vec::new();
        let symbols = self.universe(states);
        let by_symbol: BTreeMap<&str, &SymbolState> =
            states.iter().map(|s| (s.symbol.as_str(), s)).collect();
        let forager_long = self.is_forager_mode("long")?;
        let forager_short = self.is_forager_mode("short")?;
        let forager_on = forager_long || forager_short;
        let m1_volume_spans = self.forager_spans("forager_volume_ema_span_1m")?;
        let m1_lr_spans = self.forager_spans("forager_volatility_ema_span_1m")?;
        let mut required_vol = false;
        let mut required_lr = false;
        for (pside, on) in [("long", forager_long), ("short", forager_short)] {
            if !on {
                continue;
            }
            let bp = self.cfg.bot_params(pside, None)?;
            let drop = bp["forager_volume_drop_pct"].as_f64().unwrap_or(0.0);
            let w_vol = bp["forager_score_weights"]["volume"]
                .as_f64()
                .unwrap_or(0.0);
            let w_vola = bp["forager_score_weights"]["volatility"]
                .as_f64()
                .unwrap_or(0.0);
            required_vol |= drop > 0.0 || w_vol != 0.0;
            required_lr |= w_vola != 0.0;
        }

        let mut sym_values = Vec::with_capacity(symbols.len());
        let mut peek: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
        let mut incumbents: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
        for (idx, symbol) in symbols.iter().enumerate() {
            let s = by_symbol
                .get(symbol.as_str())
                .ok_or_else(|| anyhow!("no state for {symbol}"))?;
            let mut modes: Modes = PSIDES
                .iter()
                .map(|p| Ok((*p, self.mode_override(p, s)?)))
                .collect::<Result<_>>()?;
            // Exchange-unavailable cooldown planning policy (SPEC 2.3).
            let cooled = cycle.exchange_unavailable.contains(symbol);
            if cooled {
                for pside in PSIDES {
                    let m = cooldown_mode(modes[pside].as_deref(), s.has_position());
                    modes.insert(pside, m);
                }
            }
            mode_overrides.push((modes["long"].clone(), modes["short"].clone()));
            // Planning-side predicates of the EMA bundle (SPEC 3.6, 3.7).
            let normal = self.normal_planning_psides(symbol, &modes, cycle);
            let has_normal = !normal.is_empty();
            let dynamic_normal = self.dynamic_forager_normal_psides(
                &modes,
                &normal,
                cycle.dynamic_forager_eligibility.get(symbol),
            )?;
            let managed = self.dynamic_forager_managed_entry_psides(&modes, &dynamic_normal)?;
            if !dynamic_normal.is_empty() {
                next_dynamic.insert(
                    symbol.clone(),
                    dynamic_normal.iter().map(|p| p.to_string()).collect(),
                );
            }
            let has_explicit_normal = normal.difference(&dynamic_normal).next().is_some();
            let priority = s.has_position() || s.has_open_order() || has_normal;
            let cache_only = forager_on && !priority;
            let candidate_only =
                forager_on && !has_normal && !(s.has_position() || s.has_open_order());
            let flat_forager_default_normal =
                !s.has_position() && !has_explicit_normal && !managed.is_empty();
            let can_mark_nontradable =
                flat_forager_default_normal || (!has_normal && (cache_only || candidate_only));

            let spans = self.spans_for(symbol)?;
            let mut m1_close = BTreeMap::new();
            let mut m1_lr = BTreeMap::new();
            let mut m1_vol = BTreeMap::new();
            let mut h1_lr = BTreeMap::new();
            let mut forager_lr = BTreeMap::new();
            let mut unavailable = cache_only && !s.candles_available;
            let mut allow_missing = false;
            if !unavailable {
                let c = &s.candles_1m;
                let h = match &s.candles_1h {
                    Some(h) => h.clone(),
                    None => emas::aggregate_1h(c),
                };
                let mut missing_required = false;
                // `vol` / `lr1m` already produced by the projection (Python:
                // not `None` after `load_projected_open_tail_bundle`).
                let mut vol_projected = false;
                let mut lr_projected = false;
                match open_tail_gap(c, now_ms, self.tail_gap_max_ms) {
                    Some(gap) => {
                        // `load_projected_open_tail_bundle` (SPEC 3.7).
                        let project_strategy_lr = !spans.m1_lr_required.is_empty() && !cache_only;
                        let mut all: Vec<f64> = spans.close.iter().map(|k| unkey(*k)).collect();
                        let mut lr_proj: BTreeSet<u64> = BTreeSet::new();
                        let qv_proj = !forager_on;
                        if !forager_on {
                            all.extend(m1_volume_spans.iter().map(|k| unkey(*k)));
                            lr_proj.extend(m1_lr_spans.iter().chain(spans.m1_lr_required.iter()));
                        } else if project_strategy_lr {
                            lr_proj.extend(spans.m1_lr_required.iter());
                        }
                        all.extend(lr_proj.iter().map(|k| unkey(*k)));
                        let max_span = all.iter().copied().fold(0.0, f64::max);
                        match open_tail_rows(c, &gap, max_span) {
                            Some(rows) => {
                                for k in &spans.close {
                                    match projected_ema(&rows, unkey(*k), Metric::Close) {
                                        Some(v) => {
                                            m1_close.insert(*k, v);
                                        }
                                        None => missing_required = true,
                                    }
                                }
                                if qv_proj {
                                    vol_projected = true;
                                    for k in &m1_volume_spans {
                                        if let Some(v) =
                                            projected_ema(&rows, unkey(*k), Metric::QuoteVolume)
                                        {
                                            m1_vol.insert(*k, v);
                                        }
                                    }
                                }
                                if !forager_on || project_strategy_lr {
                                    lr_projected = true;
                                    for k in &lr_proj {
                                        match projected_ema(&rows, unkey(*k), Metric::LogRange) {
                                            Some(v) => {
                                                m1_lr.insert(*k, v);
                                            }
                                            None if spans.m1_lr_required.contains(k) => {
                                                missing_required = true
                                            }
                                            None => {}
                                        }
                                    }
                                }
                            }
                            // Reader `RuntimeError` -> `MissingCloseEma` for every close span.
                            None => missing_required = true,
                        }
                    }
                    None => {
                        // `fetch_close_map` with the carry-forward fallback (SPEC 3.3).
                        let prev = cycle.prev_close_ema.entry(symbol.clone()).or_default();
                        let mut missing: Vec<u64> = Vec::new();
                        for k in &spans.close {
                            match emas::latest_ema(
                                c,
                                unkey(*k),
                                ONE_MIN_MS,
                                now_ms,
                                Metric::Close,
                                GapPolicy::Provisional,
                            ) {
                                Some(v) => {
                                    m1_close.insert(*k, v);
                                    prev.insert(*k, (v, now_ms));
                                }
                                None => missing.push(*k),
                            }
                        }
                        for k in missing {
                            match prev.get(&k) {
                                Some((v, ts))
                                    if now_ms.saturating_sub(*ts)
                                        <= self.close_ema_fallback_max_age_ms
                                        && v.is_finite() =>
                                {
                                    m1_close.insert(k, *v);
                                }
                                _ => missing_required = true,
                            }
                        }
                    }
                }
                for k in &spans.h1_lr {
                    match emas::latest_ema(
                        &h,
                        unkey(*k),
                        ONE_HOUR_MS,
                        now_ms,
                        Metric::LogRange,
                        GapPolicy::Strict,
                    ) {
                        Some(v) => {
                            h1_lr.insert(*k, v);
                        }
                        None => missing_required = true,
                    }
                }
                if !vol_projected {
                    for k in &m1_volume_spans {
                        if let Some(v) = emas::latest_ema(
                            c,
                            unkey(*k),
                            ONE_MIN_MS,
                            now_ms,
                            Metric::QuoteVolume,
                            GapPolicy::Strict,
                        ) {
                            m1_vol.insert(*k, v);
                        }
                    }
                }
                if !lr_projected {
                    for k in &spans.m1_lr_required {
                        match emas::latest_ema(
                            c,
                            unkey(*k),
                            ONE_MIN_MS,
                            now_ms,
                            Metric::LogRange,
                            GapPolicy::Provisional,
                        ) {
                            Some(v) => {
                                m1_lr.insert(*k, v);
                            }
                            None => missing_required = true,
                        }
                    }
                    for k in &m1_lr_spans {
                        if let Some(v) = emas::latest_ema(
                            c,
                            unkey(*k),
                            ONE_MIN_MS,
                            now_ms,
                            Metric::LogRange,
                            GapPolicy::Provisional,
                        ) {
                            m1_lr.entry(*k).or_insert(v);
                        }
                    }
                }
                if forager_on {
                    for k in &m1_lr_spans {
                        if let Some(v) = emas::latest_ema(
                            c,
                            unkey(*k),
                            ONE_MIN_MS,
                            now_ms,
                            Metric::LogRange,
                            GapPolicy::Strict,
                        ) {
                            forager_lr.insert(*k, v);
                        }
                    }
                } else {
                    for k in &m1_lr_spans {
                        if let Some(v) = m1_lr.get(k) {
                            forager_lr.insert(*k, *v);
                        }
                    }
                }
                if missing_required {
                    if can_mark_nontradable {
                        unavailable = true;
                    } else {
                        allow_missing = true;
                    }
                }
                if !unavailable && forager_on {
                    // pb:19303-19340: required forager spans raise unless the
                    // symbol may be marked nontradable; cache-only symbols
                    // missing any forager span are marked too.
                    let missing_forager = (required_vol
                        && m1_volume_spans.iter().any(|k| !m1_vol.contains_key(k)))
                        || (required_lr && m1_lr_spans.iter().any(|k| !forager_lr.contains_key(k)));
                    if missing_forager {
                        if !can_mark_nontradable {
                            bail!(
                                "{symbol}: required forager EMA span missing for an active/normal symbol"
                            );
                        }
                        unavailable = true;
                    }
                    if cache_only
                        && (m1_volume_spans.iter().any(|k| !m1_vol.contains_key(k))
                            || m1_lr_spans.iter().any(|k| !forager_lr.contains_key(k)))
                    {
                        unavailable = true;
                    }
                }
            }
            if unavailable {
                m1_close.clear();
                m1_lr.clear();
                m1_vol.clear();
                h1_lr.clear();
                forager_lr.clear();
            }
            let pairs = |m: &BTreeMap<u64, f64>| -> Value {
                let mut v: Vec<(f64, f64)> = m.iter().map(|(k, x)| (unkey(*k), *x)).collect();
                v.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                Value::Array(v.into_iter().map(|(s, x)| json!([s, x])).collect())
            };
            // `_exchange_symbol_cooldown_blocks_tradability`: flat cooled symbols only.
            let cooldown_blocks = cooled && !s.has_position();
            let tradable = s.active && !unavailable && !cooldown_blocks;

            let mut sides = Map::new();
            for pside in PSIDES {
                let side = s.side(pside);
                if side.position_size != 0.0 {
                    peek.entry(pside).or_default().push(idx);
                } else if side.has_entry_order {
                    incumbents.entry(pside).or_default().push(idx);
                }
                let t = &side.trailing;
                sides.insert(
                    pside.to_string(),
                    json!({
                        "mode": orchestrator_mode(modes[pside].as_deref()),
                        "position": {"size": side.position_size, "price": side.position_price},
                        "trailing": {
                            "min_since_open": t.min_since_open,
                            "max_since_min": t.max_since_min,
                            "max_since_open": t.max_since_open,
                            "min_since_max": t.min_since_max,
                        },
                        "trailing_available": side.trailing_available,
                        "last_increase_fill_timestamp_ms": side.last_increase_fill_ts,
                        "bot_params": self.cfg.bot_params(pside, Some(symbol))?,
                        "strategy_params": self.cfg.strategy_params(pside, Some(symbol))?,
                    }),
                );
            }
            let m = &s.market;
            let mut sym = json!({
                "symbol_idx": idx,
                "order_book": {"bid": s.bid, "ask": s.ask},
                "exchange": {
                    "qty_step": m.qty_step, "price_step": m.price_step, "min_qty": m.min_qty,
                    "min_cost": m.min_cost, "c_mult": m.c_mult, "maker_fee": m.maker_fee, "taker_fee": m.taker_fee,
                },
                "tradable": tradable,
                "allow_missing_strategy_inputs": allow_missing,
                "next_candle": Value::Null,
                "effective_min_cost": Self::effective_min_cost(m, s.min_cost_price),
                "emas": {
                    "m1": {"close": pairs(&m1_close), "log_range": pairs(&m1_lr), "volume": pairs(&m1_vol)},
                    "h1": {"close": [], "log_range": pairs(&h1_lr), "volume": []},
                },
                "forager_m1": {"close": [], "log_range": pairs(&forager_lr), "volume": pairs(&m1_vol)},
            });
            for (k, v) in sides {
                sym[k] = v;
            }
            sym_values.push(sym);
        }

        let hyst_pct = self
            .cfg
            .live("forager_score_hysteresis_pct")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let input = json!({
            "timestamp_ms": now_ms,
            "balance": account.balance,
            "balance_raw": account.balance_raw,
            "global": self.global(account)?,
            "symbols": sym_values,
            "peek_hints": {
                "expand_grid_long": peek.get("long").cloned().unwrap_or_default(),
                "expand_grid_short": peek.get("short").cloned().unwrap_or_default(),
                "expand_close_long": peek.get("long").cloned().unwrap_or_default(),
                "expand_close_short": peek.get("short").cloned().unwrap_or_default(),
            },
            "forager_hysteresis": {
                "score_hysteresis_pct": hyst_pct,
                "incumbent_long": incumbents.get("long").cloned().unwrap_or_default(),
                "incumbent_short": incumbents.get("short").cloned().unwrap_or_default(),
            },
        });
        cycle.dynamic_forager_eligibility = next_dynamic;
        Ok(Snapshot {
            input,
            symbols,
            mode_overrides,
        })
    }
}

fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|x| x != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// `live.approved_coins` / `live.ignored_coins`: list, or `{long: [...], short: [...]}`.
fn coin_list(v: Option<&Value>, pside: &str) -> Vec<String> {
    let arr = match v {
        Some(Value::Array(a)) => Some(a),
        Some(Value::Object(o)) => o.get(pside).and_then(Value::as_array),
        _ => None,
    };
    arr.map(|a| {
        a.iter()
            .filter_map(|c| c.as_str().map(str::to_string))
            .collect()
    })
    .unwrap_or_default()
}

/// Trailing bundle from the candles after a fill anchor (SPEC 4.1 steps 3-5):
/// fold every complete minute in `(anchor_ts, latest_finalized]` through the
/// engine's `update_trailing_bundle_with_candle`.
pub fn trailing_bundle(
    candles: &[Candle],
    anchor_ts: u64,
    now_ms: u64,
) -> Option<TrailingPriceBundle> {
    let first = (anchor_ts / ONE_MIN_MS + 1) * ONE_MIN_MS;
    let latest = now_ms / ONE_MIN_MS * ONE_MIN_MS - ONE_MIN_MS;
    if latest < first {
        return Some(TrailingPriceBundle::default());
    }
    let rows = emas::window(candles, first, latest, ONE_MIN_MS, GapPolicy::Strict)?;
    if rows.first()?[0] as u64 != first {
        return None;
    }
    // The candle manager hands float32 candles to `update_trailing_bundle_py`.
    let r = |x: f64| x as f32 as f64;
    let mut b = TrailingPriceBundle::default();
    for c in &rows {
        passivbot_rust::trailing::update_trailing_bundle_with_candle(
            &mut b,
            r(c[2]),
            r(c[3]),
            r(c[4]),
        );
    }
    Some(b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn public_config(edit: impl FnOnce(&mut Value)) -> ConfigView {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/configs/fake_v8/grid_v7.json");
        let mut v: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        edit(&mut v);
        ConfigView::new(v).unwrap()
    }

    /// Complete 1m candles `[t0, t0 + n min)` with a small price wiggle.
    fn candles(t0: u64, n: u64, base: f64) -> Vec<Candle> {
        (0..n)
            .map(|i| {
                let p = base * (1.0 + 0.001 * ((i % 7) as f64 - 3.0));
                [
                    (t0 + i * ONE_MIN_MS) as f64,
                    p,
                    p * 1.001,
                    p * 0.999,
                    p,
                    100.0,
                ]
            })
            .collect()
    }

    fn market() -> MarketParams {
        MarketParams {
            qty_step: 1.0,
            price_step: 0.0001,
            min_qty: 1.0,
            min_cost: 5.0,
            c_mult: 1.0,
            maker_fee: 0.0001,
            taker_fee: 0.0006,
        }
    }

    fn state(symbol: &str, candles: Vec<Candle>, pos: f64, entry_order: bool) -> SymbolState {
        SymbolState {
            symbol: symbol.to_string(),
            market: market(),
            active: true,
            bid: 1.0,
            ask: 1.0001,
            min_cost_price: 1.0,
            candles_1m: candles,
            candles_1h: None,
            candles_available: true,
            long: SideState {
                position_size: pos,
                position_price: if pos != 0.0 { 1.0 } else { 0.0 },
                has_entry_order: entry_order,
                has_open_order: entry_order,
                ..SideState::default()
            },
            short: SideState::default(),
        }
    }

    fn account(ts: u64) -> AccountState {
        AccountState {
            timestamp_ms: ts,
            balance: 1000.0,
            balance_raw: 1000.0,
            realized_pnl_cumsum_max: 0.0,
            realized_pnl_cumsum_last: 0.0,
        }
    }

    /// Hour-aligned `t0` and complete candles long enough for every span,
    /// ending at the last closed minute before `now`.
    fn warm(builder: &SnapshotBuilder<'_>, now: u64) -> Vec<Candle> {
        let (m1, h1) = builder.max_spans("ADA/USDT:USDT").unwrap();
        let minutes = (m1.max(h1 * 60.0).max(2274.0).ceil() as u64) + 180;
        let end = emas::latest_expected_minute(now);
        let t0 = (end - minutes * ONE_MIN_MS) / ONE_HOUR_MS * ONE_HOUR_MS;
        candles(t0, (end - t0) / ONE_MIN_MS + 1, 1.0)
    }

    fn states_all(builder: &SnapshotBuilder<'_>, c: &[Candle]) -> Vec<SymbolState> {
        builder
            .universe(&[])
            .iter()
            .map(|s| state(s, c.to_vec(), 0.0, false))
            .collect()
    }

    fn sym<'a>(snap: &'a Snapshot, symbol: &str) -> &'a Value {
        let idx = snap.symbols.iter().position(|s| s == symbol).unwrap();
        &snap.input["symbols"][idx]
    }

    const NOW: u64 = 1_760_000_000_000 + 17_000; // 17 s into a minute

    #[test]
    fn cooldown_mode_table_matches_python() {
        // pb:10128-10134: None/normal always replaced; graceful_stop only when held.
        assert_eq!(cooldown_mode(None, false).as_deref(), Some("graceful_stop"));
        assert_eq!(cooldown_mode(None, true).as_deref(), Some("tp_only"));
        assert_eq!(
            cooldown_mode(Some("normal"), false).as_deref(),
            Some("graceful_stop")
        );
        assert_eq!(
            cooldown_mode(Some("normal"), true).as_deref(),
            Some("tp_only")
        );
        assert_eq!(
            cooldown_mode(Some("graceful_stop"), true).as_deref(),
            Some("tp_only")
        );
        assert_eq!(
            cooldown_mode(Some("graceful_stop"), false).as_deref(),
            Some("graceful_stop")
        );
        for m in [
            "manual",
            "panic",
            "tp_only",
            "tp_only_with_active_entry_cancellation",
        ] {
            assert_eq!(cooldown_mode(Some(m), true).as_deref(), Some(m));
            assert_eq!(cooldown_mode(Some(m), false).as_deref(), Some(m));
        }
    }

    #[test]
    fn exchange_cooldown_blocks_flat_symbols_and_reduces_held_ones() {
        let cfg = public_config(|_| {});
        let builder = SnapshotBuilder::new(&cfg).unwrap();
        let c = warm(&builder, NOW);
        let mut states = states_all(&builder, &c);
        states[1].long.position_size = 100.0; // BTC held
        let mut cycle = CycleState::default();
        cycle.exchange_unavailable.insert("ADA/USDT:USDT".into());
        cycle.exchange_unavailable.insert("BTC/USDT:USDT".into());
        let snap = builder.build(&account(NOW), &states, &mut cycle).unwrap();
        let ada = sym(&snap, "ADA/USDT:USDT");
        assert_eq!(ada["long"]["mode"], "graceful_stop");
        assert_eq!(ada["tradable"], false);
        let btc = sym(&snap, "BTC/USDT:USDT");
        assert_eq!(btc["long"]["mode"], "tp_only");
        assert_eq!(btc["tradable"], true);
        // `has_position(symbol=...)` is symbol-level: the disabled short side's
        // graceful_stop becomes tp_only too (pb:10130-10134)
        assert_eq!(btc["short"]["mode"], "tp_only");
        assert_eq!(ada["short"]["mode"], "graceful_stop");
        let doge = sym(&snap, "DOGE/USDT:USDT");
        assert_eq!(doge["long"]["mode"], Value::Null);
        assert_eq!(doge["tradable"], true);
        // the cooled modes are what the next PB_modes are derived from
        let idx = snap
            .symbols
            .iter()
            .position(|s| s == "ADA/USDT:USDT")
            .unwrap();
        assert_eq!(snap.mode_overrides[idx].0.as_deref(), Some("graceful_stop"));
    }

    #[test]
    fn pb_modes_carry_over_decides_unavailable_vs_allow_missing() {
        // ADA has no candles at all -> every required EMA is missing. Forager
        // stays on (n_positions < approved) but without *required* forager
        // spans, so the missing close EMAs decide (a required forager span
        // would raise for symbols that cannot be marked nontradable).
        let cfg = public_config(|v| {
            v["bot"]["long"]["forager"]["volume_drop_pct"] = Value::from(0.0);
            v["bot"]["long"]["forager"]["score_weights"]["volume"] = Value::from(0.0);
            v["bot"]["long"]["forager"]["score_weights"]["volatility"] = Value::from(0.0);
        });
        let builder = SnapshotBuilder::new(&cfg).unwrap();
        assert!(builder.is_forager_mode("long").unwrap());
        let c = warm(&builder, NOW);
        let ada = "ADA/USDT:USDT";
        let run = |pos: f64, order: bool, pb: Option<&str>, prev_dyn: bool| -> (bool, bool, bool) {
            let mut states = states_all(&builder, &c);
            states[0] = state(ada, Vec::new(), pos, order);
            let mut cycle = CycleState::default();
            if let Some(m) = pb {
                for s in builder.universe(&[]) {
                    let mode = if s == ada { m } else { "graceful_stop" };
                    cycle.pb_modes.insert((s, "long".into()), mode.into());
                }
            }
            if prev_dyn {
                cycle
                    .dynamic_forager_eligibility
                    .insert(ada.into(), ["long".to_string()].into_iter().collect());
            }
            let snap = builder.build(&account(NOW), &states, &mut cycle).unwrap();
            let s = sym(&snap, ada);
            (
                s["tradable"].as_bool().unwrap(),
                s["allow_missing_strategy_inputs"].as_bool().unwrap(),
                cycle
                    .dynamic_forager_eligibility
                    .get(ada)
                    .is_some_and(|p| p.contains("long")),
            )
        };
        // First cycle (no PB_modes): default-normal flat forager symbol -> nontradable.
        assert_eq!(run(0.0, false, None, false), (false, false, true));
        // Selected last cycle (PB_mode normal), still flat -> nontradable, stays dynamic.
        assert_eq!(run(0.0, false, Some("normal"), false), (false, false, true));
        // Deselected (graceful_stop), no order -> candidate-only -> nontradable.
        assert_eq!(
            run(0.0, false, Some("graceful_stop"), false),
            (false, false, false)
        );
        // Deselected with a resting entry and no retained eligibility:
        // priority (order), not candidate-only, no managed side -> the
        // strict path: tradable with allow_missing_strategy_inputs.
        assert_eq!(
            run(0.0, true, Some("graceful_stop"), false),
            (true, true, false)
        );
        // Same, but dynamically normal last cycle -> retained -> managed ->
        // flat forager default-normal symbol -> nontradable, eligibility kept.
        assert_eq!(
            run(0.0, true, Some("graceful_stop"), true),
            (false, false, true)
        );
        // A held position is never marked nontradable.
        assert_eq!(
            run(100.0, false, Some("graceful_stop"), false),
            (true, true, false)
        );
        // With a required forager span (the public config's volume weight) a
        // symbol that cannot be marked nontradable makes the cycle raise.
        let strict = public_config(|_| {});
        let b2 = SnapshotBuilder::new(&strict).unwrap();
        let mut states = states_all(&b2, &c);
        states[0] = state(ada, Vec::new(), 100.0, false);
        let err = b2
            .build(&account(NOW), &states, &mut CycleState::default())
            .unwrap_err();
        assert!(err.to_string().contains("required forager EMA"));
        states[0] = state(ada, Vec::new(), 0.0, false);
        let snap = b2
            .build(&account(NOW), &states, &mut CycleState::default())
            .unwrap();
        assert_eq!(sym(&snap, ada)["tradable"], false);
    }

    #[test]
    fn pb_modes_after_cycle_uses_explicit_active_or_stop() {
        let cfg = public_config(|_| {});
        let builder = SnapshotBuilder::new(&cfg).unwrap();
        let c = warm(&builder, NOW);
        let states = states_all(&builder, &c);
        let mut cycle = CycleState::default();
        cycle.exchange_unavailable.insert("ADA/USDT:USDT".into());
        let snap = builder.build(&account(NOW), &states, &mut cycle).unwrap();
        let pb = builder.pb_modes_after_cycle(&snap, &[(1, true, false), (2, false, false)]);
        let m = |s: &str, p: &str| pb[&(s.to_string(), p.to_string())].clone();
        assert_eq!(m("ADA/USDT:USDT", "long"), "graceful_stop"); // explicit (cooldown)
        assert_eq!(m("BTC/USDT:USDT", "long"), "normal"); // active
        assert_eq!(m("DOGE/USDT:USDT", "long"), "graceful_stop"); // inactive -> PB_mode_stop
        assert_eq!(m("DOT/USDT:USDT", "long"), "graceful_stop"); // no row -> stop
        assert_eq!(m("BTC/USDT:USDT", "short"), "graceful_stop"); // explicit (disabled side)
    }

    /// Non-forager variant (n_positions = number of approved coins): forager
    /// metrics are optional, so tail problems show in the close EMAs.
    fn non_forager() -> ConfigView {
        public_config(|v| {
            v["bot"]["long"]["risk"]["n_positions"] = Value::from(10);
            v["live"]["max_active_candle_tail_gap_minutes"] = Value::from(1);
        })
    }

    #[test]
    fn close_ema_carry_forward_within_ttl_then_stale() {
        let cfg = non_forager();
        let builder = SnapshotBuilder::new(&cfg).unwrap();
        assert!(!builder.is_forager_mode("long").unwrap());
        assert_eq!(close_ema_fallback_max_age_ms(&cfg), 10 * ONE_MIN_MS);
        assert_eq!(active_candle_tail_gap_max_ms(&cfg), ONE_MIN_MS);
        let c = warm(&builder, NOW);
        let ada = "ADA/USDT:USDT";
        let mut cycle = CycleState::default();
        // Cycle 1: complete candles -> close EMAs read and remembered.
        let states = states_all(&builder, &c);
        let snap1 = builder.build(&account(NOW), &states, &mut cycle).unwrap();
        let close1 = sym(&snap1, ada)["emas"]["m1"]["close"].clone();
        assert!(!close1.as_array().unwrap().is_empty());
        assert_eq!(
            cycle.prev_close_ema[ada].len(),
            close1.as_array().unwrap().len()
        );
        // Cycle 2, three minutes later, the buffer never received the new
        // minutes (tail gap 4 min > 1 min: no projection context):
        // the previous values are carried forward, age 3 min <= 10 min.
        let snap2 = builder
            .build(&account(NOW + 3 * ONE_MIN_MS), &states, &mut cycle)
            .unwrap();
        let s2 = sym(&snap2, ada);
        assert_eq!(s2["emas"]["m1"]["close"], close1);
        assert_eq!(s2["tradable"], true);
        assert_eq!(s2["allow_missing_strategy_inputs"], false);
        // Cycle 3, eleven minutes after the read: stale -> missing -> the
        // normal-planning symbol stays tradable with allow_missing.
        let snap3 = builder
            .build(&account(NOW + 11 * ONE_MIN_MS), &states, &mut cycle)
            .unwrap();
        let s3 = sym(&snap3, ada);
        assert_eq!(s3["emas"]["m1"]["close"].as_array().unwrap().len(), 0);
        assert_eq!(s3["allow_missing_strategy_inputs"], true);
        assert_eq!(s3["tradable"], true);
    }

    #[test]
    fn open_tail_projection_feeds_close_volume_and_log_range() {
        let cfg = public_config(|v| {
            v["bot"]["long"]["risk"]["n_positions"] = Value::from(10);
        });
        let builder = SnapshotBuilder::new(&cfg).unwrap();
        let ada = "ADA/USDT:USDT";
        let full = warm(&builder, NOW);
        // ADA's buffer stops 3 minutes before the last closed minute.
        let short: Vec<Candle> = full[..full.len() - 3].to_vec();
        let gap = open_tail_gap(&short, NOW, active_candle_tail_gap_max_ms(&cfg)).unwrap();
        assert_eq!(gap.tail_gap_ms, 3 * ONE_MIN_MS);
        let mut states = states_all(&builder, &full);
        states[0] = state(ada, short.clone(), 0.0, false);
        let mut cycle = CycleState::default();
        let snap = builder.build(&account(NOW), &states, &mut cycle).unwrap();
        let s = sym(&snap, ada);
        assert_eq!(s["tradable"], true);
        assert_eq!(s["allow_missing_strategy_inputs"], false);
        let (m1, _) = builder.max_spans(ada).unwrap();
        let rows = open_tail_rows(&short, &gap, m1.max(2274.0)).unwrap();
        assert_eq!(rows.last().unwrap()[0] as u64, gap.latest_expected_ts);
        let close = s["emas"]["m1"]["close"].as_array().unwrap();
        assert!(!close.is_empty());
        for pair in close {
            let span = pair[0].as_f64().unwrap();
            assert_eq!(
                pair[1].as_f64().unwrap(),
                projected_ema(&rows, span, Metric::Close).unwrap()
            );
        }
        // Non-forager: qv and log-range are projected too (optional maps,
        // one span per side: the long config's and the short template's).
        let vol = s["emas"]["m1"]["volume"].as_array().unwrap();
        assert_eq!(vol.len(), 2);
        for pair in vol {
            let span = pair[0].as_f64().unwrap();
            assert_eq!(
                pair[1].as_f64().unwrap(),
                projected_ema(&rows, span, Metric::QuoteVolume).unwrap()
            );
        }
        let lr = s["emas"]["m1"]["log_range"].as_array().unwrap();
        assert_eq!(lr.len(), 2);
        assert!(lr.iter().any(|p| p[0].as_f64() == Some(2274.0)));
        for pair in lr {
            let span = pair[0].as_f64().unwrap();
            assert_eq!(
                pair[1].as_f64().unwrap(),
                projected_ema(&rows, span, Metric::LogRange).unwrap()
            );
        }
        // The projection never writes the carry-forward cache.
        assert!(!cycle.prev_close_ema.contains_key(ada));
        // Beyond the tail budget nothing is projected: missing -> allow_missing.
        let shorter: Vec<Candle> = full[..full.len() - 11].to_vec();
        states[0] = state(ada, shorter, 0.0, false);
        let snap = builder.build(&account(NOW), &states, &mut cycle).unwrap();
        let s = sym(&snap, ada);
        assert_eq!(s["emas"]["m1"]["close"].as_array().unwrap().len(), 0);
        assert_eq!(s["allow_missing_strategy_inputs"], true);
    }

    #[test]
    fn forced_modes_per_symbol_and_global() {
        // Per-symbol overrides through coin_overrides.<coin>.live.forced_mode_long
        // (the grid_v7_forced fixture config) and a global forced mode.
        let cfg = public_config(|v| {
            v["coin_overrides"]["ADA"]["live"]["forced_mode_long"] = Value::from("gs");
            v["coin_overrides"]["BTC"]["live"]["forced_mode_long"] = Value::from("TP");
            v["coin_overrides"]["DOGE"]["live"]["forced_mode_long"] = Value::from("m");
            v["coin_overrides"]["DOT"]["live"]["forced_mode_long"] = Value::from("n");
        });
        let builder = SnapshotBuilder::new(&cfg).unwrap();
        let c = warm(&builder, NOW);
        let states = states_all(&builder, &c);
        let snap = builder
            .build(&account(NOW), &states, &mut CycleState::default())
            .unwrap();
        assert_eq!(sym(&snap, "ADA/USDT:USDT")["long"]["mode"], "graceful_stop");
        assert_eq!(sym(&snap, "BTC/USDT:USDT")["long"]["mode"], "tp_only");
        assert_eq!(sym(&snap, "DOGE/USDT:USDT")["long"]["mode"], "manual");
        assert_eq!(sym(&snap, "DOT/USDT:USDT")["long"]["mode"], "normal");
        assert_eq!(sym(&snap, "ETH/USDT:USDT")["long"]["mode"], Value::Null);
        assert!(expand_pb_mode("bogus").is_err());
        assert_eq!(expand_pb_mode("").unwrap(), None);
        // a global forced stop mode blocks the side's universe contribution
        // (SPEC 2.1) and turns forager off
        let cfg2 = public_config(|v| v["live"]["forced_mode_long"] = Value::from("graceful_stop"));
        let b2 = SnapshotBuilder::new(&cfg2).unwrap();
        // only the coin_overrides symbols remain (pb:16941-16953)
        assert_eq!(b2.universe(&[]), ["DOGE/USDT:USDT", "XRP/USDT:USDT"]);
        assert!(!b2.is_forager_mode("long").unwrap());
    }
}
