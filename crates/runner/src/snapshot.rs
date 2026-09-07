//! Orchestrator snapshot builder (docs/SNAPSHOT_SPEC.md, PLAN P4.2).
//!
//! Turns the runner's market/account state plus a [`ConfigView`] into the
//! JSON `OrchestratorInput` the Python bot would build for the same state.
//! The result is a `serde_json::Value` with Python's int/float typing so it
//! can be fed to the engine through the same text round trip (D8) and diffed
//! against recorded inputs (`pb-snapcheck`).
//!
//! Not reproduced (documented in SPEC section 8; STATUS lists the gaps):
//! HSL-driven modes, exchange-unavailable cooldowns, operator runtime
//! forced modes, the previous-cycle `PB_modes` influence on tradability,
//! open-tail projection, and the close-EMA carry-forward.

use crate::bot_params::{ConfigView, PSIDES};
use crate::emas::{self, Candle, GapPolicy, Metric, ONE_HOUR_MS, ONE_MIN_MS};
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
}

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
        })
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

    /// `_orchestrator_mode_override` steps 4-7 (HSL, runtime overrides and
    /// exchange cooldowns are not modelled).
    fn mode_override(&self, pside: &str, s: &SymbolState) -> Result<Option<String>> {
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

    pub fn build(&self, account: &AccountState, states: &[SymbolState]) -> Result<Snapshot> {
        let now_ms = account.timestamp_ms;
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
            let modes: BTreeMap<&str, Option<String>> = PSIDES
                .iter()
                .map(|p| Ok((*p, self.mode_override(p, s)?)))
                .collect::<Result<_>>()?;
            let explicit_normal = modes.values().any(|m| m.as_deref() == Some("normal"));
            let priority = s.has_position() || s.has_open_order() || explicit_normal;
            let cache_only = forager_on && !priority;
            let can_mark_nontradable = !s.has_position() && !explicit_normal;

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
                let h = emas::aggregate_1h(c);
                let mut missing_required = false;
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
                        }
                        None => missing_required = true,
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
                    let missing_forager = (required_vol
                        && m1_volume_spans.iter().any(|k| !m1_vol.contains_key(k)))
                        || (required_lr && m1_lr_spans.iter().any(|k| !forager_lr.contains_key(k)));
                    if missing_forager {
                        if priority {
                            bail!(
                                "{symbol}: required forager EMA span missing for an active symbol"
                            );
                        }
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
            let tradable = s.active && !unavailable;

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
        Ok(Snapshot { input, symbols })
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
    let mut b = TrailingPriceBundle::default();
    for c in &rows {
        passivbot_rust::trailing::update_trailing_bundle_with_candle(&mut b, c[2], c[3], c[4]);
    }
    Some(b)
}
