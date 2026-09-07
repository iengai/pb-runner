//! Config -> engine parameter mapping (docs/SNAPSHOT_SPEC.md sections 1.6, 1.7;
//! docs/DECISIONS.md D9).
//!
//! Reproduces, as JSON values with Python's int/float typing, what passivbot
//! v8.1.0 emits for `global_bot_params`, each symbol side's `bot_params` and
//! `strategy_params`:
//!
//! - `Passivbot.bot_value(pside, key)` = grouped lookup (`risk.*`, `forager.*`,
//!   `hsl.*`, `unstuck.*`, `config/shared_bot.py`) then the flat key.
//! - `Passivbot.bp(pside, key, symbol)` = the same on the symbol's
//!   `coin_overrides[..]["bot"][pside]` first, then the global config.
//! - `_bot_params_to_rust_dict` field list, key renames and coercions.
//! - `_strategy_params_to_rust_dict` = engine spec defaults, overlaid with the
//!   side's values, overlaid with the override's values, `entry.ema_gate_mode`
//!   lower-cased for `trailing_martingale`.
//! - `wallet_exposure_limit` = `round(twel / n_positions, 8)` (or the override's
//!   value), which the Python bot writes back into its config every cycle.
//! - Loader normalisation that the runner must repeat because it receives the
//!   raw config file: `forager.score_weights` normalised to sum 1 unless the
//!   side is disabled and the vector is all zero (`normalize_bot_forager_config`).
//!
//! Only the resolved-value mapping is ported; schema migration is not (D9).

// The module is exercised by its fixture tests; main wires it in at P4.5.
#![allow(dead_code)]

use anyhow::{anyhow, bail, Context, Result};
use passivbot_rust::strategies::registry::{strategy_kind_from_name, strategy_spec};
use passivbot_rust::strategies::StrategyKind;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

pub const PSIDES: [&str; 2] = ["long", "short"];

/// `FLAT_BOT_KEY_TO_GROUP_PATH` from `config/shared_bot.py`.
fn group_path(flat_key: &str) -> Option<(&'static str, &'static str)> {
    Some(match flat_key {
        "risk_entry_cooldown_minutes" => ("risk", "entry_cooldown_minutes"),
        "n_positions" => ("risk", "n_positions"),
        "total_wallet_exposure_limit" => ("risk", "total_wallet_exposure_limit"),
        "risk_twel_entry_gate_enabled" => ("risk", "total_exposure_entry_gate_enabled"),
        "risk_twel_enforcer_enabled" => ("risk", "total_exposure_enforcer_enabled"),
        "risk_twel_enforcer_policy" => ("risk", "total_exposure_enforcer_policy"),
        "risk_twel_enforcer_threshold" => ("risk", "total_exposure_enforcer_threshold"),
        "risk_we_excess_allowance_pct" => ("risk", "we_excess_allowance_pct"),
        "risk_we_excess_allowance_mode" => ("risk", "we_excess_allowance_mode"),
        "risk_wel_enforcer_enabled" => ("risk", "position_exposure_enforcer_enabled"),
        "risk_wel_enforcer_threshold" => ("risk", "position_exposure_enforcer_threshold"),
        "forager_score_weights" => ("forager", "score_weights"),
        "forager_volatility_ema_span_1m" => ("forager", "volatility_ema_span_1m"),
        "forager_volume_drop_pct" => ("forager", "volume_drop_pct"),
        "forager_volume_ema_span_1m" => ("forager", "volume_ema_span_1m"),
        "hsl_cooldown_minutes_after_red" => ("hsl", "cooldown_minutes_after_red"),
        "hsl_ema_span_minutes" => ("hsl", "ema_span_minutes"),
        "hsl_enabled" => ("hsl", "enabled"),
        "hsl_no_restart_drawdown_threshold" => ("hsl", "no_restart_drawdown_threshold"),
        "hsl_orange_tier_mode" => ("hsl", "orange_tier_mode"),
        "hsl_panic_close_order_type" => ("hsl", "panic_close_order_type"),
        "hsl_red_threshold" => ("hsl", "red_threshold"),
        "hsl_restart_after_red_policy" => ("hsl", "restart_after_red_policy"),
        "hsl_tier_ratios" => ("hsl", "tier_ratios"),
        "unstuck_close_pct" => ("unstuck", "close_pct"),
        "unstuck_ema_dist" => ("unstuck", "ema_dist"),
        "unstuck_ema_gating_enabled" => ("unstuck", "ema_gating_enabled"),
        "unstuck_enabled" => ("unstuck", "enabled"),
        "unstuck_loss_allowance_pct" => ("unstuck", "loss_allowance_pct"),
        "unstuck_threshold" => ("unstuck", "threshold"),
        _ => return None,
    })
}

/// `get_grouped_bot_value(bot_side, flat_key)`: group first, then flat key.
fn grouped_value<'a>(bot_side: &'a Value, flat_key: &str) -> Option<&'a Value> {
    let side = bot_side.as_object()?;
    if let Some((group, local)) = group_path(flat_key) {
        if let Some(v) = side
            .get(group)
            .and_then(Value::as_object)
            .and_then(|g| g.get(local))
        {
            return Some(v);
        }
    }
    side.get(flat_key)
}

/// Python truthiness for the JSON values a config can hold.
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

/// `float(val)` for numbers and bools; anything else is a config error.
fn as_f64(v: &Value, what: &str) -> Result<f64> {
    match v {
        Value::Number(n) => n.as_f64().ok_or_else(|| anyhow!("{what}: not a float")),
        Value::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
        Value::String(s) => s
            .trim()
            .parse::<f64>()
            .with_context(|| format!("{what}: {s:?} is not numeric")),
        other => bail!("{what}: expected number, got {other}"),
    }
}

/// `float(val or 0.0)`.
fn float_or_zero(v: &Value, what: &str) -> Result<f64> {
    if !truthy(v) {
        return Ok(0.0);
    }
    as_f64(v, what)
}

fn f(x: f64) -> Value {
    Value::from(x)
}

/// `round(x, 8)` with Python's round-half-even on the scaled value.
fn round8(x: f64) -> f64 {
    let scaled = x * 1e8;
    let r = scaled.round_ties_even();
    let out = r / 1e8;
    // Python's round() uses correctly-rounded decimal arithmetic; for the
    // magnitudes involved (wallet exposure limits) this agrees with the
    // float computation whenever the result has <= 8 decimals.
    if (out * 1e8 - r).abs() > 0.0 {
        format!("{out:.8}").parse().unwrap_or(out)
    } else {
        out
    }
}

/// Resolved view over one raw v8.1.0 config file.
#[derive(Debug, Clone)]
pub struct ConfigView {
    config: Value,
    /// coin -> override object (`{"bot": {"long": {...}}}`); keyed by coin
    /// exactly as in the file, matched to symbols by their base coin.
    overrides: BTreeMap<String, Value>,
    pub strategy_kind_name: String,
    pub strategy_kind: StrategyKind,
    hsl_signal_mode: String,
}

impl ConfigView {
    pub fn new(config: Value) -> Result<Self> {
        let mut config = config;
        hjsonify_numbers(&mut config);
        let kind_name = config
            .pointer("/live/strategy_kind")
            .and_then(Value::as_str)
            .map(|s| s.trim().to_ascii_lowercase())
            .unwrap_or_else(|| "trailing_martingale".to_string());
        let strategy_kind = strategy_kind_from_name(&kind_name).ok_or_else(|| {
            anyhow!("live.strategy_kind {kind_name:?} is not a known strategy kind")
        })?;
        let hsl_signal_mode = config
            .pointer("/live/hsl_signal_mode")
            .and_then(Value::as_str)
            .unwrap_or("unified")
            .to_string();
        let overrides = config
            .get("coin_overrides")
            .and_then(Value::as_object)
            .map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        fill_template_defaults(&mut config)?;
        normalize_forager_weights(&mut config)?;
        Ok(Self {
            config,
            overrides,
            strategy_kind_name: kind_name,
            strategy_kind,
            hsl_signal_mode,
        })
    }

    pub fn config(&self) -> &Value {
        &self.config
    }

    pub fn live(&self, key: &str) -> Option<&Value> {
        self.config.pointer(&format!("/live/{key}"))
    }

    pub fn override_coins(&self) -> impl Iterator<Item = &str> {
        self.overrides.keys().map(String::as_str)
    }

    fn override_side(&self, symbol: &str, pside: &str) -> Option<&Value> {
        let coin = symbol.split('/').next()?;
        self.overrides.get(coin)?.pointer(&format!("/bot/{pside}"))
    }

    fn global_side(&self, pside: &str) -> Value {
        self.config
            .pointer(&format!("/bot/{pside}"))
            .cloned()
            .unwrap_or(Value::Null)
    }

    /// `Passivbot.bot_value(pside, key)`; supports one dotted level
    /// (`hsl_tier_ratios.yellow`).
    pub fn bot_value(&self, pside: &str, key: &str) -> Result<Value> {
        let side = self.global_side(pside);
        if let Some((root, rest)) = key.split_once('.') {
            if let Some(root_v) = grouped_value(&side, root) {
                return root_v
                    .get(rest)
                    .cloned()
                    .ok_or_else(|| anyhow!("bot.{pside}.{root}.{rest} missing"));
            }
        }
        grouped_value(&side, key)
            .cloned()
            .ok_or_else(|| anyhow!("bot.{pside}.{key} missing from config"))
    }

    /// `Passivbot.bp(pside, key, symbol)` = `config_get(["bot", pside, key], symbol)`.
    pub fn bp(&self, pside: &str, key: &str, symbol: Option<&str>) -> Result<Value> {
        if let Some(sym) = symbol {
            if let Some(side) = self.override_side(sym, pside) {
                if let Some(v) = grouped_value(side, key) {
                    return Ok(v.clone());
                }
            }
        }
        self.bot_value(pside, key)
    }

    /// `get_wallet_exposure_limit(pside, symbol)`.
    pub fn wallet_exposure_limit(&self, pside: &str, symbol: Option<&str>) -> Result<Value> {
        if let Some(sym) = symbol {
            if let Some(v) = self
                .override_side(sym, pside)
                .and_then(|s| s.get("wallet_exposure_limit"))
            {
                if !v.is_null() {
                    return Ok(v.clone());
                }
            }
        }
        let twel = as_f64(
            &self.bot_value(pside, "total_wallet_exposure_limit")?,
            "total_wallet_exposure_limit",
        )?;
        if twel <= 0.0 {
            return Ok(f(0.0));
        }
        let n =
            as_f64(&self.bot_value(pside, "n_positions")?, "n_positions")?.round_ties_even() as i64;
        if n <= 0 {
            return Ok(f(0.0));
        }
        Ok(f(round8(twel / n as f64)))
    }

    pub fn n_positions(&self, pside: &str) -> Result<i64> {
        let v = self.bot_value(pside, "n_positions")?;
        Ok(float_or_zero(&v, "n_positions")?.round_ties_even() as i64)
    }

    pub fn is_pside_enabled(&self, pside: &str) -> Result<bool> {
        let twel = as_f64(
            &self.bot_value(pside, "total_wallet_exposure_limit")?,
            "twel",
        )?;
        let n = as_f64(&self.bot_value(pside, "n_positions")?, "n_positions")?;
        Ok(twel > 0.0 && n > 0.0)
    }

    /// `_parse_hsl_config()[pside]`: global HSL values with the
    /// `no_restart_drawdown_threshold >= red_threshold` clamp.
    fn hsl_global(&self, pside: &str) -> Result<Map<String, Value>> {
        let mut out = self.hsl_from_bot_value(pside)?;
        let red = as_f64(&out["red_threshold"], "hsl_red_threshold")?;
        let no_restart = as_f64(
            &out["no_restart_drawdown_threshold"],
            "hsl_no_restart_drawdown_threshold",
        )?;
        if no_restart < red {
            out.insert("no_restart_drawdown_threshold".into(), f(red));
        }
        Ok(out)
    }

    /// The unclamped read used for `symbol is None` in `_bot_params_to_rust_dict`.
    fn hsl_from_bot_value(&self, pside: &str) -> Result<Map<String, Value>> {
        let mut m = Map::new();
        m.insert(
            "enabled".into(),
            Value::Bool(truthy(&self.bot_value(pside, "hsl_enabled")?)),
        );
        for (k, key) in [
            ("red_threshold", "hsl_red_threshold"),
            ("ema_span_minutes", "hsl_ema_span_minutes"),
            (
                "cooldown_minutes_after_red",
                "hsl_cooldown_minutes_after_red",
            ),
            (
                "no_restart_drawdown_threshold",
                "hsl_no_restart_drawdown_threshold",
            ),
        ] {
            m.insert(k.into(), f(as_f64(&self.bot_value(pside, key)?, key)?));
        }
        m.insert(
            "restart_after_red_policy".into(),
            normalize_hsl_policy(&self.bot_value(pside, "hsl_restart_after_red_policy")?)?,
        );
        m.insert(
            "tier_ratios".into(),
            json!({
                "yellow": as_f64(&self.bot_value(pside, "hsl_tier_ratios.yellow")?, "hsl_tier_ratios.yellow")?,
                "orange": as_f64(&self.bot_value(pside, "hsl_tier_ratios.orange")?, "hsl_tier_ratios.orange")?,
            }),
        );
        m.insert(
            "orange_tier_mode".into(),
            Value::String(stringify(&self.bot_value(pside, "hsl_orange_tier_mode")?)),
        );
        m.insert(
            "panic_close_order_type".into(),
            Value::String(stringify(
                &self.bot_value(pside, "hsl_panic_close_order_type")?,
            )),
        );
        Ok(m)
    }

    /// `_equity_hard_stop_config(pside, symbol)`.
    fn hsl_for_symbol(&self, pside: &str, symbol: &str) -> Result<Map<String, Value>> {
        let global = self.hsl_global(pside)?;
        let coin = symbol.split('/').next().unwrap_or(symbol);
        if !self.overrides.contains_key(coin) || self.hsl_signal_mode != "coin" {
            return Ok(global);
        }
        let mut tier_ratios = global["tier_ratios"]
            .as_object()
            .cloned()
            .unwrap_or_default();
        if let Some(o) = self.bp(pside, "hsl_tier_ratios", Some(symbol))?.as_object() {
            for (k, v) in o {
                tier_ratios.insert(k.clone(), v.clone());
            }
        }
        let mut m = Map::new();
        m.insert(
            "cooldown_minutes_after_red".into(),
            f(as_f64(
                &self.bp(pside, "hsl_cooldown_minutes_after_red", Some(symbol))?,
                "hsl",
            )?),
        );
        m.insert(
            "ema_span_minutes".into(),
            f(as_f64(
                &self.bp(pside, "hsl_ema_span_minutes", Some(symbol))?,
                "hsl",
            )?),
        );
        m.insert(
            "enabled".into(),
            Value::Bool(truthy(&self.bp(pside, "hsl_enabled", Some(symbol))?)),
        );
        m.insert(
            "no_restart_drawdown_threshold".into(),
            f(as_f64(
                &self.bp(pside, "hsl_no_restart_drawdown_threshold", Some(symbol))?,
                "hsl",
            )?),
        );
        m.insert(
            "orange_tier_mode".into(),
            Value::String(stringify(&self.bp(
                pside,
                "hsl_orange_tier_mode",
                Some(symbol),
            )?)),
        );
        m.insert(
            "panic_close_order_type".into(),
            Value::String(stringify(&self.bp(
                pside,
                "hsl_panic_close_order_type",
                Some(symbol),
            )?)),
        );
        m.insert(
            "red_threshold".into(),
            f(as_f64(
                &self.bp(pside, "hsl_red_threshold", Some(symbol))?,
                "hsl",
            )?),
        );
        m.insert(
            "restart_after_red_policy".into(),
            normalize_hsl_policy(&self.bp(pside, "hsl_restart_after_red_policy", Some(symbol))?)?,
        );
        m.insert("tier_ratios".into(), Value::Object(tier_ratios));
        Ok(m)
    }

    /// `_bot_params_to_rust_dict(pside, symbol)` as a JSON object.
    pub fn bot_params(&self, pside: &str, symbol: Option<&str>) -> Result<Value> {
        const GLOBAL_KEYS: [&str; 5] = [
            "n_positions",
            "total_wallet_exposure_limit",
            "risk_twel_enforcer_enabled",
            "risk_twel_enforcer_policy",
            "risk_twel_enforcer_threshold",
        ];
        const BOOL_KEYS: [&str; 5] = [
            "risk_wel_enforcer_enabled",
            "risk_twel_enforcer_enabled",
            "risk_twel_entry_gate_enabled",
            "unstuck_enabled",
            "unstuck_ema_gating_enabled",
        ];
        // The 18 legacy flat strategy fields: `strategy_cfg.get(key)` never
        // matches the nested v8 shape, so they are emitted as 0.0.
        const STRATEGY_KEYS: [&str; 18] = [
            "close_grid_qty_pct",
            "close_trailing_retracement_pct",
            "close_trailing_qty_pct",
            "close_trailing_threshold_pct",
            "close_weight_volatility_1h",
            "close_weight_volatility_1m",
            "entry_grid_double_down_factor",
            "entry_grid_spacing_pct",
            "entry_volatility_ema_span_1h",
            "entry_volatility_ema_span_1m",
            "entry_weight_volatility_1h",
            "entry_weight_volatility_1m",
            "entry_we_weight",
            "entry_initial_ema_dist",
            "entry_initial_qty_pct",
            "entry_trailing_double_down_factor",
            "entry_trailing_retracement_pct",
            "entry_trailing_threshold_pct",
        ];
        const FIELDS: [&str; 40] = [
            "close_grid_qty_pct",
            "close_trailing_retracement_pct",
            "close_trailing_qty_pct",
            "close_trailing_threshold_pct",
            "close_weight_volatility_1h",
            "close_weight_volatility_1m",
            "entry_grid_double_down_factor",
            "entry_grid_spacing_pct",
            "entry_volatility_ema_span_1h",
            "entry_volatility_ema_span_1m",
            "entry_weight_volatility_1h",
            "entry_weight_volatility_1m",
            "entry_we_weight",
            "entry_initial_ema_dist",
            "entry_initial_qty_pct",
            "entry_trailing_double_down_factor",
            "entry_trailing_retracement_pct",
            "entry_trailing_threshold_pct",
            "forager_volatility_ema_span_1m",
            "forager_volume_ema_span_1m",
            "forager_volume_drop_pct",
            "forager_score_weights",
            "risk_entry_cooldown_minutes",
            "n_positions",
            "total_wallet_exposure_limit",
            "wallet_exposure_limit",
            "risk_wel_enforcer_enabled",
            "risk_wel_enforcer_threshold",
            "risk_twel_enforcer_enabled",
            "risk_twel_enforcer_policy",
            "risk_twel_entry_gate_enabled",
            "risk_twel_enforcer_threshold",
            "risk_we_excess_allowance_pct",
            "risk_we_excess_allowance_mode",
            "unstuck_enabled",
            "unstuck_close_pct",
            "unstuck_ema_gating_enabled",
            "unstuck_ema_dist",
            "unstuck_loss_allowance_pct",
            "unstuck_threshold",
        ];
        let mut out = Map::new();
        for key in FIELDS {
            let val = if key == "wallet_exposure_limit" {
                self.wallet_exposure_limit(pside, symbol)?
            } else if GLOBAL_KEYS.contains(&key) {
                self.bot_value(pside, key)?
            } else if STRATEGY_KEYS.contains(&key) {
                f(0.0)
            } else {
                self.bp(pside, key, symbol)?
            };
            let out_key = match key {
                "forager_volatility_ema_span_1m" => "filter_volatility_ema_span_1m",
                "forager_volume_ema_span_1m" => "filter_volume_ema_span_1m",
                k => k,
            };
            let what = format!("bot.{pside}.{key}");
            let v = match key {
                "forager_score_weights" => {
                    let w = val
                        .as_object()
                        .ok_or_else(|| anyhow!("{what} must be a dict"))?;
                    let get = |k: &str| -> Result<f64> {
                        as_f64(
                            w.get(k).ok_or_else(|| anyhow!("{what}.{k} missing"))?,
                            &what,
                        )
                    };
                    json!({"volume": get("volume")?, "ema_readiness": get("ema_readiness")?, "volatility": get("volatility")?})
                }
                "n_positions" => Value::from(float_or_zero(&val, &what)?.round_ties_even() as i64),
                k if BOOL_KEYS.contains(&k) => Value::Bool(truthy(&val)),
                "risk_twel_enforcer_policy" => {
                    let s = val
                        .as_str()
                        .ok_or_else(|| anyhow!("{what} must be a string"))?;
                    let n = s.trim().to_ascii_lowercase();
                    if n != "reduce_overweight" && n != "reduce_portfolio" {
                        bail!("{what} must be reduce_overweight or reduce_portfolio");
                    }
                    Value::String(n)
                }
                "risk_we_excess_allowance_mode" => {
                    let n = if val.is_null() {
                        "bounded".to_string()
                    } else {
                        stringify(&val).trim().to_ascii_lowercase()
                    };
                    if n != "bounded" && n != "legacy_raw" {
                        bail!("{what} must be bounded or legacy_raw");
                    }
                    Value::String(n)
                }
                _ => f(float_or_zero(&val, &what)?),
            };
            out.insert(out_key.to_string(), v);
        }
        let hsl = match symbol {
            Some(s) => self.hsl_for_symbol(pside, s)?,
            None => self.hsl_from_bot_value(pside)?,
        };
        out.insert("hsl_enabled".into(), Value::Bool(truthy(&hsl["enabled"])));
        for (out_key, k) in [
            ("hsl_red_threshold", "red_threshold"),
            ("hsl_ema_span_minutes", "ema_span_minutes"),
            (
                "hsl_cooldown_minutes_after_red",
                "cooldown_minutes_after_red",
            ),
            (
                "hsl_no_restart_drawdown_threshold",
                "no_restart_drawdown_threshold",
            ),
        ] {
            out.insert(out_key.into(), f(as_f64(&hsl[k], k)?));
        }
        out.insert(
            "hsl_restart_after_red_policy".into(),
            normalize_hsl_policy(&hsl["restart_after_red_policy"])?,
        );
        out.insert(
            "hsl_tier_ratio_yellow".into(),
            f(as_f64(&hsl["tier_ratios"]["yellow"], "tier_ratios.yellow")?),
        );
        out.insert(
            "hsl_tier_ratio_orange".into(),
            f(as_f64(&hsl["tier_ratios"]["orange"], "tier_ratios.orange")?),
        );
        out.insert(
            "hsl_orange_tier_mode".into(),
            Value::String(stringify(&hsl["orange_tier_mode"])),
        );
        out.insert(
            "hsl_panic_close_order_type".into(),
            Value::String(stringify(&hsl["panic_close_order_type"])),
        );
        Ok(Value::Object(out))
    }

    /// `_strategy_params_to_rust_dict(pside, symbol)`.
    pub fn strategy_params(&self, pside: &str, symbol: Option<&str>) -> Result<Value> {
        let kind = &self.strategy_kind_name;
        let side_cfg = active_strategy_side(&self.global_side(pside), kind);
        let override_cfg = symbol
            .and_then(|s| self.override_side(s, pside))
            .map(|o| active_strategy_side(o, kind))
            .unwrap_or(Value::Null);
        let spec = strategy_spec(self.strategy_kind);
        let mut result = Map::new();
        let mut keys: Vec<Vec<&str>> = Vec::new();
        for p in &spec.parameters {
            if p.side != pside || p.config_path.len() < 3 {
                continue;
            }
            let path: Vec<&str> = p.config_path[2..].to_vec();
            set_path(&mut result, &path, f(p.default));
            if !keys.contains(&path) {
                keys.push(path);
            }
        }
        for p in &spec.fixed_parameters {
            if p.side != pside || p.config_path.len() < 3 {
                continue;
            }
            let path: Vec<&str> = p.config_path[2..].to_vec();
            set_path(&mut result, &path, Value::String(p.default.to_string()));
            if !keys.contains(&path) {
                keys.push(path);
            }
        }
        // `get_strategy_param_keys` also lists keys of the other side; since a
        // side never carries the other side's keys, iterating this side's keys is
        // equivalent.
        for path in &keys {
            let from_override = get_path(&override_cfg, path);
            let from_side = get_path(&side_cfg, path);
            if let Some(v) = from_override.or(from_side) {
                set_path(
                    &mut result,
                    path,
                    normalize_strategy_value(kind, path, v.clone())?,
                );
            }
        }
        Ok(Value::Object(result))
    }
}

fn stringify(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => "None".to_string(),
        Value::Bool(true) => "True".to_string(),
        Value::Bool(false) => "False".to_string(),
        other => other.to_string(),
    }
}

fn normalize_hsl_policy(v: &Value) -> Result<Value> {
    let policy = if v.is_null() {
        "threshold".to_string()
    } else {
        stringify(v)
    };
    if !["always", "threshold", "never"].contains(&policy.as_str()) {
        bail!(
            "hsl_restart_after_red_policy must be one of always, threshold, never; got {policy:?}"
        );
    }
    Ok(Value::String(policy))
}

/// `get_active_strategy_side(bot_side, kind)` = `bot_side["strategy"][kind]` or `{}`.
fn active_strategy_side(bot_side: &Value, kind: &str) -> Value {
    bot_side
        .get("strategy")
        .and_then(|s| s.get(kind))
        .filter(|v| v.is_object())
        .cloned()
        .unwrap_or_else(|| Value::Object(Map::new()))
}

fn get_path<'a>(root: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut cur = root;
    for p in path {
        cur = cur.as_object()?.get(*p)?;
    }
    Some(cur)
}

fn set_path(root: &mut Map<String, Value>, path: &[&str], value: Value) {
    if path.len() == 1 {
        root.insert(path[0].to_string(), value);
        return;
    }
    let entry = root
        .entry(path[0].to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if !entry.is_object() {
        *entry = Value::Object(Map::new());
    }
    set_path(entry.as_object_mut().expect("object"), &path[1..], value);
}

/// `_normalize_strategy_side_value`: only `entry.ema_gate_mode` for
/// `trailing_martingale` is normalised.
fn normalize_strategy_value(kind: &str, path: &[&str], v: Value) -> Result<Value> {
    if kind == "trailing_martingale" && path == ["entry", "ema_gate_mode"] {
        let s = v
            .as_str()
            .ok_or_else(|| anyhow!("entry.ema_gate_mode must be a string"))?;
        let n = s.trim().to_ascii_lowercase();
        if !["disabled", "all", "initial", "reentry"].contains(&n.as_str()) {
            bail!("entry.ema_gate_mode must be one of disabled, all, initial, reentry");
        }
        return Ok(Value::String(n));
    }
    Ok(v)
}

/// passivbot's `get_template_config()` (v8.1.0 `config/schema.py`), `bot` and
/// `live` sections, dumped on 2026-09-07. The loader fills every missing key
/// from it; the runner does the same for the keys it reads
/// (`bot.<pside>.<group>.<key>` and `live.<key>`).
const TEMPLATE_V8_1_0: &str = include_str!("../assets/template_v8.1.0.json");

/// Deep-fill missing keys of `config.bot.{long,short}` (grouped sections) and
/// `config.live` from the template; existing values are never touched.
fn fill_template_defaults(config: &mut Value) -> Result<()> {
    let template: Value = serde_json::from_str(TEMPLATE_V8_1_0).context("template asset")?;
    fn fill(dst: &mut Value, src: &Value) {
        let (Some(d), Some(s)) = (dst.as_object_mut(), src.as_object()) else {
            return;
        };
        for (k, v) in s {
            match d.get_mut(k) {
                None => {
                    d.insert(k.clone(), v.clone());
                }
                Some(existing) if existing.is_object() && v.is_object() => fill(existing, v),
                Some(_) => {}
            }
        }
    }
    for pside in PSIDES {
        let path = format!("/bot/{pside}");
        if let (Some(dst), Some(src)) = (config.pointer_mut(&path), template.pointer(&path)) {
            for group in ["risk", "forager", "hsl", "unstuck"] {
                if let (Some(d), Some(s)) = (dst.get_mut(group), src.get(group)) {
                    fill(d, s);
                } else if let (Some(dobj), Some(s)) = (dst.as_object_mut(), src.get(group)) {
                    dobj.entry(group.to_string()).or_insert_with(|| s.clone());
                }
            }
        }
    }
    if let (Some(dst), Some(src)) = (config.pointer_mut("/live"), template.get("live")) {
        fill(dst, src);
    }
    Ok(())
}

/// passivbot reads config files with `hjson.load`, whose number parser
/// returns an `int` for every integral literal (`0.0` -> `0`, `1909.0` ->
/// `1909`, `1e3` -> `1000`). Strategy parameters are emitted verbatim, so the
/// runner must give raw config numbers the same typing.
fn hjsonify_numbers(v: &mut Value) {
    match v {
        Value::Number(n) => {
            if let Some(x) = n.as_f64() {
                if n.is_f64() && x.is_finite() && x.fract() == 0.0 && x.abs() < 9.0e15 {
                    *v = Value::from(x as i64);
                }
            }
        }
        Value::Array(a) => a.iter_mut().for_each(hjsonify_numbers),
        Value::Object(o) => o.values_mut().for_each(hjsonify_numbers),
        _ => {}
    }
}

/// `normalize_bot_forager_config`: weights normalised to sum 1 in place
/// (grouped and flat copies), except an all-zero vector on a disabled side.
fn normalize_forager_weights(config: &mut Value) -> Result<()> {
    for pside in PSIDES {
        let side_path = format!("/bot/{pside}");
        let Some(side) = config.pointer(&side_path).cloned() else {
            continue;
        };
        let Some(weights) = grouped_value(&side, "forager_score_weights").cloned() else {
            continue;
        };
        let w = weights
            .as_object()
            .ok_or_else(|| anyhow!("bot.{pside}.forager.score_weights must be a dict"))?;
        let mut vals = [0.0; 3];
        for (i, k) in ["volume", "ema_readiness", "volatility"].iter().enumerate() {
            let v = w
                .get(*k)
                .ok_or_else(|| anyhow!("bot.{pside}.forager.score_weights.{k} missing"))?;
            vals[i] = as_f64(v, k)?;
            if !vals[i].is_finite() || vals[i] < 0.0 {
                bail!("bot.{pside}.forager.score_weights.{k} must be finite and non-negative");
            }
        }
        let total: f64 = vals.iter().sum();
        let twel = grouped_value(&side, "total_wallet_exposure_limit")
            .map(|v| as_f64(v, "twel"))
            .transpose()?
            .unwrap_or(0.0);
        let n = grouped_value(&side, "n_positions")
            .map(|v| as_f64(v, "n"))
            .transpose()?
            .unwrap_or(0.0);
        let enabled = twel > 0.0 && n.round_ties_even() as i64 > 0;
        if total <= 0.0 && !enabled {
            continue;
        }
        let normalized = if total <= 0.0 {
            json!({"volume": 0.0, "ema_readiness": 1.0, "volatility": 0.0})
        } else {
            json!({"volume": vals[0] / total, "ema_readiness": vals[1] / total, "volatility": vals[2] / total})
        };
        let side_mut = config
            .pointer_mut(&side_path)
            .and_then(Value::as_object_mut)
            .expect("side object");
        if let Some(g) = side_mut.get_mut("forager").and_then(Value::as_object_mut) {
            if g.contains_key("score_weights") {
                g.insert("score_weights".into(), normalized.clone());
            }
        }
        if side_mut.contains_key("forager_score_weights") {
            side_mut.insert("forager_score_weights".into(), normalized);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
    }

    fn load_config(name: &str) -> ConfigView {
        let text = std::fs::read_to_string(
            root().join(format!("tests/fixtures/configs/fake_v8/{name}.json")),
        )
        .unwrap();
        ConfigView::new(serde_json::from_str(&text).unwrap()).unwrap()
    }

    fn universe(cfg: &ConfigView) -> Vec<String> {
        let approved = cfg.live("approved_coins").unwrap();
        let mut coins: Vec<String> = match approved {
            Value::Object(o) => o
                .values()
                .flat_map(|v| {
                    v.as_array()
                        .unwrap()
                        .iter()
                        .map(|c| c.as_str().unwrap().to_string())
                })
                .collect(),
            Value::Array(a) => a.iter().map(|c| c.as_str().unwrap().to_string()).collect(),
            _ => panic!(),
        };
        coins.sort();
        coins.dedup();
        coins.iter().map(|c| format!("{c}/USDT:USDT")).collect()
    }

    /// Every committed fake_v8 recording of `name` must reproduce exactly from
    /// the public config: global_bot_params and each symbol side's bot_params /
    /// strategy_params (Value equality distinguishes 3 from 3.0, as Python does).
    fn check_fixture_set(name: &str) {
        let cfg = load_config(name);
        let symbols = universe(&cfg);
        let dir = root().join(format!("tests/fixtures/recordings/fake_v8/{name}"));
        let mut n = 0;
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if !path.to_string_lossy().ends_with(".in.json") {
                continue;
            }
            let rec: Value =
                serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
            for pside in PSIDES {
                let expected = &rec["global"]["global_bot_params"][pside];
                let got = cfg.bot_params(pside, None).unwrap();
                assert_eq!(
                    &got,
                    expected,
                    "{name}: global_bot_params.{pside} in {}",
                    path.display()
                );
            }
            for sym in rec["symbols"].as_array().unwrap() {
                let idx = sym["symbol_idx"].as_u64().unwrap() as usize;
                let symbol = &symbols[idx];
                for pside in PSIDES {
                    let got = cfg.bot_params(pside, Some(symbol)).unwrap();
                    assert_eq!(
                        &got,
                        &sym[pside]["bot_params"],
                        "{name}: {symbol} {pside} bot_params in {}",
                        path.display()
                    );
                    let got = cfg.strategy_params(pside, Some(symbol)).unwrap();
                    assert_eq!(
                        &got,
                        &sym[pside]["strategy_params"],
                        "{name}: {symbol} {pside} strategy_params in {}",
                        path.display()
                    );
                }
            }
            n += 1;
        }
        assert!(n > 0, "no recordings in {}", dir.display());
    }

    #[test]
    fn grid_v7_fixtures_reproduce() {
        check_fixture_set("grid_v7");
    }

    #[test]
    fn tm_fixtures_reproduce() {
        check_fixture_set("tm");
    }

    #[test]
    fn wallet_exposure_limit_rounding_and_override() {
        let cfg = load_config("grid_v7");
        assert_eq!(cfg.wallet_exposure_limit("long", None).unwrap(), json!(0.5));
        assert_eq!(
            cfg.wallet_exposure_limit("long", Some("XRP/USDT:USDT"))
                .unwrap(),
            json!(0.5)
        );
        assert_eq!(
            cfg.wallet_exposure_limit("short", None).unwrap(),
            json!(0.0)
        );
        assert_eq!(round8(3.35 / 7.0), 0.47857143);
    }
}
