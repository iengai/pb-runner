//! Exchange-unavailable symbol cooldowns (docs/SNAPSHOT_SPEC.md 2.2/2.3,
//! `Passivbot._activate_exchange_symbol_unavailable_cooldown`,
//! `_active_exchange_symbol_unavailable_cooldowns`, pb:9978-10122).
//!
//! When an order write fails with an error the exchange adapter classifies as
//! a proven venue-side symbol suspension, the symbol's entries are blocked
//! for `live.exchange_symbol_unavailable_cooldown_hours`: flat symbols become
//! non-tradable, held symbols get an entry-blocking mode override so closes
//! keep flowing (the planning policy lives in `snapshot.rs`). The state is
//! per process and in memory, exactly like the Python bot's.
//!
//! Classification is connector-owned in passivbot (`exchanges/<x>.py`
//! overrides `_classify_exchange_symbol_unavailable_error`); at v8.1.0 only
//! WEEX does (`-1058`), the Bybit adapter inherits the base `None`, so a Bybit
//! bot never activates a cooldown. [`classify_symbol_unavailable`] mirrors
//! that: it returns `None` for every Bybit error, and the state machine is
//! exercised by unit tests and by any future adapter with a real classifier.

use crate::bot_params::ConfigView;
use pb_exchange_bybit::ExchangeError;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// `config/schema.py::MAX_EXCHANGE_SYMBOL_UNAVAILABLE_COOLDOWN_HOURS`.
pub const MAX_COOLDOWN_HOURS: f64 = 24.0 * 365.25 * 100.0;

/// `_classify_exchange_symbol_unavailable_error` for the Bybit adapter: no
/// classifier at v8.1.0 (only WEEX has one), so nothing is ever a proven
/// suspension. Kept as the single place a future exact-code classifier goes.
pub fn classify_symbol_unavailable(_err: &ExchangeError) -> Option<&'static str> {
    None
}

/// `live.exchange_symbol_unavailable_cooldown_hours` as the activation
/// reads it: `None` when the value is not a finite number in
/// `[0, MAX_COOLDOWN_HOURS]` (Python logs an error and does not activate).
pub fn cooldown_hours(cfg: &ConfigView) -> Option<f64> {
    let raw = cfg.live("exchange_symbol_unavailable_cooldown_hours")?;
    let hours = match raw {
        Value::Number(n) => n.as_f64()?,
        Value::Bool(b) => f64::from(*b as u8),
        Value::String(s) => s.trim().parse::<f64>().ok()?,
        _ => return None,
    };
    if !hours.is_finite() || !(0.0..=MAX_COOLDOWN_HOURS).contains(&hours) {
        return None;
    }
    Some(hours)
}

/// `_exchange_symbol_unavailable_until_ms` + reasons.
#[derive(Debug, Default, Clone)]
pub struct ExchangeCooldowns {
    until_ms: BTreeMap<String, u64>,
    reasons: BTreeMap<String, String>,
}

impl ExchangeCooldowns {
    pub fn new() -> Self {
        Self::default()
    }

    /// `_activate_exchange_symbol_unavailable_cooldown` after classification:
    /// returns whether a cooldown was (re)armed. `cooldown_hours <= 0` is a
    /// no-op, like Python.
    pub fn activate(
        &mut self,
        symbol: &str,
        reason: &str,
        now_ms: u64,
        cooldown_hours: Option<f64>,
    ) -> bool {
        let Some(hours) = cooldown_hours else {
            return false;
        };
        if hours <= 0.0 {
            return false;
        }
        let cooldown_ms = ((hours * 3_600_000.0) as u64).max(1);
        let until = now_ms + cooldown_ms;
        let refreshed = self.until_ms.get(symbol).is_some_and(|u| *u > now_ms);
        self.until_ms.insert(symbol.to_string(), until);
        self.reasons.insert(symbol.to_string(), reason.to_string());
        tracing::warn!(
            symbol,
            reason,
            cooldown_hours = hours,
            until_ms = until,
            action = if refreshed {
                "refresh_entry_block_until_retry"
            } else {
                "block_entries_until_retry"
            },
            "[config] exchange temporarily disabled API trading for symbol"
        );
        true
    }

    /// Classify one failed order write and activate the cooldown when the
    /// adapter proves a suspension (`_handle_order_write_failures`).
    pub fn note_write_failure(
        &mut self,
        cfg: &ConfigView,
        symbol: &str,
        err: &ExchangeError,
        now_ms: u64,
    ) -> bool {
        match classify_symbol_unavailable(err) {
            Some(reason) => self.activate(symbol, reason, now_ms, cooldown_hours(cfg)),
            None => false,
        }
    }

    /// `_active_exchange_symbol_unavailable_cooldowns`: expire on the bot's
    /// clock, return the symbols still cooled (restricted to `symbols` when
    /// given).
    pub fn active(&mut self, now_ms: u64, symbols: Option<&[String]>) -> BTreeSet<String> {
        let expired: Vec<String> = self
            .until_ms
            .iter()
            .filter(|(_, until)| **until <= now_ms)
            .map(|(s, _)| s.clone())
            .collect();
        for s in expired {
            self.until_ms.remove(&s);
            let reason = self
                .reasons
                .remove(&s)
                .unwrap_or_else(|| "exchange_symbol_unavailable".into());
            tracing::info!(
                symbol = %s,
                reason,
                "[config] exchange symbol API-trading cooldown expired"
            );
        }
        self.until_ms
            .iter()
            .filter(|(s, until)| {
                **until > now_ms && symbols.is_none_or(|list| list.iter().any(|x| x == *s))
            })
            .map(|(s, _)| s.clone())
            .collect()
    }

    pub fn until(&self, symbol: &str) -> Option<u64> {
        self.until_ms.get(symbol).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(hours: Value) -> ConfigView {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/configs/fake_v8/grid_v7.json");
        let mut c: Value =
            serde_json::from_str(&std::fs::read_to_string(path).expect("public config"))
                .expect("config parses");
        c["live"]["exchange_symbol_unavailable_cooldown_hours"] = hours;
        ConfigView::new(c).expect("config view")
    }

    #[test]
    fn cooldown_hours_validation_mirrors_python() {
        assert_eq!(cooldown_hours(&cfg(Value::from(6.0))), Some(6.0));
        assert_eq!(cooldown_hours(&cfg(Value::from(0))), Some(0.0));
        assert_eq!(cooldown_hours(&cfg(Value::from("1.5"))), Some(1.5));
        assert_eq!(cooldown_hours(&cfg(Value::from(-1.0))), None);
        assert_eq!(
            cooldown_hours(&cfg(Value::from(MAX_COOLDOWN_HOURS + 1.0))),
            None
        );
        assert_eq!(cooldown_hours(&cfg(Value::Null)), None);
        assert_eq!(cooldown_hours(&cfg(Value::from("nan"))), None);
    }

    #[test]
    fn activate_expire_and_refresh() {
        let mut cd = ExchangeCooldowns::new();
        // cooldown_hours <= 0 or invalid: not activated (pb:10031, 10041)
        assert!(!cd.activate("A/USDT:USDT", "r", 1_000, Some(0.0)));
        assert!(!cd.activate("A/USDT:USDT", "r", 1_000, None));
        assert!(cd.active(1_000, None).is_empty());
        // 6 h from now
        assert!(cd.activate(
            "A/USDT:USDT",
            "weex_api_symbol_unavailable",
            1_000,
            Some(6.0)
        ));
        assert_eq!(cd.until("A/USDT:USDT"), Some(1_000 + 6 * 3_600_000));
        let act = cd.active(1_000 + 6 * 3_600_000 - 1, None);
        assert_eq!(act.len(), 1);
        // restricted to a symbol list
        assert!(cd
            .active(2_000, Some(&["B/USDT:USDT".to_string()]))
            .is_empty());
        // refresh pushes the deadline out
        assert!(cd.activate("A/USDT:USDT", "r", 5_000, Some(6.0)));
        assert_eq!(cd.until("A/USDT:USDT"), Some(5_000 + 6 * 3_600_000));
        // `until <= now` expires (strictly greater keeps it)
        assert!(cd.active(5_000 + 6 * 3_600_000, None).is_empty());
        assert_eq!(cd.until("A/USDT:USDT"), None);
        // tiny cooldowns still last at least 1 ms
        assert!(cd.activate("A/USDT:USDT", "r", 10, Some(1e-12)));
        assert_eq!(cd.until("A/USDT:USDT"), Some(11));
    }

    #[test]
    fn bybit_errors_never_classify_as_suspension() {
        let c = cfg(Value::from(6.0));
        let mut cd = ExchangeCooldowns::new();
        for e in [
            ExchangeError::Rejected {
                code: "-1058".into(),
                msg: "symbol unavailable".into(),
            },
            ExchangeError::Rejected {
                code: "10001".into(),
                msg: "params error".into(),
            },
            ExchangeError::Network("timeout".into()),
            ExchangeError::Other("x".into()),
        ] {
            assert!(!cd.note_write_failure(&c, "A/USDT:USDT", &e, 0));
        }
        assert!(cd.active(0, None).is_empty());
    }
}
