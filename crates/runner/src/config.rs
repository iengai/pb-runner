//! Minimal view of a passivbot live config: only what the runner needs to
//! decide whether it may run it. Full parsing (bot params, coin_overrides,
//! forager settings) is done by re-using the pinned engine's own config
//! types where possible (P1.3) rather than re-implementing the schema here.

use anyhow::{bail, Context, Result};
use serde_json::Value;

#[derive(Debug)]
#[allow(dead_code)] // `raw` is handed to the snapshot builder in P4
pub struct LiveConfig {
    pub engine_major: u32,
    pub exchange: String,
    pub strategy_kind: String,
    pub approved_coins_long: Vec<String>,
    pub raw: Value,
}

impl LiveConfig {
    /// `expected_major` is the engine line compiled into this binary. A config
    /// from another line is refused: a strategy is only proven on the engine it
    /// was validated on (same rule as pbtb-rust `domain::engine`).
    pub fn parse(text: &str, expected_major: u32) -> Result<Self> {
        let raw: Value = serde_json::from_str(text).context("config is not valid JSON")?;
        let engine_major = match raw.get("config_version") {
            Some(Value::String(stamp)) => stamp
                .trim()
                .trim_start_matches(['v', 'V'])
                .split('.')
                .next()
                .unwrap_or_default()
                .parse::<u32>()
                .with_context(|| format!("config_version {stamp:?} is not semver-like"))?,
            Some(other) => bail!("config_version must be a string, got {other}"),
            // Pre-7.12 templates carry no stamp; only the v8 schema nests
            // wallet exposure under bot.<side>.risk.
            None => {
                if has_v8_risk_block(&raw) {
                    8
                } else {
                    7
                }
            }
        };
        if engine_major != expected_major {
            bail!(
                "config targets engine line {engine_major} but this pb-runner binary is built for line {expected_major} (docs/CONTRACT.md)"
            );
        }
        let live = raw.get("live").context("config lacks `live`")?;
        let exchange = live
            .get("user")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let strategy_kind = live
            .get("strategy_kind")
            .and_then(Value::as_str)
            .unwrap_or("trailing_grid_v7")
            .to_string();
        let approved_coins_long = live
            .get("approved_coins")
            .and_then(|a| a.get("long"))
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        Ok(Self {
            engine_major,
            exchange,
            strategy_kind,
            approved_coins_long,
            raw,
        })
    }
}

fn has_v8_risk_block(config: &Value) -> bool {
    ["long", "short"].iter().any(|side| {
        config
            .get("bot")
            .and_then(|b| b.get(side))
            .and_then(|s| s.get("risk"))
            .is_some()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_other_line() {
        let e = LiveConfig::parse(r#"{"config_version":"v7.12.0","live":{}}"#, 8).unwrap_err();
        assert!(e.to_string().contains("built for line 8"));
    }

    #[test]
    fn accepts_matching_line() {
        let c = LiveConfig::parse(
            r#"{"config_version":"v8.1.0","live":{"user":"bybit_01","strategy_kind":"trailing_grid_v7","approved_coins":{"long":["BTC","ETH"],"short":[]}}}"#,
            8,
        )
        .unwrap();
        assert_eq!(c.engine_major, 8);
        assert_eq!(c.approved_coins_long.len(), 2);
    }

    #[test]
    fn unstamped_config_classified_by_shape() {
        let v7 = LiveConfig::parse(
            r#"{"live":{},"bot":{"long":{"total_wallet_exposure_limit":2}}}"#,
            7,
        )
        .unwrap();
        assert_eq!(v7.engine_major, 7);
        let v8 = LiveConfig::parse(r#"{"live":{},"bot":{"long":{"risk":{}}}}"#, 8).unwrap();
        assert_eq!(v8.engine_major, 8);
    }
}
