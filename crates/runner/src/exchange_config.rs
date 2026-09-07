//! Lazy per-symbol exchange configuration (margin mode + leverage), ported
//! from `Passivbot.update_exchange_configs` (passivbot.py:10149-10240) and
//! its Bybit hook `update_exchange_config_by_symbols`
//! (`exchanges/bybit.py:553-582`).
//!
//! Python configures a symbol the first time it is about to create an order
//! on it (`execute_order_plan`, executor.py:861-950): symbols already done
//! are skipped, a failed symbol gets an exponential backoff
//! (`_exchange_config_backoff_seconds`: `min(5 * 2**(attempt-1), 60) +
//! U(0, 0.5)` s on Bybit, 10256-10262) and is retried on a later wave, a
//! rate-limit-like failure stops the wave's remaining configurations
//! (10221-10228), and every success is followed by a 0.2 s pause on Bybit
//! (`_exchange_config_success_pause_seconds`, 10264-10275). Creates for
//! symbols still pending are skipped without touching the error budget
//! (`_pending_exchange_config_consumes_error_budget` returns `False`,
//! 10245-10249). Nothing here ever aborts the process.

use crate::bot_params::ConfigView;
use pb_exchange_bybit::{ExchangeClient, MarginMode, MarketSpec};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

/// Bybit backoff base (`_exchange_config_backoff_seconds`: 5.0 for bybit and
/// hyperliquid, 2.0 elsewhere).
pub const BACKOFF_BASE_S: f64 = 5.0;
pub const BACKOFF_MAX_S: f64 = 60.0;
/// `_exchange_config_success_pause_seconds`: 0.2 s on Bybit.
pub const SUCCESS_PAUSE_S: f64 = 0.2;

/// `min(base * 2**(attempt-1), 60)` without the `U(0, 0.5)` jitter (added by
/// the caller so the pure arithmetic stays testable).
pub fn backoff_seconds(attempt: u32) -> f64 {
    (BACKOFF_BASE_S * 2f64.powi(attempt.saturating_sub(1).min(30) as i32)).min(BACKOFF_MAX_S)
}

/// `CCXTBot._calc_leverage_for_symbol` (ccxt_bot.py:989-1050) for the cross
/// margin path: `min(int(config_get(["live","leverage"], symbol)),
/// int(max_leverage))`; a symbol without leverage metadata uses the
/// configured value ("max leverage unavailable from exchange metadata").
pub fn leverage_for(configured: f64, max_leverage: Option<f64>) -> f64 {
    let configured = configured.max(1.0).floor();
    match max_leverage {
        Some(m) if m.is_finite() && m >= 1.0 => configured.min(m.floor()),
        _ => configured,
    }
}

#[derive(Debug, Default, Clone)]
pub struct ConfigOutcome {
    /// Symbols configured (now or earlier): creates on them may proceed.
    pub configured: HashSet<String>,
    /// Symbols whose configuration failed in this call
    /// (`_last_exchange_config_failed_attempt_symbols`).
    pub failed: Vec<String>,
    /// Symbols skipped because their backoff has not expired.
    pub in_backoff: Vec<String>,
}

pub struct ExchangeConfigurator {
    /// `live.leverage` (template default 10), per coin override aware.
    leverage_by_symbol: HashMap<String, f64>,
    default_leverage: f64,
    max_leverage: HashMap<String, f64>,
    /// `already_updated_exchange_config_symbols`.
    done: HashSet<String>,
    /// `_exchange_config_retry_attempts`.
    attempts: HashMap<String, u32>,
    /// `_exchange_config_retry_after_ms`.
    retry_after_ms: HashMap<String, u64>,
}

impl ExchangeConfigurator {
    /// `live.leverage` from the config and `coin_overrides[coin].live.leverage`
    /// (`config_get(["live", "leverage"], symbol)`, passivbot.py:4561-4590).
    pub fn from_config(cfg: &ConfigView) -> Self {
        let default_leverage = cfg.live("leverage").and_then(Value::as_f64).unwrap_or(10.0);
        let mut leverage_by_symbol = HashMap::new();
        if let Some(overrides) = cfg
            .config()
            .get("coin_overrides")
            .and_then(Value::as_object)
        {
            for (coin, o) in overrides {
                if let Some(l) = o.pointer("/live/leverage").and_then(Value::as_f64) {
                    leverage_by_symbol.insert(format!("{coin}/USDT:USDT"), l);
                }
            }
        }
        Self {
            leverage_by_symbol,
            default_leverage,
            max_leverage: HashMap::new(),
            done: HashSet::new(),
            attempts: HashMap::new(),
            retry_after_ms: HashMap::new(),
        }
    }

    /// Market leverage caps (`self.max_leverage[symbol]` from
    /// `set_market_specific_settings`); refreshed on every market reload.
    pub fn set_markets<'a>(&mut self, markets: impl IntoIterator<Item = &'a MarketSpec>) {
        self.max_leverage = markets
            .into_iter()
            .filter(|m| m.max_leverage > 0.0)
            .map(|m| (m.symbol.clone(), m.max_leverage))
            .collect();
    }

    pub fn leverage(&self, symbol: &str) -> f64 {
        let configured = self
            .leverage_by_symbol
            .get(symbol)
            .copied()
            .unwrap_or(self.default_leverage);
        leverage_for(configured, self.max_leverage.get(symbol).copied())
    }

    /// Bybit margin mode policy (`_resolve_margin_policy_for_symbol`,
    /// ccxt_bot.py:909-928): cross unless the market is isolated-only, which
    /// no Bybit linear market is; `margin_mode_preference` only decides
    /// whether an isolated-only market blocks entries.
    pub fn margin_mode(&self, _symbol: &str) -> MarginMode {
        MarginMode::Cross
    }

    pub fn is_configured(&self, symbol: &str) -> bool {
        self.done.contains(symbol)
    }

    /// `update_exchange_configs(symbols)`: configure, in order, every symbol
    /// not yet done and not in backoff; returns the configured set plus the
    /// per-call failure evidence. `sleep` performs the success pause (and
    /// lets tests skip it).
    pub async fn update(
        &mut self,
        client: &Arc<dyn ExchangeClient>,
        symbols: &[String],
        now_ms: u64,
        sleep: &(dyn Fn(f64) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
              + Sync),
    ) -> ConfigOutcome {
        let mut out = ConfigOutcome::default();
        let mut ordered: Vec<&String> = Vec::new();
        for s in symbols {
            if !ordered.contains(&s) {
                ordered.push(s);
            }
        }
        for symbol in ordered {
            if self.done.contains(symbol) {
                out.configured.insert(symbol.clone());
                continue;
            }
            if self.retry_after_ms.get(symbol).copied().unwrap_or(0) > now_ms {
                out.in_backoff.push(symbol.clone());
                continue;
            }
            let leverage = self.leverage(symbol);
            let margin = self.margin_mode(symbol);
            match client.configure_symbol(symbol, leverage, margin).await {
                Ok(()) => {
                    self.done.insert(symbol.clone());
                    out.configured.insert(symbol.clone());
                    self.attempts.remove(symbol);
                    self.retry_after_ms.remove(symbol);
                    tracing::debug!(%symbol, leverage, ?margin, "[config] exchange config set");
                    sleep(SUCCESS_PAUSE_S).await;
                }
                Err(e) => {
                    out.failed.push(symbol.clone());
                    let attempts = self.attempts.get(symbol).copied().unwrap_or(0) + 1;
                    self.attempts.insert(symbol.clone(), attempts);
                    let backoff = backoff_seconds(attempts) + jitter_s();
                    self.retry_after_ms
                        .insert(symbol.clone(), now_ms + (backoff * 1000.0) as u64);
                    if e.is_rate_limit_like() {
                        tracing::warn!(
                            "[rate] exchange config update hit rate limit for {symbol}; retrying in {backoff:.1}s"
                        );
                        break;
                    }
                    tracing::warn!(
                        "[config] exchange config update failed | symbol={symbol} retry_in={backoff:.1}s attempt={attempts} error={e}"
                    );
                }
            }
        }
        out
    }

    /// Diagnostic view: symbol -> (attempts, retry_after_ms).
    pub fn pending(&self) -> BTreeMap<String, (u32, u64)> {
        self.attempts
            .iter()
            .map(|(s, a)| {
                (
                    s.clone(),
                    (*a, self.retry_after_ms.get(s).copied().unwrap_or(0)),
                )
            })
            .collect()
    }
}

/// `random.uniform(0.0, 0.5)` from the process clock (no rand dependency).
fn jitter_s() -> f64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    (nanos % 500_000) as f64 / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use pb_exchange_bybit::*;
    use std::sync::Mutex;

    #[test]
    fn backoff_and_leverage_arithmetic() {
        assert_eq!(backoff_seconds(1), 5.0);
        assert_eq!(backoff_seconds(2), 10.0);
        assert_eq!(backoff_seconds(4), 40.0);
        assert_eq!(backoff_seconds(5), 60.0);
        assert_eq!(backoff_seconds(40), 60.0);
        assert_eq!(leverage_for(10.0, Some(100.0)), 10.0);
        assert_eq!(leverage_for(10.0, Some(5.0)), 5.0);
        assert_eq!(leverage_for(10.0, None), 10.0);
        assert_eq!(leverage_for(10.0, Some(0.0)), 10.0);
    }

    /// Client whose `configure_symbol` fails for symbols listed in `fail`.
    struct Fake {
        fail: Mutex<HashMap<String, ExchangeError>>,
        calls: Mutex<Vec<(String, f64)>>,
    }

    #[async_trait]
    impl ExchangeClient for Fake {
        async fn load_markets(&self) -> Result<Vec<MarketSpec>, ExchangeError> {
            Ok(vec![])
        }
        async fn fetch_balance(&self) -> Result<Balance, ExchangeError> {
            unreachable!()
        }
        async fn fetch_positions(&self) -> Result<Vec<Position>, ExchangeError> {
            unreachable!()
        }
        async fn fetch_open_orders(&self) -> Result<Vec<OpenOrder>, ExchangeError> {
            unreachable!()
        }
        async fn fetch_tickers(&self) -> Result<Vec<Ticker>, ExchangeError> {
            unreachable!()
        }
        async fn fetch_ohlcv(
            &self,
            _: &str,
            _: &str,
            _: Option<u64>,
            _: usize,
        ) -> Result<Vec<Candle>, ExchangeError> {
            unreachable!()
        }
        async fn fetch_fills(
            &self,
            _: Option<&str>,
            _: Option<u64>,
            _: Option<u64>,
        ) -> Result<Vec<Fill>, ExchangeError> {
            unreachable!()
        }
        async fn fetch_closed_pnl(
            &self,
            _: Option<u64>,
            _: Option<u64>,
        ) -> Result<Vec<ClosedPnl>, ExchangeError> {
            unreachable!()
        }
        async fn create_orders(&self, _: &[NewOrder]) -> Vec<OrderResult<OpenOrder>> {
            unreachable!()
        }
        async fn cancel_orders(&self, _: &[(String, String)]) -> Vec<OrderResult<CancelAck>> {
            unreachable!()
        }
        async fn set_hedge_mode(&self) -> Result<(), ExchangeError> {
            Ok(())
        }
        async fn configure_symbol(
            &self,
            symbol: &str,
            leverage: f64,
            _: MarginMode,
        ) -> Result<(), ExchangeError> {
            self.calls
                .lock()
                .unwrap()
                .push((symbol.to_string(), leverage));
            match self.fail.lock().unwrap().get(symbol) {
                Some(e) => Err(e.clone()),
                None => Ok(()),
            }
        }
    }

    fn no_sleep(_: f64) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn cfg() -> ConfigView {
        let mut v: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/configs/fake_v8/grid_v7.json"
        ))
        .unwrap();
        v["coin_overrides"] = serde_json::json!({"ADA": {"live": {"leverage": 3}}});
        ConfigView::new(v).unwrap()
    }

    fn rt<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn lazy_configuration_with_backoff_and_rate_limit_stop() {
        let fake = Arc::new(Fake {
            fail: Mutex::new(HashMap::new()),
            calls: Mutex::new(Vec::new()),
        });
        let client: Arc<dyn ExchangeClient> = fake.clone();
        let mut ec = ExchangeConfigurator::from_config(&cfg());
        ec.set_markets(&[MarketSpec {
            symbol: "BTC/USDT:USDT".into(),
            id: "BTCUSDT".into(),
            qty_step: 0.001,
            price_step: 0.1,
            min_qty: 0.001,
            min_cost: 0.1,
            min_notional: None,
            contract_size: 1.0,
            max_leverage: 5.0,
            maker_fee: 0.0,
            taker_fee: 0.0,
            active: true,
        }]);
        // Configured 10, BTC capped at the market's 5, ADA overridden to 3.
        assert_eq!(ec.leverage("BTC/USDT:USDT"), 5.0);
        assert_eq!(ec.leverage("ADA/USDT:USDT"), 3.0);
        assert_eq!(ec.leverage("DOGE/USDT:USDT"), 10.0);

        let syms: Vec<String> = ["BTC/USDT:USDT", "ADA/USDT:USDT", "DOGE/USDT:USDT"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        fake.fail.lock().unwrap().insert(
            "ADA/USDT:USDT".into(),
            ExchangeError::Rejected {
                code: "110043".into(),
                msg: "x".into(),
            },
        );
        let out = rt(ec.update(&client, &syms, 1_000_000, &no_sleep));
        assert_eq!(out.configured.len(), 2);
        assert_eq!(out.failed, vec!["ADA/USDT:USDT".to_string()]);
        assert!(!ec.is_configured("ADA/USDT:USDT"));
        // Within the 5 s backoff: skipped, no exchange call.
        let n = fake.calls.lock().unwrap().len();
        let out = rt(ec.update(&client, &syms, 1_002_000, &no_sleep));
        assert_eq!(out.in_backoff, vec!["ADA/USDT:USDT".to_string()]);
        assert_eq!(fake.calls.lock().unwrap().len(), n);
        // After the backoff the retry succeeds and the symbol is done for good.
        fake.fail.lock().unwrap().clear();
        let out = rt(ec.update(&client, &syms, 1_010_000, &no_sleep));
        assert_eq!(out.configured.len(), 3);
        assert!(ec.pending().is_empty());
        // Already-configured symbols are never re-sent.
        let n = fake.calls.lock().unwrap().len();
        rt(ec.update(&client, &syms, 1_020_000, &no_sleep));
        assert_eq!(fake.calls.lock().unwrap().len(), n);

        // A rate-limit-like failure stops the rest of the wave.
        let mut ec = ExchangeConfigurator::from_config(&cfg());
        fake.fail.lock().unwrap().insert(
            "BTC/USDT:USDT".into(),
            ExchangeError::RateLimited {
                retry_after_ms: 1000,
            },
        );
        let n = fake.calls.lock().unwrap().len();
        let out = rt(ec.update(&client, &syms, 2_000_000, &no_sleep));
        assert_eq!(fake.calls.lock().unwrap().len(), n + 1);
        assert!(out.configured.is_empty());
        assert_eq!(out.failed, vec!["BTC/USDT:USDT".to_string()]);
    }
}
