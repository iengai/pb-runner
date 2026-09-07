//! Live loop (PLAN P4.1 / P4.5 / P4.6): exchange state -> snapshot -> engine ->
//! planned orders. Execution (P4.3/P4.4) plugs in after `plan()`; until then
//! the loop runs in `--dry-run` and only logs the plan.
//!
//! Cross-cycle state kept here (docs/SNAPSHOT_SPEC.md section 8): the
//! hysteresis-snapped balance, 1m/1h candle buffers, fills since warmup
//! (trailing anchors), and the engine's previous `symbol_states`.

use crate::bot_params::{ConfigView, PSIDES};
use crate::emas::{aggregate_1h, Candle, ONE_HOUR_MS, ONE_MIN_MS};
use crate::snapshot::{
    trailing_bundle, AccountState, MarketParams, SideState, SnapshotBuilder, SymbolState,
};
use anyhow::{anyhow, bail, Context, Result};
use passivbot_rust::orchestrator::{
    compute_ideal_orders, ExecutableOrder, OrchestratorInput, OrchestratorOutput,
};
use passivbot_rust::types::TrailingPriceBundle;
use passivbot_rust::utils::hysteresis;
use pb_exchange_bybit::{
    ExchangeClient, Fill, MarketSpec, OpenOrder, Position, PositionSide, Side, Ticker,
};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// `api-keys.json` entry for `live.user` (docs/CONTRACT.md).
#[derive(Debug, Clone)]
pub struct ApiKey {
    pub exchange: String,
    pub key: String,
    pub secret: String,
}

pub fn load_api_key(path: &std::path::Path, user: &str) -> Result<ApiKey> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let v: Value = serde_json::from_str(&text)?;
    let e = v
        .get(user)
        .ok_or_else(|| anyhow!("user {user:?} not in {}", path.display()))?;
    let key = e
        .get("key")
        .or(e.get("apiKey"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let secret = e.get("secret").and_then(Value::as_str).unwrap_or_default();
    if key.is_empty() || secret.is_empty() {
        bail!("user {user:?}: key/secret missing");
    }
    Ok(ApiKey {
        exchange: e
            .get("exchange")
            .and_then(Value::as_str)
            .unwrap_or("bybit")
            .to_string(),
        key: key.to_string(),
        secret: secret.to_string(),
    })
}

/// One planned exchange action derived from an engine order.
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedOrder {
    pub symbol: String,
    pub side: Side,
    pub pside: PositionSide,
    pub qty: f64,
    pub price: f64,
    pub order_type: String,
    pub reduce_only: bool,
    pub market: bool,
    pub risk_critical: bool,
}

#[derive(Debug, Default)]
struct CandleBuffer {
    m1: Vec<Candle>,
    h1: Vec<Candle>,
}

pub struct LiveRunner {
    cfg: ConfigView,
    client: Arc<dyn ExchangeClient>,
    markets: BTreeMap<String, MarketSpec>,
    candles: HashMap<String, CandleBuffer>,
    fills: Vec<Fill>,
    prev_hysteresis_balance: f64,
    balance_hysteresis_pct: f64,
    warmup_1m_minutes: u64,
    warmup_1h_hours: u64,
    pub cycles: u64,
}

impl LiveRunner {
    pub fn new(cfg: ConfigView, client: Arc<dyn ExchangeClient>) -> Result<Self> {
        let pct = cfg
            .live("balance_hysteresis_snap_pct")
            .and_then(Value::as_f64)
            .unwrap_or(0.02);
        Ok(Self {
            cfg,
            client,
            markets: BTreeMap::new(),
            candles: HashMap::new(),
            fills: Vec::new(),
            prev_hysteresis_balance: 0.0,
            balance_hysteresis_pct: pct,
            warmup_1m_minutes: 0,
            warmup_1h_hours: 0,
            cycles: 0,
        })
    }

    pub fn config(&self) -> &ConfigView {
        &self.cfg
    }

    /// Largest spans over the universe -> warmup lengths (Python fetches
    /// `max_span * (1 + warmup_ratio)` candles per timeframe).
    fn warmup_lengths(
        &self,
        builder: &SnapshotBuilder<'_>,
        symbols: &[String],
    ) -> Result<(u64, u64)> {
        let ratio = self
            .cfg
            .live("warmup_ratio")
            .and_then(Value::as_f64)
            .unwrap_or(0.0)
            .max(0.0);
        let mut max_1m: f64 = 0.0;
        let mut max_1h: f64 = 0.0;
        for pside in PSIDES {
            for key in [
                "forager_volume_ema_span_1m",
                "forager_volatility_ema_span_1m",
            ] {
                max_1m = max_1m.max(self.cfg.bot_value(pside, key)?.as_f64().unwrap_or(0.0));
            }
        }
        for symbol in symbols {
            let (m1, h1) = builder.max_spans(symbol)?;
            max_1m = max_1m.max(m1);
            max_1h = max_1h.max(h1);
        }
        let m = ((max_1m * (1.0 + ratio)).ceil() as u64).max(10);
        let h = ((max_1h * (1.0 + ratio)).ceil() as u64).max(2);
        Ok((m, h))
    }

    /// Symbols the bot may trade: approved coins with a listed market, plus
    /// anything with a position or open order.
    fn universe_symbols(
        &self,
        builder: &SnapshotBuilder<'_>,
        positions: &[Position],
        orders: &[OpenOrder],
    ) -> Vec<String> {
        let mut set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for pside in PSIDES {
            for s in builder.approved(pside) {
                if self.markets.contains_key(s) {
                    set.insert(s.clone());
                }
            }
        }
        for coin in self.cfg.override_coins() {
            let s = format!("{coin}/USDT:USDT");
            if self.markets.contains_key(&s) {
                set.insert(s);
            }
        }
        set.extend(positions.iter().map(|p| p.symbol.clone()));
        set.extend(orders.iter().map(|o| o.symbol.clone()));
        set.into_iter().collect()
    }

    async fn refresh_candles(&mut self, symbol: &str, now: u64) -> Result<()> {
        let (m1_since, h1_since) = {
            let buf = self.candles.entry(symbol.to_string()).or_default();
            let m1_since = match buf.m1.last() {
                Some(c) => c[0] as u64,
                None => now.saturating_sub(self.warmup_1m_minutes * ONE_MIN_MS),
            };
            let h1_since = match buf.h1.last() {
                Some(c) => c[0] as u64,
                None => now.saturating_sub(self.warmup_1h_hours * ONE_HOUR_MS),
            };
            (m1_since, h1_since)
        };
        let m1 = self
            .client
            .fetch_ohlcv(symbol, "1m", Some(m1_since), 1000)
            .await?;
        let h1 = self
            .client
            .fetch_ohlcv(symbol, "1h", Some(h1_since), 1000)
            .await?;
        let buf = self.candles.get_mut(symbol).expect("buffer");
        merge_candles(&mut buf.m1, m1, self.warmup_1m_minutes as usize + 1500);
        merge_candles(&mut buf.h1, h1, self.warmup_1h_hours as usize + 50);
        Ok(())
    }

    /// Startup: markets, warmup lengths, candle history, fill history.
    pub async fn warmup(&mut self) -> Result<Vec<String>> {
        let markets = self.client.load_markets().await?;
        self.markets = markets.into_iter().map(|m| (m.symbol.clone(), m)).collect();
        tracing::info!(markets = self.markets.len(), "markets loaded");
        let (symbols, m, h) = {
            let builder = SnapshotBuilder::new(&self.cfg)?;
            let symbols = self.universe_symbols(&builder, &[], &[]);
            let (m, h) = self.warmup_lengths(&builder, &symbols)?;
            (symbols, m, h)
        };
        self.warmup_1m_minutes = m;
        self.warmup_1h_hours = h;
        tracing::info!(
            symbols = symbols.len(),
            warmup_1m = m,
            warmup_1h = h,
            "warmup"
        );
        let now = now_ms();
        for s in &symbols {
            self.refresh_candles(s, now).await?;
        }
        let lookback_days = self
            .cfg
            .live("pnls_max_lookback_days")
            .and_then(Value::as_f64)
            .unwrap_or(30.0);
        let since = now.saturating_sub((lookback_days * 86_400_000.0) as u64);
        self.fills = self.client.fetch_fills(None, Some(since), None).await?;
        tracing::info!(fills = self.fills.len(), "fill history loaded");
        Ok(symbols)
    }

    /// One planning cycle: refresh state, build the snapshot, run the engine.
    pub async fn plan(&mut self) -> Result<(Vec<PlannedOrder>, OrchestratorOutput, Value)> {
        let now = now_ms();
        let balance = self.client.fetch_balance().await?;
        let positions = self.client.fetch_positions().await?;
        let orders = self.client.fetch_open_orders().await?;
        let tickers: HashMap<String, Ticker> = self
            .client
            .fetch_tickers()
            .await?
            .into_iter()
            .map(|t| (t.symbol.clone(), t))
            .collect();
        if let Some(last) = self.fills.last().map(|f| f.timestamp_ms) {
            let new = self.client.fetch_fills(None, Some(last), None).await?;
            merge_fills(&mut self.fills, new);
        }
        let symbols = {
            let builder = SnapshotBuilder::new(&self.cfg)?;
            self.universe_symbols(&builder, &positions, &orders)
        };
        for s in &symbols {
            self.refresh_candles(s, now).await?;
        }
        let builder = SnapshotBuilder::new(&self.cfg)?;

        // Balance hysteresis (SPEC 5.1).
        let raw = balance.total_usdt;
        if !raw.is_finite() {
            bail!("exchange balance is not finite");
        }
        let snapped = if self.prev_hysteresis_balance == 0.0 {
            raw
        } else {
            hysteresis(
                raw,
                self.prev_hysteresis_balance,
                self.balance_hysteresis_pct,
            )
        };
        self.prev_hysteresis_balance = snapped;
        let account = AccountState {
            timestamp_ms: now,
            balance: snapped,
            balance_raw: raw,
            // TODO(P4.1): realized-pnl cumsum from closed-pnl history (SPEC 5.2).
            realized_pnl_cumsum_max: 0.0,
            realized_pnl_cumsum_last: 0.0,
        };

        let mut states = Vec::with_capacity(symbols.len());
        for symbol in &symbols {
            let m = self
                .markets
                .get(symbol)
                .ok_or_else(|| anyhow!("no market for {symbol}"))?;
            let t = tickers
                .get(symbol)
                .ok_or_else(|| anyhow!("no ticker for {symbol}"))?;
            let buf = self
                .candles
                .get(symbol)
                .ok_or_else(|| anyhow!("no candles for {symbol}"))?;
            let side_state = |pside: PositionSide| -> SideState {
                let pos = positions
                    .iter()
                    .find(|p| p.symbol == *symbol && p.pside == pside);
                let size = pos.map_or(0.0, |p| p.size);
                let price = pos.map_or(0.0, |p| p.entry_price);
                let side_orders: Vec<&OpenOrder> = orders
                    .iter()
                    .filter(|o| o.symbol == *symbol && o.pside == pside)
                    .collect();
                let has_entry = side_orders.iter().any(|o| !o.reduce_only);
                let anchor = self
                    .fills
                    .iter()
                    .filter(|f| f.symbol == *symbol && f.pside == pside)
                    .map(|f| f.timestamp_ms)
                    .max()
                    .or(pos.and_then(|p| p.updated_ms));
                let pside_name = if pside == PositionSide::Long {
                    "long"
                } else {
                    "short"
                };
                let required =
                    size != 0.0 && builder.is_trailing(symbol, pside_name).unwrap_or(false);
                let (trailing, avail) = match (required, anchor) {
                    (true, Some(a)) => match trailing_bundle(&buf.m1, a, now) {
                        Some(b) => (b, true),
                        None => (TrailingPriceBundle::default(), false),
                    },
                    (true, None) => (TrailingPriceBundle::default(), false),
                    _ => (TrailingPriceBundle::default(), true),
                };
                SideState {
                    position_size: size,
                    position_price: price,
                    trailing,
                    trailing_available: avail,
                    // TODO(P4.1): entry-cooldown fill timestamps (SPEC 4.4).
                    last_increase_fill_ts: None,
                    has_entry_order: has_entry,
                    has_open_order: !side_orders.is_empty(),
                }
            };
            let long = side_state(PositionSide::Long);
            let short = side_state(PositionSide::Short);
            states.push(SymbolState {
                symbol: symbol.clone(),
                market: MarketParams {
                    qty_step: m.qty_step,
                    price_step: m.price_step,
                    min_qty: m.min_qty,
                    min_cost: m.min_cost,
                    c_mult: m.contract_size,
                    maker_fee: m.maker_fee,
                    taker_fee: m.taker_fee,
                },
                active: true,
                bid: t.bid,
                ask: t.ask,
                min_cost_price: t.last,
                candles_1m: buf.m1.clone(),
                candles_1h: Some(buf.h1.clone()),
                candles_available: !buf.m1.is_empty(),
                long,
                short,
            });
        }
        let snap = builder.build(&account, &states)?;
        // Same text round trip as the Python bot (D8).
        let text = serde_json::to_string(&snap.input)?;
        let input: OrchestratorInput =
            serde_json::from_str(&text).context("engine input does not parse")?;
        let out =
            compute_ideal_orders(&input).map_err(|e| anyhow!("compute_ideal_orders: {e:?}"))?;
        let planned = out
            .orders
            .iter()
            .map(|o| to_planned(o, &snap.symbols))
            .collect::<Result<Vec<_>>>()?;
        self.cycles += 1;
        Ok((planned, out, snap.input))
    }

    pub fn sleep_between_cycles(&self) -> Duration {
        let s = self
            .cfg
            .live("execution_delay_seconds")
            .and_then(Value::as_f64)
            .unwrap_or(2.0);
        Duration::from_secs_f64(s.max(0.5))
    }
}

/// Engine order -> exchange action (side from the qty sign; closes are
/// reduce-only; market execution as decided by the engine).
fn to_planned(o: &ExecutableOrder, symbols: &[String]) -> Result<PlannedOrder> {
    let symbol = symbols
        .get(o.symbol_idx)
        .ok_or_else(|| anyhow!("order for unknown symbol_idx {}", o.symbol_idx))?
        .clone();
    let order_type = serde_json::to_value(o.order_type)?
        .as_str()
        .unwrap_or("unknown")
        .to_string();
    let pside = match serde_json::to_value(o.pside)?.as_str() {
        Some("long") => PositionSide::Long,
        _ => PositionSide::Short,
    };
    Ok(PlannedOrder {
        symbol,
        side: if o.qty > 0.0 { Side::Buy } else { Side::Sell },
        pside,
        qty: o.qty.abs(),
        price: o.price,
        reduce_only: order_type.contains("close"),
        market: serde_json::to_value(o.execution_type)?.as_str() == Some("market"),
        risk_critical: serde_json::to_value(o.execution_priority)?.as_str()
            == Some("risk_critical"),
        order_type,
    })
}

fn merge_candles(buf: &mut Vec<Candle>, new: Vec<Candle>, keep: usize) {
    for c in new {
        match buf.binary_search_by(|x| x[0].partial_cmp(&c[0]).unwrap()) {
            Ok(i) => buf[i] = c,
            Err(i) => buf.insert(i, c),
        }
    }
    if buf.len() > keep {
        let drop = buf.len() - keep;
        buf.drain(..drop);
    }
}

fn merge_fills(buf: &mut Vec<Fill>, new: Vec<Fill>) {
    let mut ids: std::collections::HashSet<String> = buf.iter().map(|f| f.id.clone()).collect();
    for f in new {
        if ids.insert(f.id.clone()) {
            buf.push(f);
        }
    }
    buf.sort_by_key(|f| (f.timestamp_ms, f.id.clone()));
}

/// Aggregate 1m to 1h when the exchange did not provide hourly candles.
pub fn hourly_from_minutes(m1: &[Candle]) -> Vec<Candle> {
    aggregate_1h(m1)
}
