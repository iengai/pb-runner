//! Live loop (PLAN P4.1 / P4.5 / P4.6): exchange state -> snapshot -> engine ->
//! planned orders. Execution (P4.3/P4.4) plugs in after `plan()`; until then
//! the loop runs in `--dry-run` and only logs the plan.
//!
//! Cross-cycle state kept here (docs/SNAPSHOT_SPEC.md section 8): the
//! hysteresis-snapped balance, 1m/1h candle buffers, fills since warmup
//! (trailing anchors), the engine's previous `symbol_states` and the order
//! churn gate history (docs/RECONCILE_SPEC.md 2.9).

use crate::bot_params::{ConfigView, PSIDES};
use crate::churn::{self, ChurnGate, ChurnParams};
use crate::cooldown::ExchangeCooldowns;
use crate::emas::{aggregate_1h, Candle, ONE_HOUR_MS, ONE_MIN_MS};
use crate::hsl::{
    balance_equity_timeline, hsl_pnl, realized_pnl_now, CycleInputs, FeePolicy, HslConfig, HslFill,
    HslPosition, HslState, RedObservation, Supervision, LONG, SHORT,
};
use crate::market_filter::{
    fetch_max_age_ms, MarketFilter, MarketSnapshot, SnapshotProvider,
    LIVE_MARKET_SNAPSHOT_MAX_AGE_MS,
};
use crate::reconcile::{self, OrderRec, PbMode, Plan, RecentExecution, ReconcileParams};
use crate::snapshot::{
    trailing_bundle, AccountState, CycleState, MarketParams, SideState, SnapshotBuilder,
    SymbolState,
};
use anyhow::{anyhow, bail, Context, Result};
use passivbot_rust::orchestrator::{
    compute_ideal_orders, ExecutableOrder, OrchestratorInput, OrchestratorOutput,
};
use passivbot_rust::types::TrailingPriceBundle;
use passivbot_rust::utils::hysteresis;
use pb_exchange_bybit::{
    ClosedPnl, ExchangeClient, ExchangeError, Fill, MarketSpec, OpenOrder, Position, PositionSide,
    Side,
};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
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

/// Result of one planning cycle.
pub struct CyclePlan {
    pub planned: Vec<PlannedOrder>,
    pub output: OrchestratorOutput,
    pub input: Value,
    pub plan: Plan,
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
    closed_pnl: Vec<ClosedPnl>,
    prev_hysteresis_balance: f64,
    balance_hysteresis_pct: f64,
    warmup_1m_minutes: u64,
    warmup_1h_hours: u64,
    /// `PB_modes[(symbol, pside)]` from the previous engine output (SPEC 1.6).
    pb_modes: HashMap<(String, PositionSide), PbMode>,
    /// Snapshot builder cross-cycle state (SNAPSHOT_SPEC 8): `PB_modes`,
    /// dynamic forager eligibility, close-EMA carry-forward, cooled symbols,
    /// HSL side modes.
    cycle: CycleState,
    /// Equity hard-stop-loss state machine (SNAPSHOT_SPEC 2.3 step 1,
    /// `hsl.rs`); `None` when no side enables it.
    hsl: Option<HslState>,
    /// Exchange-unavailable symbol cooldowns fed by write failures.
    cooldowns: ExchangeCooldowns,
    /// Order churn gate history and create-attempt window (SPEC 2.9).
    churn: ChurnGate,
    /// Ticker snapshot cache (`MarketSnapshotProvider`): planning reads it
    /// with the 5 s fetch TTL, the pre-create gate with the 10 s hard TTL.
    snapshots: SnapshotProvider,
    /// Pre-create snapshot freshness gate + distance filter (SPEC 2.10).
    market_filter: MarketFilter,
    /// Wall clock (`utc_ms`) and monotonic clock (`time.monotonic()`), injected
    /// so a harness can pin them to scenario time (`pb-mockrun`, D13/D17).
    wall: Arc<dyn Fn() -> u64 + Send + Sync>,
    mono: Arc<dyn Fn() -> f64 + Send + Sync>,
    /// `pb-mockrun` only (D17): reproduce the fake harness, where no
    /// background candle refresh runs, so forager cache-only symbols are
    /// never fetched (`cache_only_never_fetched`, pb:18244-18247).
    harness_secondary_never_fetched: bool,
    pub cycles: u64,
}

impl LiveRunner {
    pub fn new(cfg: ConfigView, client: Arc<dyn ExchangeClient>) -> Result<Self> {
        Self::with_clocks(
            cfg,
            client,
            Arc::new(now_ms),
            Arc::new(churn::monotonic_seconds),
        )
    }

    /// `new` with explicit clocks: `wall` replaces `now_ms()` (scenario
    /// time in the mock harness), `mono` the churn gate's monotonic seconds.
    pub fn with_clocks(
        cfg: ConfigView,
        client: Arc<dyn ExchangeClient>,
        wall: Arc<dyn Fn() -> u64 + Send + Sync>,
        mono: Arc<dyn Fn() -> f64 + Send + Sync>,
    ) -> Result<Self> {
        let pct = cfg
            .live("balance_hysteresis_snap_pct")
            .and_then(Value::as_f64)
            .unwrap_or(0.02);
        let churn = ChurnGate::new(ChurnParams::from_config(&cfg));
        let market_filter = MarketFilter::from_config(&cfg)?;
        let hsl_cfg = HslConfig::from_config(&cfg)?;
        let hsl = hsl_cfg.any_enabled().then(|| HslState::new(hsl_cfg));
        Ok(Self {
            cfg,
            client,
            markets: BTreeMap::new(),
            candles: HashMap::new(),
            fills: Vec::new(),
            closed_pnl: Vec::new(),
            prev_hysteresis_balance: 0.0,
            balance_hysteresis_pct: pct,
            warmup_1m_minutes: 0,
            warmup_1h_hours: 0,
            pb_modes: HashMap::new(),
            cycle: CycleState::default(),
            hsl,
            cooldowns: ExchangeCooldowns::new(),
            churn,
            snapshots: SnapshotProvider::new(),
            market_filter,
            wall,
            mono,
            harness_secondary_never_fetched: false,
            cycles: 0,
        })
    }

    /// Fake-harness compatibility (D17): mark every symbol's candles as never
    /// fetched by a background refresh, so the snapshot builder treats
    /// forager cache-only symbols as unavailable exactly like the Python bot
    /// under `run_fake_live.py`. Never set on a real exchange.
    pub fn set_harness_secondary_never_fetched(&mut self, on: bool) {
        self.harness_secondary_never_fetched = on;
    }

    pub fn config(&self) -> &ConfigView {
        &self.cfg
    }

    /// `_handle_order_write_failures`: classify every rejected write and arm
    /// the exchange-unavailable cooldown for proven suspensions
    /// (`cooldown.rs`; no Bybit error classifies at v8.1.0).
    pub fn note_write_failures(&mut self, failures: &[(String, ExchangeError)], now_ms: u64) {
        for (symbol, err) in failures {
            self.cooldowns
                .note_write_failure(&self.cfg, symbol, err, now_ms);
        }
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

    /// Only finalized candles are ever read (`latest_finalized_range`,
    /// `trailing_bundle`), so a timeframe is refetched only once a new
    /// bucket has started since the last stored candle: one 1m and one 1h
    /// request per symbol per minute/hour, as the Python candle manager
    /// paces `update_ohlcvs_1m`, instead of two per cycle.
    async fn refresh_candles(&mut self, symbol: &str, now: u64) -> Result<()> {
        let (m1_since, h1_since) = {
            let buf = self.candles.entry(symbol.to_string()).or_default();
            let m1_since = match buf.m1.last() {
                Some(c) => candle_refresh_since(c[0] as u64, now, ONE_MIN_MS),
                None => Some(now.saturating_sub(self.warmup_1m_minutes * ONE_MIN_MS)),
            };
            let h1_since = match buf.h1.last() {
                Some(c) => candle_refresh_since(c[0] as u64, now, ONE_HOUR_MS),
                None => Some(now.saturating_sub(self.warmup_1h_hours * ONE_HOUR_MS)),
            };
            (m1_since, h1_since)
        };
        if let Some(since) = m1_since {
            let m1 = self
                .client
                .fetch_ohlcv(symbol, "1m", Some(since), 1000)
                .await?;
            let buf = self.candles.get_mut(symbol).expect("buffer");
            merge_candles(&mut buf.m1, m1, self.warmup_1m_minutes as usize + 1500);
        }
        if let Some(since) = h1_since {
            let h1 = self
                .client
                .fetch_ohlcv(symbol, "1h", Some(since), 1000)
                .await?;
            let buf = self.candles.get_mut(symbol).expect("buffer");
            merge_candles(&mut buf.h1, h1, self.warmup_1h_hours as usize + 50);
        }
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
        let now = (self.wall)();
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
        self.closed_pnl = self.client.fetch_closed_pnl(Some(since), None).await?;
        tracing::info!(
            fills = self.fills.len(),
            closed_pnl = self.closed_pnl.len(),
            "fill history loaded"
        );
        if self.hsl.is_some() {
            self.initialize_hsl(now).await?;
        }
        Ok(symbols)
    }

    /// `_equity_hard_stop_initialize_from_history` at start-up (D16): the
    /// Python bot never reads a persisted HSL state for decisions, it
    /// replays the balance/equity timeline from the fill history and 1m
    /// closes over `pnls_max_lookback_days`. The runner does the same from
    /// its warmup buffers; a position whose symbol has no 1m candle at a
    /// timeline minute keeps its previous close (carry-forward, like the
    /// Python timeline), and the present sample uses the newest 1m close.
    async fn initialize_hsl(&mut self, now: u64) -> Result<()> {
        let balance = self.client.fetch_balance().await?.total_usdt;
        let positions = hsl_positions(&self.client.fetch_positions().await?);
        let Some(hsl) = self.hsl.as_mut() else {
            return Ok(());
        };
        let lookback = hsl.cfg.lookback;
        let start = lookback.event_history_start_ms(now);
        let candles = &self.candles;
        let markets = &self.markets;
        let close_at = |symbol: &str, minute: u64| -> Option<f64> {
            let m1 = &candles.get(symbol)?.m1;
            let i = m1.partition_point(|c| (c[0] as u64) < minute);
            m1.get(i).filter(|c| c[0] as u64 == minute).map(|c| c[4])
        };
        let c_mult = |symbol: &str| markets.get(symbol).map_or(1.0, |m| m.contract_size);
        let fills = hsl_fills(&self.fills, &self.closed_pnl, &hsl.cfg.fee, &c_mult);
        let qty_step = |symbol: &str| markets.get(symbol).map_or(0.0, |m| m.qty_step);
        let known = |symbol: &str| markets.contains_key(symbol);
        let timeline = balance_equity_timeline(
            now, balance, lookback, &fills, &positions, &close_at, &c_mult, &qty_step, &known,
        );
        let latest = |symbol: &str| candles.get(symbol).and_then(|b| b.m1.last()).map(|c| c[4]);
        let mut unrealized = [0.0; 2];
        for p in &positions {
            let Some(price) = latest(&p.symbol) else {
                continue;
            };
            unrealized[p.pside] += hsl_pnl(p.pside, p.price, price, p.size, c_mult(&p.symbol));
        }
        hsl.initialize_from_history(
            now,
            balance,
            &fills,
            &timeline,
            realized_pnl_now(&fills, start, None),
            [
                realized_pnl_now(&fills, start, Some(LONG)),
                realized_pnl_now(&fills, start, Some(SHORT)),
            ],
            unrealized,
        )?;
        self.cycle.hsl = hsl.modes();
        tracing::info!(
            timeline_rows = timeline.len(),
            long = ?self.cycle.hsl.sides[LONG],
            short = ?self.cycle.hsl.sides[SHORT],
            "hsl initialized from history"
        );
        Ok(())
    }

    /// Startup exchange configuration (Python `update_exchange_config*`):
    /// hedge mode for the account, then margin mode + leverage per symbol.
    pub async fn configure_exchange(&self, symbols: &[String]) -> Result<()> {
        let hedge = self
            .cfg
            .live("hedge_mode")
            .map(|v| v.as_bool().unwrap_or(true))
            .unwrap_or(true);
        if hedge {
            self.client.set_hedge_mode().await?;
        }
        let leverage = self
            .cfg
            .live("leverage")
            .and_then(Value::as_f64)
            .unwrap_or(10.0);
        let margin = match self
            .cfg
            .live("margin_mode_preference")
            .and_then(Value::as_str)
        {
            Some("isolated") => pb_exchange_bybit::MarginMode::Isolated,
            _ => pb_exchange_bybit::MarginMode::Cross,
        };
        for s in symbols {
            self.client.configure_symbol(s, leverage, margin).await?;
        }
        tracing::info!(
            symbols = symbols.len(),
            leverage,
            ?margin,
            hedge,
            "exchange configured"
        );
        Ok(())
    }

    /// One planning cycle: refresh state, build the snapshot, run the engine,
    /// reconcile against the open orders.
    pub async fn plan(&mut self, recent: &[RecentExecution]) -> Result<CyclePlan> {
        let now = (self.wall)();
        let wall = self.wall.clone();
        let balance = self.client.fetch_balance().await?;
        let positions = self.client.fetch_positions().await?;
        let orders = self.client.fetch_open_orders().await?;
        if let Some(last) = self.fills.last().map(|f| f.timestamp_ms) {
            let new = self.client.fetch_fills(None, Some(last), None).await?;
            merge_fills(&mut self.fills, new);
        }
        if let Some(last) = self.closed_pnl.last().map(|p| p.timestamp_ms) {
            let new = self.client.fetch_closed_pnl(Some(last), None).await?;
            for p in new {
                if !self
                    .closed_pnl
                    .iter()
                    .any(|x| x.order_id == p.order_id && x.timestamp_ms == p.timestamp_ms)
                {
                    self.closed_pnl.push(p);
                }
            }
            self.closed_pnl
                .sort_by_key(|p| (p.timestamp_ms, p.order_id.clone()));
        }
        let symbols = {
            let builder = SnapshotBuilder::new(&self.cfg)?;
            self.universe_symbols(&builder, &positions, &orders)
        };
        for s in &symbols {
            self.refresh_candles(s, now).await?;
        }
        // Planning market snapshots (`get_orchestrator_market_snapshots`):
        // cached tickers younger than the fetch TTL are reused, the rest
        // come from one bulk `fetch_tickers`; `fetched_ms` is the local
        // receive time and drives the pre-create freshness gate later.
        let client = self.client.clone();
        let fetch = || client.fetch_tickers();
        let planning: HashMap<String, MarketSnapshot> = self
            .snapshots
            .get_snapshots(
                &fetch,
                &symbols,
                fetch_max_age_ms(LIVE_MARKET_SNAPSHOT_MAX_AGE_MS),
                &*wall,
            )
            .await
            .context("planning market snapshots")?;

        // Balance hysteresis (SPEC 5.1).
        let raw = balance.total_usdt;
        if !raw.is_finite() {
            bail!("exchange balance is not finite");
        }
        // HSL (SNAPSHOT_SPEC 2.3 step 1): sample the raw balance and the
        // realized/unrealized pnl, then run the red supervisor on the sides
        // whose red latch is active; the side modes go into the snapshot.
        if let Some(hsl) = self.hsl.as_mut() {
            let markets = &self.markets;
            let c_mult = |symbol: &str| markets.get(symbol).map_or(1.0, |m| m.contract_size);
            let fills = hsl_fills(&self.fills, &self.closed_pnl, &hsl.cfg.fee, &c_mult);
            let hsl_pos = hsl_positions(&positions);
            let start = hsl.cfg.lookback.event_history_start_ms(now);
            let mut unrealized = [0.0; 2];
            for p in &hsl_pos {
                let Some(t) = planning.get(&p.symbol) else {
                    continue;
                };
                unrealized[p.pside] += hsl_pnl(p.pside, p.price, t.last, p.size, c_mult(&p.symbol));
            }
            let inp = CycleInputs {
                now_ms: now,
                balance: raw,
                realized_pnl_total: realized_pnl_now(&fills, start, None),
                realized_pnl: [
                    realized_pnl_now(&fills, start, Some(LONG)),
                    realized_pnl_now(&fills, start, Some(SHORT)),
                ],
                unrealized_pnl: unrealized,
                positions: &hsl_pos,
                fills: &fills,
            };
            hsl.check(&inp)?;
            for pside in [LONG, SHORT] {
                if !hsl.red_active(pside) {
                    continue;
                }
                let obs = hsl_observation(&positions, &orders, pside);
                let step = hsl.supervise_red(pside, obs, &inp, Supervision::Production)?;
                tracing::warn!(
                    pside = PSIDES[pside],
                    finalized = step.finalized,
                    panic = step.needs_panic_execution,
                    "hsl red supervisor"
                );
            }
            let modes = hsl.modes();
            if modes != self.cycle.hsl {
                tracing::warn!(long = ?modes.sides[LONG], short = ?modes.sides[SHORT], "hsl modes");
            }
            self.cycle.hsl = modes;
        }
        let builder = SnapshotBuilder::new(&self.cfg)?.with_hsl(&self.cycle.hsl);
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
        let lookback_days = self
            .cfg
            .live("pnls_max_lookback_days")
            .and_then(Value::as_f64)
            .unwrap_or(30.0);
        let (cum_max, cum_last) = if builder.uses_realized_pnl()? {
            realized_pnl_cumsum(
                &self.fills,
                &self.closed_pnl,
                now.saturating_sub((lookback_days * 86_400_000.0) as u64),
            )
        } else {
            (0.0, 0.0)
        };
        let account = AccountState {
            timestamp_ms: now,
            balance: snapped,
            balance_raw: raw,
            realized_pnl_cumsum_max: cum_max,
            realized_pnl_cumsum_last: cum_last,
        };

        let mut states = Vec::with_capacity(symbols.len());
        for symbol in &symbols {
            let m = self
                .markets
                .get(symbol)
                .ok_or_else(|| anyhow!("no market for {symbol}"))?;
            let t = planning
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
                // SPEC 4.1 step 3: Python fetches candles from the first
                // full minute after the fill up to the latest finalized
                // minute; until one has closed the fetch is empty and the
                // side is `missing_exact_trailing_candles` (pb:9651), i.e.
                // unavailable for the cycle right after a fill (D17).
                let minute_closed_after = |a: u64| {
                    let first = (a / ONE_MIN_MS + 1) * ONE_MIN_MS;
                    let latest = (now / ONE_MIN_MS * ONE_MIN_MS).saturating_sub(ONE_MIN_MS);
                    latest >= first
                };
                let (trailing, avail) = match (required, anchor) {
                    (true, Some(a)) if !minute_closed_after(a) => {
                        (TrailingPriceBundle::default(), false)
                    }
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
                    last_increase_fill_ts: last_increase_fill_ts(
                        &self.cfg,
                        &self.fills,
                        symbol,
                        pside,
                        now,
                    ),
                    has_entry_order: has_entry,
                    has_open_order: !side_orders.is_empty(),
                }
            };
            let long = side_state(PositionSide::Long);
            let short = side_state(PositionSide::Short);
            // `candles_available` = the candle manager has fetched this
            // symbol (SNAPSHOT_SPEC 3.7); the runner refreshes every
            // universe symbol each cycle, except under the harness flag.
            let candles_available = !buf.m1.is_empty() && !self.harness_secondary_never_fetched;
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
                candles_available,
                long,
                short,
            });
        }
        // Exchange-unavailable cooldowns of this cycle (SPEC 2.3), then the
        // snapshot with the carried state.
        self.cycle.exchange_unavailable = self.cooldowns.active(now, Some(&symbols));
        let snap = builder.build(&account, &states, &mut self.cycle)?;
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

        // PB_modes from this output's symbol_states (SPEC 1.6), kept both
        // for the reconciler and for the next snapshot (SNAPSHOT_SPEC 3.6).
        let active: Vec<(usize, bool, bool)> = out
            .diagnostics
            .symbol_states
            .iter()
            .map(|st| (st.symbol_idx, st.long.active, st.short.active))
            .collect();
        self.cycle.pb_modes = builder.pb_modes_after_cycle(&snap, &active);
        self.pb_modes = self
            .cycle
            .pb_modes
            .iter()
            .map(|((symbol, pside), mode)| {
                let ps = if pside == "long" {
                    PositionSide::Long
                } else {
                    PositionSide::Short
                };
                ((symbol.clone(), ps), PbMode::parse(mode))
            })
            .collect();

        // Reconcile (SPEC 2).
        let hedge_mode = self
            .cfg
            .live("hedge_mode")
            .map(|v| v.as_bool().unwrap_or(true))
            .unwrap_or(true);
        let params = ReconcileParams {
            hedge_mode,
            match_tolerance: self
                .cfg
                .live("order_match_tolerance_pct")
                .and_then(Value::as_f64)
                .unwrap_or(0.0002),
            max_cancels_per_batch: self
                .cfg
                .live("max_n_cancellations_per_batch")
                .and_then(Value::as_u64)
                .unwrap_or(5) as usize,
            max_creates_per_batch: self
                .cfg
                .live("max_n_creations_per_batch")
                .and_then(Value::as_u64)
                .unwrap_or(3) as usize,
        };
        let pos_size = |symbol: &str, pside: PositionSide| -> f64 {
            positions
                .iter()
                .find(|p| p.symbol == symbol && p.pside == pside)
                .map_or(0.0, |p| p.size)
        };
        let last = |symbol: &str| -> f64 { planning.get(symbol).map_or(0.0, |t| t.last) };
        let mut ideal: Vec<OrderRec> = reconcile::to_executable(&planned, &pos_size, &last);
        // Churn evidence (SPEC 2.9) on the executable ideals, before reconciliation.
        let mono = (self.mono)();
        let risk_pairs = risk_active_pairs(&out, &snap.symbols);
        self.churn.evaluate(&symbols, &mut ideal, &risk_pairs, mono);
        let open: Vec<OrderRec> = orders
            .iter()
            .map(|o| reconcile::normalize_open_order(o, hedge_mode))
            .collect();
        let pb_modes = &self.pb_modes;
        let modes = |symbol: &str, pside: PositionSide| -> PbMode {
            pb_modes
                .get(&(symbol.to_string(), pside))
                .copied()
                .unwrap_or(PbMode::Normal)
        };
        let mut plan = reconcile::reconcile(&ideal, &open, &modes, &last, recent, now, &params);
        // SPEC 3.1 step 8: pre-create market snapshot freshness gate and
        // distance filter, then (step 9) churn admission + create capacity.
        let outcome = self
            .market_filter
            .filter_fresh_creations(
                &fetch,
                &mut self.snapshots,
                &planning,
                std::mem::take(&mut plan.creates),
                &*wall,
            )
            .await;
        plan.creates = outcome.kept;
        plan.skipped_market_snapshot = outcome.skipped_snapshot;
        plan.skipped_market_distance = outcome.skipped_distance;
        reconcile::admit_and_cap(&mut plan, &params, Some((&mut self.churn, mono)));
        self.cycles += 1;
        Ok(CyclePlan {
            planned,
            output: out,
            input: snap.input,
            plan,
        })
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

/// `order_churn_risk_active_pairs_from_rust_output`: `(symbol, pside)` pairs
/// with a risk-critical order or a `loss_gate_blocks` entry this cycle.
pub fn risk_active_pairs(
    out: &OrchestratorOutput,
    symbols: &[String],
) -> HashSet<(String, PositionSide)> {
    let pside_of = |p: &passivbot_rust::orchestrator::PositionSide| match serde_json::to_value(p)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
    {
        Some(s) if s == "long" => PositionSide::Long,
        _ => PositionSide::Short,
    };
    let mut pairs = HashSet::new();
    for o in &out.orders {
        let critical = serde_json::to_value(o.execution_priority)
            .ok()
            .and_then(|v| v.as_str().map(|s| s == "risk_critical"))
            .unwrap_or(false);
        if critical {
            if let Some(symbol) = symbols.get(o.symbol_idx) {
                pairs.insert((symbol.clone(), pside_of(&o.pside)));
            }
        }
    }
    for b in &out.diagnostics.loss_gate_blocks {
        if let Some(symbol) = symbols.get(b.symbol_idx) {
            pairs.insert((symbol.clone(), pside_of(&b.pside)));
        }
    }
    pairs
}

/// `Some(since)` when a new `period_ms` bucket has started after the last
/// stored candle `last_ts` (which may be the still-open one, refetched so it
/// gets its final values); `None` while nothing can have been finalized.
pub fn candle_refresh_since(last_ts: u64, now_ms: u64, period_ms: u64) -> Option<u64> {
    (now_ms / period_ms * period_ms > last_ts).then_some(last_ts)
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

/// SPEC 5.2: chronological net pnl (`closedPnl` of the order attached to its
/// last fill, plus signed fees: paid fees are negative) over the lookback;
/// returns `(max(0, running max), last)`.
pub fn realized_pnl_cumsum(fills: &[Fill], closed: &[ClosedPnl], start_ms: u64) -> (f64, f64) {
    let mut pnl_by_order: HashMap<&str, f64> = HashMap::new();
    for p in closed {
        *pnl_by_order.entry(p.order_id.as_str()).or_default() += p.pnl;
    }
    let mut last_fill_of_order: HashMap<&str, &str> = HashMap::new();
    for f in fills {
        last_fill_of_order.insert(f.order_id.as_str(), f.id.as_str());
    }
    let mut cum = 0.0;
    let mut max = 0.0f64;
    let mut any = false;
    for f in fills.iter().filter(|f| f.timestamp_ms >= start_ms) {
        let pnl = if last_fill_of_order.get(f.order_id.as_str()) == Some(&f.id.as_str()) {
            pnl_by_order
                .get(f.order_id.as_str())
                .copied()
                .unwrap_or(0.0)
        } else {
            0.0
        };
        let fee_paid = if f.fee < 0.0 {
            f.fee.abs()
        } else {
            -f.fee.abs()
        };
        cum += pnl + fee_paid;
        max = max.max(cum);
        any = true;
    }
    if any {
        (max.max(0.0), cum)
    } else {
        (0.0, 0.0)
    }
}

fn pside_idx(pside: PositionSide) -> usize {
    match pside {
        PositionSide::Long => LONG,
        PositionSide::Short => SHORT,
    }
}

/// HSL fill events (`_equity_hard_stop_fill_events`): the same ledger the
/// realized-pnl cumsum reads (SPEC 5.2) with the `closedPnl` of an order
/// attached to its last fill and the fill manager's signed `fee_paid`
/// ([`FeePolicy`]), ordered by (timestamp, id) like the fill manager.
pub fn hsl_fills(
    fills: &[Fill],
    closed: &[ClosedPnl],
    fee: &FeePolicy,
    c_mult: &dyn Fn(&str) -> f64,
) -> Vec<HslFill> {
    let mut pnl_by_order: HashMap<&str, f64> = HashMap::new();
    for p in closed {
        *pnl_by_order.entry(p.order_id.as_str()).or_default() += p.pnl;
    }
    let mut last_fill_of_order: HashMap<&str, &str> = HashMap::new();
    for f in fills {
        last_fill_of_order.insert(f.order_id.as_str(), f.id.as_str());
    }
    let mut out: Vec<(u64, &str, HslFill)> = fills
        .iter()
        .filter(|f| f.qty > 0.0 && f.price > 0.0)
        .map(|f| {
            let pnl = if last_fill_of_order.get(f.order_id.as_str()) == Some(&f.id.as_str()) {
                pnl_by_order
                    .get(f.order_id.as_str())
                    .copied()
                    .unwrap_or(0.0)
            } else {
                0.0
            };
            let fee_paid = fee.signed_fee_paid(f.fee, f.qty.abs() * f.price * c_mult(&f.symbol));
            let pside = pside_idx(f.pside);
            (
                f.timestamp_ms,
                f.id.as_str(),
                HslFill {
                    timestamp_ms: f.timestamp_ms,
                    symbol: f.symbol.clone(),
                    pside,
                    qty: f.qty.abs(),
                    price: f.price,
                    increase: (pside == LONG) == (f.side == Side::Buy),
                    pnl,
                    fee_paid,
                },
            )
        })
        .collect();
    out.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
    out.into_iter().map(|r| r.2).collect()
}

pub fn hsl_positions(positions: &[Position]) -> Vec<HslPosition> {
    positions
        .iter()
        .filter(|p| p.size != 0.0)
        .map(|p| HslPosition {
            symbol: p.symbol.clone(),
            pside: pside_idx(p.pside),
            size: p.size,
            price: p.entry_price,
        })
        .collect()
}

/// `_equity_hard_stop_count_open_positions` +
/// `_equity_hard_stop_count_blocking_open_orders` for one side: open
/// positions, entry (non-reduce-only) orders and reduce-only orders whose
/// custom id does not mark a panic close.
pub fn hsl_observation(
    positions: &[Position],
    orders: &[OpenOrder],
    pside: usize,
) -> RedObservation {
    let mut obs = RedObservation {
        n_positions: positions
            .iter()
            .filter(|p| pside_idx(p.pside) == pside && p.size != 0.0)
            .count(),
        ..RedObservation::default()
    };
    for o in orders.iter().filter(|o| pside_idx(o.pside) == pside) {
        if !o.reduce_only {
            obs.entry_orders += 1;
        } else if !reconcile::pb_order_type_from_custom_id(o.client_id.as_deref()).contains("panic")
        {
            obs.nonpanic_close_orders += 1;
        }
    }
    obs
}

/// SPEC 4.4 `fill_ts` part: newest fill that increased the position on
/// `pside` within the cooldown lookback, when the side's cooldown is > 0.
fn last_increase_fill_ts(
    cfg: &ConfigView,
    fills: &[Fill],
    symbol: &str,
    pside: PositionSide,
    now: u64,
) -> Option<u64> {
    let pside_name = if pside == PositionSide::Long {
        "long"
    } else {
        "short"
    };
    let cd = cfg
        .bp(pside_name, "risk_entry_cooldown_minutes", Some(symbol))
        .ok()?
        .as_f64()
        .unwrap_or(0.0);
    if cd <= 0.0 {
        return None;
    }
    let lookback_min = if cd < 1.0 { 1.0 } else { cd.ceil() + 1.0 };
    let start = now.saturating_sub((lookback_min * 60_000.0) as u64);
    let increases = |f: &Fill| match pside {
        PositionSide::Long => f.side == Side::Buy,
        PositionSide::Short => f.side == Side::Sell,
    };
    fills
        .iter()
        .rev()
        .find(|f| f.symbol == symbol && f.pside == pside && f.timestamp_ms >= start && increases(f))
        .map(|f| f.timestamp_ms)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candle_refresh_only_after_a_new_bucket() {
        let t = 1_700_000_000_000 / ONE_MIN_MS * ONE_MIN_MS; // open 1m bucket
        assert_eq!(candle_refresh_since(t, t + 30_000, ONE_MIN_MS), None);
        assert_eq!(candle_refresh_since(t, t + ONE_MIN_MS, ONE_MIN_MS), Some(t));
        assert_eq!(
            candle_refresh_since(t, t + 5 * ONE_MIN_MS, ONE_MIN_MS),
            Some(t)
        );
        let h = t / ONE_HOUR_MS * ONE_HOUR_MS;
        assert_eq!(
            candle_refresh_since(h, h + ONE_HOUR_MS - 1, ONE_HOUR_MS),
            None
        );
        assert_eq!(
            candle_refresh_since(h, h + ONE_HOUR_MS, ONE_HOUR_MS),
            Some(h)
        );
    }

    fn fill(id: &str, order: &str, ts: u64, side: Side, fee: f64) -> Fill {
        Fill {
            id: id.into(),
            order_id: order.into(),
            client_id: None,
            symbol: "A/USDT:USDT".into(),
            side,
            pside: PositionSide::Long,
            qty: 1.0,
            price: 1.0,
            fee,
            is_maker: true,
            timestamp_ms: ts,
        }
    }

    #[test]
    fn realized_pnl_cumsum_attaches_closed_pnl_to_last_fill_and_negates_fees() {
        let fills = vec![
            fill("f1", "o1", 100, Side::Buy, 0.1),
            fill("f2", "o2", 200, Side::Sell, 0.1),
            fill("f3", "o2", 300, Side::Sell, 0.1),
        ];
        let closed = vec![ClosedPnl {
            order_id: "o2".into(),
            symbol: "A/USDT:USDT".into(),
            pside: PositionSide::Long,
            pnl: 5.0,
            timestamp_ms: 300,
        }];
        // cumsum: -0.1, -0.2, -0.3 + 5 = 4.7 -> max 4.7, last 4.7
        let (max, last) = realized_pnl_cumsum(&fills, &closed, 0);
        assert!((last - 4.7).abs() < 1e-12 && (max - 4.7).abs() < 1e-12);
        // losses only: max clamps at 0
        let (max, last) = realized_pnl_cumsum(&fills[..1], &[], 0);
        assert_eq!(max, 0.0);
        assert!((last + 0.1).abs() < 1e-12);
        // lookback excludes old fills
        let (_, last) = realized_pnl_cumsum(&fills, &closed, 250);
        assert!((last - 4.9).abs() < 1e-12);
    }

    #[test]
    fn merge_candles_dedups_and_trims() {
        let mut buf = vec![
            [1.0, 0.0, 0.0, 0.0, 1.0, 0.0],
            [2.0, 0.0, 0.0, 0.0, 2.0, 0.0],
        ];
        merge_candles(
            &mut buf,
            vec![
                [2.0, 0.0, 0.0, 0.0, 2.5, 0.0],
                [3.0, 0.0, 0.0, 0.0, 3.0, 0.0],
            ],
            2,
        );
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0][4], 2.5);
        assert_eq!(buf[1][0], 3.0);
    }
}
