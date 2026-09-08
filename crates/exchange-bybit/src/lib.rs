//! Exchange boundary of the runner.
//!
//! The runner never talks to an exchange directly; it talks to
//! [`ExchangeClient`]. The first implementation is a hand-written Bybit v5
//! USDT-linear client ([`bybit::BybitClient`], docs/DECISIONS.md D11) whose
//! request shapes and field reads mirror what passivbot's Python adapter does
//! through ccxt (docs/PORT_INVENTORY.md section 3). A mock implementation for
//! paper trading and tests is planned in P5.
//!
//! The method set is exactly what the Python live loop uses: nothing more.

pub mod bybit;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, thiserror::Error)]
pub enum ExchangeError {
    #[error("network: {0}")]
    Network(String),
    #[error("rate limited: retry after {retry_after_ms} ms")]
    RateLimited { retry_after_ms: u64 },
    #[error("authentication: {0}")]
    Auth(String),
    #[error("rejected by exchange: {code} {msg}")]
    Rejected { code: String, msg: String },
    #[error("not supported: {0}")]
    NotSupported(String),
    #[error("malformed response: {0}")]
    Malformed(String),
    #[error("{0}")]
    Other(String),
}

impl ExchangeError {
    /// Exchange return code, when the error came from a Bybit envelope.
    pub fn code(&self) -> Option<&str> {
        match self {
            ExchangeError::Rejected { code, .. } => Some(code),
            _ => None,
        }
    }

    /// `Passivbot._is_rate_limit_like_exception` (passivbot.py:10247-10251):
    /// ccxt `RateLimitExceeded`, or a message containing `rate limit`,
    /// `too many`, `429` or `10006`.
    pub fn is_rate_limit_like(&self) -> bool {
        if matches!(self, ExchangeError::RateLimited { .. }) {
            return true;
        }
        let msg = self.to_string().to_ascii_lowercase();
        ["rate limit", "too many", "429", "10006"]
            .iter()
            .any(|t| msg.contains(t))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PositionSide {
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MarginMode {
    Cross,
    Isolated,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Balance {
    /// passivbot's "balance": UTA equity minus perpetual unrealised pnl.
    pub total_usdt: f64,
    pub available_usdt: f64,
    /// Raw account type string (`UNIFIED`, `CONTRACT`, ...).
    pub account_type: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub pside: PositionSide,
    pub size: f64,
    pub entry_price: f64,
    pub leverage: Option<f64>,
    pub margin_mode: Option<MarginMode>,
    pub updated_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenOrder {
    pub id: String,
    pub client_id: Option<String>,
    pub symbol: String,
    pub side: Side,
    pub pside: PositionSide,
    pub qty: f64,
    pub price: f64,
    pub reduce_only: bool,
    pub created_ms: Option<u64>,
}

/// Execution type of a new order: the engine's `execution_type`
/// (`limit` / `market`), passed to the exchange as ccxt `type`
/// (Python `execute_order`, passivbot.py:22351-22367: `type=order.get("type", "limit")`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewOrder {
    pub client_id: String,
    pub symbol: String,
    pub side: Side,
    pub pside: PositionSide,
    pub qty: f64,
    /// Ignored by the exchange for market orders (ccxt only sends `price`
    /// for limit orders, `bybit.py::create_order_request`).
    pub price: f64,
    pub reduce_only: bool,
    /// `timeInForce: PostOnly`; never applied to market orders (ccxt
    /// refuses `postOnly` market orders, `handle_post_only`).
    pub post_only: bool,
    pub order_type: OrderType,
}

impl NewOrder {
    pub fn is_market(&self) -> bool {
        self.order_type == OrderType::Market
    }
}

/// Outcome of one acknowledged cancel. `already_gone` = the exchange said
/// the order no longer exists (Python `execute_cancellation`'s
/// `_ambiguous_cancel_success_result`, passivbot.py:22373-22408): the cancel
/// counts as success but the symbol is marked state-dirty for the wave.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelAck {
    pub id: String,
    pub already_gone: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Ticker {
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub last: f64,
    pub quote_volume_24h: f64,
}

/// One fill (execution) as returned by `/v5/execution/list`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fill {
    pub id: String,
    pub order_id: String,
    pub client_id: Option<String>,
    pub symbol: String,
    pub side: Side,
    pub pside: PositionSide,
    pub qty: f64,
    pub price: f64,
    pub fee: f64,
    pub is_maker: bool,
    pub timestamp_ms: u64,
}

/// One closed-pnl record (`/v5/position/closed-pnl`): realized pnl of the
/// order that reduced/closed a position (Python: `exchanges/bybit.py::fetch_pnl`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClosedPnl {
    pub order_id: String,
    pub symbol: String,
    /// `sell` closes a long, `buy` closes a short.
    pub pside: PositionSide,
    pub pnl: f64,
    pub timestamp_ms: u64,
}

/// `[ts_ms, open, high, low, close, volume]`
pub type Candle = [f64; 6];

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketSpec {
    /// Unified symbol, e.g. `BTC/USDT:USDT`.
    pub symbol: String,
    /// Exchange id, e.g. `BTCUSDT`.
    pub id: String,
    pub qty_step: f64,
    pub price_step: f64,
    pub min_qty: f64,
    /// What passivbot ends up with as `min_costs[symbol]`:
    /// `market["limits"]["cost"]["min"] or 0.1`. ccxt 4.5.66 (pinned by
    /// passivbot v8.1.0) leaves `cost.min` unset for Bybit linear markets, so
    /// this is always 0.1; the venue's `minNotionalValue` is kept in
    /// `min_notional` for information only.
    pub min_cost: f64,
    pub min_notional: Option<f64>,
    pub contract_size: f64,
    pub max_leverage: f64,
    pub maker_fee: f64,
    pub taker_fee: f64,
    /// ccxt `market["active"]` (Bybit: `status == "Trading"`). Inactive
    /// markets stay listed so that positions and orders on a delisted or
    /// suspended symbol remain visible; the Python bot reads
    /// `markets_dict[symbol]["active"]` into `tradable` (passivbot.py:19903).
    pub active: bool,
}

/// Per-order outcome of a batch call; order and index match the request.
pub type OrderResult<T> = Result<T, ExchangeError>;

#[async_trait]
pub trait ExchangeClient: Send + Sync {
    /// Align the client's request timestamps with the exchange clock and
    /// return the new `server - local` offset in ms (ccxt
    /// `load_time_difference`, which the Python bot asks for once via
    /// `adjustForTimeDifference`, passivbot.py:2512). Default: a client
    /// whose requests carry no signed timestamp has nothing to align.
    async fn sync_time(&self) -> Result<i64, ExchangeError> {
        Ok(0)
    }
    async fn load_markets(&self) -> Result<Vec<MarketSpec>, ExchangeError>;
    async fn fetch_balance(&self) -> Result<Balance, ExchangeError>;
    async fn fetch_positions(&self) -> Result<Vec<Position>, ExchangeError>;
    async fn fetch_open_orders(&self) -> Result<Vec<OpenOrder>, ExchangeError>;
    async fn fetch_tickers(&self) -> Result<Vec<Ticker>, ExchangeError>;
    /// Candles of `timeframe` (`"1m"` or `"1h"`) from `since_ms` (rounded down
    /// to the bucket), ascending, at most 5 pages of `limit`
    /// (Python: `fetch_ohlcvs_1m`; the candle manager uses the same call with
    /// `timeframe="1h"` for hourly windows).
    async fn fetch_ohlcv(
        &self,
        symbol: &str,
        timeframe: &str,
        since_ms: Option<u64>,
        limit: usize,
    ) -> Result<Vec<Candle>, ExchangeError>;
    /// Fills between `start_ms` and `end_ms` (inclusive), ascending by time,
    /// deduplicated by execution id. With `start_ms` the whole range is
    /// walked (Bybit returns at most 7 days per request; Python
    /// `exchanges/bybit.py::fetch_fills` walks backwards by `endTime`).
    async fn fetch_fills(
        &self,
        symbol: Option<&str>,
        start_ms: Option<u64>,
        end_ms: Option<u64>,
    ) -> Result<Vec<Fill>, ExchangeError>;
    /// Closed-pnl records between `start_ms` and `end_ms`, ascending by
    /// time; with `start_ms` the range is walked in 7-day windows (Python
    /// `fetch_pnls_sub`).
    async fn fetch_closed_pnl(
        &self,
        start_ms: Option<u64>,
        end_ms: Option<u64>,
    ) -> Result<Vec<ClosedPnl>, ExchangeError>;
    /// One request per order, sent concurrently (Python: `asyncio.gather`).
    async fn create_orders(&self, orders: &[NewOrder]) -> Vec<OrderResult<OpenOrder>>;
    /// Cancel by exchange id; an order that is already gone counts as
    /// cancelled with `already_gone` set (Python: `execute_cancellation`).
    async fn cancel_orders(&self, orders: &[(String, String)]) -> Vec<OrderResult<CancelAck>>;
    /// Hedge mode for the whole linear account (Bybit `switch-mode` 3;
    /// Python `update_exchange_config` = ccxt `set_position_mode(True)`),
    /// "not modified" tolerated.
    async fn set_hedge_mode(&self) -> Result<(), ExchangeError>;
    /// Margin mode + leverage for one symbol (Python
    /// `update_exchange_config_by_symbols`: ccxt `set_margin_mode` then
    /// `set_leverage`), "not modified" responses tolerated.
    async fn configure_symbol(
        &self,
        symbol: &str,
        leverage: f64,
        margin_mode: MarginMode,
    ) -> Result<(), ExchangeError>;
}
