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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewOrder {
    pub client_id: String,
    pub symbol: String,
    pub side: Side,
    pub pside: PositionSide,
    pub qty: f64,
    pub price: f64,
    pub reduce_only: bool,
    pub post_only: bool,
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
}

/// Per-order outcome of a batch call; order and index match the request.
pub type OrderResult<T> = Result<T, ExchangeError>;

#[async_trait]
pub trait ExchangeClient: Send + Sync {
    async fn load_markets(&self) -> Result<Vec<MarketSpec>, ExchangeError>;
    async fn fetch_balance(&self) -> Result<Balance, ExchangeError>;
    async fn fetch_positions(&self) -> Result<Vec<Position>, ExchangeError>;
    async fn fetch_open_orders(&self) -> Result<Vec<OpenOrder>, ExchangeError>;
    async fn fetch_tickers(&self) -> Result<Vec<Ticker>, ExchangeError>;
    /// 1m candles from `since_ms` (rounded down to the minute), ascending,
    /// at most 5 pages of `limit` (Python: `fetch_ohlcvs_1m`).
    async fn fetch_ohlcv_1m(
        &self,
        symbol: &str,
        since_ms: Option<u64>,
        limit: usize,
    ) -> Result<Vec<Candle>, ExchangeError>;
    /// Fills between `start_ms` and `end_ms` (inclusive), ascending by time.
    async fn fetch_fills(
        &self,
        symbol: Option<&str>,
        start_ms: Option<u64>,
        end_ms: Option<u64>,
    ) -> Result<Vec<Fill>, ExchangeError>;
    /// One request per order, sent concurrently (Python: `asyncio.gather`).
    async fn create_orders(&self, orders: &[NewOrder]) -> Vec<OrderResult<OpenOrder>>;
    /// Cancel by exchange id; an order that is already gone counts as
    /// cancelled (Python: `execute_cancellation`).
    async fn cancel_orders(&self, orders: &[(String, String)]) -> Vec<OrderResult<String>>;
    /// Hedge mode for the whole linear account (Bybit `switch-mode` 3).
    async fn set_hedge_mode(&self) -> Result<(), ExchangeError>;
    /// Margin mode + leverage for one symbol, "already set" responses ignored.
    async fn configure_symbol(
        &self,
        symbol: &str,
        leverage: f64,
        margin_mode: MarginMode,
    ) -> Result<(), ExchangeError>;
}
