//! Exchange boundary of the runner.
//!
//! The runner never talks to an exchange directly; it talks to
//! [`ExchangeClient`]. The first (and for now only) implementation targets
//! Bybit USDT-linear perpetuals and is planned on top of the ccxt official
//! Rust port (docs/DECISIONS.md D3). A mock implementation for paper trading
//! and tests is planned in P5.
//!
//! The method set mirrors what passivbot's Python live loop actually uses
//! (docs/PORT_INVENTORY.md section 3): nothing more.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
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
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PositionSide {
    Long,
    Short,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Balance {
    pub total_usdt: f64,
    pub available_usdt: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub pside: PositionSide,
    pub size: f64,
    pub entry_price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenOrder {
    pub id: String,
    pub client_id: Option<String>,
    pub symbol: String,
    pub side: Side,
    pub pside: PositionSide,
    pub qty: f64,
    pub price: f64,
    pub reduce_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ticker {
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub last: f64,
    pub quote_volume_24h: f64,
}

/// `[ts_ms, open, high, low, close, volume]`
pub type Candle = [f64; 6];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSpec {
    pub symbol: String,
    pub qty_step: f64,
    pub price_step: f64,
    pub min_qty: f64,
    pub min_cost: f64,
    pub contract_size: f64,
    pub max_leverage: f64,
}

#[async_trait]
pub trait ExchangeClient: Send + Sync {
    async fn load_markets(&self) -> Result<Vec<MarketSpec>, ExchangeError>;
    async fn fetch_balance(&self) -> Result<Balance, ExchangeError>;
    async fn fetch_positions(&self) -> Result<Vec<Position>, ExchangeError>;
    async fn fetch_open_orders(&self) -> Result<Vec<OpenOrder>, ExchangeError>;
    async fn fetch_tickers(&self) -> Result<Vec<Ticker>, ExchangeError>;
    async fn fetch_ohlcv_1m(
        &self,
        symbol: &str,
        since_ms: u64,
        limit: usize,
    ) -> Result<Vec<Candle>, ExchangeError>;
    async fn create_orders(&self, orders: &[NewOrder]) -> Result<Vec<OpenOrder>, ExchangeError>;
    async fn cancel_orders(&self, ids: &[String]) -> Result<Vec<String>, ExchangeError>;
    async fn set_leverage(&self, symbol: &str, leverage: f64) -> Result<(), ExchangeError>;
}
