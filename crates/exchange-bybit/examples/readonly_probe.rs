//! P3.4 read-only integration probe against Bybit mainnet.
//!
//! Reads a passivbot-style `api-keys.json` (`{"<user>": {"exchange": "bybit",
//! "key": ..., "secret": ...}}`) and exercises every read path of
//! [`pb_exchange_bybit::ExchangeClient`]. Never places or cancels orders.
//!
//!     PB_API_KEYS=E:/projects/passivbot/api-keys.json PB_USER=415196485 \
//!       cargo run -p pb-exchange-bybit --example readonly_probe -- [--out summary.json]
//!
//! Secrets are never printed. The summary is what P3.4 diffs against the
//! Python adapter's view of the same account.

use pb_exchange_bybit::bybit::{BybitClient, BybitConfig};
use pb_exchange_bybit::ExchangeClient;
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let keys_path = std::env::var("PB_API_KEYS")?;
    let user = std::env::var("PB_USER")?;
    let out_path = std::env::args().skip_while(|a| a != "--out").nth(1);
    let keys: Value = serde_json::from_str(&std::fs::read_to_string(&keys_path)?)?;
    let entry = keys
        .get(&user)
        .ok_or_else(|| anyhow::anyhow!("user {user} not in {keys_path}"))?;
    anyhow::ensure!(entry["exchange"] == "bybit", "entry is not a bybit key");
    let key = entry["key"]
        .as_str()
        .or(entry["apiKey"].as_str())
        .unwrap_or_default();
    let secret = entry["secret"].as_str().unwrap_or_default();
    anyhow::ensure!(!key.is_empty() && !secret.is_empty(), "key/secret missing");

    let client = BybitClient::new(BybitConfig::mainnet(key, secret))?;
    let t0 = std::time::Instant::now();
    let markets = client.load_markets().await?;
    let t_markets = t0.elapsed().as_millis();
    let balance = client.fetch_balance().await?;
    let positions = client.fetch_positions().await?;
    let open_orders = client.fetch_open_orders().await?;
    let tickers = client.fetch_tickers().await?;
    let probe_symbol = positions
        .first()
        .map(|p| p.symbol.clone())
        .unwrap_or_else(|| "XRP/USDT:USDT".to_string());
    let since = now_ms() - 2 * 60 * 60 * 1000;
    let candles = client
        .fetch_ohlcv_1m(&probe_symbol, Some(since), 1000)
        .await?;
    let fills = client
        .fetch_fills(None, Some(now_ms() - 24 * 60 * 60 * 1000), None)
        .await?;

    let btc = markets.iter().find(|m| m.symbol == "BTC/USDT:USDT");
    let btc_ticker = tickers.iter().find(|t| t.symbol == "BTC/USDT:USDT");
    let summary = json!({
        "user": user,
        "markets": {"count": markets.len(), "load_ms": t_markets, "btc": btc},
        "balance": balance,
        "positions": positions,
        "open_orders": open_orders,
        "tickers": {"count": tickers.len(), "btc": btc_ticker},
        "ohlcv_1m": {
            "symbol": probe_symbol,
            "count": candles.len(),
            "first_ts": candles.first().map(|c| c[0]),
            "last_ts": candles.last().map(|c| c[0]),
            "last": candles.last(),
        },
        "fills_24h": {"count": fills.len(), "last": fills.last()},
        "elapsed_ms": t0.elapsed().as_millis(),
    });
    let text = serde_json::to_string_pretty(&summary)?;
    println!("{text}");
    if let Some(p) = out_path {
        std::fs::write(p, text)?;
    }
    Ok(())
}
