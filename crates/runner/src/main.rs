//! pb-runner entry point.
//!
//! Container contract (docs/CONTRACT.md): `pb-runner <config.json>`, with
//! `api-keys.json` in the working directory, exactly like
//! `python src/main.py configs/<BOT_ID>.json` today, so pbtb-rust only needs
//! a new task-definition family per engine line.
//!
//! Modes: `--check-only` validates the config and exits; `--dry-run`
//! (default until P4.4 lands) runs the full planning loop against the live
//! account with a read-only key and logs the orders it would place.

use pb_runner::config;

use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;

/// Engine line compiled in
#[cfg(feature = "engine-v8")]
pub const ENGINE_MAJOR: u32 = 8;
#[cfg(all(feature = "engine-v7", not(feature = "engine-v8")))]
pub const ENGINE_MAJOR: u32 = 7;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// passivbot live config (JSON) of the engine line this binary targets
    config: PathBuf,
    /// Validate the config and exit without connecting to the exchange.
    #[arg(long)]
    check_only: bool,
    /// Plan and log orders every cycle, never send anything (default).
    #[arg(long, default_value_t = true)]
    dry_run: bool,
    /// Run a single planning cycle and exit (with --dry-run).
    #[arg(long)]
    once: bool,
    /// `api-keys.json` (passivbot format), default: next to the working directory.
    #[arg(long, default_value = "api-keys.json")]
    api_keys: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    let text = std::fs::read_to_string(&args.config)
        .with_context(|| format!("reading {}", args.config.display()))?;
    let cfg = config::LiveConfig::parse(&text, ENGINE_MAJOR)?;
    tracing::info!(
        engine_line = cfg.engine_major,
        exchange = %cfg.exchange,
        strategy_kind = %cfg.strategy_kind,
        approved_coins_long = cfg.approved_coins_long.len(),
        "config accepted"
    );
    if args.check_only {
        tracing::info!("check-only: config valid, exiting");
        return Ok(());
    }
    #[cfg(feature = "engine-v8")]
    {
        run_live(&args, &text).await
    }
    #[cfg(not(feature = "engine-v8"))]
    {
        anyhow::bail!("this binary was built without an engine")
    }
}

#[cfg(feature = "engine-v8")]
async fn run_live(args: &Args, config_text: &str) -> Result<()> {
    use pb_exchange_bybit::bybit::{BybitClient, BybitConfig};
    use pb_runner::bot_params::ConfigView;
    use pb_runner::live::{load_api_key, LiveRunner};
    use std::sync::Arc;

    let raw: serde_json::Value = serde_json::from_str(config_text)?;
    let user = raw
        .pointer("/live/user")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("live.user missing"))?
        .to_string();
    let key = load_api_key(&args.api_keys, &user)?;
    anyhow::ensure!(
        key.exchange == "bybit",
        "only bybit is supported (got {})",
        key.exchange
    );
    let client = Arc::new(BybitClient::new(BybitConfig::mainnet(key.key, key.secret))?);
    let view = ConfigView::new(raw)?;
    let mut runner = LiveRunner::new(view, client)?;
    let symbols = runner.warmup().await?;
    tracing::info!(?symbols, dry_run = args.dry_run, "warmup complete");
    if !args.dry_run {
        anyhow::bail!("live execution is not implemented yet (P4.3/P4.4); run with --dry-run");
    }
    loop {
        let t0 = std::time::Instant::now();
        match runner.plan().await {
            Ok((planned, out, _input)) => {
                tracing::info!(
                    cycle = runner.cycles,
                    ms = t0.elapsed().as_millis(),
                    orders = planned.len(),
                    warnings = out.diagnostics.warnings.len(),
                    "planned"
                );
                for o in &planned {
                    tracing::info!(
                        symbol = %o.symbol, side = ?o.side, pside = ?o.pside, qty = o.qty, price = o.price,
                        order_type = %o.order_type, reduce_only = o.reduce_only, market = o.market,
                        "dry-run order"
                    );
                }
            }
            Err(e) => tracing::error!(error = %e, "planning cycle failed"),
        }
        if args.once {
            return Ok(());
        }
        tokio::select! {
            _ = tokio::time::sleep(runner.sleep_between_cycles()) => {}
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutdown requested");
                return Ok(());
            }
        }
    }
}
