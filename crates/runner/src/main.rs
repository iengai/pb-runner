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

use pb_runner::{config, startup};

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
    /// passivbot live config (JSON) of the engine line this binary targets.
    /// Optional when BUCKET/USER_ID/BOT_ID are set (the config is downloaded).
    config: Option<PathBuf>,
    /// Validate the config and exit without connecting to the exchange.
    #[arg(long)]
    check_only: bool,
    /// Send orders. Without this flag the loop only plans and logs (dry run).
    #[arg(long)]
    live: bool,
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
    let mut args = Args::parse();
    // Container contract: fetch config + keys from S3 when BUCKET/USER_ID/BOT_ID are set.
    match startup::s3_inputs_from_env() {
        Ok(None) => {}
        Ok(Some(inputs)) => match startup::download(&inputs).await {
            Ok(d) => {
                args.config = Some(d.config);
                args.api_keys = d.api_keys;
            }
            Err((code, e)) => {
                tracing::error!(error = %e, exit = code, "startup download failed");
                std::process::exit(code);
            }
        },
        Err(code) => {
            tracing::error!(
                exit = code,
                "BUCKET, USER_ID and BOT_ID must be set together"
            );
            std::process::exit(code);
        }
    }
    let config_path = args
        .config
        .clone()
        .ok_or_else(|| anyhow::anyhow!("config path required (or set BUCKET/USER_ID/BOT_ID)"))?;
    let text = std::fs::read_to_string(&config_path)
        .with_context(|| format!("reading {}", config_path.display()))?;
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
    use pb_runner::execute::Executor;
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
    let view = ConfigView::new(raw.clone())?;
    let mut runner = LiveRunner::new(view, client.clone())?;
    let symbols = runner.warmup().await?;
    let dry_run = !args.live;
    tracing::info!(?symbols, dry_run, "warmup complete");
    let post_only = raw
        .pointer("/live/time_in_force")
        .and_then(serde_json::Value::as_str)
        .map(|s| s == "post_only")
        .unwrap_or(true);
    if !dry_run {
        runner.configure_exchange(&symbols).await?;
    }
    let mut executor = Executor::new(client.clone(), post_only);
    loop {
        let t0 = std::time::Instant::now();
        let now = pb_runner::live::now_ms();
        let recent = executor.recent_executions(now).to_vec();
        match runner.plan(&recent).await {
            Ok(cycle) => {
                let p = &cycle.plan;
                tracing::info!(
                    cycle = runner.cycles,
                    ms = t0.elapsed().as_millis(),
                    ideal = cycle.planned.len(),
                    cancels = p.cancels.len(),
                    creates = p.creates.len(),
                    matched = p.matched_exact + p.matched_tolerance,
                    deferred = p.deferred_by_barrier + p.deferred_recent + p.deferred_capacity,
                    warnings = cycle.output.diagnostics.warnings.len(),
                    "planned"
                );
                if dry_run {
                    for o in &p.cancels {
                        tracing::info!(symbol = %o.symbol, side = ?o.side, pside = ?o.pside, qty = o.qty, price = o.price, order_type = %o.pb_order_type, "dry-run cancel");
                    }
                    for o in &p.creates {
                        tracing::info!(symbol = %o.symbol, side = ?o.side, pside = ?o.pside, qty = o.qty, price = o.price, order_type = %o.pb_order_type, limit = o.limit, "dry-run create");
                    }
                } else {
                    match executor.execute(p, now).await {
                        Ok(r) => tracing::info!(
                            cancels_ok = r.cancels_ok,
                            creates_ok = r.creates_ok,
                            failures = r.failures,
                            "wave done"
                        ),
                        Err(e) => {
                            tracing::error!(error = %e, "restart requested");
                            std::process::exit(30);
                        }
                    }
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
