//! pb-runner entry point.
//!
//! Container contract (docs/CONTRACT.md): `pb-runner <config.json>`, with
//! `api-keys.json` in the working directory, exactly like
//! `python src/main.py configs/<BOT_ID>.json` today, so pbtb-rust only needs
//! a new task-definition family per engine line.
//!
//! Current state: P0 skeleton. Loads the config, refuses any config whose
//! engine line differs from the one this binary was built for, and exits.
//! The loop itself is P4.

use pb_runner::config;

use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;

/// Engine line compiled into this binary (docs/DECISIONS.md D6).
#[cfg(feature = "engine-v8")]
pub const ENGINE_MAJOR: u32 = 8;
#[cfg(all(feature = "engine-v7", not(feature = "engine-v8")))]
pub const ENGINE_MAJOR: u32 = 7;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// passivbot live config (JSON) of the engine line this binary targets
    config: PathBuf,
    /// Validate and exit (no exchange connection). Default until P4.
    #[arg(long, default_value_t = true)]
    check_only: bool,
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
        tracing::info!("check-only mode: exiting (live loop is P4, see docs/PLAN.md)");
        return Ok(());
    }
    anyhow::bail!("live loop not implemented yet (P4)")
}
