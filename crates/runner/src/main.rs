//! pb-runner entry point.
//!
//! Container contract (docs/CONTRACT.md): `pb-runner <config.json>`, with
//! `api-keys.json` in the working directory, exactly like
//! `python src/main.py configs/<BOT_ID>.json` today, so pbtb-rust only needs
//! a new task-definition family per engine line.
//!
//! Modes: `--check-only` validates the config and exits; without `--live`
//! the loop runs the full planning cycle against the account with a
//! read-only key and logs the orders it would place (dry run / shadow run).
//!
//! Lifecycle mirrors passivbot's `main()` (passivbot.py:22675-22736): the
//! bot runs until its hourly error budget trips (`RestartBotException`),
//! then it is torn down, the process sleeps the 60 s cooldown and starts a
//! fresh bot in-process; restarts in the last 24 h are counted and the
//! process exits (code 30) once they exceed `live.max_n_restarts_per_day`.

use pb_runner::{config, startup};

use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;

/// Engine line compiled in
#[cfg(feature = "engine-v8")]
pub const ENGINE_MAJOR: u32 = 8;
#[cfg(all(feature = "engine-v7", not(feature = "engine-v8")))]
pub const ENGINE_MAJOR: u32 = 7;

/// Exit code when the in-process restart budget is exhausted (Python's
/// `main()` leaves its loop with exit 0 there; 30 makes the reason visible
/// in ECS / CloudWatch).
pub const EXIT_RESTARTS_EXCEEDED: i32 = 30;
/// `cooldown_secs = 60` (passivbot.py:22688).
pub const RESTART_COOLDOWN_S: u64 = 60;

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

/// `idx:tradable:min_cost:long_size@price/short_size@price` per symbol that
/// carries a position or is untradable — the two states that turn an idle
/// cycle from "nothing to do" into a fault worth looking at.
fn positions_summary(input: &serde_json::Value) -> String {
    let f = |v: &serde_json::Value, k: &str| v.get(k).and_then(|x| x.as_f64()).unwrap_or(0.0);
    let mut out = Vec::new();
    for s in input
        .get("symbols")
        .and_then(|s| s.as_array())
        .unwrap_or(&Vec::new())
    {
        let tradable = s.get("tradable").and_then(|t| t.as_bool()).unwrap_or(true);
        let side = |k: &str| {
            s.get(k)
                .and_then(|x| x.get("position"))
                .map(|p| (f(p, "size"), f(p, "price")))
                .unwrap_or((0.0, 0.0))
        };
        let (ls, lp) = side("long");
        let (ss, sp) = side("short");
        if tradable && ls == 0.0 && ss == 0.0 {
            continue;
        }
        out.push(format!(
            "{}:tradable={tradable}:min_cost={}:long={ls}@{lp}:short={ss}@{sp}",
            f(s, "symbol_idx"),
            f(s, "effective_min_cost"),
        ));
    }
    if out.is_empty() {
        "none".to_string()
    } else {
        out.join(" ")
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    // The task definition points at a MOVING tag (`v810`), so the image a
    // container ran cannot be read back off the task definition or the ECS
    // console -- the tag has usually moved on by the time anyone asks. This
    // line is the only record of which build is running, so it is logged
    // before anything that can fail. `PB_RUNNER_BUILD` is baked in by
    // docker/Dockerfile from the workflow's `GIT_SHA`.
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        engine_line = ENGINE_MAJOR,
        build = std::env::var("PB_RUNNER_BUILD")
            .as_deref()
            .unwrap_or("unknown"),
        "pb-runner starting"
    );
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

/// Why one bot lifetime ended.
#[cfg(feature = "engine-v8")]
enum BotExit {
    /// `--once` completed or ctrl-c: leave the process.
    Stop,
    /// Error budget tripped (`RestartBotException`) or startup failed: the
    /// outer loop restarts after the cooldown.
    Restart(String),
}

#[cfg(feature = "engine-v8")]
async fn run_live(args: &Args, config_text: &str) -> Result<()> {
    use pb_exchange_bybit::bybit::{BybitClient, BybitConfig};
    use pb_runner::live::load_api_key;
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
    // `live.recv_window_ms` is what the Python bot hands ccxt
    // (`options.recvWindow`); without it every signed request carried the
    // client default of 5 s regardless of the config.
    let mut bybit_cfg = BybitConfig::mainnet(key.key, key.secret);
    if let Some(ms) = raw
        .pointer("/live/recv_window_ms")
        .and_then(serde_json::Value::as_u64)
    {
        bybit_cfg.recv_window_ms = ms;
    }
    // The broker code is an environment override, not a config key: the
    // config objects in S3 are the SAME objects the Python bots read, and a
    // pb-runner-only field in them would be a field passivbot has to tolerate.
    // passivbot overrides its own registry by env too
    // (`PASSIVBOT_BROKER_CODES_PATH`). Empty value = send no `Referer`, which
    // costs the sub-minimum-notional waiver (D25/D26).
    if let Ok(id) = std::env::var("PB_RUNNER_BROKER_ID") {
        bybit_cfg.broker_id = id;
    }
    tracing::info!(
        broker_id = %if bybit_cfg.broker_id.is_empty() { "<none>" } else { &bybit_cfg.broker_id },
        "[config] broker attribution"
    );
    let client = Arc::new(BybitClient::new(bybit_cfg)?);
    let max_restarts = raw
        .pointer("/live/max_n_restarts_per_day")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(10) as usize;
    let mut restarts: Vec<u64> = Vec::new();
    loop {
        let exit = run_bot(args, raw.clone(), client.clone()).await;
        match exit {
            BotExit::Stop => return Ok(()),
            BotExit::Restart(reason) => {
                tracing::warn!(reason = %reason, "restarting bot...");
            }
        }
        // passivbot.py:22711-22736: 60 s countdown, then count the restart
        // against the last 24 h and stop when the cap is exceeded.
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(RESTART_COOLDOWN_S)) => {}
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutdown requested during restart cooldown");
                return Ok(());
            }
        }
        let now = pb_runner::live::now_ms();
        restarts.push(now);
        restarts.retain(|t| *t > now.saturating_sub(24 * 60 * 60 * 1000));
        if restarts.len() > max_restarts {
            tracing::error!(
                restarts = restarts.len(),
                max_restarts,
                exit = EXIT_RESTARTS_EXCEEDED,
                "n restarts exceeded last 24h; exiting"
            );
            std::process::exit(EXIT_RESTARTS_EXCEEDED);
        }
    }
}

/// One bot lifetime (`setup_bot` + `start_bot`): fresh runner state, fresh
/// error budget, warmup, then the execution loop until it asks for a restart.
#[cfg(feature = "engine-v8")]
async fn run_bot(
    args: &Args,
    raw: serde_json::Value,
    client: std::sync::Arc<pb_exchange_bybit::bybit::BybitClient>,
) -> BotExit {
    use pb_runner::bot_params::ConfigView;
    use pb_runner::exchange_config::ExchangeConfigurator;
    use pb_runner::execute::Executor;
    use pb_runner::live::LiveRunner;

    let view = match ConfigView::new(raw) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "config view");
            return BotExit::Restart(format!("config: {e:#}"));
        }
    };
    // `live.time_in_force` from the resolved config (template default
    // `good_till_cancelled`, config/schema.py:446): PostOnly only when the
    // config says `post_only` (exchanges/bybit.py:545-549).
    let post_only = view
        .live("time_in_force")
        .and_then(serde_json::Value::as_str)
        .map(|s| s == "post_only")
        .unwrap_or(false);
    let mut exchange_config = ExchangeConfigurator::from_config(&view);
    let mut runner = match LiveRunner::new(view, client.clone()) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "runner init");
            return BotExit::Restart(format!("init: {e:#}"));
        }
    };
    let symbols = match runner.warmup().await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "warmup failed");
            return BotExit::Restart(format!("warmup: {e:#}"));
        }
    };
    let dry_run = !args.live;
    tracing::info!(?symbols, dry_run, post_only, "warmup complete");
    if !dry_run {
        if let Err(e) = runner.configure_exchange().await {
            tracing::error!(error = %e, "exchange configuration failed");
            return BotExit::Restart(format!("configure: {e:#}"));
        }
    }
    exchange_config.set_markets(runner.markets().values());
    let mut markets_seen_ms = runner.markets_loaded_ms();
    let mut executor = Executor::new(client.clone(), post_only, exchange_config);
    loop {
        let t0 = std::time::Instant::now();
        let now = pb_runner::live::now_ms();
        let recent = executor.recent_executions(now).to_vec();
        let planned = runner.plan(&recent).await;
        // Non-fatal failures Python charges to the budget (hourly reload,
        // symbols with exposure dropped from the cycle).
        for _ in 0..runner.take_budget_errors() {
            if let Err(e) = executor.note_error(now) {
                return BotExit::Restart(e.to_string());
            }
        }
        if runner.markets_loaded_ms() != markets_seen_ms {
            markets_seen_ms = runner.markets_loaded_ms();
            executor
                .exchange_config_mut()
                .set_markets(runner.markets().values());
        }
        match planned {
            Ok(cycle) => {
                let p = &cycle.plan;
                tracing::info!(
                    cycle = runner.cycles,
                    ms = t0.elapsed().as_millis(),
                    ideal = cycle.planned.len(),
                    cancels = p.cancels.len(),
                    creates = p.creates.len(),
                    matched = p.matched_exact + p.matched_tolerance,
                    deferred = p.deferred_by_barrier
                        + p.deferred_recent
                        + p.deferred_churn
                        + p.deferred_capacity,
                    skipped = p.skipped_market_snapshot + p.skipped_market_distance,
                    skipped_symbols = cycle.skipped_symbols.len(),
                    warnings = cycle.output.diagnostics.warnings.len(),
                    "planned"
                );
                // The count alone cannot be acted on: a bot that plans nothing
                // looks identical to a bot with nothing to do. Name the engine's
                // warnings and the per-symbol activity so an idle cycle is
                // diagnosable from the log alone.
                if !cycle.output.diagnostics.warnings.is_empty() {
                    tracing::warn!(
                        warnings = ?cycle.output.diagnostics.warnings,
                        "engine warnings"
                    );
                }
                if cycle.planned.is_empty() {
                    tracing::warn!(
                        balance = ?cycle.input.get("balance"),
                        balance_raw = ?cycle.input.get("balance_raw"),
                        positions = %positions_summary(&cycle.input),
                        states = ?cycle.output.diagnostics.symbol_states,
                        loss_gate_blocks = ?cycle.output.diagnostics.loss_gate_blocks,
                        "no ideal orders this cycle"
                    );
                }
                // A count of deferrals is not diagnosable: it says the plan
                // wanted something it did not send and never says what. That
                // gap is what made the abot shadow's standing `deferred=1`
                // unreadable -- a dry run cannot perform its own cancels, so
                // the cancel-first barrier holds the replacement forever and
                // the log repeats without ever naming the order. Python logs
                // the same thing (`cancel-first barrier deferred N`).
                for (o, why) in &p.deferred_orders {
                    tracing::info!(symbol = %o.symbol, side = ?o.side, pside = ?o.pside, qty = o.qty, price = o.price, order_type = %o.pb_order_type, reason = why, "deferred");
                }
                if dry_run {
                    for o in &p.cancels {
                        tracing::info!(symbol = %o.symbol, side = ?o.side, pside = ?o.pside, qty = o.qty, price = o.price, order_type = %o.pb_order_type, "dry-run cancel");
                    }
                    for o in &p.creates {
                        tracing::info!(symbol = %o.symbol, side = ?o.side, pside = ?o.pside, qty = o.qty, price = o.price, order_type = %o.pb_order_type, limit = o.limit, "dry-run create");
                    }
                } else {
                    match executor.execute(p, now).await {
                        Ok(r) => {
                            runner.note_write_failures(&r.write_failures, now);
                            tracing::info!(
                                cancels_ok = r.cancels_ok,
                                creates_ok = r.creates_ok,
                                failures = r.failures,
                                skipped_dirty = r.skipped_dirty,
                                skipped_pending_config = r.skipped_pending_config,
                                "wave done"
                            )
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "restart requested");
                            return BotExit::Restart(e.to_string());
                        }
                    }
                }
            }
            Err(e) => {
                // run_execution_loop (passivbot.py:6657-6760): a rate limit
                // backs off 5 s, any other exception 1 s
                // (`_handle_execution_loop_failure`); both charge the same
                // error budget as write failures.
                let rate_limited = e.chain().any(|c| {
                    matches!(
                        c.downcast_ref::<pb_exchange_bybit::ExchangeError>(),
                        Some(pb_exchange_bybit::ExchangeError::RateLimited { .. })
                    )
                });
                tracing::error!(error = %e, rate_limited, "[error] operation=run_execution_loop action=record_error_restart_backoff cycle=abandoned");
                if let Err(e) = executor.note_error(now) {
                    tracing::error!(error = %e, "restart requested");
                    return BotExit::Restart(e.to_string());
                }
                if !args.once {
                    let backoff = if rate_limited { 5.0 } else { 1.0 };
                    tokio::time::sleep(std::time::Duration::from_secs_f64(backoff)).await;
                }
            }
        }
        if args.once {
            return BotExit::Stop;
        }
        tokio::select! {
            _ = tokio::time::sleep(runner.sleep_between_cycles()) => {}
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutdown requested");
                return BotExit::Stop;
            }
        }
    }
}
