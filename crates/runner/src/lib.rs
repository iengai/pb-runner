//! pb-runner library: config validation, config -> engine parameter mapping,
//! EMA inputs and the orchestrator snapshot builder (docs/PLAN.md P4).
//!
//! The binary in `main.rs` drives the live loop; `bin/snapcheck.rs` replays
//! committed recordings through the snapshot builder (P4.2 acceptance).

#[cfg(feature = "engine-v8")]
pub mod bot_params;
#[cfg(feature = "engine-v8")]
pub mod churn;
pub mod config;
#[cfg(feature = "engine-v8")]
pub mod cooldown;
#[cfg(feature = "engine-v8")]
pub mod emas;
#[cfg(feature = "engine-v8")]
pub mod exchange_config;
#[cfg(feature = "engine-v8")]
pub mod execute;
#[cfg(feature = "engine-v8")]
pub mod hsl;
#[cfg(feature = "engine-v8")]
pub mod hsl_coin;
pub mod jsonexact;
#[cfg(feature = "engine-v8")]
pub mod live;
#[cfg(feature = "engine-v8")]
pub mod market_filter;
pub mod mock_exchange;
#[cfg(feature = "engine-v8")]
pub mod reconcile;
#[cfg(feature = "engine-v8")]
pub mod snapshot;
pub mod startup;
