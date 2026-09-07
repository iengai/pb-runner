//! diffcheck: the first gate of the whole project (docs/PLAN.md P1/P2).
//!
//! For every recording in a directory:
//!   1. parse the input JSON (must round-trip through `OrchestratorInput`
//!      once the engine feature is on),
//!   2. call `passivbot_rust::orchestrator::compute_ideal_orders`,
//!   3. compare the produced `orders` with the recorded output, order by order
//!      (symbol_idx, pside, qty, price, order_type, execution_type).
//!
//! Exit code 0 only if every recording matches exactly.
//!
//! Without the `engine` feature it only validates the recordings (useful to
//! check the recorder patch before P1.1 lands).

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Directory of `<utc_ms>_<hash>.in.json` / `.out.json` pairs
    #[arg(long, default_value = "tests/fixtures/recordings")]
    dir: PathBuf,
    /// Stop at first mismatch
    #[arg(long)]
    fail_fast: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let files = pb_snapshot::list_recordings(&args.dir)?;
    if files.is_empty() {
        eprintln!(
            "no recordings in {} (see docs/RECORDER.md)",
            args.dir.display()
        );
        std::process::exit(2);
    }
    let mut ok = 0usize;
    let mut bad = 0usize;
    for f in &files {
        let rec = pb_snapshot::load(f)?;
        let n_symbols = rec
            .input
            .get("symbols")
            .and_then(|s| s.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        let n_orders = rec
            .output
            .as_ref()
            .and_then(|o| o.get("orders"))
            .and_then(|o| o.as_array())
            .map(|a| a.len());
        match replay(&rec) {
            Ok(()) => {
                ok += 1;
                println!("OK   {} symbols={n_symbols} orders={n_orders:?}", rec.stem);
            }
            Err(e) => {
                bad += 1;
                println!("FAIL {} {e}", rec.stem);
                if args.fail_fast {
                    break;
                }
            }
        }
    }
    println!("{ok} ok, {bad} failed, {} total", files.len());
    if bad > 0 {
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(not(feature = "engine"))]
fn replay(rec: &pb_snapshot::RecordedCall) -> Result<()> {
    // Validation-only mode: the pair must exist and look like an orchestrator call.
    anyhow::ensure!(rec.input.get("global").is_some(), "input lacks `global`");
    anyhow::ensure!(rec.input.get("symbols").is_some(), "input lacks `symbols`");
    anyhow::ensure!(rec.output.is_some(), "no .out.json (engine raised?)");
    Ok(())
}

#[cfg(feature = "engine")]
fn replay(rec: &pb_snapshot::RecordedCall) -> Result<()> {
    // P1.2: deserialize into passivbot_rust::orchestrator::OrchestratorInput,
    // run compute_ideal_orders, compare `orders` with rec.output["orders"].
    // Comparison must be exact on (symbol_idx, pside, order_type, execution_type)
    // and on qty/price within 1 ulp; report the first differing order.
    let _ = rec;
    anyhow::bail!("engine replay not implemented yet (P1.2)")
}
