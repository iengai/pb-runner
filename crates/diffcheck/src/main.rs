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
    use passivbot_rust::orchestrator::{compute_ideal_orders, OrchestratorInput};

    let recorded_text = rec
        .output_text
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("no .out.json (engine raised?)"))?;
    // Same call as `compute_ideal_orders_json` in python.rs: parse the raw
    // text with `from_str` (deny_unknown_fields). serde_json's default float
    // parsing is best-effort (+-1 ulp on some literals), so going through
    // `serde_json::Value` first would NOT reproduce the wheel's inputs.
    let input: OrchestratorInput = serde_json::from_str(&rec.input_text)
        .map_err(|e| anyhow::anyhow!("input does not parse as OrchestratorInput: {e}"))?;
    let out = compute_ideal_orders(&input)
        .map_err(|e| anyhow::anyhow!("compute_ideal_orders failed: {e:?}"))?;
    // The recorded text was produced by `serde_json::to_string` of the same
    // struct in the same serde_json version: identical f64s => identical bytes.
    let produced_text = serde_json::to_string(&out)?;
    if produced_text == recorded_text {
        return Ok(());
    }
    // Explain the first difference. Both texts go through the same parser,
    // so any +-1 ulp parse imprecision cancels out in the report.
    let recorded: serde_json::Value = serde_json::from_str(recorded_text)?;
    let produced: serde_json::Value = serde_json::from_str(&produced_text)?;
    compare_orders(
        recorded.get("orders").and_then(|o| o.as_array()),
        produced.get("orders").and_then(|o| o.as_array()),
    )?;
    match first_json_diff("", &recorded, &produced) {
        Some(path) => anyhow::bail!("orders match but output differs at {path}"),
        // Same parsed values but different bytes: a +-1 ulp float that the
        // best-effort parser collapses. Show the first differing byte instead.
        None => anyhow::bail!(
            "output text differs (sub-ulp float difference){}",
            text_diff_excerpt(recorded_text, &produced_text)
        ),
    }
}

/// First differing byte of two strings with some context on each side.
#[cfg(feature = "engine")]
fn text_diff_excerpt(a: &str, b: &str) -> String {
    let i = a
        .bytes()
        .zip(b.bytes())
        .position(|(x, y)| x != y)
        .unwrap_or(a.len().min(b.len()));
    let window = |s: &str| {
        let mut start = i.saturating_sub(60);
        while !s.is_char_boundary(start) {
            start -= 1;
        }
        let mut end = (i + 40).min(s.len());
        while !s.is_char_boundary(end) {
            end += 1;
        }
        s[start..end].to_string()
    };
    format!(
        " at byte {i}\n  recorded: ...{}...\n  produced: ...{}...",
        window(a),
        window(b)
    )
}

/// Exact, positional comparison of two order lists. Categorical fields must be
/// identical JSON values; `qty` and `price` must be bitwise-equal f64s.
#[cfg(feature = "engine")]
fn compare_orders(
    recorded: Option<&Vec<serde_json::Value>>,
    produced: Option<&Vec<serde_json::Value>>,
) -> Result<()> {
    let recorded = recorded.ok_or_else(|| anyhow::anyhow!("recorded output lacks `orders`"))?;
    let produced = produced.ok_or_else(|| anyhow::anyhow!("produced output lacks `orders`"))?;
    if recorded.len() != produced.len() {
        anyhow::bail!(
            "order count differs: recorded {} vs produced {}",
            recorded.len(),
            produced.len()
        );
    }
    const EXACT: [&str; 5] = [
        "symbol_idx",
        "pside",
        "order_type",
        "execution_type",
        "execution_priority",
    ];
    for (i, (r, p)) in recorded.iter().zip(produced.iter()).enumerate() {
        for key in EXACT {
            if r.get(key) != p.get(key) {
                anyhow::bail!(
                    "order[{i}].{key} differs: recorded {} vs produced {}\n  recorded: {r}\n  produced: {p}",
                    r.get(key).cloned().unwrap_or(serde_json::Value::Null),
                    p.get(key).cloned().unwrap_or(serde_json::Value::Null)
                );
            }
        }
        for key in ["qty", "price"] {
            let rv = r.get(key).and_then(|v| v.as_f64());
            let pv = p.get(key).and_then(|v| v.as_f64());
            let same = match (rv, pv) {
                (Some(a), Some(b)) => a.to_bits() == b.to_bits(),
                (None, None) => true,
                _ => false,
            };
            if !same {
                anyhow::bail!(
                    "order[{i}].{key} differs: recorded {rv:?} vs produced {pv:?}\n  recorded: {r}\n  produced: {p}"
                );
            }
        }
    }
    Ok(())
}

/// Path of the first difference between two JSON documents, if any.
/// Numbers compare bitwise (recorded JSON was produced by the same serde
/// serializer, so any textual difference is a real difference).
#[cfg(feature = "engine")]
fn first_json_diff(path: &str, a: &serde_json::Value, b: &serde_json::Value) -> Option<String> {
    use serde_json::Value;
    match (a, b) {
        (Value::Object(ma), Value::Object(mb)) => {
            for (k, va) in ma {
                match mb.get(k) {
                    None => return Some(format!("{path}/{k} (missing in produced)")),
                    Some(vb) => {
                        if let Some(d) = first_json_diff(&format!("{path}/{k}"), va, vb) {
                            return Some(d);
                        }
                    }
                }
            }
            mb.keys()
                .find(|k| !ma.contains_key(*k))
                .map(|k| format!("{path}/{k} (missing in recorded)"))
        }
        (Value::Array(xa), Value::Array(xb)) => {
            if xa.len() != xb.len() {
                return Some(format!("{path} (len {} vs {})", xa.len(), xb.len()));
            }
            xa.iter()
                .zip(xb)
                .enumerate()
                .find_map(|(i, (va, vb))| first_json_diff(&format!("{path}/{i}"), va, vb))
        }
        (Value::Number(na), Value::Number(nb)) => {
            let same = match (na.as_f64(), nb.as_f64()) {
                (Some(x), Some(y)) => x.to_bits() == y.to_bits(),
                _ => na == nb,
            };
            (!same).then(|| format!("{path} ({na} vs {nb})"))
        }
        _ => (a != b).then(|| format!("{path} ({a} vs {b})")),
    }
}
