//! Recorded orchestrator calls.
//!
//! passivbot (Python) calls `passivbot_rust.compute_ideal_orders_json(input_json)`
//! once per execution cycle (both the v7.12.0 and v8.1.0 lines expose this
//! JSON API). The recorder patch (docs/RECORDER.md) dumps every call as a pair
//! of files. This crate is the single definition of that on-disk format so
//! `diffcheck` (replay + compare) and `runner` (tests) agree.
//!
//! The orchestrator's own types (`OrchestratorInput` / `OrchestratorOutput`)
//! are NOT redefined here: they come from the pinned `passivbot_rust` crate
//! once P1.1 (rlib feature branch) is done. Until then the payloads are kept
//! as raw `serde_json::Value`.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

/// File name stem: `<utc_ms>_<input_hash16>`; suffixes `.in.json` / `.out.json`.
pub const IN_SUFFIX: &str = ".in.json";
pub const OUT_SUFFIX: &str = ".out.json";

/// One recorded call. `input`/`output` are the exact JSON documents that
/// crossed the Python<->Rust boundary (output is absent if the engine raised).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordedCall {
    pub stem: String,
    pub utc_ms: u64,
    /// First 16 hex chars of sha256(input_json).
    pub input_hash: String,
    pub input: serde_json::Value,
    pub output: Option<serde_json::Value>,
}

pub fn input_hash(input_json: &str) -> String {
    let digest = Sha256::digest(input_json.as_bytes());
    hex::encode(digest)[..16].to_string()
}

pub fn stem_for(utc_ms: u64, input_json: &str) -> String {
    format!("{utc_ms}_{}", input_hash(input_json))
}

/// Enumerate recordings in a directory, sorted by stem (i.e. by time).
pub fn list_recordings(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut stems: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with(IN_SUFFIX))
                .unwrap_or(false)
        })
        .collect();
    stems.sort();
    Ok(stems)
}

pub fn load(in_path: &Path) -> Result<RecordedCall> {
    let name = in_path
        .file_name()
        .and_then(|n| n.to_str())
        .context("recording file name is not utf-8")?;
    let stem = name
        .strip_suffix(IN_SUFFIX)
        .with_context(|| format!("{name} does not end with {IN_SUFFIX}"))?
        .to_string();
    let (ts, hash) = stem
        .split_once('_')
        .with_context(|| format!("stem {stem} is not <utc_ms>_<hash>"))?;
    let utc_ms: u64 = ts
        .parse()
        .with_context(|| format!("bad utc_ms in {stem}"))?;
    let hash = hash.to_string();
    let input_text = std::fs::read_to_string(in_path)?;
    let input: serde_json::Value = serde_json::from_str(&input_text)
        .with_context(|| format!("{name}: input is not valid JSON"))?;
    let out_path = in_path.with_file_name(format!("{stem}{OUT_SUFFIX}"));
    let output = if out_path.exists() {
        Some(serde_json::from_str(&std::fs::read_to_string(&out_path)?)?)
    } else {
        None
    };
    Ok(RecordedCall {
        stem,
        utc_ms,
        input_hash: hash,
        input,
        output,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_16_hex() {
        let h = input_hash("{}");
        assert_eq!(h.len(), 16);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn stem_roundtrip() {
        let s = stem_for(1_700_000_000_000, "{\"a\":1}");
        assert!(s.starts_with("1700000000000_"));
    }
}
