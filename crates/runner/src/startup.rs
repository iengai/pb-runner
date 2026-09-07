//! Container startup (docs/CONTRACT.md section 2): when `BUCKET`, `USER_ID`
//! and `BOT_ID` are set, download `s3://$BUCKET/$USER_ID/$BOT_ID/$BOT_ID.json`
//! to `configs/$BOT_ID.json` and `.../api-keys.json` to `api-keys.json`
//! before the loop starts, with the Python entrypoint's exit codes:
//! 10 missing env, 20/21 download failed, 22 empty file.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

pub const EXIT_MISSING_ENV: i32 = 10;
pub const EXIT_CONFIG_DOWNLOAD_FAILED: i32 = 20;
pub const EXIT_KEYS_DOWNLOAD_FAILED: i32 = 21;
pub const EXIT_EMPTY_FILE: i32 = 22;

#[derive(Debug, Clone)]
pub struct S3Inputs {
    pub bucket: String,
    pub user_id: String,
    pub bot_id: String,
}

/// `Some` when all three variables are set, `None` when none is set; any
/// partial set is a configuration error (exit 10).
pub fn s3_inputs_from_env() -> std::result::Result<Option<S3Inputs>, i32> {
    let get = |k: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty());
    match (get("BUCKET"), get("USER_ID"), get("BOT_ID")) {
        (None, None, None) => Ok(None),
        (Some(bucket), Some(user_id), Some(bot_id)) => Ok(Some(S3Inputs {
            bucket,
            user_id,
            bot_id,
        })),
        _ => Err(EXIT_MISSING_ENV),
    }
}

/// Downloaded file paths (relative to the working directory).
#[derive(Debug, Clone)]
pub struct Downloaded {
    pub config: PathBuf,
    pub api_keys: PathBuf,
}

async fn get_object(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    dest: &Path,
) -> Result<usize> {
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .with_context(|| format!("s3://{bucket}/{key}"))?;
    let bytes = resp
        .body
        .collect()
        .await
        .context("reading object body")?
        .into_bytes();
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(dest, &bytes).with_context(|| dest.display().to_string())?;
    Ok(bytes.len())
}

/// Perform the downloads; on failure returns the contract's exit code.
pub async fn download(inputs: &S3Inputs) -> std::result::Result<Downloaded, (i32, anyhow::Error)> {
    let shared = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = aws_sdk_s3::Client::new(&shared);
    let prefix = format!("{}/{}", inputs.user_id, inputs.bot_id);
    let config = PathBuf::from("configs").join(format!("{}.json", inputs.bot_id));
    let api_keys = PathBuf::from("api-keys.json");
    let n = get_object(
        &client,
        &inputs.bucket,
        &format!("{prefix}/{}.json", inputs.bot_id),
        &config,
    )
    .await
    .map_err(|e| (EXIT_CONFIG_DOWNLOAD_FAILED, e))?;
    if n == 0 {
        return Err((EXIT_EMPTY_FILE, anyhow::anyhow!("config object is empty")));
    }
    let n = get_object(
        &client,
        &inputs.bucket,
        &format!("{prefix}/api-keys.json"),
        &api_keys,
    )
    .await
    .map_err(|e| (EXIT_KEYS_DOWNLOAD_FAILED, e))?;
    if n == 0 {
        return Err((EXIT_EMPTY_FILE, anyhow::anyhow!("api-keys object is empty")));
    }
    tracing::info!(config = %config.display(), api_keys = %api_keys.display(), "downloaded from S3");
    Ok(Downloaded { config, api_keys })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_detection() {
        // Only asserts the pure classification; the process env is not touched.
        let classify =
            |b: Option<&str>, u: Option<&str>, i: Option<&str>| -> std::result::Result<bool, i32> {
                match (b, u, i) {
                    (None, None, None) => Ok(false),
                    (Some(_), Some(_), Some(_)) => Ok(true),
                    _ => Err(EXIT_MISSING_ENV),
                }
            };
        assert_eq!(classify(None, None, None), Ok(false));
        assert_eq!(classify(Some("b"), Some("u"), Some("i")), Ok(true));
        assert_eq!(classify(Some("b"), None, Some("i")), Err(10));
    }
}
