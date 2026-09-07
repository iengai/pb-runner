//! Mock exchange: an [`ExchangeClient`] that reproduces passivbot's fake
//! exchange (`src/exchanges/fake.py` at v8.1.0, `FakeCCXTClient`) so the
//! runner can be driven through the same scenario the Python bot ran
//! (PLAN P5.1, docs/MOCK_EXCHANGE.md).
//!
//! Semantics mirrored, with `fake.py` line references in the code:
//! scenario loading (scripted `timeline` rows or a candle `replay`, boot
//! positions / fills / orders), the fill model (a resting limit order fills
//! when the step candle's range reaches its price, a new limit order fills at
//! once when the step price already crosses it, market orders fill at the
//! step price), fees, balance and position bookkeeping, order and trade ids,
//! tickers = step price, `fetch_open_orders` ordering.
//!
//! Deliberately *not* mirrored: the fake's `fetch_ohlcv` returns the
//! newest `limit` rows; the runner expects the Bybit client's contract
//! (oldest rows from `since`, up to 5 pages), which `tools/fake_live_clock.py`
//! also restores for the Python harness.

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use pb_exchange_bybit::{
    Balance, Candle, ClosedPnl, ExchangeClient, ExchangeError, Fill, MarginMode, MarketSpec,
    NewOrder, OpenOrder, OrderResult, Position, PositionSide, Side, Ticker,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const ONE_MIN_MS: u64 = 60_000;
const ONE_HOUR_MS: u64 = 3_600_000;

/// `fake.py:205-236` (`_build_markets`).
#[derive(Debug, Clone, PartialEq)]
pub struct SymbolMeta {
    pub id: String,
    pub qty_step: f64,
    pub price_step: f64,
    pub min_qty: f64,
    pub min_cost: f64,
    pub contract_size: f64,
    pub maker_fee: f64,
    pub taker_fee: f64,
}

/// One timeline step (`fake.py:285-294`, `fake.py:368-377`).
#[derive(Debug, Clone)]
pub struct Step {
    pub timestamp_ms: u64,
    pub prices: BTreeMap<String, f64>,
    pub candles: BTreeMap<String, Candle>,
    pub actions: Vec<Value>,
}

/// A parsed fake scenario (`fake.py:89-175`).
#[derive(Debug, Clone)]
pub struct Scenario {
    pub name: String,
    pub quote: String,
    pub tick_interval_ms: u64,
    pub boot_index: usize,
    pub balance: f64,
    pub boot_positions: Vec<Value>,
    pub boot_fills: Vec<Value>,
    pub boot_orders: Vec<Value>,
    /// Sorted by symbol (`fake.py:135`).
    pub symbols: BTreeMap<String, SymbolMeta>,
    /// Full 1m candle history per symbol, one row per timeline step.
    pub candles: BTreeMap<String, Vec<Candle>>,
    pub timeline: Vec<Step>,
}

fn num(v: Option<&Value>, default: f64) -> f64 {
    v.and_then(Value::as_f64).unwrap_or(default)
}

/// `fake.py:17-42` (`_parse_time_to_ms`): numbers below 1e11 are seconds;
/// strings are integers or ISO-8601 (`Z` or `+HH:MM` offsets, naive = UTC).
pub fn parse_time_to_ms(v: &Value) -> Result<u64> {
    match v {
        Value::Number(n) => {
            let x = n.as_f64().ok_or_else(|| anyhow!("bad timestamp {n}"))?;
            let mut ts = x as i64;
            if ts < 100_000_000_000 {
                ts *= 1000;
            }
            Ok(ts.max(0) as u64)
        }
        Value::String(s) => {
            let text = s.trim();
            if text.is_empty() {
                bail!("Fake scenario timestamp is empty");
            }
            if let Ok(mut ts) = text.parse::<i64>() {
                if ts < 100_000_000_000 {
                    ts *= 1000;
                }
                return Ok(ts.max(0) as u64);
            }
            parse_iso8601_ms(text)
        }
        Value::Null => bail!("Fake scenario timestamp is required"),
        other => bail!("unsupported timestamp {other}"),
    }
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn parse_iso8601_ms(text: &str) -> Result<u64> {
    let mut s = text.to_string();
    let mut offset_ms: i64 = 0;
    if let Some(stripped) = s.strip_suffix('Z') {
        s = stripped.to_string();
    } else if s.len() > 6 {
        let tail = &s[s.len() - 6..];
        let b = tail.as_bytes();
        if (b[0] == b'+' || b[0] == b'-') && b[3] == b':' {
            let h: i64 = tail[1..3].parse()?;
            let m: i64 = tail[4..6].parse()?;
            offset_ms = (h * 60 + m) * 60_000 * if b[0] == b'-' { -1 } else { 1 };
            s.truncate(s.len() - 6);
        }
    }
    let (date, time) = match s.split_once(['T', ' ']) {
        Some((d, t)) => (d.to_string(), t.to_string()),
        None => (s.clone(), String::new()),
    };
    let mut dp = date.split('-');
    let y: i64 = dp.next().context("year")?.parse()?;
    let m: u32 = dp.next().context("month")?.parse()?;
    let d: u32 = dp.next().context("day")?.parse()?;
    let mut hh = 0i64;
    let mut mm = 0i64;
    let mut ss = 0f64;
    if !time.is_empty() {
        let mut tp = time.split(':');
        hh = tp.next().unwrap_or("0").parse()?;
        mm = tp.next().unwrap_or("0").parse()?;
        ss = tp.next().unwrap_or("0").parse()?;
    }
    let days = days_from_civil(y, m, d);
    let ms =
        days * 86_400_000 + (hh * 3600 + mm * 60) * 1000 + (ss * 1000.0).round() as i64 - offset_ms;
    Ok(ms.max(0) as u64)
}

fn parse_timeframe_ms(tf: &str) -> Result<u64, ExchangeError> {
    match tf {
        "1m" => Ok(ONE_MIN_MS),
        "1h" => Ok(ONE_HOUR_MS),
        other => Err(ExchangeError::NotSupported(format!("timeframe {other}"))),
    }
}

/// Minimal `.npy` reader for float64 C-order `(n, 6)` arrays (the dev box's
/// Bybit 1m candle cache; same reader as `pb-snapcheck`).
pub fn read_npy_rows(path: &Path) -> Result<Vec<Candle>> {
    let bytes = std::fs::read(path).with_context(|| path.display().to_string())?;
    if bytes.len() < 10 || &bytes[..6] != b"\x93NUMPY" {
        bail!("{}: not a .npy file", path.display());
    }
    let major = bytes[6];
    let (header_len, offset) = if major == 1 {
        (u16::from_le_bytes([bytes[8], bytes[9]]) as usize, 10)
    } else {
        (
            u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize,
            12,
        )
    };
    let header = std::str::from_utf8(&bytes[offset..offset + header_len])?;
    if !header.contains("'<f8'") || header.contains("'fortran_order': True") {
        bail!("{}: unexpected npy header {header}", path.display());
    }
    let data = &bytes[offset + header_len..];
    if data.len() % 48 != 0 {
        bail!("{}: data length not a multiple of 6 f64", path.display());
    }
    let mut out = Vec::with_capacity(data.len() / 48);
    for row in data.chunks_exact(48) {
        let mut c = [0.0; 6];
        for (i, v) in c.iter_mut().enumerate() {
            *v = f64::from_le_bytes(row[i * 8..i * 8 + 8].try_into().unwrap());
        }
        out.push(c);
    }
    Ok(out)
}

fn split_symbol(symbol: &str) -> Result<(String, String)> {
    let (left, right) = symbol
        .split_once('/')
        .ok_or_else(|| anyhow!("Fake symbol '{symbol}' must look like BASE/QUOTE:SETTLE"))?;
    let quote = right.split(':').next().unwrap_or(right);
    Ok((left.to_string(), quote.to_string()))
}

impl Scenario {
    /// Load `scenario.json` (the JSON subset of the fake's HJSON loader,
    /// `fake.py:45-55`); relative replay paths resolve against its directory.
    pub fn load(path: &Path) -> Result<Scenario> {
        let text = std::fs::read_to_string(path).with_context(|| path.display().to_string())?;
        let v: Value = serde_json::from_str(&text).with_context(|| path.display().to_string())?;
        let base = path
            .canonicalize()
            .unwrap_or_else(|_| path.to_path_buf())
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let name = v
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| {
                path.file_stem()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_else(|| "scenario".into())
            });
        Self::from_value(&v, &base, name)
    }

    pub fn from_value(v: &Value, base_dir: &Path, name: String) -> Result<Scenario> {
        let tick_interval_ms = (num(v.get("tick_interval_seconds"), 60.0) * 1000.0) as i64;
        if tick_interval_ms <= 0 {
            bail!("Fake scenario tick_interval_seconds must be > 0");
        }
        let tick_interval_ms = tick_interval_ms as u64;
        let start_time_ms = match v.get("start_time") {
            Some(Value::Null) | None => None,
            Some(t) => Some(parse_time_to_ms(t)?),
        };
        let account = v.get("account").cloned().unwrap_or(Value::Null);
        let balance = num(account.get("balance"), 0.0);
        let quote = v
            .get("quote")
            .and_then(Value::as_str)
            .unwrap_or("USDT")
            .to_string();

        // `_build_markets` (fake.py:205-236)
        let symbols_cfg = v
            .get("symbols")
            .and_then(Value::as_object)
            .filter(|m| !m.is_empty())
            .ok_or_else(|| anyhow!("Fake scenario must define symbols"))?;
        let mut symbols: BTreeMap<String, SymbolMeta> = BTreeMap::new();
        for (symbol, meta) in symbols_cfg {
            if !meta.is_object() {
                bail!("Fake symbol config for {symbol} must be a mapping");
            }
            split_symbol(symbol)?;
            let qty_step = num(meta.get("qty_step"), 0.001);
            let price_step = num(meta.get("price_step"), 0.1);
            symbols.insert(
                symbol.clone(),
                SymbolMeta {
                    id: meta
                        .get("id")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                        .unwrap_or_else(|| symbol.replace('/', "").replace(':', "_")),
                    qty_step,
                    price_step,
                    min_qty: num(meta.get("min_qty"), qty_step),
                    min_cost: num(meta.get("min_cost"), 5.0),
                    contract_size: num(meta.get("contractSize"), 1.0),
                    maker_fee: num(meta.get("maker_fee"), 0.0002),
                    taker_fee: num(meta.get("taker_fee"), 0.00055),
                },
            );
        }
        let symbol_names: Vec<String> = symbols.keys().cloned().collect();

        let mut candles: BTreeMap<String, Vec<Candle>> = symbol_names
            .iter()
            .map(|s| (s.clone(), Vec::new()))
            .collect();
        let mut timeline = build_timeline(
            v.get("timeline").and_then(Value::as_array),
            start_time_ms,
            tick_interval_ms,
            &symbol_names,
            &mut candles,
        )?;
        if timeline.is_empty() {
            timeline =
                build_replay_timeline(v.get("replay"), &symbol_names, base_dir, &mut candles)?;
        }
        if timeline.is_empty() {
            bail!("Fake scenario must define timeline rows or replay candles");
        }
        let boot_index = num(v.get("boot_index"), 0.0) as usize;
        if boot_index >= timeline.len() {
            bail!(
                "Fake scenario boot_index {boot_index} is outside timeline size {}",
                timeline.len()
            );
        }
        let list = |k: &str| -> Vec<Value> {
            account
                .get(k)
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        };
        Ok(Scenario {
            name,
            quote,
            tick_interval_ms,
            boot_index,
            balance,
            boot_positions: list("positions"),
            boot_fills: list("fills"),
            boot_orders: list("open_orders"),
            symbols,
            candles,
            timeline,
        })
    }
}

/// `_build_timeline` (fake.py:246-298): scripted rows with per-step prices.
fn build_timeline(
    rows: Option<&Vec<Value>>,
    start_time_ms: Option<u64>,
    tick_ms: u64,
    symbols: &[String],
    candles: &mut BTreeMap<String, Vec<Candle>>,
) -> Result<Vec<Step>> {
    let Some(rows) = rows.filter(|r| !r.is_empty()) else {
        return Ok(Vec::new());
    };
    let start = start_time_ms
        .ok_or_else(|| anyhow!("Fake scripted timeline requires scenario start_time"))?;
    let mut prev_prices: BTreeMap<String, f64> = BTreeMap::new();
    let mut out = Vec::new();
    for (idx, row) in rows.iter().enumerate() {
        let obj = row
            .as_object()
            .ok_or_else(|| anyhow!("Fake timeline rows must be mappings"))?;
        let step_num = obj.get("t").and_then(Value::as_i64).unwrap_or(idx as i64);
        let mut prices = prev_prices.clone();
        if let Some(update) = obj.get("prices") {
            let m = update
                .as_object()
                .ok_or_else(|| anyhow!("Fake timeline row prices must be a mapping"))?;
            for (s, p) in m {
                prices.insert(s.clone(), num(Some(p), 0.0));
            }
        }
        let missing: Vec<&String> = symbols
            .iter()
            .filter(|s| !prices.contains_key(*s))
            .collect();
        if !missing.is_empty() {
            bail!(
                "Fake timeline row {idx} missing prices for symbols: {}",
                missing
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        let timestamp = start + step_num as u64 * tick_ms;
        let volume = num(obj.get("volume"), 0.0);
        let mut step_candles = BTreeMap::new();
        for (symbol, close) in &prices {
            let open = prev_prices.get(symbol).copied().unwrap_or(*close);
            let c: Candle = [
                timestamp as f64,
                open,
                open.max(*close),
                open.min(*close),
                *close,
                volume,
            ];
            candles.entry(symbol.clone()).or_default().push(c);
            step_candles.insert(symbol.clone(), c);
        }
        out.push(Step {
            timestamp_ms: timestamp,
            prices: prices.clone(),
            candles: step_candles,
            actions: obj
                .get("actions")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
        });
        prev_prices = prices;
    }
    Ok(out)
}

/// `_build_replay_timeline` (fake.py:300-381): one step per distinct candle
/// timestamp; a symbol without a candle at a step gets a flat candle at its
/// last close.
fn build_replay_timeline(
    replay: Option<&Value>,
    symbols: &[String],
    base_dir: &Path,
    candles: &mut BTreeMap<String, Vec<Candle>>,
) -> Result<Vec<Step>> {
    let Some(replay) = replay.filter(|r| r.is_object() && !r.as_object().unwrap().is_empty())
    else {
        return Ok(Vec::new());
    };
    let specs: BTreeMap<String, Value> = match replay.get("symbols") {
        Some(Value::Object(m)) if !m.is_empty() => {
            m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        }
        Some(Value::Object(_)) | None => {
            if symbols.len() != 1 {
                bail!("Fake replay config without replay.symbols requires exactly one market");
            }
            [(symbols[0].clone(), replay.clone())].into_iter().collect()
        }
        Some(_) => bail!("Fake replay.symbols must be a mapping"),
    };
    let replay_start = match replay.get("start_time") {
        Some(Value::Null) | None => None,
        Some(t) => Some(parse_time_to_ms(t)?),
    };
    let replay_end = match replay.get("end_time") {
        Some(Value::Null) | None => None,
        Some(t) => Some(parse_time_to_ms(t)?),
    };
    let source_dir = replay.get("source_dir").and_then(Value::as_str);
    let mut per_symbol: BTreeMap<String, Vec<Candle>> = BTreeMap::new();
    for symbol in symbols {
        let spec = specs
            .get(symbol)
            .ok_or_else(|| anyhow!("Fake replay missing symbol spec for {symbol}"))?;
        let mut rows = load_replay_rows(symbol, spec, base_dir, source_dir)?;
        if let Some(s) = replay_start {
            rows.retain(|r| r[0] as u64 >= s);
        }
        if let Some(e) = replay_end {
            rows.retain(|r| r[0] as u64 <= e);
        }
        if rows.is_empty() {
            bail!("Fake replay for {symbol} produced no candles after filtering");
        }
        per_symbol.insert(symbol.clone(), rows);
    }
    let mut all_ts: Vec<u64> = per_symbol
        .values()
        .flat_map(|rows| rows.iter().map(|r| r[0] as u64))
        .collect();
    all_ts.sort_unstable();
    all_ts.dedup();
    let mut last: BTreeMap<String, Candle> = BTreeMap::new();
    let mut cursor: BTreeMap<String, usize> = symbols.iter().map(|s| (s.clone(), 0)).collect();
    let mut out = Vec::with_capacity(all_ts.len());
    for ts in all_ts {
        let mut step_candles = BTreeMap::new();
        let mut prices = BTreeMap::new();
        for symbol in symbols {
            let rows = &per_symbol[symbol];
            let i = cursor[symbol];
            let candle = if i < rows.len() && rows[i][0] as u64 == ts {
                *cursor.get_mut(symbol).unwrap() += 1;
                rows[i]
            } else if let Some(prev) = last.get(symbol) {
                let c = prev[4];
                [ts as f64, c, c, c, c, 0.0]
            } else {
                bail!("Fake replay for {symbol} is missing an initial candle at {ts}");
            };
            last.insert(symbol.clone(), candle);
            candles.entry(symbol.clone()).or_default().push(candle);
            step_candles.insert(symbol.clone(), candle);
            prices.insert(symbol.clone(), candle[4]);
        }
        out.push(Step {
            timestamp_ms: ts,
            prices,
            candles: step_candles,
            actions: Vec::new(),
        });
    }
    Ok(out)
}

fn resolve_replay_path(value: &str, base_dir: &Path, source_dir: Option<&str>) -> PathBuf {
    let p = PathBuf::from(value);
    if p.is_absolute() {
        return p;
    }
    let mut base = base_dir.to_path_buf();
    if let Some(sd) = source_dir {
        let s = PathBuf::from(sd);
        base = if s.is_absolute() { s } else { base.join(s) };
    }
    base.join(p)
}

/// `_load_replay_rows` (fake.py:383-427): inline `candles`, `file`, `files`,
/// `glob`; rows deduplicated by timestamp (last wins) and sorted.
fn load_replay_rows(
    symbol: &str,
    spec: &Value,
    base_dir: &Path,
    source_dir: Option<&str>,
) -> Result<Vec<Candle>> {
    let spec = spec
        .as_object()
        .ok_or_else(|| anyhow!("Fake replay spec for {symbol} must be a mapping"))?;
    let mut rows: Vec<Candle> = Vec::new();
    if let Some(inline) = spec.get("candles").and_then(Value::as_array) {
        for c in inline {
            rows.push(normalize_inline_candle(c)?);
        }
    }
    let mut files: Vec<PathBuf> = Vec::new();
    if let Some(f) = spec.get("file").and_then(Value::as_str) {
        files.push(resolve_replay_path(f, base_dir, source_dir));
    }
    for f in spec
        .get("files")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(s) = f.as_str() {
            files.push(resolve_replay_path(s, base_dir, source_dir));
        }
    }
    if let Some(pattern) = spec.get("glob").and_then(Value::as_str) {
        let p = resolve_replay_path(pattern, base_dir, source_dir);
        let dir = p.parent().map(Path::to_path_buf).unwrap_or_default();
        let name = p
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        let mut matched: Vec<PathBuf> = std::fs::read_dir(&dir)
            .with_context(|| dir.display().to_string())?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|path| {
                path.file_name()
                    .map(|f| glob_match(&name, &f.to_string_lossy()))
                    .unwrap_or(false)
            })
            .collect();
        matched.sort();
        files.extend(matched);
    }
    for path in files {
        let ext = path
            .extension()
            .map(|e| e.to_string_lossy().to_string())
            .unwrap_or_default();
        if ext != "npy" {
            bail!(
                "{}: only .npy replay files are supported by the mock",
                path.display()
            );
        }
        rows.extend(read_npy_rows(&path)?);
    }
    let mut dedup: BTreeMap<u64, Candle> = BTreeMap::new();
    for r in rows {
        dedup.insert(r[0] as u64, r);
    }
    Ok(dedup.into_values().collect())
}

/// `*` and `?` only, enough for the fake's day-file globs.
fn glob_match(pattern: &str, name: &str) -> bool {
    fn rec(p: &[u8], n: &[u8]) -> bool {
        match (p.first(), n.first()) {
            (None, None) => true,
            (Some(b'*'), _) => rec(&p[1..], n) || (!n.is_empty() && rec(p, &n[1..])),
            (Some(b'?'), Some(_)) => rec(&p[1..], &n[1..]),
            (Some(a), Some(b)) if a == b => rec(&p[1..], &n[1..]),
            _ => false,
        }
    }
    rec(pattern.as_bytes(), name.as_bytes())
}

fn normalize_inline_candle(c: &Value) -> Result<Candle> {
    if let Some(m) = c.as_object() {
        return Ok([
            parse_time_to_ms(m.get("timestamp").unwrap_or(&Value::Null))? as f64,
            num(m.get("open"), 0.0),
            num(m.get("high"), 0.0),
            num(m.get("low"), 0.0),
            num(m.get("close"), 0.0),
            num(m.get("volume"), 0.0),
        ]);
    }
    if let Some(a) = c.as_array().filter(|a| a.len() == 6) {
        let mut row = [0.0; 6];
        row[0] = parse_time_to_ms(&a[0])? as f64;
        for (i, v) in a.iter().enumerate().skip(1) {
            row[i] = num(Some(v), 0.0);
        }
        return Ok(row);
    }
    bail!("Unsupported fake replay candle shape: {c}")
}

// ---------------------------------------------------------------------------
// Exchange state

#[derive(Debug, Clone, Serialize)]
pub struct MockOrder {
    pub id: String,
    pub symbol: String,
    pub order_type: String,
    pub side: Side,
    pub pside: PositionSide,
    pub amount: f64,
    pub price: f64,
    pub timestamp_ms: u64,
    pub client_order_id: String,
    pub status: String,
    pub reduce_only: bool,
    pub filled: f64,
    pub remaining: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MockFill {
    pub id: String,
    pub order_id: String,
    pub timestamp_ms: u64,
    pub symbol: String,
    pub side: Side,
    pub pside: PositionSide,
    pub amount: f64,
    pub price: f64,
    pub pnl: f64,
    pub fee: f64,
    pub client_order_id: String,
    pub reduce_only: bool,
    /// `maker` / `taker` / `historical` (boot fills).
    pub liquidity: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct MockPosition {
    pub size: f64,
    pub entry_price: f64,
}

/// One exchange request as the fake's `_record_request` logs it
/// (`fake.py:177-191`), restricted to the fields the comparison needs.
#[derive(Debug, Clone, Serialize)]
pub struct RequestRecord {
    pub timestamp_ms: u64,
    pub step_index: usize,
    pub request: Request,
}

#[derive(Debug, Clone, Serialize)]
pub enum Request {
    Create {
        symbol: String,
        side: Side,
        pside: PositionSide,
        amount: f64,
        price: f64,
        reduce_only: bool,
        client_order_id: String,
        order_id: String,
        /// Filled at creation (market, or a limit already crossed).
        filled: bool,
    },
    Cancel {
        symbol: Option<String>,
        order_id: String,
        found: bool,
    },
    Other {
        method: &'static str,
        symbol: Option<String>,
        rows: usize,
    },
}

#[derive(Debug)]
struct State {
    current_index: usize,
    now_ms: u64,
    balance_total: f64,
    balance_free: f64,
    realized_pnl: f64,
    realized_fees: f64,
    position_mode: bool,
    leverage_by_symbol: BTreeMap<String, i64>,
    margin_mode_by_symbol: BTreeMap<String, String>,
    /// Insertion order = `for symbol in sorted(symbols): for pside in (long, short)`
    /// (`fake.py:154-161`); `PositionSide` orders `Long < Short`.
    positions: BTreeMap<(String, PositionSide), MockPosition>,
    /// Python dict order: insertion order, removals keep it.
    open_orders: Vec<MockOrder>,
    fills: Vec<MockFill>,
    requests: Vec<RequestRecord>,
    next_order_id: u64,
    next_trade_id: u64,
}

/// The mock exchange. Shared through `Arc`; the harness driver steps it
/// with [`MockExchange::advance`] between runner cycles exactly as
/// `run_fake_live._run_fake_bot` steps the fake (`advance_time` then one
/// planning cycle).
pub struct MockExchange {
    scenario: Scenario,
    state: Mutex<State>,
}

/// Snapshot of the account for comparisons (`export_state`, `fake.py:972-988`).
#[derive(Debug, Clone, Serialize)]
pub struct AccountSnapshot {
    pub now_ms: u64,
    pub current_index: usize,
    pub balance_total: f64,
    pub balance_free: f64,
    pub realized_pnl: f64,
    pub realized_fees: f64,
    pub open_orders: Vec<MockOrder>,
    /// `(symbol, pside, size, entry_price)` for non-zero positions, sorted.
    pub positions: Vec<(String, PositionSide, f64, f64)>,
    pub fills: Vec<MockFill>,
    pub prices: BTreeMap<String, f64>,
}

fn pside_of(v: Option<&Value>, default: PositionSide) -> PositionSide {
    match v
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("long") => PositionSide::Long,
        Some("short") => PositionSide::Short,
        _ => default,
    }
}

fn side_of(v: Option<&Value>) -> Result<Side> {
    match v
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("buy") => Ok(Side::Buy),
        Some("sell") => Ok(Side::Sell),
        other => bail!("bad side {other:?}"),
    }
}

impl MockExchange {
    pub fn new(scenario: Scenario) -> Result<Self> {
        let boot = &scenario.timeline[scenario.boot_index];
        let mut positions = BTreeMap::new();
        for symbol in scenario.symbols.keys() {
            for pside in [PositionSide::Long, PositionSide::Short] {
                positions.insert(
                    (symbol.clone(), pside),
                    MockPosition {
                        size: 0.0,
                        entry_price: 0.0,
                    },
                );
            }
        }
        let mut st = State {
            current_index: scenario.boot_index,
            now_ms: boot.timestamp_ms,
            balance_total: scenario.balance,
            balance_free: scenario.balance,
            realized_pnl: 0.0,
            realized_fees: 0.0,
            position_mode: true,
            leverage_by_symbol: BTreeMap::new(),
            margin_mode_by_symbol: BTreeMap::new(),
            positions,
            open_orders: Vec::new(),
            fills: Vec::new(),
            requests: Vec::new(),
            next_order_id: 1,
            next_trade_id: 1,
        };
        // `_load_boot_position` (fake.py:468-478)
        for p in &scenario.boot_positions {
            let symbol = p["symbol"].as_str().unwrap_or_default().to_string();
            if !scenario.symbols.contains_key(&symbol) {
                bail!("Unknown fake symbol in boot position: {symbol}");
            }
            let pside = pside_of(p.get("position_side"), PositionSide::Long);
            st.positions.insert(
                (symbol, pside),
                MockPosition {
                    size: num(p.get("qty"), 0.0).abs(),
                    entry_price: num(p.get("price"), 0.0),
                },
            );
        }
        // `_load_boot_fill` (fake.py:511-587): ids bump the counters, pnl and
        // fee join the realized totals but not the balance.
        for f in &scenario.boot_fills {
            let symbol = f["symbol"].as_str().unwrap_or_default().to_string();
            if !scenario.symbols.contains_key(&symbol) {
                bail!("Unknown fake symbol in boot fill: {symbol}");
            }
            let trade_id = f
                .get("id")
                .or(f.get("trade_id"))
                .map(value_to_string)
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| st.next_trade_id.to_string());
            match trade_id.parse::<u64>() {
                Ok(n) => st.next_trade_id = st.next_trade_id.max(n + 1),
                Err(_) => st.next_trade_id += 1,
            }
            let order_id = f
                .get("order")
                .or(f.get("order_id"))
                .or(f.get("orderId"))
                .map(value_to_string)
                .unwrap_or_default();
            if let Ok(n) = order_id.parse::<u64>() {
                st.next_order_id = st.next_order_id.max(n + 1);
            }
            let timestamp = match f.get("timestamp") {
                Some(Value::Null) | None => st.now_ms,
                Some(t) => parse_time_to_ms(t)?,
            };
            let side = side_of(f.get("side"))?;
            let amount = ["amount", "qty", "size", "contracts"]
                .iter()
                .find_map(|k| f.get(*k).and_then(Value::as_f64))
                .map(f64::abs)
                .ok_or_else(|| anyhow!("Fake boot fill amount missing for {symbol}"))?;
            let price = num(f.get("price"), 0.0);
            if amount <= 0.0 {
                bail!("Fake boot fill amount must be > 0 for {symbol}");
            }
            if price <= 0.0 {
                bail!("Fake boot fill price must be > 0 for {symbol}");
            }
            let info = f.get("info").cloned().unwrap_or(Value::Null);
            let pside = pside_of(
                f.get("position_side")
                    .or(f.get("pside"))
                    .or(info.get("positionSide")),
                PositionSide::Long,
            );
            let fee = match f.get("fee") {
                Some(Value::Object(m)) => num(m.get("cost"), 0.0),
                Some(Value::Null) | None => num(f.get("fee_cost"), 0.0),
                Some(v) => num(Some(v), 0.0),
            };
            let pnl = num(f.get("pnl"), 0.0);
            let reduce_only = f
                .get("reduceOnly")
                .or(f.get("reduce_only"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            st.realized_pnl += pnl;
            st.realized_fees += fee;
            st.fills.push(MockFill {
                id: trade_id,
                order_id,
                timestamp_ms: timestamp,
                symbol,
                side,
                pside,
                amount,
                price,
                pnl,
                fee,
                client_order_id: ["clientOrderId", "client_order_id", "custom_id"]
                    .iter()
                    .find_map(|k| f.get(*k).and_then(Value::as_str))
                    .unwrap_or_default()
                    .to_string(),
                reduce_only,
                liquidity: info
                    .get("liquidity")
                    .or(f.get("liquidity"))
                    .and_then(Value::as_str)
                    .unwrap_or("historical")
                    .to_string(),
            });
        }
        // `_load_boot_order` (fake.py:480-509)
        for o in &scenario.boot_orders {
            let symbol = o["symbol"].as_str().unwrap_or_default().to_string();
            let order_id = o
                .get("id")
                .map(value_to_string)
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| st.next_order_id.to_string());
            match order_id.parse::<u64>() {
                Ok(n) => st.next_order_id = st.next_order_id.max(n + 1),
                Err(_) => st.next_order_id += 1,
            }
            let amount = num(o.get("amount"), 0.0).abs();
            let reduce_only = o
                .get("reduce_only")
                .or(o.get("reduceOnly"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            st.open_orders.push(MockOrder {
                id: order_id,
                symbol,
                order_type: o
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("limit")
                    .to_ascii_lowercase(),
                side: side_of(o.get("side"))?,
                pside: pside_of(o.get("position_side"), PositionSide::Long),
                amount,
                price: num(o.get("price"), 0.0),
                timestamp_ms: o
                    .get("timestamp")
                    .and_then(Value::as_u64)
                    .unwrap_or(st.now_ms),
                client_order_id: o
                    .get("clientOrderId")
                    .or(o.get("custom_id"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                status: "open".into(),
                reduce_only,
                filled: 0.0,
                remaining: amount,
            });
        }
        let me = Self {
            scenario,
            state: Mutex::new(st),
        };
        {
            let mut st = me.state.lock().unwrap();
            me.process_resting_orders(&mut st);
        }
        Ok(me)
    }

    pub fn scenario(&self) -> &Scenario {
        &self.scenario
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn now_ms(&self) -> u64 {
        self.lock().now_ms
    }

    pub fn step_index(&self) -> usize {
        self.lock().current_index
    }

    pub fn has_next_step(&self) -> bool {
        self.lock().current_index < self.scenario.timeline.len() - 1
    }

    fn step<'a>(&'a self, st: &State) -> &'a Step {
        &self.scenario.timeline[st.current_index]
    }

    fn c_mult(&self, symbol: &str) -> f64 {
        self.scenario
            .symbols
            .get(symbol)
            .map_or(1.0, |m| m.contract_size)
    }

    fn fee_rate(&self, symbol: &str, liquidity: &str) -> f64 {
        let m = &self.scenario.symbols[symbol];
        if liquidity == "maker" {
            m.maker_fee
        } else {
            m.taker_fee
        }
    }

    fn record(&self, st: &mut State, request: Request) {
        st.requests.push(RequestRecord {
            timestamp_ms: st.now_ms,
            step_index: st.current_index,
            request,
        });
    }

    fn record_other(
        &self,
        st: &mut State,
        method: &'static str,
        symbol: Option<&str>,
        rows: usize,
    ) {
        self.record(
            st,
            Request::Other {
                method,
                symbol: symbol.map(str::to_string),
                rows,
            },
        );
    }

    /// `advance_time(1)` (fake.py:878-888): next step, scripted actions,
    /// then resting orders reached by the new candle fill.
    pub fn advance(&self) -> bool {
        let mut st = self.lock();
        if st.current_index >= self.scenario.timeline.len() - 1 {
            return false;
        }
        st.current_index += 1;
        st.now_ms = self.scenario.timeline[st.current_index].timestamp_ms;
        let actions = self.step(&st).actions.clone();
        for a in &actions {
            self.apply_action(&mut st, a);
        }
        self.process_resting_orders(&mut st);
        true
    }

    /// `_apply_step_actions` (fake.py:890-959): `manual_fill` and
    /// `cancel_open_orders`. Unknown types panic like the fake raises.
    fn apply_action(&self, st: &mut State, action: &Value) {
        let kind = action
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        match kind.as_str() {
            "manual_fill" => {
                let symbol = action["symbol"].as_str().unwrap_or_default().to_string();
                let side = side_of(action.get("side")).expect("manual_fill side");
                let pside = pside_of(action.get("position_side"), PositionSide::Long);
                let qty = num(action.get("qty"), 0.0).abs();
                let price = match action.get("price").and_then(Value::as_f64) {
                    Some(p) if p != 0.0 => p,
                    _ => self.step(st).prices[&symbol],
                };
                let reduce_only = action
                    .get("reduce_only")
                    .or(action.get("reduceOnly"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let id = action
                    .get("id")
                    .map(value_to_string)
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| format!("manual_{}", st.next_order_id));
                let order = MockOrder {
                    id,
                    symbol,
                    order_type: "market".into(),
                    side,
                    pside,
                    amount: qty,
                    price,
                    timestamp_ms: st.now_ms,
                    client_order_id: action
                        .get("clientOrderId")
                        .or(action.get("client_order_id"))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    status: "open".into(),
                    reduce_only,
                    filled: 0.0,
                    remaining: qty,
                };
                st.next_order_id += 1;
                self.fill_order(st, order, price, "taker");
            }
            "cancel_open_orders" => {
                let symbol = action.get("symbol").and_then(Value::as_str);
                let pside = action
                    .get("position_side")
                    .and_then(Value::as_str)
                    .map(|s| pside_of(Some(&Value::String(s.into())), PositionSide::Long));
                let side = action.get("side").and_then(|v| side_of(Some(v)).ok());
                let reduce_only = action.get("reduce_only").and_then(Value::as_bool);
                let cid = action
                    .get("clientOrderId")
                    .or(action.get("client_order_id"))
                    .and_then(Value::as_str);
                st.open_orders.retain(|o| {
                    let matches = symbol.is_none_or(|s| o.symbol == s)
                        && side.is_none_or(|s| o.side == s)
                        && pside.is_none_or(|p| o.pside == p)
                        && reduce_only.is_none_or(|r| o.reduce_only == r)
                        && cid.is_none_or(|c| o.client_order_id == c);
                    !matches
                });
            }
            other => panic!("Unsupported fake step action type: {other:?}"),
        }
    }

    /// `_process_resting_orders_for_current_step` (fake.py:1015-1029): a
    /// resting buy fills when the step candle's low reaches its price, a
    /// sell when the high does; fills happen in book order at the order price
    /// as maker.
    fn process_resting_orders(&self, st: &mut State) {
        let candles = self.step(st).candles.clone();
        let mut fill_ids: Vec<String> = Vec::new();
        for o in &st.open_orders {
            let c = &candles[&o.symbol];
            let (high, low) = (c[2], c[3]);
            let hit = match o.side {
                Side::Buy => low <= o.price,
                Side::Sell => high >= o.price,
            };
            if hit {
                fill_ids.push(o.id.clone());
            }
        }
        for id in fill_ids {
            let pos = st.open_orders.iter().position(|o| o.id == id).unwrap();
            let order = st.open_orders.remove(pos);
            let price = order.price;
            self.fill_order(st, order, price, "maker");
        }
    }

    /// `_limit_crossed_now` (fake.py:1031-1036) on the step's last price.
    fn limit_crossed_now(&self, st: &State, symbol: &str, side: Side, price: f64) -> bool {
        let last = self.step(st).prices[symbol];
        match side {
            Side::Buy => last <= price,
            Side::Sell => last >= price,
        }
    }

    /// `_fill_order` (fake.py:1063-1140): position netting per pside with
    /// average entry price, pnl on the closing side (clamped to the position,
    /// never flips), fee on the notional, balance += pnl - fee, one trade.
    fn fill_order(&self, st: &mut State, mut order: MockOrder, fill_price: f64, liquidity: &str) {
        order.status = "closed".into();
        order.filled = order.amount;
        order.remaining = 0.0;
        order.price = fill_price;
        let symbol = order.symbol.clone();
        let c_mult = self.c_mult(&symbol);
        let qty = order.amount.abs();
        let key = (symbol.clone(), order.pside);
        let pos = st.positions.entry(key).or_insert(MockPosition {
            size: 0.0,
            entry_price: 0.0,
        });
        let mut pnl = 0.0;
        let increases = matches!(
            (order.pside, order.side),
            (PositionSide::Long, Side::Buy) | (PositionSide::Short, Side::Sell)
        );
        if increases {
            let new_size = pos.size + qty;
            let new_entry = if new_size == 0.0 {
                0.0
            } else {
                (pos.entry_price * pos.size + fill_price * qty) / new_size
            };
            pos.size = new_size;
            pos.entry_price = new_entry;
        } else {
            let close_qty = pos.size.min(qty);
            pnl = match order.pside {
                PositionSide::Long => (fill_price - pos.entry_price) * close_qty * c_mult,
                PositionSide::Short => (pos.entry_price - fill_price) * close_qty * c_mult,
            };
            pos.size = (pos.size - close_qty).max(0.0);
            if pos.size == 0.0 {
                pos.entry_price = 0.0;
            }
        }
        let fee_rate = self.fee_rate(&symbol, liquidity);
        let fee_cost = fill_price * qty * c_mult * fee_rate;
        st.realized_pnl += pnl;
        st.realized_fees += fee_cost;
        st.balance_total += pnl - fee_cost;
        st.balance_free = st.balance_total;
        let trade_id = st.next_trade_id.to_string();
        st.next_trade_id += 1;
        st.fills.push(MockFill {
            id: trade_id,
            order_id: order.id.clone(),
            timestamp_ms: st.now_ms,
            symbol,
            side: order.side,
            pside: order.pside,
            amount: qty,
            price: fill_price,
            pnl,
            fee: fee_cost,
            client_order_id: order.client_order_id.clone(),
            reduce_only: order.reduce_only,
            liquidity: liquidity.to_string(),
        });
    }

    fn to_open_order(o: &MockOrder) -> OpenOrder {
        OpenOrder {
            id: o.id.clone(),
            client_id: (!o.client_order_id.is_empty()).then(|| o.client_order_id.clone()),
            symbol: o.symbol.clone(),
            side: o.side,
            pside: o.pside,
            qty: o.remaining.max(0.0),
            price: o.price,
            reduce_only: o.reduce_only,
            created_ms: Some(o.timestamp_ms),
        }
    }

    /// `create_order` (fake.py:729-822) for one limit order (the runner's
    /// Bybit client only ever sends `orderType: Limit`, so [`NewOrder`] has
    /// no type; market orders are reachable only through scenario actions).
    fn create_one(&self, st: &mut State, o: &NewOrder) -> OrderResult<OpenOrder> {
        if !self.scenario.symbols.contains_key(&o.symbol) {
            return Err(ExchangeError::Rejected {
                code: "fake_unknown_symbol".into(),
                msg: format!("Unknown fake symbol {}", o.symbol),
            });
        }
        let amount_abs = o.qty.abs();
        if o.reduce_only {
            let pos = st.positions[&(o.symbol.clone(), o.pside)];
            let opens = matches!(
                (o.pside, o.side),
                (PositionSide::Long, Side::Buy) | (PositionSide::Short, Side::Sell)
            );
            if opens {
                return Err(ExchangeError::Rejected {
                    code: "fake_reduce_only".into(),
                    msg: format!(
                        "Fake reduce-only order would increase {:?} position: {:?} {}",
                        o.pside, o.side, o.symbol
                    ),
                });
            }
            if amount_abs > pos.size + 1e-12 {
                return Err(ExchangeError::Rejected {
                    code: "fake_reduce_only".into(),
                    msg: format!(
                        "Fake reduce-only order amount {amount_abs} exceeds {:?} position size {} for {}",
                        o.pside, pos.size, o.symbol
                    ),
                });
            }
        }
        let order_id = st.next_order_id.to_string();
        st.next_order_id += 1;
        let order = MockOrder {
            id: order_id.clone(),
            symbol: o.symbol.clone(),
            order_type: "limit".into(),
            side: o.side,
            pside: o.pside,
            amount: amount_abs,
            price: o.price,
            timestamp_ms: st.now_ms,
            client_order_id: o.client_id.clone(),
            status: "open".into(),
            reduce_only: o.reduce_only,
            filled: 0.0,
            remaining: amount_abs,
        };
        let filled = self.limit_crossed_now(st, &o.symbol, o.side, o.price);
        self.record(
            st,
            Request::Create {
                symbol: o.symbol.clone(),
                side: o.side,
                pside: o.pside,
                amount: amount_abs,
                price: o.price,
                reduce_only: o.reduce_only,
                client_order_id: o.client_id.clone(),
                order_id,
                filled,
            },
        );
        let ack = Self::to_open_order(&order);
        if filled {
            let price = order.price;
            self.fill_order(st, order, price, "maker");
        } else {
            st.open_orders.push(order);
        }
        Ok(ack)
    }

    /// `cancel_order` (fake.py:824-842): the order is removed before the
    /// symbol check; unknown ids raise.
    fn cancel_one(&self, st: &mut State, id: &str, symbol: &str) -> OrderResult<String> {
        let pos = st.open_orders.iter().position(|o| o.id == id);
        self.record(
            st,
            Request::Cancel {
                symbol: Some(symbol.to_string()),
                order_id: id.to_string(),
                found: pos.is_some(),
            },
        );
        let Some(pos) = pos else {
            return Err(ExchangeError::Rejected {
                code: "fake_order_not_found".into(),
                msg: format!("Fake order {id} not found"),
            });
        };
        let order = st.open_orders.remove(pos);
        if order.symbol != symbol {
            return Err(ExchangeError::Rejected {
                code: "fake_order_symbol_mismatch".into(),
                msg: format!("Fake order {id} belongs to {}, not {symbol}", order.symbol),
            });
        }
        Ok(id.to_string())
    }

    /// Requests recorded so far (`export_request_log`).
    pub fn requests(&self) -> Vec<RequestRecord> {
        self.lock().requests.clone()
    }

    pub fn request_count(&self) -> usize {
        self.lock().requests.len()
    }

    /// Requests recorded at or after index `from`.
    pub fn requests_since(&self, from: usize) -> Vec<RequestRecord> {
        self.lock().requests.iter().skip(from).cloned().collect()
    }

    pub fn snapshot(&self) -> AccountSnapshot {
        let st = self.lock();
        AccountSnapshot {
            now_ms: st.now_ms,
            current_index: st.current_index,
            balance_total: st.balance_total,
            balance_free: st.balance_free,
            realized_pnl: st.realized_pnl,
            realized_fees: st.realized_fees,
            open_orders: st.open_orders.clone(),
            positions: st
                .positions
                .iter()
                .filter(|(_, p)| p.size != 0.0)
                .map(|((s, ps), p)| (s.clone(), *ps, p.size, p.entry_price))
                .collect(),
            fills: st.fills.clone(),
            prices: self.step(&st).prices.clone(),
        }
    }

    fn fetch_ohlcv_sync(
        &self,
        symbol: &str,
        timeframe: &str,
        since_ms: Option<u64>,
        limit: usize,
    ) -> Result<Vec<Candle>, ExchangeError> {
        let period = parse_timeframe_ms(timeframe)?;
        let mut st = self.lock();
        let all = self
            .scenario
            .candles
            .get(symbol)
            .ok_or_else(|| ExchangeError::Rejected {
                code: "fake_unknown_symbol".into(),
                msg: format!("Unknown fake symbol {symbol}"),
            })?;
        // `candles[: current_index + 1]` (fake.py:674): the current, still
        // open minute is included.
        let mut rows: Vec<Candle> = all[..=st.current_index].to_vec();
        if period != ONE_MIN_MS {
            rows = aggregate(&rows, period);
        }
        // Bybit client contract (`bybit/mod.rs::fetch_ohlcv`): `since`
        // floored to the bucket, then up to 5 pages of `limit` rows, each
        // page starting at the last row of the previous one.
        let page = if limit == 0 { 1000 } else { limit.min(1000) };
        let out: Vec<Candle> = match since_ms {
            None => rows.into_iter().rev().take(page).rev().collect(),
            Some(since) => {
                let mut since = since / period * period;
                let mut acc: BTreeMap<u64, Candle> = BTreeMap::new();
                for _ in 0..5 {
                    let page_rows: Vec<Candle> = rows
                        .iter()
                        .filter(|r| r[0] as u64 >= since)
                        .take(page)
                        .copied()
                        .collect();
                    if page_rows.is_empty() {
                        break;
                    }
                    let n = page_rows.len();
                    for c in page_rows {
                        acc.insert(c[0] as u64, c);
                    }
                    if n < page {
                        break;
                    }
                    since = acc.keys().next_back().copied().unwrap_or(since);
                }
                acc.into_values().collect()
            }
        };
        self.record_other(&mut st, "fetch_ohlcv", Some(symbol), out.len());
        Ok(out)
    }
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// `_aggregate_candles` (fake.py:1038-1061): bucket by `ts // tf * tf`,
/// open of the first row, max high, min low, last close, summed volume.
pub fn aggregate(rows: &[Candle], timeframe_ms: u64) -> Vec<Candle> {
    if timeframe_ms <= ONE_MIN_MS {
        return rows.to_vec();
    }
    let mut out: Vec<Candle> = Vec::new();
    for r in rows {
        let bucket = ((r[0] as u64) / timeframe_ms * timeframe_ms) as f64;
        match out.last_mut() {
            Some(b) if b[0] == bucket => {
                b[2] = b[2].max(r[2]);
                b[3] = b[3].min(r[3]);
                b[4] = r[4];
                b[5] += r[5];
            }
            _ => out.push([bucket, r[1], r[2], r[3], r[4], r[5]]),
        }
    }
    out
}

#[async_trait]
impl ExchangeClient for MockExchange {
    async fn load_markets(&self) -> Result<Vec<MarketSpec>, ExchangeError> {
        let mut st = self.lock();
        self.record_other(&mut st, "load_markets", None, self.scenario.symbols.len());
        Ok(self
            .scenario
            .symbols
            .iter()
            .map(|(symbol, m)| MarketSpec {
                symbol: symbol.clone(),
                id: m.id.clone(),
                qty_step: m.qty_step,
                price_step: m.price_step,
                min_qty: m.min_qty,
                // `market["limits"]["cost"]["min"]` is set by the fake
                // (`fake.py:230`), unlike Bybit through ccxt.
                min_cost: m.min_cost,
                min_notional: None,
                contract_size: m.contract_size,
                // No leverage metadata in the fake ("max leverage unavailable
                // from exchange metadata; using configured leverage").
                max_leverage: 0.0,
                maker_fee: m.maker_fee,
                taker_fee: m.taker_fee,
            })
            .collect())
    }

    async fn fetch_balance(&self) -> Result<Balance, ExchangeError> {
        let mut st = self.lock();
        self.record_other(&mut st, "fetch_balance", None, 1);
        Ok(Balance {
            total_usdt: st.balance_total,
            available_usdt: st.balance_free,
            account_type: "FAKE".into(),
        })
    }

    async fn fetch_positions(&self) -> Result<Vec<Position>, ExchangeError> {
        let mut st = self.lock();
        let out: Vec<Position> = st
            .positions
            .iter()
            .filter(|(_, p)| p.size != 0.0)
            .map(|((symbol, pside), p)| Position {
                symbol: symbol.clone(),
                pside: *pside,
                size: p.size,
                entry_price: p.entry_price,
                leverage: None,
                margin_mode: None,
                updated_ms: None,
            })
            .collect();
        self.record_other(&mut st, "fetch_positions", None, out.len());
        Ok(out)
    }

    async fn fetch_open_orders(&self) -> Result<Vec<OpenOrder>, ExchangeError> {
        let mut st = self.lock();
        let mut orders: Vec<&MockOrder> = st.open_orders.iter().collect();
        // `sorted(orders, key=(timestamp, id))` with the id as a string (fake.py:635).
        orders
            .sort_by(|a, b| (a.timestamp_ms, a.id.as_str()).cmp(&(b.timestamp_ms, b.id.as_str())));
        let out: Vec<OpenOrder> = orders.iter().map(|o| Self::to_open_order(o)).collect();
        self.record_other(&mut st, "fetch_open_orders", None, out.len());
        Ok(out)
    }

    async fn fetch_tickers(&self) -> Result<Vec<Ticker>, ExchangeError> {
        let mut st = self.lock();
        let prices = self.step(&st).prices.clone();
        self.record_other(&mut st, "fetch_tickers", None, prices.len());
        Ok(self
            .scenario
            .symbols
            .keys()
            .map(|s| {
                let p = prices[s];
                Ticker {
                    symbol: s.clone(),
                    bid: p,
                    ask: p,
                    last: p,
                    quote_volume_24h: 0.0,
                }
            })
            .collect())
    }

    async fn fetch_ohlcv(
        &self,
        symbol: &str,
        timeframe: &str,
        since_ms: Option<u64>,
        limit: usize,
    ) -> Result<Vec<Candle>, ExchangeError> {
        self.fetch_ohlcv_sync(symbol, timeframe, since_ms, limit)
    }

    async fn fetch_fills(
        &self,
        symbol: Option<&str>,
        start_ms: Option<u64>,
        end_ms: Option<u64>,
    ) -> Result<Vec<Fill>, ExchangeError> {
        let mut st = self.lock();
        let mut fills: Vec<&MockFill> = st
            .fills
            .iter()
            .filter(|f| symbol.is_none_or(|s| f.symbol == s))
            .filter(|f| start_ms.is_none_or(|s| f.timestamp_ms >= s))
            .filter(|f| end_ms.is_none_or(|e| f.timestamp_ms <= e))
            .collect();
        fills.sort_by(|a, b| (a.timestamp_ms, a.id.as_str()).cmp(&(b.timestamp_ms, b.id.as_str())));
        let out: Vec<Fill> = fills
            .iter()
            .map(|f| Fill {
                id: f.id.clone(),
                order_id: f.order_id.clone(),
                client_id: (!f.client_order_id.is_empty()).then(|| f.client_order_id.clone()),
                symbol: f.symbol.clone(),
                side: f.side,
                pside: f.pside,
                qty: f.amount,
                price: f.price,
                fee: f.fee,
                is_maker: f.liquidity == "maker",
                timestamp_ms: f.timestamp_ms,
            })
            .collect();
        self.record_other(&mut st, "fetch_my_trades", symbol, out.len());
        Ok(out)
    }

    /// The fake has no closed-pnl endpoint: its fills carry `realizedPnl`
    /// (`fake.py:1135`), which the Python bot reads per fill. One record per
    /// position-reducing fill gives the runner's `realized_pnl_cumsum` the
    /// same series.
    async fn fetch_closed_pnl(
        &self,
        start_ms: Option<u64>,
        end_ms: Option<u64>,
    ) -> Result<Vec<ClosedPnl>, ExchangeError> {
        let st = self.lock();
        let mut out: Vec<ClosedPnl> = st
            .fills
            .iter()
            .filter(|f| start_ms.is_none_or(|s| f.timestamp_ms >= s))
            .filter(|f| end_ms.is_none_or(|e| f.timestamp_ms <= e))
            .filter(|f| {
                matches!(
                    (f.pside, f.side),
                    (PositionSide::Long, Side::Sell) | (PositionSide::Short, Side::Buy)
                )
            })
            .map(|f| ClosedPnl {
                order_id: f.order_id.clone(),
                symbol: f.symbol.clone(),
                pside: f.pside,
                pnl: f.pnl,
                timestamp_ms: f.timestamp_ms,
            })
            .collect();
        out.sort_by(|a, b| {
            (a.timestamp_ms, a.order_id.as_str()).cmp(&(b.timestamp_ms, b.order_id.as_str()))
        });
        Ok(out)
    }

    /// Sequential in slice order: `asyncio.gather` starts the fake's
    /// coroutines in list order and `create_order` has no await before the
    /// id is taken, so ids follow the request order.
    async fn create_orders(&self, orders: &[NewOrder]) -> Vec<OrderResult<OpenOrder>> {
        let mut st = self.lock();
        orders.iter().map(|o| self.create_one(&mut st, o)).collect()
    }

    async fn cancel_orders(&self, orders: &[(String, String)]) -> Vec<OrderResult<String>> {
        let mut st = self.lock();
        orders
            .iter()
            .map(|(id, symbol)| self.cancel_one(&mut st, id, symbol))
            .collect()
    }

    async fn set_hedge_mode(&self) -> Result<(), ExchangeError> {
        let mut st = self.lock();
        st.position_mode = true;
        self.record_other(&mut st, "set_position_mode", None, 1);
        Ok(())
    }

    async fn configure_symbol(
        &self,
        symbol: &str,
        leverage: f64,
        margin_mode: MarginMode,
    ) -> Result<(), ExchangeError> {
        let mut st = self.lock();
        st.leverage_by_symbol
            .insert(symbol.to_string(), leverage as i64);
        self.record_other(&mut st, "set_leverage", Some(symbol), 1);
        st.margin_mode_by_symbol.insert(
            symbol.to_string(),
            match margin_mode {
                MarginMode::Cross => "cross".into(),
                MarginMode::Isolated => "isolated".into(),
            },
        );
        self.record_other(&mut st, "set_margin_mode", Some(symbol), 1);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const T0: u64 = 1_700_000_000_000 / 60_000 * 60_000;

    fn scenario(rows: Vec<Value>, account: Value) -> Scenario {
        let v = json!({
            "name": "t",
            "start_time": T0,
            "tick_interval_seconds": 60,
            "boot_index": 0,
            "account": account,
            "symbols": {
                "BTC/USDT:USDT": {"qty_step": 0.001, "price_step": 0.1, "min_qty": 0.001,
                                  "min_cost": 5.0, "contractSize": 1.0,
                                  "maker_fee": 0.0001, "taker_fee": 0.0006},
                "ADA/USDT:USDT": {"qty_step": 1.0, "price_step": 0.0001, "min_qty": 1.0,
                                  "min_cost": 5.0, "contractSize": 2.0,
                                  "maker_fee": 0.0001, "taker_fee": 0.0006}
            },
            "timeline": rows,
        });
        Scenario::from_value(&v, Path::new("."), "t".into()).unwrap()
    }

    fn prices(btc: f64, ada: f64) -> Value {
        json!({"prices": {"BTC/USDT:USDT": btc, "ADA/USDT:USDT": ada}})
    }

    fn order(
        symbol: &str,
        side: Side,
        pside: PositionSide,
        qty: f64,
        price: f64,
        ro: bool,
    ) -> NewOrder {
        NewOrder {
            client_id: format!("0x0004{:0>30}", "1"),
            symbol: symbol.into(),
            side,
            pside,
            qty,
            price,
            reduce_only: ro,
            post_only: false,
        }
    }

    fn rt<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn scripted_timeline_builds_candles_from_consecutive_closes() {
        let sc = scenario(
            vec![prices(100.0, 1.0), prices(90.0, 1.2), prices(95.0, 1.1)],
            json!({"balance": 1000.0}),
        );
        assert_eq!(sc.timeline.len(), 3);
        assert_eq!(sc.timeline[1].timestamp_ms, T0 + 60_000);
        // fake.py:271-282: open = previous close, high/low = max/min(open, close)
        assert_eq!(
            sc.candles["BTC/USDT:USDT"][1],
            [(T0 + 60_000) as f64, 100.0, 100.0, 90.0, 90.0, 0.0]
        );
        assert_eq!(
            sc.candles["ADA/USDT:USDT"][0],
            [T0 as f64, 1.0, 1.0, 1.0, 1.0, 0.0]
        );
    }

    #[test]
    fn resting_limit_buy_fills_when_the_next_candle_low_reaches_it() {
        let sc = scenario(
            vec![prices(100.0, 1.0), prices(90.0, 1.0), prices(95.0, 1.0)],
            json!({"balance": 1000.0}),
        );
        let ex = MockExchange::new(sc).unwrap();
        let acks = rt(ex.create_orders(&[order(
            "BTC/USDT:USDT",
            Side::Buy,
            PositionSide::Long,
            0.5,
            92.0,
            false,
        )]));
        let ack = acks[0].as_ref().unwrap();
        assert_eq!(ack.id, "1");
        assert_eq!(rt(ex.fetch_open_orders()).unwrap().len(), 1);
        // step 1: low 90 <= 92 -> fills at the order price (fake.py:1023, 1029)
        assert!(ex.advance());
        assert!(rt(ex.fetch_open_orders()).unwrap().is_empty());
        let snap = ex.snapshot();
        assert_eq!(
            snap.positions,
            vec![("BTC/USDT:USDT".to_string(), PositionSide::Long, 0.5, 92.0)]
        );
        let fee = 92.0 * 0.5 * 1.0 * 0.0001;
        assert_eq!(snap.balance_total, 1000.0 - fee);
        assert_eq!(snap.balance_free, snap.balance_total);
        assert_eq!(snap.realized_fees, fee);
        assert_eq!(snap.realized_pnl, 0.0);
        let fills = rt(ex.fetch_fills(None, None, None)).unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].id, "1");
        assert_eq!(fills[0].order_id, "1");
        assert!(fills[0].is_maker);
        assert_eq!(fills[0].timestamp_ms, T0 + 60_000);
        assert_eq!(fills[0].price, 92.0);
        assert_eq!(fills[0].fee, fee);
    }

    #[test]
    fn crossed_limit_fills_at_creation_at_the_order_price() {
        let sc = scenario(
            vec![prices(100.0, 1.0), prices(100.0, 1.0)],
            json!({"balance": 1000.0}),
        );
        let ex = MockExchange::new(sc).unwrap();
        // buy at 105 with last 100: last <= price -> immediate maker fill at 105 (fake.py:817-819)
        let acks = rt(ex.create_orders(&[order(
            "BTC/USDT:USDT",
            Side::Buy,
            PositionSide::Long,
            1.0,
            105.0,
            false,
        )]));
        assert!(acks[0].is_ok());
        assert!(rt(ex.fetch_open_orders()).unwrap().is_empty());
        let snap = ex.snapshot();
        assert_eq!(snap.positions[0].2, 1.0);
        assert_eq!(snap.positions[0].3, 105.0);
        assert_eq!(snap.fills[0].price, 105.0);
        assert_eq!(snap.fills[0].liquidity, "maker");
        let reqs = ex.requests();
        let last_create = reqs
            .iter()
            .rev()
            .find(|r| matches!(r.request, Request::Create { .. }))
            .unwrap();
        assert!(matches!(
            last_create.request,
            Request::Create { filled: true, .. }
        ));
        // a sell resting above the market rests
        let acks = rt(ex.create_orders(&[order(
            "BTC/USDT:USDT",
            Side::Sell,
            PositionSide::Long,
            1.0,
            110.0,
            true,
        )]));
        assert_eq!(acks[0].as_ref().unwrap().id, "2");
        assert_eq!(rt(ex.fetch_open_orders()).unwrap().len(), 1);
    }

    #[test]
    fn closes_realize_pnl_and_clamp_to_the_position() {
        let sc = scenario(
            vec![prices(100.0, 1.0), prices(100.0, 1.0), prices(120.0, 1.0)],
            json!({"balance": 1000.0,
                   "positions": [{"symbol": "ADA/USDT:USDT", "position_side": "long", "qty": 10.0, "price": 1.0}]}),
        );
        let ex = MockExchange::new(sc).unwrap();
        // not reduce-only, larger than the position: closes 10, never flips (fake.py:1085-1089)
        let acks = rt(ex.create_orders(&[order(
            "ADA/USDT:USDT",
            Side::Sell,
            PositionSide::Long,
            15.0,
            0.9,
            false,
        )]));
        assert!(acks[0].is_ok());
        let snap = ex.snapshot();
        assert!(snap.positions.is_empty());
        let c_mult = 2.0;
        let pnl = (0.9 - 1.0) * 10.0 * c_mult;
        let fee = 0.9 * 15.0 * c_mult * 0.0001;
        assert_eq!(snap.realized_pnl, pnl);
        assert_eq!(snap.balance_total, 1000.0 + pnl - fee);
        let closed = rt(ex.fetch_closed_pnl(None, None)).unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].pnl, pnl);
        assert_eq!(closed[0].pside, PositionSide::Long);
    }

    #[test]
    fn entry_price_averages_in_the_fake_order_of_operations() {
        let sc = scenario(
            vec![prices(100.0, 1.0), prices(100.0, 1.0)],
            json!({"balance": 1000.0}),
        );
        let ex = MockExchange::new(sc).unwrap();
        rt(ex.create_orders(&[
            order(
                "BTC/USDT:USDT",
                Side::Buy,
                PositionSide::Long,
                1.0,
                100.0,
                false,
            ),
            order(
                "BTC/USDT:USDT",
                Side::Buy,
                PositionSide::Long,
                3.0,
                110.0,
                false,
            ),
        ]));
        let snap = ex.snapshot();
        // (100*1 + 110*3) / 4 (fake.py:1079-1081)
        assert_eq!(snap.positions[0].2, 4.0);
        assert_eq!(snap.positions[0].3, (100.0 * 1.0 + 110.0 * 3.0) / 4.0);
        assert_eq!(
            snap.fills.iter().map(|f| f.id.as_str()).collect::<Vec<_>>(),
            ["1", "2"]
        );
    }

    #[test]
    fn short_side_mirrors_long() {
        let sc = scenario(
            vec![prices(100.0, 1.0), prices(110.0, 1.0), prices(80.0, 1.0)],
            json!({"balance": 1000.0}),
        );
        let ex = MockExchange::new(sc).unwrap();
        // sell short at 105 rests (last 100 < 105); step 1 high 110 >= 105 -> fills
        rt(ex.create_orders(&[order(
            "BTC/USDT:USDT",
            Side::Sell,
            PositionSide::Short,
            2.0,
            105.0,
            false,
        )]));
        assert!(ex.advance());
        let snap = ex.snapshot();
        assert_eq!(
            snap.positions,
            vec![("BTC/USDT:USDT".to_string(), PositionSide::Short, 2.0, 105.0)]
        );
        // reduce-only buy at 90: last 110 > 90 rests; step 2 low 80 <= 90 -> fills with pnl (105-90)*2
        rt(ex.create_orders(&[order(
            "BTC/USDT:USDT",
            Side::Buy,
            PositionSide::Short,
            2.0,
            90.0,
            true,
        )]));
        assert!(ex.advance());
        let snap = ex.snapshot();
        assert!(snap.positions.is_empty());
        assert_eq!(snap.realized_pnl, (105.0 - 90.0) * 2.0);
    }

    #[test]
    fn reduce_only_validation_rejects_without_consuming_an_id() {
        let sc = scenario(
            vec![prices(100.0, 1.0), prices(100.0, 1.0)],
            json!({"balance": 1000.0}),
        );
        let ex = MockExchange::new(sc).unwrap();
        let acks = rt(ex.create_orders(&[
            // reduce-only buy on a long would increase it (fake.py:764-767)
            order(
                "BTC/USDT:USDT",
                Side::Buy,
                PositionSide::Long,
                1.0,
                90.0,
                true,
            ),
            // reduce-only sell larger than the (empty) position (fake.py:768-772)
            order(
                "BTC/USDT:USDT",
                Side::Sell,
                PositionSide::Long,
                1.0,
                110.0,
                true,
            ),
            order(
                "BTC/USDT:USDT",
                Side::Buy,
                PositionSide::Long,
                1.0,
                90.0,
                false,
            ),
        ]));
        assert!(matches!(acks[0], Err(ExchangeError::Rejected { .. })));
        assert!(matches!(acks[1], Err(ExchangeError::Rejected { .. })));
        assert_eq!(acks[2].as_ref().unwrap().id, "1");
    }

    #[test]
    fn boot_fills_and_orders_seed_the_id_counters() {
        let sc = scenario(
            vec![prices(100.0, 1.0), prices(100.0, 1.0)],
            json!({"balance": 1000.0,
                   "positions": [{"symbol": "BTC/USDT:USDT", "position_side": "long", "qty": 0.001, "price": 100.0}],
                   "fills": [{"id": "1", "order_id": "1000", "symbol": "BTC/USDT:USDT", "side": "buy",
                              "position_side": "long", "amount": 0.001, "price": 100.0, "timestamp": T0 - 60_000}],
                   "open_orders": [{"id": "7", "symbol": "ADA/USDT:USDT", "side": "buy", "amount": 5.0, "price": 0.5,
                                    "position_side": "long", "clientOrderId": "boot"}]}),
        );
        let ex = MockExchange::new(sc).unwrap();
        let snap = ex.snapshot();
        assert_eq!(snap.fills.len(), 1);
        assert_eq!(snap.fills[0].liquidity, "historical");
        assert_eq!(snap.balance_total, 1000.0);
        assert_eq!(snap.open_orders.len(), 1);
        let acks = rt(ex.create_orders(&[order(
            "BTC/USDT:USDT",
            Side::Sell,
            PositionSide::Long,
            0.001,
            200.0,
            true,
        )]));
        // fake.py:521-525: next order id = max(next, 1000 + 1) -> 1001; boot order 7 < 1001
        assert_eq!(acks[0].as_ref().unwrap().id, "1001");
        let fills = rt(ex.fetch_fills(None, Some(T0), None)).unwrap();
        assert!(fills.is_empty(), "boot fill is before `since`");
        // the crossing sell fills as trade 2 (fake.py:517: next trade id = 1 + 1)
        rt(ex.create_orders(&[order(
            "BTC/USDT:USDT",
            Side::Sell,
            PositionSide::Long,
            0.001,
            90.0,
            true,
        )]));
        assert_eq!(ex.snapshot().fills.last().unwrap().id, "2");
    }

    #[test]
    fn cancel_removes_or_rejects() {
        let sc = scenario(
            vec![prices(100.0, 1.0), prices(100.0, 1.0)],
            json!({"balance": 1000.0}),
        );
        let ex = MockExchange::new(sc).unwrap();
        rt(ex.create_orders(&[order(
            "BTC/USDT:USDT",
            Side::Buy,
            PositionSide::Long,
            1.0,
            90.0,
            false,
        )]));
        let r = rt(ex.cancel_orders(&[
            ("1".into(), "BTC/USDT:USDT".into()),
            ("1".into(), "BTC/USDT:USDT".into()),
        ]));
        assert!(r[0].is_ok());
        assert!(matches!(r[1], Err(ExchangeError::Rejected { .. })));
        assert!(rt(ex.fetch_open_orders()).unwrap().is_empty());
        let reqs = ex.requests();
        let cancels: Vec<bool> = reqs
            .iter()
            .filter_map(|r| match &r.request {
                Request::Cancel { found, .. } => Some(*found),
                _ => None,
            })
            .collect();
        assert_eq!(cancels, [true, false]);
    }

    #[test]
    fn open_orders_sort_by_timestamp_then_id_string() {
        let sc = scenario(
            vec![prices(100.0, 1.0), prices(100.0, 1.0)],
            json!({"balance": 1000.0}),
        );
        let ex = MockExchange::new(sc).unwrap();
        let mut orders = Vec::new();
        for i in 0..11 {
            orders.push(order(
                "BTC/USDT:USDT",
                Side::Buy,
                PositionSide::Long,
                1.0,
                50.0 + i as f64,
                false,
            ));
        }
        rt(ex.create_orders(&orders));
        let ids: Vec<String> = rt(ex.fetch_open_orders())
            .unwrap()
            .into_iter()
            .map(|o| o.id)
            .collect();
        // same timestamp: "1" < "10" < "11" < "2" ... (fake.py:635)
        assert_eq!(ids[..4], ["1", "10", "11", "2"]);
    }

    #[test]
    fn fetch_ohlcv_pages_from_since_and_aggregates_hours() {
        let mut rows = Vec::new();
        for i in 0..130 {
            rows.push(prices(100.0 + i as f64, 1.0));
        }
        let mut sc = scenario(rows, json!({"balance": 1000.0}));
        sc.boot_index = 129;
        let ex = MockExchange::new(sc).unwrap();
        let m1 = rt(ex.fetch_ohlcv("BTC/USDT:USDT", "1m", Some(T0 + 10 * 60_000 + 5), 50)).unwrap();
        // since floored to the minute, oldest first, up to 5 pages of 50 (with overlap)
        assert_eq!(m1[0][0] as u64, T0 + 10 * 60_000);
        assert_eq!(m1.len(), 120);
        assert_eq!(m1.last().unwrap()[0] as u64, T0 + 129 * 60_000);
        let h1 = rt(ex.fetch_ohlcv("BTC/USDT:USDT", "1h", Some(0), 1000)).unwrap();
        let first_bucket = T0 / ONE_HOUR_MS * ONE_HOUR_MS;
        assert_eq!(h1[0][0] as u64, first_bucket);
        assert_eq!(h1[0][1], 100.0, "open of the first minute");
        let n_first = ((first_bucket + ONE_HOUR_MS - T0) / 60_000) as f64;
        assert_eq!(
            h1[0][4],
            100.0 + n_first - 1.0,
            "close of the last minute in the bucket"
        );
        assert_eq!(h1[0][2], h1[0][4], "high = max close (rising series)");
        assert!(rt(ex.fetch_ohlcv("BTC/USDT:USDT", "5m", None, 10)).is_err());
        // no `since`: newest `limit` rows
        let tail = rt(ex.fetch_ohlcv("BTC/USDT:USDT", "1m", None, 3)).unwrap();
        assert_eq!(tail.len(), 3);
        assert_eq!(tail[2][0] as u64, T0 + 129 * 60_000);
    }

    #[test]
    fn scenario_actions_manual_fill_and_cancel() {
        let sc = scenario(
            vec![
                prices(100.0, 1.0),
                json!({"prices": {"BTC/USDT:USDT": 100.0, "ADA/USDT:USDT": 1.0},
                       "actions": [
                           {"type": "manual_fill", "symbol": "BTC/USDT:USDT", "side": "buy", "position_side": "long", "qty": 2.0},
                           {"type": "cancel_open_orders", "symbol": "ADA/USDT:USDT"}]}),
            ],
            json!({"balance": 1000.0}),
        );
        let ex = MockExchange::new(sc).unwrap();
        rt(ex.create_orders(&[
            order(
                "ADA/USDT:USDT",
                Side::Buy,
                PositionSide::Long,
                5.0,
                0.5,
                false,
            ),
            order(
                "BTC/USDT:USDT",
                Side::Buy,
                PositionSide::Long,
                1.0,
                50.0,
                false,
            ),
        ]));
        assert!(ex.advance());
        let snap = ex.snapshot();
        assert_eq!(snap.open_orders.len(), 1);
        assert_eq!(snap.open_orders[0].symbol, "BTC/USDT:USDT");
        assert_eq!(snap.positions[0].2, 2.0);
        assert_eq!(snap.fills[0].liquidity, "taker");
        assert_eq!(snap.fills[0].fee, 100.0 * 2.0 * 0.0006);
    }

    #[test]
    fn iso_timestamps_parse() {
        assert_eq!(
            parse_time_to_ms(&json!("2025-10-24T00:00:00Z")).unwrap(),
            1_761_264_000_000
        );
        assert_eq!(
            parse_time_to_ms(&json!("2025-10-24T02:00:00+02:00")).unwrap(),
            1_761_264_000_000
        );
        assert_eq!(
            parse_time_to_ms(&json!("2025-10-24")).unwrap(),
            1_761_264_000_000
        );
        assert_eq!(
            parse_time_to_ms(&json!(1_761_264_000)).unwrap(),
            1_761_264_000_000
        );
        assert_eq!(
            parse_time_to_ms(&json!("1761264000000")).unwrap(),
            1_761_264_000_000
        );
    }
}
