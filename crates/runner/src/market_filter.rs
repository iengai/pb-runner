//! Pre-create market snapshot freshness gate and the
//! `limit_order_create_max_market_dist_pct` filter (docs/RECONCILE_SPEC.md
//! 3.1 step 8, section 2.10).
//!
//! Python sources (passivbot v8.1.0): `live/market_snapshot.py`
//! (`MarketSnapshot`, `MarketSnapshotProvider`), `live/market_data.py`
//! (`filter_fresh_market_snapshot_creations` md.py:152-250,
//! `_filter_limit_order_creations_by_market_distance` md.py:271-320,
//! `live_market_snapshot_max_age_ms` md.py:656,
//! `market_snapshot_signature_invalid` md.py:712) and
//! `live/planning_gates.py` (`current_planning_snapshot_invalid_for_creations`).
//!
//! Freshness is measured on the *local receive time* of the ticker fetch
//! (`fetched_ms = utc_ms()` after `fetch_tickers` returned), never on an
//! exchange timestamp: the Bybit connector's `_normalize_tickers`
//! (`exchanges/ccxt_bot.py:1219`) keeps only `bid/ask/last`, so
//! `MarketSnapshot.exchange_timestamp_ms` is `None` there and is not used by
//! any freshness check anyway. The hard max age is the constant 10 s
//! (`live_market_snapshot_max_age_ms`), not a config value; planning fetches
//! use the stricter `fetch` TTL (half of it, at least 1 s).

use crate::bot_params::ConfigView;
use crate::reconcile::{order_market_diff, OrderRec};
use anyhow::{bail, Result};
use pb_exchange_bybit::{ExchangeError, PositionSide, Side, Ticker};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::future::Future;

/// `live_market_snapshot_max_age_ms` (md.py:656): hard safety TTL, constant.
pub const LIVE_MARKET_SNAPSHOT_MAX_AGE_MS: u64 = 10_000;

/// `live_market_snapshot_fetch_max_age_ms` (md.py:661): the planning fetch
/// TTL, `max(1000, min(max_age, int(max_age * 0.5)))`.
pub fn fetch_max_age_ms(max_age_ms: u64) -> u64 {
    max_age_ms.min(max_age_ms / 2).max(1_000)
}

/// The one-hour throttle on the INFO distance-skip log per group key.
const DISTANCE_LOG_INFO_INTERVAL_MS: u64 = 60 * 60 * 1000;

/// `MarketSnapshot` (market_snapshot.py:14): bid/ask/last plus the local
/// receive time of the fetch that produced it.
#[derive(Debug, Clone, PartialEq)]
pub struct MarketSnapshot {
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub last: f64,
    pub fetched_ms: u64,
}

impl MarketSnapshot {
    /// `MarketSnapshot.is_valid`: all three prices finite and > 0.
    pub fn is_valid(&self) -> bool {
        [self.bid, self.ask, self.last]
            .iter()
            .all(|p| p.is_finite() && *p > 0.0)
    }

    /// `MarketSnapshotProvider._snapshot_from_ticker`: `None` unless bid,
    /// ask and last are all finite and positive (the Bybit connector already
    /// substitutes `bid` for a missing `last`, see `parse_tickers`).
    pub fn from_ticker(t: &Ticker, fetched_ms: u64) -> Option<Self> {
        let s = Self {
            symbol: t.symbol.clone(),
            bid: t.bid,
            ask: t.ask,
            last: t.last,
            fetched_ms,
        };
        s.is_valid().then_some(s)
    }
}

/// Why `get_snapshots` could not deliver every requested symbol.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("[market] ticker snapshot fetch failed for bybit; missing={missing}: {source}")]
    Fetch {
        missing: usize,
        #[source]
        source: ExchangeError,
    },
    #[error("[market] ticker snapshots incomplete | exchange=bybit missing={} symbols={}", .0.len(), .0.iter().take(12).cloned().collect::<Vec<_>>().join(","))]
    Incomplete(Vec<String>),
}

impl SnapshotError {
    /// `bounded_exception_type`: a short type label for logs.
    pub fn error_type(&self) -> &'static str {
        match self {
            SnapshotError::Fetch { .. } => "TickerFetchFailed",
            SnapshotError::Incomplete(_) => "TickerSnapshotsIncomplete",
        }
    }
}

/// `MarketSnapshotProvider` with the `bulk` ticker strategy (Bybit): a cache
/// of the last valid snapshot per symbol, refilled by a bulk
/// `fetch_tickers` when a requested symbol is missing or older than the
/// caller's `max_age_ms`.
#[derive(Debug, Default)]
pub struct SnapshotProvider {
    cache: HashMap<String, MarketSnapshot>,
}

impl SnapshotProvider {
    pub fn new() -> Self {
        Self::default()
    }

    /// `get_cached`: the cached snapshot unless it is older than
    /// `max_age_ms` (strictly) or invalid.
    pub fn get_cached(
        &self,
        symbol: &str,
        now_ms: u64,
        max_age_ms: u64,
    ) -> Option<&MarketSnapshot> {
        let snap = self.cache.get(symbol)?;
        if now_ms.saturating_sub(snap.fetched_ms) > max_age_ms {
            return None;
        }
        snap.is_valid().then_some(snap)
    }

    /// Cache every valid ticker of a bulk fetch (Python caches all symbols
    /// the exchange returned, not only the requested ones). Returns the
    /// number cached.
    pub fn ingest(
        &mut self,
        tickers: &[Ticker],
        fetched_ms: u64,
        only: Option<&[String]>,
    ) -> usize {
        let mut cached = 0;
        for t in tickers {
            if only.is_some_and(|o| !o.contains(&t.symbol)) {
                continue;
            }
            if let Some(s) = MarketSnapshot::from_ticker(t, fetched_ms) {
                self.cache.insert(t.symbol.clone(), s);
                cached += 1;
            }
        }
        cached
    }

    /// `MarketSnapshotProvider.get_snapshots` (bulk strategy): cache hits
    /// within `max_age_ms`, one bulk fetch for the rest, one more bulk fetch
    /// for symbols still missing (Python's `fetch_tickers_for_symbols` retry;
    /// on Bybit ccxt's `fetch_tickers(symbols)` hits the same
    /// `/v5/market/tickers?category=linear` endpoint), then
    /// `Incomplete` if any requested symbol still has no valid snapshot.
    pub async fn get_snapshots<F, Fut>(
        &mut self,
        fetch: F,
        symbols: &[String],
        max_age_ms: u64,
        now_ms: &(dyn Fn() -> u64 + Sync),
    ) -> Result<HashMap<String, MarketSnapshot>, SnapshotError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<Vec<Ticker>, ExchangeError>>,
    {
        let mut ordered: Vec<&String> = Vec::new();
        for s in symbols {
            if !s.is_empty() && !ordered.contains(&s) {
                ordered.push(s);
            }
        }
        let mut out: HashMap<String, MarketSnapshot> = HashMap::new();
        if ordered.is_empty() {
            return Ok(out);
        }
        let now = now_ms();
        for s in &ordered {
            if let Some(snap) = self.get_cached(s, now, max_age_ms) {
                out.insert((*s).clone(), snap.clone());
            }
        }
        let missing: Vec<String> = ordered
            .iter()
            .filter(|s| !out.contains_key(**s))
            .map(|s| (*s).clone())
            .collect();
        if missing.is_empty() {
            return Ok(out);
        }
        let fetched = match fetch().await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(
                    "[market] ticker snapshot fetch failed | exchange=bybit symbols={} error_type={} action=propagate",
                    missing.len(),
                    e
                );
                return Err(SnapshotError::Fetch {
                    missing: missing.len(),
                    source: e,
                });
            }
        };
        let fetched_ms = now_ms();
        let cached = self.ingest(&fetched, fetched_ms, None);
        for s in &missing {
            if let Some(snap) = self.get_cached(s, fetched_ms, max_age_ms) {
                out.insert(s.clone(), snap.clone());
            }
        }
        let missing_after: Vec<String> = missing
            .iter()
            .filter(|s| !out.contains_key(*s))
            .cloned()
            .collect();
        if !missing_after.is_empty() {
            let retry = match fetch().await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(
                        "[market] ticker missing-symbol retry failed | exchange=bybit symbols={} error_type={} action=propagate",
                        missing_after.len(),
                        e
                    );
                    return Err(SnapshotError::Fetch {
                        missing: missing_after.len(),
                        source: e,
                    });
                }
            };
            let retry_ms = now_ms();
            let retry_cached = self.ingest(&retry, retry_ms, Some(&missing_after));
            for s in &missing_after {
                if let Some(snap) = self.get_cached(s, retry_ms, max_age_ms) {
                    out.insert(s.clone(), snap.clone());
                }
            }
            let retry_misses = missing_after
                .iter()
                .filter(|s| !out.contains_key(*s))
                .count();
            tracing::debug!(
                "[market] ticker missing-symbol retry complete | exchange=bybit requested={} hits={} misses={} cached={} source=fetch_tickers_symbols",
                missing_after.len(),
                missing_after.len() - retry_misses,
                retry_misses,
                retry_cached
            );
        }
        let still_missing: Vec<String> = missing
            .iter()
            .filter(|s| !out.contains_key(*s))
            .cloned()
            .collect();
        tracing::debug!(
            "[market] ticker snapshots ready | exchange=bybit requested={} hits={} misses={} cached={} source=fetch_tickers",
            ordered.len(),
            missing.len() - still_missing.len(),
            still_missing.len(),
            cached
        );
        if !still_missing.is_empty() {
            return Err(SnapshotError::Incomplete(still_missing));
        }
        Ok(out)
    }
}

/// One entry of the "invalid" lists (`PlanningSnapshot.invalid_details`,
/// `market_snapshot_signature_invalid`).
#[derive(Debug, Clone, PartialEq)]
pub struct InvalidDetail {
    pub surface: &'static str,
    pub reason: &'static str,
    pub symbols: Vec<String>,
    pub age_ms: Option<i64>,
    pub max_age_ms: Option<u64>,
}

impl InvalidDetail {
    fn to_json(&self) -> Value {
        let mut v = json!({"surface": self.surface, "reason": self.reason});
        match self.symbols.len() {
            0 => {}
            1 => v["symbol"] = json!(log_symbol(&self.symbols[0])),
            _ => {
                v["symbols"] = json!(self
                    .symbols
                    .iter()
                    .map(|s| log_symbol(s))
                    .collect::<Vec<_>>())
            }
        }
        if let Some(a) = self.age_ms {
            v["age_ms"] = json!(a);
        }
        if let Some(m) = self.max_age_ms {
            v["max_age_ms"] = json!(m);
        }
        v
    }
}

/// `current_planning_snapshot_invalid_for_creations` restricted to what the
/// runner models: the planning snapshot is the map of market snapshots the
/// engine input was built from. Reasons: `snapshot_too_old` for any planning
/// row older than `max_age_ms` (refreshable), and
/// `creation_symbols_not_in_snapshot` for creates on symbols the plan did not
/// cover (not refreshable). Surface epochs are not modelled: the runner
/// refreshes every surface at the top of each cycle, so they are always
/// current.
pub fn planning_snapshot_invalid(
    create_symbols: &[String],
    planning: &HashMap<String, MarketSnapshot>,
    now_ms: u64,
    max_age_ms: u64,
) -> Vec<InvalidDetail> {
    let mut invalid = Vec::new();
    let mut rows: Vec<&MarketSnapshot> = planning.values().collect();
    rows.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    for row in rows {
        let age = now_ms as i64 - row.fetched_ms as i64;
        if age > max_age_ms as i64 {
            invalid.push(InvalidDetail {
                surface: "market_snapshot",
                reason: "snapshot_too_old",
                symbols: vec![row.symbol.clone()],
                age_ms: Some(age),
                max_age_ms: Some(max_age_ms),
            });
        }
    }
    let missing: Vec<String> = create_symbols
        .iter()
        .filter(|s| !planning.contains_key(*s))
        .cloned()
        .collect();
    if !missing.is_empty() {
        invalid.push(InvalidDetail {
            surface: "planning_snapshot",
            reason: "creation_symbols_not_in_snapshot",
            symbols: missing,
            age_ms: None,
            max_age_ms: None,
        });
    }
    invalid
}

/// `market_snapshot_signature_invalid` over the snapshots just recorded:
/// a requested symbol without a valid snapshot is `missing`, one whose
/// `fetched_ms` is more than `max_age_ms` before `now_ms` is `stale`.
pub fn snapshot_signature_invalid(
    symbols: &[String],
    snapshots: &HashMap<String, MarketSnapshot>,
    now_ms: u64,
    max_age_ms: u64,
) -> Vec<InvalidDetail> {
    let mut expected: Vec<&String> = symbols.iter().filter(|s| !s.is_empty()).collect();
    expected.sort();
    expected.dedup();
    let mut invalid = Vec::new();
    for s in expected {
        match snapshots.get(s).filter(|snap| snap.is_valid()) {
            None => invalid.push(InvalidDetail {
                surface: "market_snapshot",
                reason: "missing",
                symbols: vec![s.clone()],
                age_ms: None,
                max_age_ms: None,
            }),
            Some(snap) => {
                let age = now_ms as i64 - snap.fetched_ms as i64;
                if age > max_age_ms as i64 {
                    invalid.push(InvalidDetail {
                        surface: "market_snapshot",
                        reason: "stale",
                        symbols: vec![s.clone()],
                        age_ms: Some(age),
                        max_age_ms: Some(max_age_ms),
                    });
                }
            }
        }
    }
    invalid
}

/// Result of the pre-create filter for one cycle.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct FilterOutcome {
    pub kept: Vec<OrderRec>,
    /// Creates dropped because the whole cycle was skipped (planning
    /// snapshot invalid, refresh failed, stale snapshots).
    pub skipped_snapshot: usize,
    /// Limit creates dropped by the market-distance filter.
    pub skipped_distance: usize,
}

/// The distance filter with its config threshold and the per-group INFO
/// log throttle (`_limit_order_distance_guard_log_state`).
#[derive(Debug, Clone)]
pub struct MarketFilter {
    /// `live.limit_order_create_max_market_dist_pct` (fraction; 0 disables
    /// the skip but not the distance annotation).
    pub threshold: f64,
    log_state: HashMap<(String, String, String, String), u64>,
}

impl MarketFilter {
    /// `_limit_order_create_max_market_dist_pct`: numeric, finite,
    /// `0 <= threshold < 1`; missing -> 0.8.
    pub fn from_config(cfg: &ConfigView) -> Result<Self> {
        let threshold = match cfg.live("limit_order_create_max_market_dist_pct") {
            None => 0.8,
            Some(v) => match v.as_f64() {
                Some(t) => t,
                None => bail!("live.limit_order_create_max_market_dist_pct must be numeric"),
            },
        };
        Self::new(threshold)
    }

    pub fn new(threshold: f64) -> Result<Self> {
        if !threshold.is_finite() || !(0.0..1.0).contains(&threshold) {
            bail!(
                "live.limit_order_create_max_market_dist_pct must be finite and >= 0.0 and < 1.0"
            );
        }
        Ok(Self {
            threshold,
            log_state: HashMap::new(),
        })
    }

    /// `_filter_limit_order_creations_by_market_distance`: market orders and
    /// symbols without a valid snapshot pass untouched; every other order
    /// gets `market_distance` (`_churn_gate_market_distance`) from the
    /// snapshot's `last`, and is skipped when `threshold > 0` and the
    /// distance exceeds it. Returns `(kept, skipped_count)`.
    pub fn filter_by_market_distance(
        &mut self,
        orders: Vec<OrderRec>,
        snapshots: &HashMap<String, MarketSnapshot>,
        now_ms: u64,
    ) -> (Vec<OrderRec>, usize) {
        let mut kept = Vec::with_capacity(orders.len());
        let mut skipped: Vec<(OrderRec, f64, f64)> = Vec::new();
        for mut o in orders {
            if !o.limit {
                kept.push(o);
                continue;
            }
            let Some(snap) = snapshots.get(&o.symbol).filter(|s| s.is_valid()) else {
                kept.push(o);
                continue;
            };
            let dist = order_market_diff(o.side, o.price, snap.last);
            o.market_distance = Some(dist);
            if self.threshold > 0.0 && dist > self.threshold {
                skipped.push((o, snap.last, dist));
                continue;
            }
            kept.push(o);
        }
        let n = skipped.len();
        if n > 0 {
            self.log_distance_skips(&skipped, now_ms);
        }
        (kept, n)
    }

    /// `_log_limit_order_distance_skips`: INFO at most once per hour per
    /// (symbol, pside, side, pb_order_type) group, DEBUG otherwise.
    fn log_distance_skips(&mut self, skipped: &[(OrderRec, f64, f64)], now_ms: u64) {
        let mut grouped: Vec<((String, String, String, String), usize)> = Vec::new();
        let mut symbols: Vec<String> = Vec::new();
        for (o, _, _) in skipped {
            let key = (
                o.symbol.clone(),
                pside_name(o.pside).to_string(),
                side_name(o.side).to_string(),
                o.pb_order_type.clone(),
            );
            match grouped.iter_mut().find(|(k, _)| *k == key) {
                Some((_, c)) => *c += 1,
                None => grouped.push((key, 1)),
            }
            if !symbols.contains(&o.symbol) {
                symbols.push(o.symbol.clone());
            }
        }
        let mut should_info = false;
        for (key, _) in &grouped {
            let last = self.log_state.get(key).copied().unwrap_or(0);
            if now_ms.saturating_sub(last) >= DISTANCE_LOG_INFO_INTERVAL_MS {
                should_info = true;
                self.log_state.insert(key.clone(), now_ms);
            }
        }
        let samples: Vec<String> = skipped
            .iter()
            .take(8)
            .map(|(o, market, _)| {
                format!(
                    "{}:{}:{}/market={}",
                    log_symbol(&o.symbol),
                    side_name(o.side),
                    fmt_g10(o.price),
                    fmt_g10(*market)
                )
            })
            .collect();
        let summary: Vec<String> = grouped
            .iter()
            .map(|((symbol, pside, side, t), count)| {
                format!("{} {side} {pside} {t}={count}", log_symbol(symbol))
            })
            .collect();
        symbols.sort();
        let msg = format!(
            "[order] skipped far-from-market limit order creates | skipped={} symbols={} threshold={:.4} min_multiplier={:.4} max_multiplier={:.4} groups={} samples={} reason=limit_order_create_market_distance",
            skipped.len(),
            log_symbols(&symbols, 12),
            self.threshold,
            1.0 - self.threshold,
            1.0 + self.threshold,
            summary.join(", "),
            samples.join(", ")
        );
        if should_info {
            tracing::info!("{msg}");
        } else {
            tracing::debug!("{msg}");
        }
    }

    /// `filter_fresh_market_snapshot_creations` (md.py:152-250): the whole
    /// create list is dropped when the planning snapshot is invalid for a
    /// non-refreshable reason, when the pre-create ticker refresh fails or
    /// leaves a symbol without a valid snapshot, or when any refreshed
    /// snapshot is already older than the hard TTL; otherwise the distance
    /// filter runs on the refreshed snapshots. `planning` is the snapshot
    /// map the engine input was built from.
    pub async fn filter_fresh_creations<F, Fut>(
        &mut self,
        fetch: F,
        provider: &mut SnapshotProvider,
        planning: &HashMap<String, MarketSnapshot>,
        orders: Vec<OrderRec>,
        now_ms: &(dyn Fn() -> u64 + Sync),
    ) -> FilterOutcome
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<Vec<Ticker>, ExchangeError>>,
    {
        if orders.is_empty() {
            return FilterOutcome {
                kept: orders,
                ..Default::default()
            };
        }
        let mut symbols: Vec<String> = orders
            .iter()
            .filter(|o| !o.symbol.is_empty())
            .map(|o| o.symbol.clone())
            .collect();
        symbols.sort();
        symbols.dedup();
        if symbols.is_empty() {
            return FilterOutcome {
                kept: orders,
                ..Default::default()
            };
        }
        let skip_all = |orders: Vec<OrderRec>| FilterOutcome {
            skipped_snapshot: orders.len(),
            kept: Vec::new(),
            skipped_distance: 0,
        };
        let max_age = LIVE_MARKET_SNAPSHOT_MAX_AGE_MS;
        let planning_invalid = planning_snapshot_invalid(&symbols, planning, now_ms(), max_age);
        if !planning_invalid.is_empty() {
            let refreshable = planning_invalid
                .iter()
                .all(|d| d.surface == "market_snapshot" && d.reason == "snapshot_too_old");
            if !refreshable {
                tracing::warn!(
                    "[market] skipping order creation; planning snapshot invalid before create | symbols={} details={}",
                    log_symbols(&symbols, 12),
                    details_json(&planning_invalid)
                );
                return skip_all(orders);
            }
            tracing::info!(
                "[market] refreshing stale planning market snapshot before create | symbols={} stale={}",
                log_symbols(&symbols, 12),
                planning_invalid.len()
            );
        }
        let snapshots = match provider
            .get_snapshots(fetch, &symbols, max_age, now_ms)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    "[market] skipping order creation; failed pre-create market snapshot refresh | symbols={} error_type={} action=skip_create",
                    log_symbols(&symbols, 12),
                    e.error_type()
                );
                return skip_all(orders);
            }
        };
        let invalid = snapshot_signature_invalid(&symbols, &snapshots, now_ms(), max_age);
        if !invalid.is_empty() {
            tracing::warn!(
                "[market] skipping order creation; stale pre-create market snapshots | symbols={} details={}",
                log_symbols(&symbols, 12),
                details_json(&invalid)
            );
            return skip_all(orders);
        }
        let (kept, skipped_distance) = self.filter_by_market_distance(orders, &snapshots, now_ms());
        FilterOutcome {
            kept,
            skipped_snapshot: 0,
            skipped_distance,
        }
    }
}

fn details_json(details: &[InvalidDetail]) -> String {
    Value::Array(details.iter().take(8).map(InvalidDetail::to_json).collect()).to_string()
}

fn pside_name(p: PositionSide) -> &'static str {
    match p {
        PositionSide::Long => "long",
        PositionSide::Short => "short",
    }
}

fn side_name(s: Side) -> &'static str {
    match s {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

/// `Passivbot._log_symbol`: the coin part of `COIN/USDT:USDT`.
pub fn log_symbol(symbol: &str) -> &str {
    symbol.split('/').next().unwrap_or(symbol)
}

/// `Passivbot._log_symbols(symbols, limit)`.
pub fn log_symbols(symbols: &[String], limit: usize) -> String {
    let vals: Vec<&str> = symbols.iter().map(|s| log_symbol(s)).collect();
    if vals.len() > limit {
        format!("{},+{} more", vals[..limit].join(","), vals.len() - limit)
    } else {
        vals.join(",")
    }
}

/// Python's `{:.10g}`: 10 significant digits, trailing zeros trimmed.
pub fn fmt_g10(x: f64) -> String {
    if x == 0.0 || !x.is_finite() {
        return format!("{x}");
    }
    let exp = x.abs().log10().floor() as i32;
    // `%g` uses fixed notation for -4 <= exp < precision, scientific otherwise.
    if (-4..10).contains(&exp) {
        let decimals = (9 - exp).max(0) as usize;
        let s = format!("{x:.decimals$}");
        let s = s.trim_end_matches('0').trim_end_matches('.');
        s.to_string()
    } else {
        let s = format!("{x:.9e}");
        let (mant, e) = s.split_once('e').unwrap_or((&s, "0"));
        let mant = mant.trim_end_matches('0').trim_end_matches('.');
        let e: i32 = e.parse().unwrap_or(0);
        format!("{mant}e{}{:02}", if e < 0 { '-' } else { '+' }, e.abs())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    const SYMBOL: &str = "POPCAT/USDT:USDT";

    fn order(side: Side, pside: PositionSide, price: f64, t: &str, limit: bool) -> OrderRec {
        OrderRec {
            symbol: SYMBOL.into(),
            side,
            pside,
            qty: 1.0,
            price,
            reduce_only: t.contains("close"),
            limit,
            pb_order_type: t.into(),
            risk_critical: false,
            churn_evidenced: false,
            market_distance: None,
            id: None,
            custom_id: None,
        }
    }

    fn snap(symbol: &str, price: f64, fetched_ms: u64) -> MarketSnapshot {
        MarketSnapshot {
            symbol: symbol.into(),
            bid: price,
            ask: price,
            last: price,
            fetched_ms,
        }
    }

    fn snaps(price: f64, fetched_ms: u64) -> HashMap<String, MarketSnapshot> {
        HashMap::from([(SYMBOL.to_string(), snap(SYMBOL, price, fetched_ms))])
    }

    fn ticker(symbol: &str, price: f64) -> Ticker {
        Ticker {
            symbol: symbol.into(),
            bid: price,
            ask: price,
            last: price,
            quote_volume_24h: 0.0,
        }
    }

    /// `test_pre_create_snapshot_filter_skips_far_limit_order_creations`.
    #[test]
    fn distance_filter_skips_far_limit_orders_only() {
        let mut f = MarketFilter::new(0.8).unwrap();
        let kept_buy = order(
            Side::Buy,
            PositionSide::Long,
            20.0,
            "entry_grid_normal_long",
            true,
        );
        let skipped_buy = order(
            Side::Buy,
            PositionSide::Long,
            19.99,
            "entry_grid_cropped_long",
            true,
        );
        let kept_sell = order(
            Side::Sell,
            PositionSide::Short,
            180.0,
            "entry_grid_normal_short",
            true,
        );
        let skipped_sell = order(
            Side::Sell,
            PositionSide::Short,
            180.01,
            "entry_grid_cropped_short",
            true,
        );
        let skipped_close = order(
            Side::Sell,
            PositionSide::Long,
            180.02,
            "close_grid_long",
            true,
        );
        let market_order = order(
            Side::Buy,
            PositionSide::Long,
            1.0,
            "close_panic_long",
            false,
        );
        let (kept, skipped) = f.filter_by_market_distance(
            vec![
                kept_buy.clone(),
                skipped_buy,
                kept_sell.clone(),
                skipped_sell,
                skipped_close,
                market_order.clone(),
            ],
            &snaps(100.0, 0),
            0,
        );
        assert_eq!(skipped, 3);
        let prices: Vec<f64> = kept.iter().map(|o| o.price).collect();
        assert_eq!(prices, vec![20.0, 180.0, 1.0]);
        // Limit orders are annotated with the signed distance, market orders are not.
        assert!((kept[0].market_distance.unwrap() - 0.8).abs() < 1e-12);
        assert!((kept[1].market_distance.unwrap() - 0.8).abs() < 1e-12);
        assert_eq!(kept[2].market_distance, None);
        // Same call again within the hour: log throttled to DEBUG, result identical.
        let (kept2, skipped2) = f.filter_by_market_distance(
            vec![kept_buy, kept_sell, market_order],
            &snaps(100.0, 0),
            1,
        );
        assert_eq!((kept2.len(), skipped2), (3, 0));
    }

    /// `test_pre_create_snapshot_filter_disables_limit_order_distance_guard`
    /// and `test_disabled_generic_distance_guard_still_annotates_churn_distance`.
    #[test]
    fn zero_threshold_disables_skip_but_annotates() {
        let mut f = MarketFilter::new(0.0).unwrap();
        let o = order(
            Side::Buy,
            PositionSide::Long,
            99.0,
            "entry_grid_normal_long",
            true,
        );
        let (kept, skipped) = f.filter_by_market_distance(vec![o], &snaps(100.0, 0), 0);
        assert_eq!(skipped, 0);
        assert!((kept[0].market_distance.unwrap() - 0.01).abs() < 1e-12);
        let far = order(
            Side::Buy,
            PositionSide::Long,
            1.0,
            "entry_grid_normal_long",
            true,
        );
        let (kept, skipped) = f.filter_by_market_distance(vec![far], &snaps(100.0, 0), 0);
        assert_eq!((kept.len(), skipped), (1, 0));
    }

    #[test]
    fn symbols_without_valid_snapshot_pass_unannotated() {
        let mut f = MarketFilter::new(0.1).unwrap();
        let far = order(
            Side::Buy,
            PositionSide::Long,
            1.0,
            "entry_grid_normal_long",
            true,
        );
        let (kept, skipped) = f.filter_by_market_distance(vec![far.clone()], &HashMap::new(), 0);
        assert_eq!((kept.len(), skipped), (1, 0));
        assert_eq!(kept[0].market_distance, None);
        let invalid = HashMap::from([(SYMBOL.to_string(), snap(SYMBOL, 0.0, 0))]);
        let (kept, skipped) = f.filter_by_market_distance(vec![far], &invalid, 0);
        assert_eq!((kept.len(), skipped), (1, 0));
    }

    /// `test_config_utils_helpers` threshold validation.
    #[test]
    fn threshold_validation() {
        assert!(MarketFilter::new(0.0).is_ok());
        assert!(MarketFilter::new(0.999).is_ok());
        assert!(MarketFilter::new(1.0).is_err());
        assert!(MarketFilter::new(-0.1).is_err());
        assert!(MarketFilter::new(f64::NAN).is_err());
        assert!(MarketFilter::new(f64::INFINITY).is_err());
    }

    #[test]
    fn max_age_constants() {
        assert_eq!(LIVE_MARKET_SNAPSHOT_MAX_AGE_MS, 10_000);
        assert_eq!(fetch_max_age_ms(10_000), 5_000);
        assert_eq!(fetch_max_age_ms(1_500), 1_000);
        assert_eq!(fetch_max_age_ms(100), 1_000);
    }

    #[test]
    fn signature_invalid_reports_missing_and_stale() {
        let s = snaps(100.0, 1_000);
        assert!(snapshot_signature_invalid(&[SYMBOL.into()], &s, 11_000, 10_000).is_empty());
        let stale = snapshot_signature_invalid(&[SYMBOL.into()], &s, 11_001, 10_000);
        assert_eq!(stale.len(), 1);
        assert_eq!((stale[0].reason, stale[0].age_ms), ("stale", Some(10_001)));
        let missing = snapshot_signature_invalid(&["X/USDT:USDT".into()], &s, 0, 10_000);
        assert_eq!(missing[0].reason, "missing");
    }

    #[test]
    fn planning_invalid_reasons() {
        let p = snaps(100.0, 1_000);
        assert!(planning_snapshot_invalid(&[SYMBOL.into()], &p, 11_000, 10_000).is_empty());
        let old = planning_snapshot_invalid(&[SYMBOL.into()], &p, 11_001, 10_000);
        assert_eq!(old[0].reason, "snapshot_too_old");
        let foreign = planning_snapshot_invalid(&["X/USDT:USDT".into()], &p, 0, 10_000);
        assert_eq!(foreign[0].reason, "creation_symbols_not_in_snapshot");
        assert_eq!(foreign[0].symbols, vec!["X/USDT:USDT".to_string()]);
    }

    #[test]
    fn provider_cache_and_validity() {
        let mut p = SnapshotProvider::new();
        assert_eq!(
            p.ingest(
                &[ticker(SYMBOL, 100.0), ticker("BAD/USDT:USDT", 0.0)],
                5_000,
                None
            ),
            1
        );
        assert!(p.get_cached(SYMBOL, 15_000, 10_000).is_some());
        assert!(p.get_cached(SYMBOL, 15_001, 10_000).is_none());
        assert!(p.get_cached("BAD/USDT:USDT", 5_000, 10_000).is_none());
        // `only` restricts the retry ingest to the missing symbols.
        assert_eq!(
            p.ingest(
                &[ticker("A/USDT:USDT", 1.0)],
                0,
                Some(&["B/USDT:USDT".to_string()])
            ),
            0
        );
    }

    struct Clock(AtomicU64);
    impl Clock {
        fn now(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn provider_fetches_only_when_cache_is_older_than_max_age() {
        let clock = Clock(AtomicU64::new(1_000));
        let calls = Mutex::new(0usize);
        let fetch = || {
            *calls.lock().unwrap() += 1;
            std::future::ready(Ok(vec![ticker(SYMBOL, 100.0)]))
        };
        let now = || clock.now();
        let mut p = SnapshotProvider::new();
        let out = p
            .get_snapshots(&fetch, &[SYMBOL.into()], 10_000, &now)
            .await
            .unwrap();
        assert_eq!(out[SYMBOL].fetched_ms, 1_000);
        assert_eq!(*calls.lock().unwrap(), 1);
        clock.0.store(11_000, Ordering::SeqCst);
        p.get_snapshots(&fetch, &[SYMBOL.into()], 10_000, &now)
            .await
            .unwrap();
        assert_eq!(*calls.lock().unwrap(), 1); // age 10 000 is still fresh
        clock.0.store(11_001, Ordering::SeqCst);
        let out = p
            .get_snapshots(&fetch, &[SYMBOL.into()], 10_000, &now)
            .await
            .unwrap();
        assert_eq!(*calls.lock().unwrap(), 2);
        assert_eq!(out[SYMBOL].fetched_ms, 11_001);
        // Empty request: no fetch.
        p.get_snapshots(&fetch, &[], 10_000, &now).await.unwrap();
        assert_eq!(*calls.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn provider_retries_once_then_reports_incomplete() {
        let calls = Mutex::new(0usize);
        let fetch = || {
            *calls.lock().unwrap() += 1;
            std::future::ready(Ok(vec![ticker(SYMBOL, 100.0)]))
        };
        let now = || 0u64;
        let mut p = SnapshotProvider::new();
        let err = p
            .get_snapshots(&fetch, &[SYMBOL.into(), "X/USDT:USDT".into()], 10_000, &now)
            .await
            .unwrap_err();
        assert_eq!(*calls.lock().unwrap(), 2);
        match err {
            SnapshotError::Incomplete(m) => assert_eq!(m, vec!["X/USDT:USDT".to_string()]),
            other => panic!("{other:?}"),
        }
        let failing = || std::future::ready(Err(ExchangeError::Network("down".into())));
        let err = p
            .get_snapshots(&failing, &["Y/USDT:USDT".into()], 10_000, &now)
            .await
            .unwrap_err();
        assert!(matches!(err, SnapshotError::Fetch { missing: 1, .. }));
    }

    /// `test_pre_create_market_filter_records_exact_existing_gate_reasons`:
    /// planning invalid -> all skipped; refresh failure -> all skipped;
    /// fresh snapshots -> distance filter.
    #[tokio::test]
    async fn fresh_creations_gate_reasons() {
        let now = || 1_000u64;
        let o = order(
            Side::Buy,
            PositionSide::Long,
            1.0,
            "entry_initial_normal_long",
            true,
        );
        let market = order(
            Side::Buy,
            PositionSide::Long,
            1.0,
            "close_panic_long",
            false,
        );
        let ok_fetch = || std::future::ready(Ok(vec![ticker(SYMBOL, 100.0)]));
        let mut f = MarketFilter::new(0.1).unwrap();

        // Planning snapshot does not cover the create symbol: nothing is created, market orders included.
        let mut p = SnapshotProvider::new();
        let out = f
            .filter_fresh_creations(
                &ok_fetch,
                &mut p,
                &HashMap::new(),
                vec![o.clone(), market.clone()],
                &now,
            )
            .await;
        assert_eq!(
            (out.kept.len(), out.skipped_snapshot, out.skipped_distance),
            (0, 2, 0)
        );

        // Refresh failure.
        let failing = || std::future::ready(Err(ExchangeError::Network("down".into())));
        let out = f
            .filter_fresh_creations(
                &failing,
                &mut p,
                &snaps(100.0, 1_000),
                vec![o.clone()],
                &now,
            )
            .await;
        assert_eq!((out.kept.len(), out.skipped_snapshot), (0, 1));

        // Fresh: the far order is dropped by distance (0.99 > 0.1), the market order passes.
        let out = f
            .filter_fresh_creations(
                &ok_fetch,
                &mut p,
                &snaps(100.0, 1_000),
                vec![o.clone(), market.clone()],
                &now,
            )
            .await;
        assert_eq!(
            (out.kept.len(), out.skipped_snapshot, out.skipped_distance),
            (1, 0, 1)
        );
        assert!(!out.kept[0].limit);

        // Stale planning row is refreshable: a refetch happens and the near order passes.
        let calls = Mutex::new(0usize);
        let counting = || {
            *calls.lock().unwrap() += 1;
            std::future::ready(Ok(vec![ticker(SYMBOL, 100.0)]))
        };
        let late = || 30_000u64;
        let near = order(
            Side::Buy,
            PositionSide::Long,
            95.0,
            "entry_initial_normal_long",
            true,
        );
        let out = f
            .filter_fresh_creations(&counting, &mut p, &snaps(100.0, 1_000), vec![near], &late)
            .await;
        assert_eq!(*calls.lock().unwrap(), 1);
        assert_eq!(out.kept.len(), 1);
        assert!((out.kept[0].market_distance.unwrap() - 0.05).abs() < 1e-12);

        // Empty input is returned as-is without touching the exchange.
        let out = f
            .filter_fresh_creations(&failing, &mut p, &HashMap::new(), vec![], &now)
            .await;
        assert_eq!(out, FilterOutcome::default());
    }

    /// A cached snapshot that is exactly at the TTL when reused, but past it
    /// by the time the signature is checked, skips the cycle (`stale`).
    #[tokio::test]
    async fn fresh_creations_stale_after_reuse() {
        let clock = Clock(AtomicU64::new(10_000));
        let now = || clock.now();
        let mut p = SnapshotProvider::new();
        p.ingest(&[ticker(SYMBOL, 100.0)], 0, None);
        let mut f = MarketFilter::new(0.8).unwrap();
        let o = order(
            Side::Buy,
            PositionSide::Long,
            95.0,
            "entry_initial_normal_long",
            true,
        );
        // Advance the clock on every read: planning check at 10 000 (fresh),
        // get_snapshots at 10 001 -> refetch... so pin the sequence instead:
        // planning check fresh, cache hit at 10 000, signature check at 10 001.
        let reads = AtomicU64::new(0);
        let seq = || {
            let i = reads.fetch_add(1, Ordering::SeqCst);
            [10_000u64, 10_000, 10_001][i as usize % 3]
        };
        let _ = now;
        let fetch = || std::future::ready(Ok(vec![ticker(SYMBOL, 100.0)]));
        let out = f
            .filter_fresh_creations(&fetch, &mut p, &snaps(100.0, 5_000), vec![o], &seq)
            .await;
        assert_eq!((out.kept.len(), out.skipped_snapshot), (0, 1));
    }

    #[test]
    fn log_helpers() {
        assert_eq!(log_symbol("BTC/USDT:USDT"), "BTC");
        assert_eq!(log_symbol("BTCUSDT"), "BTCUSDT");
        let many: Vec<String> = (0..14).map(|i| format!("C{i}/USDT:USDT")).collect();
        assert!(log_symbols(&many, 12).ends_with(",+2 more"));
        assert_eq!(log_symbols(&many[..2], 12), "C0,C1");
        assert_eq!(fmt_g10(20.0), "20");
        assert_eq!(fmt_g10(19.99), "19.99");
        assert_eq!(fmt_g10(0.000012345), "1.2345e-05");
        assert_eq!(fmt_g10(123456.789), "123456.789");
    }
}
