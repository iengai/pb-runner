//! EMA inputs of the orchestrator snapshot (docs/SNAPSHOT_SPEC.md section 3).
//!
//! Given a symbol's 1m candles (ascending, `[ts, o, h, l, c, v]`) and the
//! planning time, compute the latest bias-corrected EMA of a metric over the
//! window of `ceil(span)` *closed* candles ending at the last closed bucket,
//! exactly like `candlestick_manager._latest_finalized_range` + `_ema`
//! (the Python bot calls the engine's `ema_last_f64` for the fold).
//!
//! Hourly candles are aggregated from the 1m array the way the fake exchange
//! and ccxt do it (first open, max high, min low, last close, summed volume).

use passivbot_rust::utils::ema_last_f64;

pub type Candle = [f64; 6];
pub const ONE_MIN_MS: u64 = 60_000;
pub const ONE_HOUR_MS: u64 = 3_600_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    Close,
    /// Quote volume `bv * (h + l + c) / 3` (cm `_ema_metric_series`, key `qv`).
    QuoteVolume,
    /// `ln(max(h, 1e-12) / max(l, 1e-12))`.
    LogRange,
}

/// Gap policy of the reader (cm `allow_provisional_internal_gaps`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GapPolicy {
    /// Internal missing buckets are synthesised as flat zero-volume candles at
    /// the previous close (fetched symbols reading close / strategy log-range).
    Provisional,
    /// Any internal gap makes the value unavailable (quote volume, forager
    /// log-range, and every 1h window).
    Strict,
}

/// `(start_ts, end_ts)` of the window: `ceil(span)` closed buckets ending at
/// the last closed bucket before `now_ms`.
pub fn latest_finalized_range(span: f64, period_ms: u64, now_ms: u64) -> (u64, u64) {
    let span_candles = (span.ceil() as u64).max(1);
    let end_ts = (now_ms / period_ms) * period_ms - period_ms;
    let start_ts = end_ts.saturating_sub(period_ms * (span_candles - 1));
    (start_ts, end_ts)
}

/// Aggregate ascending 1m candles into 1h buckets (`ts = floor(ts / 1h) * 1h`).
pub fn aggregate_1h(candles: &[Candle]) -> Vec<Candle> {
    let mut out: Vec<Candle> = Vec::new();
    for c in candles {
        let bucket = ((c[0] as u64) / ONE_HOUR_MS * ONE_HOUR_MS) as f64;
        match out.last_mut() {
            Some(last) if last[0] == bucket => {
                last[2] = last[2].max(c[2]);
                last[3] = last[3].min(c[3]);
                last[4] = c[4];
                last[5] += c[5];
            }
            _ => out.push([bucket, c[1], c[2], c[3], c[4], c[5]]),
        }
    }
    out
}

/// The candle manager stores o/h/l/c/bv as `float32` (`CANDLE_DTYPE`) and
/// converts back to f64 when building a series; reproduce that rounding.
fn f32r(x: f64) -> f64 {
    x as f32 as f64
}

fn series_value(c: &Candle, metric: Metric) -> f64 {
    let (h, l, close, bv) = (f32r(c[2]), f32r(c[3]), f32r(c[4]), f32r(c[5]));
    match metric {
        Metric::Close => close,
        Metric::QuoteVolume => bv * (h + l + close) / 3.0,
        Metric::LogRange => (h.max(1e-12) / l.max(1e-12)).ln(),
    }
}

/// Rows of `candles` inside `[start_ts, end_ts]`, with the coverage rules of
/// `_ema_window_has_required_coverage`: the row at `end_ts` must exist;
/// missing leading history is tolerated; internal gaps are filled (provisional)
/// or fatal (strict). Returns `None` when the window is unusable.
pub fn window(
    candles: &[Candle],
    start_ts: u64,
    end_ts: u64,
    period_ms: u64,
    policy: GapPolicy,
) -> Option<Vec<Candle>> {
    let lo = candles.partition_point(|c| (c[0] as u64) < start_ts);
    let hi = candles.partition_point(|c| (c[0] as u64) <= end_ts);
    let rows = &candles[lo..hi];
    let last = rows.last()?;
    if last[0] as u64 != end_ts {
        return None;
    }
    let mut out: Vec<Candle> = Vec::with_capacity(rows.len());
    for c in rows {
        if let Some(prev) = out.last().copied() {
            let mut t = prev[0] as u64 + period_ms;
            while t < c[0] as u64 {
                if policy == GapPolicy::Strict {
                    return None;
                }
                out.push([t as f64, prev[4], prev[4], prev[4], prev[4], 0.0]);
                t += period_ms;
            }
        }
        out.push(*c);
    }
    Some(out)
}

/// Latest EMA of `metric` over the closed window for `span` buckets of
/// `period_ms`, or `None` when coverage rules fail or no finite value exists.
pub fn latest_ema(
    candles: &[Candle],
    span: f64,
    period_ms: u64,
    now_ms: u64,
    metric: Metric,
    policy: GapPolicy,
) -> Option<f64> {
    if !(span.is_finite() && span > 0.0) {
        return None;
    }
    let (start_ts, end_ts) = latest_finalized_range(span, period_ms, now_ms);
    let rows = window(candles, start_ts, end_ts, period_ms, policy)?;
    if period_ms > ONE_MIN_MS {
        // `_candle_range_has_full_coverage`: every bucket present.
        let expected = (end_ts - start_ts) / period_ms + 1;
        if rows.len() as u64 != expected {
            return None;
        }
    }
    let values: Vec<f64> = rows.iter().map(|c| series_value(c, metric)).collect();
    let v = ema_last_f64(&values, span);
    if v.is_finite() {
        Some(v)
    } else {
        None
    }
}

/// `floor(now / 1m) * 1m - 1m`: the minute the planner expects to be closed.
pub fn latest_expected_minute(now_ms: u64) -> u64 {
    (now_ms / ONE_MIN_MS) * ONE_MIN_MS - ONE_MIN_MS
}

/// Open-tail projection context (`_active_tail_gap_projection_context`,
/// pb:11823, on `cm.get_completed_candle_health(symbol, {"1m": 1})` and
/// `_completed_candle_tail_gap_fallback_signature`, pb:11764).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenTailGap {
    pub latest_expected_ts: u64,
    pub last_cached_ts: u64,
    pub tail_gap_ms: u64,
}

/// `Some` when the last closed minute is missing from `candles`, a cached
/// candle exists before it, and the gap is within `max_tail_gap_ms`
/// (`live.max_active_candle_tail_gap_minutes`, default 10 min). Bounded
/// historical gaps are not the concern here: the health window is the single
/// bucket at `latest_expected`, so the only missing span is that bucket.
pub fn open_tail_gap(candles: &[Candle], now_ms: u64, max_tail_gap_ms: u64) -> Option<OpenTailGap> {
    let latest_expected = latest_expected_minute(now_ms);
    let hi = candles.partition_point(|c| (c[0] as u64) <= latest_expected);
    let last = candles[..hi].last()?;
    let last_ts = last[0] as u64;
    if last_ts == latest_expected || last_ts == 0 {
        return None;
    }
    let tail_gap_ms = latest_expected - last_ts;
    if tail_gap_ms > max_tail_gap_ms {
        return None;
    }
    Some(OpenTailGap {
        latest_expected_ts: latest_expected,
        last_cached_ts: last_ts,
        tail_gap_ms,
    })
}

/// Candle rows of `cm.get_projected_open_tail_ema_metrics` (cm:9221) for 1m:
/// the cached rows in `[min(latest_expected - (ceil(max_span) - 1) min,
/// last_cached), latest_expected]` with internal gaps synthesised (the
/// `allow_provisional_internal_gaps=True` read), plus flat zero-volume rows
/// at the previous close for every minute after the newest cached row up to
/// `latest_expected`. `None` reproduces the reader's `RuntimeError`s (no
/// local candles, newest cached row older than the anchor).
pub fn open_tail_rows(candles: &[Candle], gap: &OpenTailGap, max_span: f64) -> Option<Vec<Candle>> {
    if gap.latest_expected_ts < gap.last_cached_ts {
        return None;
    }
    let window_candles = (max_span.ceil() as u64).max(1);
    let start_ts =
        (gap.latest_expected_ts - ONE_MIN_MS * (window_candles - 1)).min(gap.last_cached_ts);
    let lo = candles.partition_point(|c| (c[0] as u64) < start_ts);
    let hi = candles.partition_point(|c| (c[0] as u64) <= gap.latest_expected_ts);
    let rows = &candles[lo..hi];
    let newest = rows.last()?;
    if (newest[0] as u64) < gap.last_cached_ts {
        return None;
    }
    let mut out: Vec<Candle> =
        Vec::with_capacity(rows.len() + (gap.tail_gap_ms / ONE_MIN_MS) as usize);
    for c in rows {
        if let Some(prev) = out.last().copied() {
            let mut t = prev[0] as u64 + ONE_MIN_MS;
            while t < c[0] as u64 {
                out.push([t as f64, prev[4], prev[4], prev[4], prev[4], 0.0]);
                t += ONE_MIN_MS;
            }
        }
        out.push(*c);
    }
    let newest_ts = newest[0] as u64;
    if newest_ts < gap.latest_expected_ts {
        let prev_close = f32r(newest[4]);
        let mut t = newest_ts + ONE_MIN_MS;
        while t <= gap.latest_expected_ts {
            out.push([
                t as f64, prev_close, prev_close, prev_close, prev_close, 0.0,
            ]);
            t += ONE_MIN_MS;
        }
    }
    Some(out)
}

/// Per-span value of the projection: `_ema(series[-ceil(span):], span)`
/// (all rows when fewer). `None` for an empty tail or a non-finite result.
pub fn projected_ema(rows: &[Candle], span: f64, metric: Metric) -> Option<f64> {
    if !(span.is_finite() && span > 0.0) || rows.is_empty() {
        return None;
    }
    let n = (span.ceil() as usize).max(1);
    let tail = if rows.len() > n {
        &rows[rows.len() - n..]
    } else {
        rows
    };
    let values: Vec<f64> = tail.iter().map(|c| series_value(c, metric)).collect();
    let v = ema_last_f64(&values, span);
    if v.is_finite() {
        Some(v)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flat(ts: u64, price: f64) -> Candle {
        [ts as f64, price, price, price, price, 1.0]
    }

    #[test]
    fn range_uses_ceil_and_last_closed_bucket() {
        let now = 1_700_000_040_000 + 12_345; // 12.3 s into a minute
        let (s, e) = latest_finalized_range(3.0, ONE_MIN_MS, now);
        assert_eq!(e, 1_700_000_040_000 - ONE_MIN_MS);
        assert_eq!(s, e - 2 * ONE_MIN_MS);
        let (s2, _) = latest_finalized_range(2.1, ONE_MIN_MS, now);
        assert_eq!(s2, e - 2 * ONE_MIN_MS); // ceil(2.1) = 3 candles
    }

    #[test]
    fn missing_tail_is_fatal_leading_gap_is_not() {
        let t0 = 1_700_000_040_000u64;
        let candles: Vec<Candle> = (0..5)
            .map(|i| flat(t0 + i * ONE_MIN_MS, 1.0 + i as f64))
            .collect();
        let now = t0 + 5 * ONE_MIN_MS + 10;
        assert!(latest_ema(
            &candles,
            10.0,
            ONE_MIN_MS,
            now,
            Metric::Close,
            GapPolicy::Strict
        )
        .is_some());
        let now_late = t0 + 7 * ONE_MIN_MS + 10; // last closed = t0+6min, absent
        assert!(latest_ema(
            &candles,
            3.0,
            ONE_MIN_MS,
            now_late,
            Metric::Close,
            GapPolicy::Provisional
        )
        .is_none());
    }

    #[test]
    fn internal_gap_policy() {
        let t0 = 1_700_000_040_000u64;
        let candles = vec![
            flat(t0, 1.0),
            flat(t0 + ONE_MIN_MS, 2.0),
            flat(t0 + 3 * ONE_MIN_MS, 4.0),
        ];
        let now = t0 + 4 * ONE_MIN_MS + 1;
        assert!(latest_ema(
            &candles,
            4.0,
            ONE_MIN_MS,
            now,
            Metric::Close,
            GapPolicy::Strict
        )
        .is_none());
        let v = latest_ema(
            &candles,
            4.0,
            ONE_MIN_MS,
            now,
            Metric::Close,
            GapPolicy::Provisional,
        )
        .unwrap();
        // synthesised flat row at close 2.0 for the missing minute
        assert_eq!(v, ema_last_f64(&[1.0, 2.0, 2.0, 4.0], 4.0));
    }

    #[test]
    fn hourly_aggregation() {
        let t0 = 1_700_000_040_000u64 / ONE_HOUR_MS * ONE_HOUR_MS;
        let mut candles = Vec::new();
        for i in 0..120u64 {
            let p = 100.0 + i as f64;
            candles.push([
                (t0 + i * ONE_MIN_MS) as f64,
                p,
                p + 0.5,
                p - 0.5,
                p + 0.1,
                2.0,
            ]);
        }
        let h = aggregate_1h(&candles);
        assert_eq!(h.len(), 2);
        assert_eq!(h[0], [t0 as f64, 100.0, 159.5, 99.5, 159.1, 120.0]);
        assert_eq!(h[1][0], (t0 + ONE_HOUR_MS) as f64);
        let now = t0 + 2 * ONE_HOUR_MS + 5 * ONE_MIN_MS;
        assert!(latest_ema(
            &h,
            2.0,
            ONE_HOUR_MS,
            now,
            Metric::LogRange,
            GapPolicy::Strict
        )
        .is_some());
        assert!(latest_ema(
            &h,
            3.0,
            ONE_HOUR_MS,
            now,
            Metric::LogRange,
            GapPolicy::Strict
        )
        .is_none()); // needs 3 full buckets
    }

    #[test]
    fn open_tail_gap_detection() {
        let t0 = 1_700_000_040_000u64;
        let candles: Vec<Candle> = (0..10)
            .map(|i| flat(t0 + i * ONE_MIN_MS, 1.0 + i as f64))
            .collect();
        let max = 10 * ONE_MIN_MS;
        // last closed minute present -> no context
        assert_eq!(open_tail_gap(&candles, t0 + 10 * ONE_MIN_MS + 5, max), None);
        // three minutes missing -> context anchored at the last cached row
        let now = t0 + 13 * ONE_MIN_MS + 5;
        assert_eq!(
            open_tail_gap(&candles, now, max),
            Some(OpenTailGap {
                latest_expected_ts: t0 + 12 * ONE_MIN_MS,
                last_cached_ts: t0 + 9 * ONE_MIN_MS,
                tail_gap_ms: 3 * ONE_MIN_MS,
            })
        );
        // beyond the budget, or no candles at all -> none
        assert_eq!(open_tail_gap(&candles, t0 + 21 * ONE_MIN_MS, max), None);
        assert_eq!(open_tail_gap(&[], now, max), None);
        // an open (current) minute in the buffer is ignored
        let mut with_open = candles.clone();
        with_open.push(flat(t0 + 13 * ONE_MIN_MS, 9.0));
        assert_eq!(
            open_tail_gap(&with_open, now, max).map(|g| g.last_cached_ts),
            Some(t0 + 9 * ONE_MIN_MS)
        );
    }

    #[test]
    fn open_tail_rows_and_projected_ema_match_python_arithmetic() {
        // cm:9221: window start = min(latest_expected - (ceil(span)-1) min,
        // last_cached); internal gaps synthesised; flat zero-volume tail at
        // the previous close; per span EMA over the last ceil(span) rows.
        let t0 = 1_700_000_040_000u64;
        let candles = vec![
            flat(t0, 1.0),
            flat(t0 + ONE_MIN_MS, 2.0),
            flat(t0 + 3 * ONE_MIN_MS, 4.0), // minute 2 missing (internal gap)
        ];
        let gap = OpenTailGap {
            latest_expected_ts: t0 + 5 * ONE_MIN_MS,
            last_cached_ts: t0 + 3 * ONE_MIN_MS,
            tail_gap_ms: 2 * ONE_MIN_MS,
        };
        // span 3: window start = min(t0+3, t0+3) -> the anchor row plus two flat rows
        let rows3 = open_tail_rows(&candles, &gap, 3.0).unwrap();
        let ts3: Vec<u64> = rows3.iter().map(|c| c[0] as u64).collect();
        assert_eq!(ts3, (3..6).map(|i| t0 + i * ONE_MIN_MS).collect::<Vec<_>>());
        // span 10: the whole history, internal gap synthesised
        let rows = open_tail_rows(&candles, &gap, 10.0).unwrap();
        let ts: Vec<u64> = rows.iter().map(|c| c[0] as u64).collect();
        assert_eq!(ts, (0..6).map(|i| t0 + i * ONE_MIN_MS).collect::<Vec<_>>());
        assert_eq!(
            rows[2],
            [(t0 + 2 * ONE_MIN_MS) as f64, 2.0, 2.0, 2.0, 2.0, 0.0]
        );
        assert_eq!(
            rows[4],
            [(t0 + 4 * ONE_MIN_MS) as f64, 4.0, 4.0, 4.0, 4.0, 0.0]
        );
        assert_eq!(rows[5][4], 4.0);
        // span 3 -> the last three closes [4, 4, 4]; span 10 -> all six rows
        assert_eq!(
            projected_ema(&rows, 3.0, Metric::Close),
            Some(ema_last_f64(&[4.0, 4.0, 4.0], 3.0))
        );
        assert_eq!(
            projected_ema(&rows, 10.0, Metric::Close),
            Some(ema_last_f64(&[1.0, 2.0, 2.0, 4.0, 4.0, 4.0], 10.0))
        );
        // the tail rows carry zero volume
        assert_eq!(
            projected_ema(&rows, 2.0, Metric::QuoteVolume),
            Some(ema_last_f64(&[0.0, 0.0], 2.0))
        );
        // anchor missing from the local candles -> reader RuntimeError
        let stale = OpenTailGap {
            latest_expected_ts: t0 + 5 * ONE_MIN_MS,
            last_cached_ts: t0 + 4 * ONE_MIN_MS,
            tail_gap_ms: ONE_MIN_MS,
        };
        assert_eq!(open_tail_rows(&candles, &stale, 3.0), None);
        assert_eq!(open_tail_rows(&[], &gap, 3.0), None);
        assert_eq!(projected_ema(&[], 3.0, Metric::Close), None);
    }

    #[test]
    fn series_formulas() {
        let c = [0.0, 1.0, 3.0, 2.0, 2.5, 4.0];
        assert_eq!(
            series_value(&c, Metric::QuoteVolume),
            4.0 * (3.0 + 2.0 + 2.5) / 3.0
        );
        assert_eq!(series_value(&c, Metric::LogRange), (3.0f64 / 2.0).ln());
    }
}
