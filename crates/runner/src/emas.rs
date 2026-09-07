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
    fn series_formulas() {
        let c = [0.0, 1.0, 3.0, 2.0, 2.5, 4.0];
        assert_eq!(
            series_value(&c, Metric::QuoteVolume),
            4.0 * (3.0 + 2.0 + 2.5) / 3.0
        );
        assert_eq!(series_value(&c, Metric::LogRange), (3.0f64 / 2.0).ln());
    }
}
