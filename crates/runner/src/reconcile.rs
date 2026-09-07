//! Engine output -> cancel/create plan (docs/RECONCILE_SPEC.md sections 1-2,
//! "minimal faithful subset" 4.1).
//!
//! Given the engine's executable orders, the positions, the open orders and
//! the per-side `PB_modes`, produce the ordered, batched lists of orders to
//! cancel and to create exactly as `calc_orders_to_cancel_and_create` does:
//! conversion with reduce-only trimming, open-order normalisation, exact
//! 8-key matching, mode filters, tolerance matching (maximum-cardinality),
//! market-distance sorting, batch limits, cancel-first barrier and the
//! recent-execution guard.

use crate::live::PlannedOrder;
use passivbot_rust::types::OrderType;
use pb_exchange_bybit::{OpenOrder, PositionSide, Side};
use std::collections::{BTreeMap, HashSet};
use strum::IntoEnumIterator;

pub const CUSTOM_ID_MAX_LEN: usize = 36;

/// Bot-side order (a resting open order or a planned create), normalised the
/// way `snapshot_actual_orders` / `to_executable_orders` do.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderRec {
    pub symbol: String,
    pub side: Side,
    pub pside: PositionSide,
    pub qty: f64,
    pub price: f64,
    pub reduce_only: bool,
    /// `true` = limit, `false` = market (open orders are always limit here).
    pub limit: bool,
    /// Snake order type from the custom id / engine, `"unknown"` otherwise.
    pub pb_order_type: String,
    pub risk_critical: bool,
    /// Exchange id (open orders only).
    pub id: Option<String>,
    pub custom_id: Option<String>,
}

impl OrderRec {
    fn cohort(&self) -> (String, PositionSide, Side, bool, bool, String) {
        (
            self.symbol.clone(),
            self.pside,
            self.side,
            self.reduce_only,
            self.limit,
            self.pb_order_type.clone(),
        )
    }

    fn exact_key(&self) -> (String, Side, PositionSide, bool, bool, String, u64, u64) {
        (
            self.symbol.clone(),
            self.side,
            self.pside,
            self.reduce_only,
            self.limit,
            self.pb_order_type.clone(),
            self.qty.to_bits(),
            self.price.to_bits(),
        )
    }
}

/// `type_token(id) + uuid4().hex`, truncated to 36 chars (SPEC 1.4).
pub fn format_custom_id(order_type: &str) -> String {
    let id = OrderType::from_snake(order_type)
        .map(|t| t.id())
        .unwrap_or(0);
    let token = format!("0x{id:04x}");
    let rand = uuid::Uuid::new_v4().simple().to_string();
    let mut s = token + &rand;
    s.truncate(CUSTOM_ID_MAX_LEN);
    s
}

/// `custom_id_to_snake` restricted to ids with the explicit `0x` marker.
pub fn pb_order_type_from_custom_id(custom_id: Option<&str>) -> String {
    let Some(cid) = custom_id else {
        return "unknown".into();
    };
    let Some(pos) = cid.find("0x") else {
        return "unknown".into();
    };
    let hex = &cid[pos + 2..];
    if hex.len() < 4 {
        return "unknown".into();
    }
    match u16::from_str_radix(&hex[..4], 16)
        .ok()
        .and_then(|id| OrderType::iter().find(|t| t.id() == id))
    {
        Some(t) => serde_json::to_value(t)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or("unknown".into()),
        None => "unknown".into(),
    }
}

/// `order_market_diff` = engine `calc_order_price_diff`: buy `1 - p/m`, sell `p/m - 1`.
pub fn order_market_diff(side: Side, price: f64, market: f64) -> f64 {
    if !price.is_finite() || !market.is_finite() || market <= 0.0 {
        return 0.0;
    }
    match side {
        Side::Buy => 1.0 - price / market,
        Side::Sell => price / market - 1.0,
    }
}

fn round12(x: f64) -> f64 {
    (x * 1e12).round() / 1e12
}

/// Step B + C of SPEC 1.2: planned engine orders -> executable order dicts
/// with reduce-only quantities trimmed to the position (per order and in
/// aggregate per pside).
pub fn to_executable(
    planned: &[PlannedOrder],
    position_size: &dyn Fn(&str, PositionSide) -> f64,
    last_price: &dyn Fn(&str) -> f64,
) -> Vec<OrderRec> {
    let mut out: Vec<OrderRec> = planned
        .iter()
        .filter(|o| o.qty != 0.0)
        .map(|o| OrderRec {
            symbol: o.symbol.clone(),
            side: o.side,
            pside: o.pside,
            qty: o.qty.abs(),
            price: o.price,
            reduce_only: o.reduce_only,
            limit: !o.market,
            pb_order_type: o.order_type.clone(),
            risk_critical: o.risk_critical,
            id: None,
            custom_id: Some(format_custom_id(&o.order_type)),
        })
        .collect();
    // Per-order cap.
    for o in out.iter_mut().filter(|o| o.reduce_only) {
        let pos = position_size(&o.symbol, o.pside).abs();
        if o.qty > pos {
            o.qty = round12(pos);
        }
    }
    // Aggregate cap per (symbol, pside): trim ordinary closes farthest from
    // market first, protective (risk-critical) reducers last.
    let mut groups: BTreeMap<(String, PositionSide), Vec<usize>> = BTreeMap::new();
    for (i, o) in out.iter().enumerate() {
        if o.reduce_only {
            groups
                .entry((o.symbol.clone(), o.pside))
                .or_default()
                .push(i);
        }
    }
    for ((symbol, pside), idxs) in groups {
        let pos = position_size(&symbol, pside).abs();
        let total: f64 = idxs.iter().map(|&i| out[i].qty).sum();
        let tol = 4.0 * f64::EPSILON * total.abs().max(pos.abs());
        if total <= pos + tol {
            continue;
        }
        let mut excess = total - pos;
        let mkt = last_price(&symbol);
        let mut order: Vec<usize> = idxs.clone();
        order.sort_by(|&a, &b| {
            let ka = (
                u8::from(!out[a].risk_critical),
                order_market_diff(out[a].side, out[a].price, mkt),
            );
            let kb = (
                u8::from(!out[b].risk_critical),
                order_market_diff(out[b].side, out[b].price, mkt),
            );
            kb.partial_cmp(&ka).unwrap_or(std::cmp::Ordering::Equal)
        });
        for i in order {
            if excess <= 0.0 {
                break;
            }
            let cut = excess.min(out[i].qty);
            out[i].qty = round12(out[i].qty - cut);
            excess -= cut;
        }
    }
    out.retain(|o| o.qty > 0.0);
    out
}

/// SPEC 2.1: an exchange open order as the reconciler sees it.
pub fn normalize_open_order(o: &OpenOrder, hedge_mode: bool) -> OrderRec {
    let reduce_only = if hedge_mode {
        matches!(
            (o.pside, o.side),
            (PositionSide::Long, Side::Sell) | (PositionSide::Short, Side::Buy)
        )
    } else {
        o.reduce_only
    };
    OrderRec {
        symbol: o.symbol.clone(),
        side: o.side,
        pside: o.pside,
        qty: o.qty,
        price: o.price,
        reduce_only,
        limit: true,
        pb_order_type: pb_order_type_from_custom_id(o.client_id.as_deref()),
        risk_critical: false,
        id: Some(o.id.clone()),
        custom_id: o.client_id.clone(),
    }
}

/// `PB_modes[pside][symbol]` (SPEC 1.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PbMode {
    Normal,
    GracefulStop,
    Panic,
    TpOnly,
    TpOnlyWithActiveEntryCancellation,
    Manual,
}

impl PbMode {
    pub fn parse(s: &str) -> PbMode {
        match s {
            "normal" => PbMode::Normal,
            "graceful_stop" => PbMode::GracefulStop,
            "panic" => PbMode::Panic,
            "tp_only" => PbMode::TpOnly,
            "tp_only_with_active_entry_cancellation" => PbMode::TpOnlyWithActiveEntryCancellation,
            _ => PbMode::Manual,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReconcileParams {
    pub hedge_mode: bool,
    /// `live.order_match_tolerance_pct` (fraction; <= 0 disables).
    pub match_tolerance: f64,
    pub max_cancels_per_batch: usize,
    pub max_creates_per_batch: usize,
}

#[derive(Debug, Default, Clone)]
pub struct Plan {
    pub cancels: Vec<OrderRec>,
    pub creates: Vec<OrderRec>,
    pub matched_exact: usize,
    pub matched_tolerance: usize,
    pub deferred_by_barrier: usize,
    pub deferred_recent: usize,
    pub deferred_capacity: usize,
}

/// A create acknowledged less than 15 s ago (SPEC 2.8).
#[derive(Debug, Clone)]
pub struct RecentExecution {
    pub order: OrderRec,
    pub timestamp_ms: u64,
}

fn tolerance_match(prev: &OrderRec, cur: &OrderRec, tol: f64) -> Option<f64> {
    if prev.cohort() != cur.cohort() || cur.price <= 0.0 || cur.qty <= 0.0 {
        return None;
    }
    let dp = (prev.price - cur.price).abs() / cur.price;
    let dq = (prev.qty - cur.qty).abs() / cur.qty;
    (dp <= tol && dq <= tol).then_some(dp + dq)
}

/// Maximum-cardinality one-to-one matching over the tolerance candidates,
/// candidates tried in `(distance, cur index, prev index)` order.
fn max_matching(cur: &[OrderRec], prev: &[OrderRec], tol: f64) -> Vec<(usize, usize)> {
    let mut adj: Vec<Vec<(f64, usize)>> = vec![Vec::new(); cur.len()];
    for (ci, c) in cur.iter().enumerate() {
        for (pi, p) in prev.iter().enumerate() {
            if let Some(d) = tolerance_match(p, c, tol) {
                adj[ci].push((d, pi));
            }
        }
        adj[ci].sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    }
    let mut match_prev: Vec<Option<usize>> = vec![None; prev.len()];
    fn augment(
        ci: usize,
        adj: &[Vec<(f64, usize)>],
        seen: &mut [bool],
        match_prev: &mut [Option<usize>],
    ) -> bool {
        for &(_, pi) in &adj[ci] {
            if seen[pi] {
                continue;
            }
            seen[pi] = true;
            if match_prev[pi].is_none() || augment(match_prev[pi].unwrap(), adj, seen, match_prev) {
                match_prev[pi] = Some(ci);
                return true;
            }
        }
        false
    }
    for ci in 0..cur.len() {
        let mut seen = vec![false; prev.len()];
        augment(ci, &adj, &mut seen, &mut match_prev);
    }
    match_prev
        .iter()
        .enumerate()
        .filter_map(|(pi, c)| c.map(|ci| (ci, pi)))
        .collect()
}

/// SPEC 2.2-2.8. `modes` maps `(symbol, pside)` to the current `PB_mode`;
/// `last_price` supplies the market price for sorting.
#[allow(clippy::too_many_arguments)]
pub fn reconcile(
    ideal: &[OrderRec],
    open: &[OrderRec],
    modes: &dyn Fn(&str, PositionSide) -> PbMode,
    last_price: &dyn Fn(&str) -> f64,
    recent: &[RecentExecution],
    now_ms: u64,
    params: &ReconcileParams,
) -> Plan {
    let mut plan = Plan::default();
    let symbols: HashSet<&str> = ideal
        .iter()
        .chain(open.iter())
        .map(|o| o.symbol.as_str())
        .collect();
    let mut to_cancel: Vec<OrderRec> = Vec::new();
    let mut to_create: Vec<OrderRec> = Vec::new();
    let mut symbols: Vec<&str> = symbols.into_iter().collect();
    symbols.sort();
    for symbol in symbols {
        let actual: Vec<&OrderRec> = open.iter().filter(|o| o.symbol == symbol).collect();
        let wanted: Vec<&OrderRec> = ideal.iter().filter(|o| o.symbol == symbol).collect();
        // 2.2 exact matching, first match wins in ideal-list order.
        let mut used = vec![false; actual.len()];
        let mut creates: Vec<OrderRec> = Vec::new();
        for w in &wanted {
            let key = w.exact_key();
            match actual
                .iter()
                .enumerate()
                .find(|(i, a)| !used[*i] && a.exact_key() == key)
            {
                Some((i, _)) => {
                    used[i] = true;
                    plan.matched_exact += 1;
                }
                None => creates.push((*w).clone()),
            }
        }
        let mut cancels: Vec<OrderRec> = actual
            .iter()
            .enumerate()
            .filter(|(i, _)| !used[*i])
            .map(|(_, a)| (*a).clone())
            .collect();
        // 2.4 mode filters.
        cancels.retain(|o| match modes(symbol, o.pside) {
            PbMode::Manual => false,
            PbMode::TpOnly => o.reduce_only,
            _ => true,
        });
        creates.retain(|o| match modes(symbol, o.pside) {
            PbMode::Manual => false,
            PbMode::TpOnly | PbMode::TpOnlyWithActiveEntryCancellation => o.reduce_only,
            _ => true,
        });
        // 2.3 tolerance matching.
        if params.match_tolerance > 0.0 {
            let prev: Vec<OrderRec> = cancels
                .iter()
                .filter(|o| o.pb_order_type != "unknown")
                .cloned()
                .collect();
            let pairs = max_matching(&creates, &prev, params.match_tolerance);
            if !pairs.is_empty() {
                let matched_cur: HashSet<usize> = pairs.iter().map(|p| p.0).collect();
                let matched_prev_ids: HashSet<Option<String>> =
                    pairs.iter().map(|p| prev[p.1].id.clone()).collect();
                creates = creates
                    .into_iter()
                    .enumerate()
                    .filter(|(i, _)| !matched_cur.contains(i))
                    .map(|(_, o)| o)
                    .collect();
                cancels.retain(|o| !matched_prev_ids.contains(&o.id));
                plan.matched_tolerance += pairs.len();
            }
        }
        to_cancel.extend(cancels);
        to_create.extend(creates);
    }
    // 2.5 sort by market distance ascending.
    let dist = |o: &OrderRec| order_market_diff(o.side, o.price, last_price(&o.symbol));
    to_cancel.sort_by(|a, b| {
        dist(a)
            .partial_cmp(&dist(b))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    to_create.sort_by(|a, b| {
        dist(a)
            .partial_cmp(&dist(b))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    // 2.6 cancel capacity: reduce-only first when over capacity.
    if to_cancel.len() > params.max_cancels_per_batch {
        let (mut ro, rest): (Vec<OrderRec>, Vec<OrderRec>) =
            to_cancel.into_iter().partition(|o| o.reduce_only);
        ro.extend(rest);
        plan.deferred_capacity += ro.len() - params.max_cancels_per_batch;
        ro.truncate(params.max_cancels_per_batch);
        to_cancel = ro;
    }
    // 2.7 cancel-first barrier.
    if !to_cancel.is_empty() {
        let scopes: HashSet<(String, Option<PositionSide>)> = to_cancel
            .iter()
            .map(|o| {
                (
                    o.symbol.clone(),
                    if params.hedge_mode {
                        Some(o.pside)
                    } else {
                        None
                    },
                )
            })
            .collect();
        let before = to_create.len();
        to_create.retain(|o| {
            let scope = (
                o.symbol.clone(),
                if params.hedge_mode {
                    Some(o.pside)
                } else {
                    None
                },
            );
            let bypass = o.reduce_only && !o.limit && o.pb_order_type.starts_with("close_panic");
            bypass || !scopes.contains(&scope)
        });
        plan.deferred_by_barrier += before - to_create.len();
    }
    // 2.8 recent-execution guard (15 s; qty 1 %, price 0.2 %).
    let before = to_create.len();
    to_create.retain(|o| {
        !recent.iter().any(|r| {
            now_ms.saturating_sub(r.timestamp_ms) < 15_000
                && r.order.symbol == o.symbol
                && r.order.side == o.side
                && r.order.pside == o.pside
                && r.order.reduce_only == o.reduce_only
                && (r.order.qty - o.qty).abs() <= o.qty * 0.01
                && (r.order.price - o.price).abs() <= o.price * 0.002
        })
    });
    plan.deferred_recent += before - to_create.len();
    // 2.6 create capacity: risk-critical first (stable), then truncate.
    if to_create.len() > params.max_creates_per_batch {
        let (mut rc, rest): (Vec<OrderRec>, Vec<OrderRec>) =
            to_create.into_iter().partition(|o| o.risk_critical);
        rc.extend(rest);
        plan.deferred_capacity += rc.len() - params.max_creates_per_batch;
        rc.truncate(params.max_creates_per_batch);
        to_create = rc;
    }
    plan.cancels = to_cancel;
    plan.creates = to_create;
    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(
        symbol: &str,
        side: Side,
        pside: PositionSide,
        qty: f64,
        price: f64,
        t: &str,
        id: Option<&str>,
    ) -> OrderRec {
        OrderRec {
            symbol: symbol.into(),
            side,
            pside,
            qty,
            price,
            reduce_only: t.contains("close"),
            limit: true,
            pb_order_type: t.into(),
            risk_critical: false,
            id: id.map(str::to_string),
            custom_id: None,
        }
    }

    fn params() -> ReconcileParams {
        ReconcileParams {
            hedge_mode: false,
            match_tolerance: 0.0002,
            max_cancels_per_batch: 5,
            max_creates_per_batch: 3,
        }
    }

    #[test]
    fn custom_id_format_and_decode() {
        let id = format_custom_id("entry_grid_normal_long");
        assert_eq!(id.len(), 36);
        assert!(id.starts_with("0x0004"));
        assert_eq!(
            pb_order_type_from_custom_id(Some(&id)),
            "entry_grid_normal_long"
        );
        assert_eq!(
            pb_order_type_from_custom_id(Some("0x00048ead933b53284cf7b9836e62b694cf")),
            "entry_grid_normal_long"
        );
        assert_eq!(pb_order_type_from_custom_id(Some("manual-123")), "unknown");
        assert_eq!(pb_order_type_from_custom_id(None), "unknown");
    }

    #[test]
    fn exact_match_keeps_resting_order_and_cancels_foreign() {
        let ideal = vec![rec(
            "A",
            Side::Buy,
            PositionSide::Long,
            10.0,
            1.0,
            "entry_grid_normal_long",
            None,
        )];
        let open = vec![
            rec(
                "A",
                Side::Buy,
                PositionSide::Long,
                10.0,
                1.0,
                "entry_grid_normal_long",
                Some("1"),
            ),
            rec(
                "A",
                Side::Buy,
                PositionSide::Long,
                5.0,
                0.9,
                "unknown",
                Some("2"),
            ),
        ];
        let plan = reconcile(
            &ideal,
            &open,
            &|_, _| PbMode::Normal,
            &|_| 1.0,
            &[],
            0,
            &params(),
        );
        assert_eq!(plan.matched_exact, 1);
        assert!(plan.creates.is_empty());
        assert_eq!(plan.cancels.len(), 1);
        assert_eq!(plan.cancels[0].id.as_deref(), Some("2"));
    }

    #[test]
    fn tolerance_match_skips_recreate_and_barrier_defers() {
        let ideal = vec![
            rec(
                "A",
                Side::Buy,
                PositionSide::Long,
                10.0,
                1.0001,
                "entry_grid_normal_long",
                None,
            ),
            rec(
                "A",
                Side::Sell,
                PositionSide::Long,
                3.0,
                1.2,
                "close_grid_long",
                None,
            ),
        ];
        let open = vec![
            rec(
                "A",
                Side::Buy,
                PositionSide::Long,
                10.0,
                1.0,
                "entry_grid_normal_long",
                Some("1"),
            ),
            rec(
                "A",
                Side::Sell,
                PositionSide::Long,
                3.0,
                1.3,
                "close_grid_long",
                Some("2"),
            ),
        ];
        let plan = reconcile(
            &ideal,
            &open,
            &|_, _| PbMode::Normal,
            &|_| 1.0,
            &[],
            0,
            &params(),
        );
        assert_eq!(plan.matched_tolerance, 1);
        assert_eq!(plan.cancels.len(), 1); // the 1.3 close
                                           // the 1.2 close is deferred by the cancel-first barrier (same symbol, one-way scope)
        assert!(plan.creates.is_empty());
        assert_eq!(plan.deferred_by_barrier, 1);
        let hedged = ReconcileParams {
            hedge_mode: true,
            ..params()
        };
        let plan = reconcile(
            &ideal,
            &open,
            &|_, _| PbMode::Normal,
            &|_| 1.0,
            &[],
            0,
            &hedged,
        );
        assert!(plan.creates.is_empty()); // same (symbol, pside) scope still deferred
    }

    #[test]
    fn mode_filters() {
        let ideal = vec![
            rec(
                "A",
                Side::Buy,
                PositionSide::Long,
                10.0,
                1.0,
                "entry_grid_normal_long",
                None,
            ),
            rec(
                "A",
                Side::Sell,
                PositionSide::Long,
                3.0,
                1.2,
                "close_grid_long",
                None,
            ),
        ];
        let open = vec![rec(
            "A",
            Side::Buy,
            PositionSide::Long,
            7.0,
            0.8,
            "unknown",
            Some("m"),
        )];
        let plan = reconcile(
            &ideal,
            &open,
            &|_, _| PbMode::TpOnly,
            &|_| 1.0,
            &[],
            0,
            &params(),
        );
        assert!(plan.cancels.is_empty()); // manual entry kept under tp_only (not reduce-only)
        assert_eq!(plan.creates.len(), 1);
        assert!(plan.creates[0].reduce_only);
        let plan = reconcile(
            &ideal,
            &open,
            &|_, _| PbMode::Manual,
            &|_| 1.0,
            &[],
            0,
            &params(),
        );
        assert!(plan.cancels.is_empty() && plan.creates.is_empty());
    }

    #[test]
    fn sorting_and_capacity() {
        let mut ideal = Vec::new();
        for i in 0..6 {
            ideal.push(rec(
                "A",
                Side::Buy,
                PositionSide::Long,
                1.0,
                1.0 - 0.01 * (i as f64 + 1.0),
                "entry_grid_normal_long",
                None,
            ));
        }
        ideal[4].risk_critical = true;
        let plan = reconcile(
            &ideal,
            &[],
            &|_, _| PbMode::Normal,
            &|_| 1.0,
            &[],
            0,
            &params(),
        );
        assert_eq!(plan.creates.len(), 3);
        assert!(plan.creates[0].risk_critical);
        assert!(plan.creates[1].price > plan.creates[2].price); // closest to market first
        assert_eq!(plan.deferred_capacity, 3);
    }

    #[test]
    fn reduce_only_trim() {
        let planned = vec![
            PlannedOrder {
                symbol: "A".into(),
                side: Side::Sell,
                pside: PositionSide::Long,
                qty: 6.0,
                price: 1.1,
                order_type: "close_grid_long".into(),
                reduce_only: true,
                market: false,
                risk_critical: false,
            },
            PlannedOrder {
                symbol: "A".into(),
                side: Side::Sell,
                pside: PositionSide::Long,
                qty: 6.0,
                price: 1.2,
                order_type: "close_grid_long".into(),
                reduce_only: true,
                market: false,
                risk_critical: false,
            },
        ];
        let out = to_executable(&planned, &|_, _| 10.0, &|_| 1.0);
        let total: f64 = out.iter().map(|o| o.qty).sum();
        assert_eq!(total, 10.0);
        assert_eq!(out.iter().find(|o| o.price == 1.2).unwrap().qty, 4.0); // farthest trimmed first
    }
}
