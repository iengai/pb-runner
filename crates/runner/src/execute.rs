//! Order execution (docs/RECONCILE_SPEC.md section 3, subset 4.2).
//!
//! Cancels first (gathered), then creates (gathered), never interleaved; no
//! retries inside a cycle; "already gone" cancels count as success but mark
//! the symbol state-dirty for the wave (3.2), as does a cancel that was not
//! acknowledged, and creates on dirty symbols are skipped (3.1 step 6);
//! symbols are configured lazily before their first create (3.1 step 7,
//! `exchange_config.rs`); every failed write batch feeds the 10-per-hour
//! error budget shared with the planning loop (`restart_bot_on_too_many_errors`,
//! passivbot.py:20544-20566); acknowledged creates are remembered for the
//! 15 s recent-execution guard.

use crate::exchange_config::ExchangeConfigurator;
use crate::reconcile::{OrderRec, Plan, RecentExecution};
use anyhow::Result;
use pb_exchange_bybit::{ExchangeClient, ExchangeError, NewOrder, OrderType};
use std::collections::HashSet;
use std::sync::Arc;

pub const ERROR_BUDGET_PER_HOUR: usize = 10;

#[derive(Debug, Default, Clone)]
pub struct WaveReport {
    pub cancels_sent: usize,
    pub cancels_ok: usize,
    pub creates_sent: usize,
    pub creates_ok: usize,
    pub failures: usize,
    /// A cancel went out: the next cycle must refresh every surface before
    /// creating in its scope (cancel-first barrier, the loop refreshes all
    /// surfaces at the start of every cycle anyway).
    pub full_refresh_requested: bool,
    /// Every rejected write as `(symbol, error)`, in wave order: the input of
    /// `_handle_order_write_failures` (exchange-unavailable cooldowns,
    /// `cooldown.rs`).
    pub write_failures: Vec<(String, ExchangeError)>,
    /// `state_change_detected_by_symbol` after the cancel batch: symbols with
    /// a non-acknowledged or already-gone cancel (or every cancel symbol on
    /// a response length mismatch).
    pub dirty_symbols: Vec<String>,
    /// Creates skipped because their symbol was dirty (SPEC 3.1 step 6).
    pub skipped_dirty: usize,
    /// Creates skipped because their symbol's exchange configuration is
    /// still pending (SPEC 3.1 step 7).
    pub skipped_pending_config: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("error budget exhausted ({0} errors in the last hour)")]
    RestartRequested(usize),
}

pub struct Executor {
    client: Arc<dyn ExchangeClient>,
    post_only: bool,
    recent: Vec<RecentExecution>,
    /// `error_counts` (passivbot.py:20549-20553), shared by write failures
    /// and planning-loop failures.
    error_times: Vec<u64>,
    exchange_config: ExchangeConfigurator,
}

impl Executor {
    pub fn new(
        client: Arc<dyn ExchangeClient>,
        post_only: bool,
        exchange_config: ExchangeConfigurator,
    ) -> Self {
        Self {
            client,
            post_only,
            recent: Vec::new(),
            error_times: Vec::new(),
            exchange_config,
        }
    }

    pub fn exchange_config_mut(&mut self) -> &mut ExchangeConfigurator {
        &mut self.exchange_config
    }

    /// Creates acknowledged in the last 15 s (SPEC 2.8).
    pub fn recent_executions(&mut self, now_ms: u64) -> &[RecentExecution] {
        self.recent
            .retain(|r| now_ms.saturating_sub(r.timestamp_ms) < 15_000);
        &self.recent
    }

    /// `restart_bot_on_too_many_errors` (passivbot.py:20544-20566): append
    /// now, prune to the last hour, log `[health] error_budget`, and ask for
    /// a restart at 10. Called once per failed write batch
    /// (`_handle_order_write_failures`) and once per failed planning cycle
    /// (`_handle_execution_loop_failure`, 6183-6240) -- one counter.
    pub fn note_error(&mut self, now_ms: u64) -> Result<(), ExecError> {
        self.error_times.push(now_ms);
        self.error_times
            .retain(|t| now_ms.saturating_sub(*t) < 3_600_000);
        let n = self.error_times.len();
        let action = if n >= ERROR_BUDGET_PER_HOUR {
            "restart_at_limit"
        } else {
            "continue"
        };
        tracing::info!(
            count = n,
            limit = ERROR_BUDGET_PER_HOUR,
            window = "1h",
            action,
            "[health] error_budget"
        );
        if n >= ERROR_BUDGET_PER_HOUR {
            return Err(ExecError::RestartRequested(n));
        }
        Ok(())
    }

    pub fn error_count(&self) -> usize {
        self.error_times.len()
    }

    fn to_new_order(&self, o: &OrderRec) -> NewOrder {
        NewOrder {
            client_id: o.custom_id.clone().unwrap_or_default(),
            symbol: o.symbol.clone(),
            side: o.side,
            pside: o.pside,
            qty: o.qty,
            price: o.price,
            reduce_only: o.reduce_only,
            post_only: self.post_only && o.limit,
            order_type: if o.limit {
                OrderType::Limit
            } else {
                OrderType::Market
            },
        }
    }

    /// Execute one wave. Returns the report; `Err` only when the error
    /// budget asks for a restart (the wave itself never aborts early).
    pub async fn execute(&mut self, plan: &Plan, now_ms: u64) -> Result<WaveReport, ExecError> {
        let mut report = WaveReport::default();
        let mut dirty: HashSet<String> = HashSet::new();
        if !plan.cancels.is_empty() {
            let ids: Vec<(String, String)> = plan
                .cancels
                .iter()
                .filter_map(|o| o.id.clone().map(|id| (id, o.symbol.clone())))
                .collect();
            for o in &plan.cancels {
                tracing::info!(
                    symbol = %o.symbol, side = ?o.side, pside = ?o.pside, qty = o.qty, price = o.price,
                    order_type = %o.pb_order_type, id = o.id.as_deref().unwrap_or("?"),
                    "[order] cancel"
                );
            }
            report.cancels_sent = ids.len();
            report.full_refresh_requested = true;
            let results = self.client.cancel_orders(&ids).await;
            if results.len() != ids.len() {
                // executor.py:1345-1360: every symbol marked dirty, execution
                // rescheduled; no error-budget charge.
                tracing::warn!(
                    "cancel response length mismatch; treating the batch as unacknowledged"
                );
                dirty.extend(ids.iter().map(|(_, s)| s.clone()));
            } else {
                let mut failed = false;
                for (r, (id, symbol)) in results.iter().zip(ids.iter()) {
                    match r {
                        Ok(ack) => {
                            report.cancels_ok += 1;
                            if ack.already_gone {
                                // `_passivbot_cancel_requires_full_authoritative_confirmation`
                                // (executor.py:1420-1440): the symbol is
                                // state-dirty for this wave.
                                dirty.insert(symbol.clone());
                            }
                        }
                        Err(e) => {
                            failed = true;
                            report.failures += 1;
                            report.write_failures.push((symbol.clone(), e.clone()));
                            dirty.insert(symbol.clone());
                            tracing::warn!(%symbol, %id, error = %e, "[order] cancel not acknowledged");
                        }
                    }
                }
                if failed {
                    self.note_error(now_ms)?;
                }
            }
        }
        report.dirty_symbols = {
            let mut v: Vec<String> = dirty.iter().cloned().collect();
            v.sort();
            v
        };
        // SPEC 3.1 step 6: creates on dirty symbols wait for the next cycle
        // (executor.py:808-850; the dedicated-protective-market-panic bypass
        // only exists on the protective supervisor path, never here).
        let mut creates: Vec<&OrderRec> = plan.creates.iter().collect();
        if !dirty.is_empty() {
            let before = creates.len();
            creates.retain(|o| !dirty.contains(&o.symbol));
            report.skipped_dirty = before - creates.len();
            if report.skipped_dirty > 0 {
                tracing::info!(
                    symbols = ?report.dirty_symbols,
                    skipped = report.skipped_dirty,
                    "[order] state change detected; skipping order creation until next cycle"
                );
            }
        }
        // SPEC 3.1 step 7: lazy exchange configuration of the create symbols
        // (executor.py:861-950); creates on symbols still pending are
        // skipped, without an error-budget charge.
        if !creates.is_empty() {
            let mut symbols: Vec<String> = creates.iter().map(|o| o.symbol.clone()).collect();
            symbols.sort();
            symbols.dedup();
            let client = self.client.clone();
            let outcome = self
                .exchange_config
                .update(&client, &symbols, now_ms, &|s| {
                    Box::pin(tokio::time::sleep(std::time::Duration::from_secs_f64(s)))
                })
                .await;
            let before = creates.len();
            creates.retain(|o| outcome.configured.contains(&o.symbol));
            report.skipped_pending_config = before - creates.len();
            if report.skipped_pending_config > 0 {
                let pending: Vec<&String> = symbols
                    .iter()
                    .filter(|s| !outcome.configured.contains(*s))
                    .collect();
                tracing::warn!(
                    ?pending,
                    blocked = report.skipped_pending_config,
                    "[config] skipping exposure-increasing order creation for symbols pending exchange config"
                );
            }
        }
        if !creates.is_empty() {
            let orders: Vec<NewOrder> = creates.iter().map(|o| self.to_new_order(o)).collect();
            for (o, n) in creates.iter().zip(orders.iter()) {
                if n.is_market() {
                    tracing::info!(
                        symbol = %o.symbol, side = ?o.side, pside = ?o.pside, qty = o.qty, price = o.price,
                        order_type = %o.pb_order_type, "[order] MARKET order submission"
                    );
                }
                tracing::info!(
                    symbol = %o.symbol, side = ?o.side, pside = ?o.pside, qty = o.qty, price = o.price,
                    order_type = %o.pb_order_type, limit = o.limit, custom_id = o.custom_id.as_deref().unwrap_or(""),
                    "[order] post"
                );
            }
            report.creates_sent = orders.len();
            let results = self.client.create_orders(&orders).await;
            if results.len() != orders.len() {
                tracing::warn!("create response length mismatch; treating the batch as ambiguous");
                self.note_error(now_ms)?;
            } else {
                let mut failed = false;
                for (r, o) in results.iter().zip(creates.iter()) {
                    match r {
                        Ok(ack) => {
                            report.creates_ok += 1;
                            let mut done = (*o).clone();
                            done.id = Some(ack.id.clone());
                            self.recent.push(RecentExecution {
                                order: done,
                                timestamp_ms: now_ms,
                            });
                        }
                        Err(e) => {
                            failed = true;
                            report.failures += 1;
                            report.write_failures.push((o.symbol.clone(), e.clone()));
                            tracing::warn!(symbol = %o.symbol, error = %e, "[order] create not acknowledged");
                        }
                    }
                }
                if failed {
                    self.note_error(now_ms)?;
                }
            }
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bot_params::ConfigView;
    use async_trait::async_trait;
    use pb_exchange_bybit::*;
    use std::sync::Mutex;

    /// Scripted client: cancels succeed / are already gone / fail per id,
    /// creates always succeed and are recorded.
    #[derive(Default)]
    struct Fake {
        gone: Mutex<HashSet<String>>,
        fail: Mutex<HashSet<String>>,
        created: Mutex<Vec<NewOrder>>,
        short_cancel_response: Mutex<bool>,
    }

    #[async_trait]
    impl ExchangeClient for Fake {
        async fn load_markets(&self) -> Result<Vec<MarketSpec>, ExchangeError> {
            Ok(vec![])
        }
        async fn fetch_balance(&self) -> Result<Balance, ExchangeError> {
            unreachable!()
        }
        async fn fetch_positions(&self) -> Result<Vec<Position>, ExchangeError> {
            unreachable!()
        }
        async fn fetch_open_orders(&self) -> Result<Vec<OpenOrder>, ExchangeError> {
            unreachable!()
        }
        async fn fetch_tickers(&self) -> Result<Vec<Ticker>, ExchangeError> {
            unreachable!()
        }
        async fn fetch_ohlcv(
            &self,
            _: &str,
            _: &str,
            _: Option<u64>,
            _: usize,
        ) -> Result<Vec<Candle>, ExchangeError> {
            unreachable!()
        }
        async fn fetch_fills(
            &self,
            _: Option<&str>,
            _: Option<u64>,
            _: Option<u64>,
        ) -> Result<Vec<Fill>, ExchangeError> {
            unreachable!()
        }
        async fn fetch_closed_pnl(
            &self,
            _: Option<u64>,
            _: Option<u64>,
        ) -> Result<Vec<ClosedPnl>, ExchangeError> {
            unreachable!()
        }
        async fn create_orders(&self, orders: &[NewOrder]) -> Vec<OrderResult<OpenOrder>> {
            self.created.lock().unwrap().extend(orders.iter().cloned());
            orders
                .iter()
                .enumerate()
                .map(|(i, o)| {
                    Ok(OpenOrder {
                        id: format!("n{i}"),
                        client_id: Some(o.client_id.clone()),
                        symbol: o.symbol.clone(),
                        side: o.side,
                        pside: o.pside,
                        qty: o.qty,
                        price: o.price,
                        reduce_only: o.reduce_only,
                        created_ms: None,
                    })
                })
                .collect()
        }
        async fn cancel_orders(&self, orders: &[(String, String)]) -> Vec<OrderResult<CancelAck>> {
            if *self.short_cancel_response.lock().unwrap() {
                return vec![];
            }
            orders
                .iter()
                .map(|(id, _)| {
                    if self.fail.lock().unwrap().contains(id) {
                        Err(ExchangeError::Network("boom".into()))
                    } else {
                        Ok(CancelAck {
                            id: id.clone(),
                            already_gone: self.gone.lock().unwrap().contains(id),
                        })
                    }
                })
                .collect()
        }
        async fn set_hedge_mode(&self) -> Result<(), ExchangeError> {
            Ok(())
        }
        async fn configure_symbol(
            &self,
            _: &str,
            _: f64,
            _: MarginMode,
        ) -> Result<(), ExchangeError> {
            Ok(())
        }
    }

    fn rec(symbol: &str, t: &str, id: Option<&str>, limit: bool) -> OrderRec {
        OrderRec {
            symbol: symbol.into(),
            side: Side::Buy,
            pside: PositionSide::Long,
            qty: 1.0,
            price: 1.0,
            reduce_only: t.contains("close"),
            limit,
            pb_order_type: t.into(),
            risk_critical: false,
            churn_evidenced: false,
            market_distance: None,
            id: id.map(str::to_string),
            custom_id: Some("0x0004abc".into()),
        }
    }

    fn cfg() -> ConfigView {
        ConfigView::new(
            serde_json::from_str(include_str!(
                "../../../tests/fixtures/configs/fake_v8/grid_v7.json"
            ))
            .unwrap(),
        )
        .unwrap()
    }

    fn rt<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(f)
    }

    fn executor(fake: &Arc<Fake>) -> Executor {
        Executor::new(
            fake.clone(),
            false,
            ExchangeConfigurator::from_config(&cfg()),
        )
    }

    #[test]
    fn already_gone_and_failed_cancels_mark_symbol_dirty_and_skip_its_creates() {
        let fake = Arc::new(Fake::default());
        fake.gone.lock().unwrap().insert("a1".into());
        fake.fail.lock().unwrap().insert("b1".into());
        let mut ex = executor(&fake);
        let plan = Plan {
            cancels: vec![
                rec("A", "entry_grid_normal_long", Some("a1"), true),
                rec("B", "entry_grid_normal_long", Some("b1"), true),
                rec("C", "entry_grid_normal_long", Some("c1"), true),
            ],
            creates: vec![
                rec("A", "close_grid_long", None, true),
                rec("B", "entry_grid_normal_long", None, true),
                rec("C", "entry_grid_normal_long", None, true),
            ],
            ..Default::default()
        };
        let r = rt(ex.execute(&plan, 1_000)).unwrap();
        assert_eq!(r.cancels_ok, 2); // already-gone counts as success
        assert_eq!(r.failures, 1);
        assert_eq!(r.dirty_symbols, vec!["A".to_string(), "B".to_string()]);
        assert_eq!(r.skipped_dirty, 2);
        assert_eq!(r.creates_sent, 1);
        let created = fake.created.lock().unwrap();
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].symbol, "C");
        // One failed batch = one error-budget entry.
        assert_eq!(ex.error_count(), 1);
    }

    #[test]
    fn cancel_length_mismatch_marks_dirty_without_budget_charge() {
        let fake = Arc::new(Fake::default());
        *fake.short_cancel_response.lock().unwrap() = true;
        let mut ex = executor(&fake);
        let plan = Plan {
            cancels: vec![rec("A", "entry_grid_normal_long", Some("a1"), true)],
            creates: vec![rec("A", "entry_grid_normal_long", None, true)],
            ..Default::default()
        };
        let r = rt(ex.execute(&plan, 1_000)).unwrap();
        assert_eq!(r.dirty_symbols, vec!["A".to_string()]);
        assert_eq!(r.creates_sent, 0);
        assert_eq!(ex.error_count(), 0);
    }

    #[test]
    fn market_creates_carry_the_order_type_and_never_post_only() {
        let fake = Arc::new(Fake::default());
        let mut ex = Executor::new(
            fake.clone(),
            true,
            ExchangeConfigurator::from_config(&cfg()),
        );
        let plan = Plan {
            creates: vec![
                rec("A", "close_panic_long", None, false),
                rec("A", "entry_grid_normal_long", None, true),
            ],
            ..Default::default()
        };
        let r = rt(ex.execute(&plan, 1_000)).unwrap();
        assert_eq!(r.creates_ok, 2);
        let created = fake.created.lock().unwrap();
        assert_eq!(created[0].order_type, OrderType::Market);
        assert!(!created[0].post_only);
        assert_eq!(created[1].order_type, OrderType::Limit);
        assert!(created[1].post_only);
    }

    #[test]
    fn shared_error_budget_trips_at_ten() {
        let fake = Arc::new(Fake::default());
        let mut ex = executor(&fake);
        for i in 0..9 {
            ex.note_error(i).unwrap();
        }
        assert!(matches!(
            ex.note_error(9),
            Err(ExecError::RestartRequested(10))
        ));
        // Entries older than an hour fall out of the window.
        let mut ex = executor(&fake);
        for i in 0..9 {
            ex.note_error(i).unwrap();
        }
        ex.note_error(3_600_000 + 100).unwrap();
        assert_eq!(ex.error_count(), 1);
    }
}
