//! Order execution (docs/RECONCILE_SPEC.md section 3, subset 4.2).
//!
//! Cancels first (gathered), then creates (gathered), never interleaved; no
//! retries inside a cycle; "already gone" cancels count as success; every
//! failed write feeds a 10-per-hour error budget that asks the supervisor to
//! restart; acknowledged creates are remembered for the 15 s recent-execution
//! guard.

use crate::reconcile::{OrderRec, Plan, RecentExecution};
use anyhow::Result;
use pb_exchange_bybit::{ExchangeClient, ExchangeError, NewOrder};
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
}

#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("error budget exhausted ({0} write failures in the last hour)")]
    RestartRequested(usize),
}

pub struct Executor {
    client: Arc<dyn ExchangeClient>,
    post_only: bool,
    recent: Vec<RecentExecution>,
    error_times: Vec<u64>,
}

impl Executor {
    pub fn new(client: Arc<dyn ExchangeClient>, post_only: bool) -> Self {
        Self {
            client,
            post_only,
            recent: Vec::new(),
            error_times: Vec::new(),
        }
    }

    /// Creates acknowledged in the last 15 s (SPEC 2.8).
    pub fn recent_executions(&mut self, now_ms: u64) -> &[RecentExecution] {
        self.recent
            .retain(|r| now_ms.saturating_sub(r.timestamp_ms) < 15_000);
        &self.recent
    }

    fn note_failure(&mut self, now_ms: u64) -> Result<(), ExecError> {
        self.error_times.push(now_ms);
        self.error_times
            .retain(|t| now_ms.saturating_sub(*t) < 3_600_000);
        let n = self.error_times.len();
        tracing::warn!(
            count = n,
            limit = ERROR_BUDGET_PER_HOUR,
            "[health] error_budget"
        );
        if n >= ERROR_BUDGET_PER_HOUR {
            return Err(ExecError::RestartRequested(n));
        }
        Ok(())
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
        }
    }

    /// Execute one wave. Returns the report; `Err` only when the error
    /// budget asks for a restart (the wave itself never aborts early).
    pub async fn execute(&mut self, plan: &Plan, now_ms: u64) -> Result<WaveReport, ExecError> {
        let mut report = WaveReport::default();
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
                tracing::warn!(
                    "cancel response length mismatch; treating the batch as unacknowledged"
                );
                self.note_failure(now_ms)?;
            } else {
                let mut failed = false;
                for (r, (id, symbol)) in results.iter().zip(ids.iter()) {
                    match r {
                        Ok(_) => report.cancels_ok += 1,
                        Err(e) => {
                            failed = true;
                            report.failures += 1;
                            report.write_failures.push((symbol.clone(), e.clone()));
                            tracing::warn!(%symbol, %id, error = %e, "[order] cancel not acknowledged");
                        }
                    }
                }
                if failed {
                    self.note_failure(now_ms)?;
                }
            }
        }
        if !plan.creates.is_empty() {
            let orders: Vec<NewOrder> = plan.creates.iter().map(|o| self.to_new_order(o)).collect();
            for o in &plan.creates {
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
                self.note_failure(now_ms)?;
            } else {
                let mut failed = false;
                for (r, o) in results.iter().zip(plan.creates.iter()) {
                    match r {
                        Ok(ack) => {
                            report.creates_ok += 1;
                            let mut done = o.clone();
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
                    self.note_failure(now_ms)?;
                }
            }
        }
        Ok(report)
    }
}
