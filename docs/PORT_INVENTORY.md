# Port inventory (line 8 = passivbot v8.1.0, commit 7af64f3e9)

What the Python live loop does, split into: (A) reuse from the engine crate,
(B) port to Rust, (C) not ported. Line references are into
`E:\projects\passivbot\src\` at v8.1.0. Update this file when the pin moves.

## 1. Loop shape (what the runner must reproduce)

`passivbot.py`
- `start_bot` 3414 -> `run_execution_loop` 6260 -> `execute_to_exchange` 7856
  each cycle:
  1. refresh state: `update_open_orders` 15740, `update_positions` 16301,
     `update_positions_and_balance` 16351, candles via
     `candlestick_manager.py` and `maintain_forager_ws_candles` 21739,
     `update_effective_min_cost` 16417
  2. build snapshot: `calc_ideal_orders_orchestrator` 19752 ->
     `calc_ideal_orders_orchestrator_from_snapshot` 17216 (~2.5k lines)
  3. engine: `pbr.compute_ideal_orders_json` ~20016
  4. post-process: `live/reconciler.py:parse_and_validate_rust_orchestrator_output`
     2860, `order_churn_risk_active_pairs_from_rust_output` 2881
  5. diff vs open orders: `calc_orders_to_cancel_and_create` 20378
  6. execute: `live/executor.py` (1478 lines), cancels before creates,
     recently-cancelled guard (`reconciler.add_to_recent_order_cancellations`)
- Hourly/maintenance: `maintain_hourly_cycle` 21596 (markets refresh,
  leverage/margin mode), `maintain_monitor_snapshot` 21665.

## 2. (B) Snapshot construction — the part that must be ported faithfully

Inside `calc_ideal_orders_orchestrator_from_snapshot` (17216-19752) and helpers:

| Concern | Where (v8.1.0) | Notes |
|---|---|---|
| Symbol universe, approved/ignored coins, `tradable`, dated-futures policy | `calc_ideal_orders_orchestrator_from_snapshot` head; `live/planning_gates.py` (511) | |
| Per-symbol `ExchangeParams`, `effective_min_cost` | `update_effective_min_cost` 16417 | needs market specs |
| EMA bundles (`emas`, `forager_m1`) per timeframe/span | `_get_forager_cached_ema_metrics` 20244, `ema_forager_lr_1m` 18859, `candlestick_manager.py` | candle warmup length derives from max EMA span |
| Forager candidate filtering & scoring inputs (volume, noisiness, `volume_drop_pct`, score weights) | `_forager_score_weight` 17750, `_forager_volume_drop_pct` 17765, `candidate_only_forager_symbol` 17859, `fetch_cached_forager_metrics` 18307 | scoring itself is in the engine (`coin_selection.rs`); Python supplies metrics and eligibility |
| Forager mode/psides, hysteresis state from open entry orders | `dynamic_forager_normal_psides` 18082, `dynamic_forager_managed_entry_psides` 18118, `forager_hysteresis` input | |
| Trailing price bundle & availability, last increase fill timestamp | from fill events (`fill_events_manager.py` 8578) | P4 may start with REST `fetch_my_trades` reconstruction |
| Unstuck / realized-pnl cumsum max/last, `max_realized_loss_pct` | fill history aggregation | same source as above |
| HSL (hard stop loss) supervisor | `_run_latched_hsl_supervisor_if_active` 6231, `_hsl_*` 14521-14658, engine `equity_hard_stop_loss.rs` | engine has the state machine; Python feeds fills and equity |
| Balance hysteresis (`balance` vs `balance_raw`) | `live/balance_composition.py` (515) | |
| coin_overrides application to per-symbol `bot_params` / `strategy_params` | `config/overrides.py` (810) | reuse engine config types if exposed; else port |
| Peek hints | `peek_hints` input | live-only order expansion hint |

## 3. (B) Exchange calls actually used (Bybit)

`exchanges/bybit.py` (595) + `exchanges/ccxt_bot.py` (1429):
`load_markets`, `fetch_balance`, `fetch_positions`, `fetch_open_orders`,
`fetch_tickers`, `fetch_ohlcv` (1m), `fetch_my_trades` / fills,
`create_orders` (batch, postOnly limit, reduceOnly), `cancel_orders` (batch),
`set_leverage`, margin-mode/position-mode checks, private WS for
orders/positions/fills (`live/candle_ws.py` for candles). Map each to
`pb-exchange-bybit::ExchangeClient`; extend the trait only when a call is
proven necessary.

## 4. (A) Reused from the engine crate unchanged

`orchestrator.rs` (compute_ideal_orders, forager selection, hysteresis,
twel gating), `entries.rs`, `closes.rs`, `trailing.rs`, `risk.rs`
(unstuck allowance), `equity_hard_stop_loss.rs`, `strategies/`,
`coin_selection.rs` (non-`_py` functions), `utils.rs` rounding helpers,
`types.rs`.

## 5. (C) Not ported (diagnostics / tooling, ~35k lines)

`live/smoke_report.py` 12014, `live/performance_report.py` 6287,
`live/event_emitters.py` 5451, `live/event_bus.py` 4517,
`live/event_query.py` 1930, `live/incident_bundle.py` 1571,
`live/restart_smoke_*.py`, `live/runtime_attribution.py`,
`live/log_secret_inventory.py`, backtest/optimizer/downloader code,
non-Bybit exchanges. The runner logs structured JSON lines instead; anything
pbtb-rust needs from logs is listed in CONTRACT.md.

## 6. Line 7 (v7.12.0) delta

`E:\projects\pb-v712\src\passivbot.py` is 13.5k lines; the orchestrator JSON
API and `OrchestratorInput` exist (`passivbot-rust/src/orchestrator.rs:351`).
Inventory for line 7 is written when P1 for line 8 is done; expect the same
loop shape with fewer inputs (no `forager_m1`, fewer HSL fields). Confirm by
diffing `OrchestratorInput` between the two checkouts.
