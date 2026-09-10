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

Semantics observed in v8.1.0 (`exchanges/bybit.py`, `exchanges/ccxt_bot.py`,
`passivbot.py`), to reproduce in the Rust adapter (P3.2):

- Client config: `apiKey`/`secret` from api-keys.json, `enableRateLimit`,
  `timeout` 30 s, `options.defaultType = "swap"`; hedge mode via
  `set_position_mode(True)` once (Bybit error `110025` / "not modified" is
  ignored). Per symbol: `set_margin_mode(mode, symbol, {leverage})`
  (ignore `110026`) then `set_leverage(leverage, symbol)` (ignore `110043`).
- Balance: `fetch_balance()` -> `info.result.list[0]`; if
  `accountType == "UNIFIED"`: `totalEquity - totalPerpUPL`, else sum over
  coins with `marginCollateral && collateralSwitch` of
  `usdValue - unrealisedPnl`; non-UTA falls back to `total[quote]`.
- Positions: `fetch_positions({limit: 200})` paginated by
  `info.nextPageCursor` (`{cursor, limit}`), keyed by `symbol + side`;
  normalized `{symbol, position_side = side.lower(), size = contracts,
  price = entryPrice}`.
- Open orders: `fetch_open_orders(symbol=None, limit=50)` paginated the same
  way, keyed by id, sorted by timestamp; `qty = amount`; position side from
  `info.positionIdx` (1 long, 2 short, 0 one-way -> derived), else
  `positionSide`, else error.
- Tickers: `fetch_tickers()` filtered to known markets; `{bid, ask,
  last or bid}` with `None -> 0`.
- 1m candles: `fetch_ohlcv(symbol, "1m", since, limit=1000)`, since rounded
  down to the minute, at most 5 forward pages (`since = last ts`), dedupe by
  ts, sorted.
- Create: one `create_order(symbol, type, side, amount=|qty|, price,
  params)` per order, all in parallel (`asyncio.gather`); params
  `{positionIdx: 1|2, timeInForce: "postOnly"|"GTC" (from live.time_in_force),
  orderLinkId: custom_id}`. Cancel: `cancel_order(id, symbol)` in parallel;
  Bybit `110001` / "order not exists|too late to cancel|already filled|..."
  is treated as already gone, not an error.
- Fills / pnl history: `fetch_my_trades` and `fetch_positions_history` (for
  the fill-events manager); needed by P4.1 only for realized-pnl lookback.

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

## 6. Line 7 (v7.12.0) inventory

Read-only survey of tag `v7.12.0` (commit `fc6b9e016`, 2026-05-27; the tag
is an ancestor of `v8.1.0`, 2747 commits apart) done 2026-09-08 with
`git -C E:\projects\passivbot show v7.12.0:<path>`. Line numbers are at the
tag. `pb7` = `src/passivbot.py` (13478 lines vs 22729 at v8.1.0),
`orch7` = `passivbot-rust/src/orchestrator.rs` (4668 lines vs 8420),
`types7` = `passivbot-rust/src/types.rs`, `py7` = `passivbot-rust/src/python.rs`.
Note: the `## v7.12.0` section of `CHANGELOG.md` *at v8.1.0* contains v8
items merged in later; use the changelog at the v7.12.0 tag itself.

### 6.1 What the v7.12.0 engine crate exposes

There **is** a whole-account orchestrator, same shape as line 8:
`orchestrator::compute_ideal_orders(&OrchestratorInput)` (orch7:1722, plus
`compute_ideal_orders_with_workspace` 1729) wrapped by the pyo3 function
`compute_ideal_orders_json(input_json: &str) -> String` (py7:2460-2484,
registered at `lib.rs:62`). The v7 wrapper runs only one pre-compute
validator, `validate_forager_score_weights_pair` (py7:2469); v8.1.0 runs
five (`python.rs:4240-4252`).

`#[pymodule] passivbot_rust` (`lib.rs:22-72`) registers 3 classes
(`HlcvsBundlePy`, `EquityHardStopRollingPeakPy`, `EquityHardStopRuntimePy`)
and 45 functions: rounding/`calc_diff`/`qty_to_cost`/`cost_to_qty`/pnl/
wallet-exposure helpers from `utils.rs`; the per-symbol per-side family
`calc_next_entry_{long,short}_py`, `calc_next_close_{long,short}_py`,
`calc_entries_{long,short}_py`, `calc_closes_{long,short}_py` (py7:1588-2331),
`calc_twel_enforcer_orders_py` (2332), `gate_entries_by_twel_py` (510),
`calc_unstucking_close_py` (641), `calc_min_entry_qty_py` (2148),
`trailing_bundle_default_py` / `update_trailing_bundle_py` (385-419),
`equity_hard_stop_step_py` (442), `run_backtest` / `run_backtest_bundle`
(836/853), `calc_auto_unstuck_allowance`, `hysteresis`, the order-type id
helpers (2428-2458), `select_coin_indices_py` / `select_forager_candidates_py`
(`coin_selection.rs:700/727`). Their argument structs are pyo3 dict
conversions of `BotParams` / `ExchangeParams` / `StateParams` / `Position`
/ `TrailingPriceBundle` (py7:1374-1527).

**The live loop does not use the per-symbol family.** In `pb7` the only
engine calls are `compute_ideal_orders_json` (pb7:10305 replay path,
pb7:11286 live path), `update_trailing_bundle_py` (pb7:269),
`calc_auto_unstuck_allowance` (pb7:8336), `hysteresis` (pb7:9567),
`calc_min_entry_qty_py`/`qty_to_cost` (inside
`_calc_effective_min_cost_at_price` pb7:9795), `equity_hard_stop_step_py`
(HSL supervisor, `src/passivbot_hsl.py`) and the order-type id helpers.
`select_coin_indices_py` is called only from `get_filtered_coins` (pb7:6459,
138 lines) which has no caller left in pb7 (legacy); the per-symbol
`calc_entries_*_py` family is used only by `src/trailing_diagnostics.py`.

Input/output structs, with the field delta to v8.1.0 (v8 line numbers per
SNAPSHOT_SPEC 1.1-1.5):

| Struct (v7 line) | v7 fields | Missing vs v8.1.0 (v8-only fields) |
|---|---|---|
| `OrchestratorInput` (orch7:351-362) | `balance`, `balance_raw` (default NaN), `global`, `symbols`, `peek_hints: Option<EntryPeekHints>`, `forager_hysteresis: Option<ForagerHysteresisState>` | `timestamp_ms` |
| `OrchestratorGlobal` (orch7:270-296) | `filter_by_min_effective_cost`, `market_orders_allowed`, `market_order_near_touch_threshold`, `panic_close_market`, **`unstuck_allowance_long`, `unstuck_allowance_short` (required, no serde default)**, `max_realized_loss_pct`, `realized_pnl_cumsum_max`, `realized_pnl_cumsum_last`, `sort_global`, `global_bot_params`, `hedge_mode` | `market_order_slippage_pct`, `auto_unstuck_allowed`, `strategy_kind` |
| `SymbolInput` (orch7:335-349) | `symbol_idx`, `order_book`, `exchange`, `tradable`, `next_candle`, `effective_min_cost`, `emas: EmaBundle{m1,h1}` (orch7:255-268, identical to v8), `long`, `short` | `allow_missing_strategy_inputs`, `forager_m1` |
| `SymbolSideInput` (orch7:315-323) | `mode: Option<TradingMode>`, `position`, `trailing`, `bot_params` | `trailing_available`, `last_increase_fill_timestamp_ms`, `strategy_params`, `parsed_strategy_params`, `runtime_budget` |
| `BotParams` (types7:379, 48 fields) | flat v7 strategy fields (`close_grid_markup_start/end`, `close_trailing_grid_ratio`, `entry_grid_spacing_we_weight`, `entry_grid_spacing_volatility_weight`, `entry_volatility_ema_span_hours`, `entry_trailing_{retracement,threshold}_{we,volatility}_weight`, `entry_trailing_grid_ratio`), `filter_volatility_ema_span`, `filter_volume_ema_span`, the shared `hsl_*` fields, `risk_wel_enforcer_threshold`, `risk_twel_enforcer_threshold`, `risk_we_excess_allowance_pct`, `unstuck_*` | v8 has 54 fields: no flat strategy fields (they live in `strategy_params`), adds `entry_we_weight`, `entry/close_weight_volatility_{1m,1h}`, `entry_volatility_ema_span_{1m,1h}`, `filter_*_ema_span_1m`, `is_forced_active`, `hsl_restart_after_red_policy`, `risk_entry_cooldown_minutes`, `risk_{wel,twel}_enforcer_enabled`, `risk_twel_entry_gate_enabled`, `risk_twel_enforcer_policy`, `risk_we_excess_allowance_mode`, `unstuck_enabled`, `unstuck_ema_gating_enabled` |
| `ExchangeParams` (types7:136), `Position`, `OrderBook`, `TrailingPriceBundle` (types7:444) | identical field sets to v8 | - |
| `ExecutableOrder` (orch7:104-111) | `symbol_idx`, `pside`, `qty` (signed), `price`, `order_type`, `execution_type` | `execution_priority` |
| `OrchestratorDiagnostics` (orch7:212-224), `OrchestratorOutput` (226) | same 5 / 2 fields as v8 | - |
| `OrderType` (types7:478) | 27 variants | v8 has 31 (adds the 4 `ema_anchor` types) |

All input structs are `#[serde(deny_unknown_fields)]` in both lines, so a v8
snapshot is rejected by the v7 engine and vice versa; the line split (D6) is
also a serialisation split.

Engine divergence between the tags (`git diff --stat v7.12.0 v8.1.0`):
`orchestrator.rs` 5982 changed lines, `entries.rs` 1102, `closes.rs` 1117,
`risk.rs` 699, `equity_hard_stop_loss.rs` 444; `coin_selection.rs`,
`trailing.rs` and `constants.rs` are byte-identical. `strategies/` and
`dynamic.rs` do not exist at v7. v7 has 129 `#[test]`s (30 in orch7).

### 6.2 Python vs Rust split of the live orchestration at v7.12.0

Rust (inside `compute_ideal_orders`): per-symbol entries/closes/trailing,
forager candidate scoring and selection (`build_forager_candidates_into`
orch7:1178, `select_forager_candidates_with_diagnostics` orch7:1879/1963 —
metrics come from `emas.m1.volume` / `emas.m1.log_range` at spans
`filter_volume_ema_span` / `filter_volatility_ema_span`), hysteresis,
one-way arbitration, TWEL entry gate (`gate_entries_by_twel_deterministic`
orch7:1448, used at 2893/2927), TWEL/WEL enforcer
(`calc_twel_enforcer_actions` orch7:2755/2811), unstuck selection and close
(`calc_unstucking_action`, orch7:2697 consumes the two allowances), realized
loss gate, panic close, min-effective-cost gating, market/limit execution
type. Same ownership as line 8 except: **the unstuck allowance amount is
computed in Python** (`_calc_unstuck_allowances` pb7:8316-8349 via
`pbr.calc_auto_unstuck_allowance(balance_raw, pct * twel, cumsum_max,
cumsum_last)` and `_get_realized_pnl_cumsum_stats` pb7:8350) and passed as
`unstuck_allowance_long/short`; v8 moved that into Rust behind
`auto_unstuck_allowed`.

Python (sizes from `def`-to-next-`def` line counts at the tag):

| Concern | v7.12.0 function (pb7 line, lines) | v8.1.0 counterpart |
|---|---|---|
| Loop | `run_execution_loop` 4642 (266): `refresh_authoritative_state` (`live/state_refresh.py`, 516 lines) -> `_equity_hard_stop_check` 4715 / `_equity_hard_stop_run_red_supervisor` 4722 -> `prepare_planning_universe` 4750 -> `refresh_market_state_if_needed` 4759 -> `execute_to_exchange` 4788 | section 1 |
| Symbol universe | `_build_live_symbol_universe` 9977 (13), `get_symbols_approved_or_has_pos` 11815 (17), `refresh_approved_ignored_coins_lists` 12933 (139), `live/planning_gates.py` (425) | same names, `planning_gates.py` 511 |
| Mode handling | `_orchestrator_mode_override` 10068 (37: HSL red -> `panic`, halted mode, orange tier `graceful_stop`/`tp_only_with_active_entry_cancellation`, `_runtime_forced_modes`, `live.forced_mode_<side>`, inactive market -> `tp_only`, `ineligible_symbols`), `_build_orchestrator_mode_overrides` 10106 (10), `_mode_override_to_orchestrator_mode` 9991, `_apply_orchestrator_symbol_states` 10014 (53), `get_forced_PB_mode` 6786 (25) | SNAPSHOT_SPEC 2.3; v8 adds the exchange-unavailable cooldown policy (no `exchange_symbol_unavailable_cooldown_hours` at v7) |
| Snapshot assembly | `calc_ideal_orders` 9847 (3) -> `calc_ideal_orders_orchestrator` 11086 (304); `_bot_params_to_rust_dict` 9851 (~108, 37-entry field list + 9 `hsl_*` keys, renames `forager_volatility_ema_span -> filter_volatility_ema_span`, `forager_volume_ema_span -> filter_volume_ema_span`, `global_keys = {n_positions, total_wallet_exposure_limit, risk_twel_enforcer_threshold, unstuck_loss_allowance_pct}`); `_build_orchestrator_runtime_hints` 5714 (58, identical logic to v8's peek hints / forager hysteresis); `calc_ideal_orders_orchestrator_from_snapshot` 10139 (255, replay variant) | `calc_ideal_orders_orchestrator` 19752; SNAPSHOT_SPEC 1.6 |
| EMA bundle / forager metrics | `_load_orchestrator_ema_bundle` 10395 (690; returns m1 close/volume/log-range and h1 log-range maps), `_refresh_forager_candidate_candles` 12083 (311) + budget helpers 11845-12082, `update_ohlcvs_1m_for_actives` 12421 (90), `candlestick_manager.py` (7391 lines vs 10636) | `_load_orchestrator_ema_bundle` 17500 (~2250); SNAPSHOT_SPEC 3 |
| Unstuck inputs | `_calc_unstuck_allowances` 8316 (33), `_get_realized_pnl_cumsum_stats` 8350 (12), `has_open_unstuck_order` 4545, `_get_effective_pnl_events` 1248, `_assert_no_pending_pnl_events` 1256 | realized-pnl cumsum only (SNAPSHOT_SPEC 5) |
| Balance hysteresis | `_prepare_balance_snapshot` 9498 (81), `_commit_balance_snapshot` 9580, `get_hysteresis_snapped_balance` 6863 | `live/balance_composition.py` |
| HSL | `passivbot_hsl.py` (1627 lines) + 32 `_equity_hard_stop_*` methods in pb7 (~890 lines, 1118-2090): Python state machine with `equity_hard_stop_step_py`, sets `_runtime_forced_modes`, `panic_close_market` derived from `hsl_enabled && hsl_panic_close_order_type == "market"` (pb7:11145-11152) | `passivbot_hsl.py` 8378 lines; engine `equity_hard_stop_loss.rs` |
| Post-processing / reconcile | `live/reconciler.py` (1134 lines vs 4022): `calc_orders_to_cancel_and_create` 663 (98) -> `_snapshot_actual_orders` pb7:11497, `_reconcile_symbol_orders` pb7:11501 (5-key exact match `symbol, side, position_side, qty, price`), `annotate_order_deltas` 808, `apply_order_match_tolerance` 896, `_apply_initial_entry_distance_gate` pb7:11532 (uses `live.initial_entry_exec_max_market_dist_pct`), `_sort_orders_by_market_diff` pb7:11708, `_apply_freshness_creation_guardrails` pb7:7656, `apply_mode_filters` 962, `to_executable_orders` 1003, `finalize_reduce_only_orders` 1071 | RECONCILE_SPEC; v8 adds the churn gate (`live/order_churn_gate.py`, absent at v7), the cancel-first barrier (no `barrier` in v7 `executor.py`), `execution_priority`, `limit_order_create_max_market_dist_pct` |
| Execute | `live/executor.py` (356 lines vs 1478): `execute_to_exchange` 32 (148: low-balance filter, `order_was_recently_updated` 15 s guard pb7:5417, state-change guard, exchange-config guard, `filter_fresh_market_snapshot_creations` `live/market_data.py:47`), `execute_orders_parent` 182 (80, `max_n_creations_per_batch`), `execute_cancellations_parent` 264 (93, `max_n_cancellations_per_batch`, reduce-only first); `execute_multiple` pb7:12806 (46) | same names |
| Fills / PnL | `fill_events_manager.py` (5857 lines vs 8557; 3778-line diff), `init_pnls` 7909 (69), `_update_pnls_locked` 7989 (228); "pnl=pending" enrichment blocks the authoritative refresh with backoff (pb7:4695-4710, v7.11.0/v7.12.0 changelog) | v8 replaced this with proven-coverage semantics (v8.1.0 changelog) |

Rough totals: snapshot construction in pb7 ~1.8k lines (assembly 304 +
EMA bundle 690 + forager candle refresh 311 + modes/hints/params ~330 +
universe ~170), reconcile 1134 + executor 356 + state refresh 516 + market
data 408 + planning gates 425, HSL ~2.5k, plus the two big data managers
(candles 7.4k, fills 5.9k) that feed them. The Rust side of v7 is a strict
subset of v8's inputs; the Python side is *smaller* than v8's but is not a
subset — EMA tail projection, the initial-entry distance gate, the freshness
guardrails and the pending-PnL block have v7-specific semantics that a
line-7 SNAPSHOT_SPEC / RECONCILE_SPEC would have to re-derive from pb7, not
copy from the v8 specs.

### 6.3 Config schema: v7.12.0 vs v8.1.0

`config_version` stamp `"v7.12.0"` (`src/config/schema.py:3` at v7;
`SUPPORTED_PREVIOUS_CONFIG_SCHEMA_VERSIONS` is `{"v8.0.0"}` at v8.1.0,
`schema.py:8`). The v8 loader's `migrate_config_version`
(`config/migrations/legacy_v7.py:31-86`) does not reject a major-7 stamp,
it restamps it, but the flat v7 strategy keys are *removed* fields in v8
and are not converted by normal loading (`docs/release_notes_v8.0.0.md:13-14`);
conversion is the explicit `passivbot tool migrate-config-v7`
(`config/migrations/trailing_grid_v7.py`, ENTRY/CLOSE/ROOT field maps at
lines 27-71). `crates/runner/src/config.rs:23-47` already classifies by
major and refuses the other line.

**Which path the live bots actually took, settled 2026-09-10.** Upstream's
tool, not a local script. `passivbot tool migrate-config-v7`
(`src/passivbot_cli/main.py:190-193` -> `src/tools/migrate_config_v7.py` ->
`src/config/migrations/trailing_grid_v7.py`) was run over the lab configs and
left its own reports beside the outputs in `strategy_lab/configs/v81/`
(`*.v8.json` + `*.migration-report.json`, nine pairs): `source_version
"v7.12.0"`, `destination_strategy_kind "trailing_grid_v7"`,
`canonical_validation {"status": "ok"}`, written with
`--allow-manual-review-output`. The deployed S3 objects' `bot` and
`coin_overrides` are byte-identical to those outputs, so no strategy parameter
was hand-edited anywhere along the way. No hand-written schema migration exists
in pb-runner, pbtb-rust or the passivbot checkout.

The only local code in the pipeline is `strategy_lab/scripts/make_pbtb_template.py`
(~29 lines, and `strategy_lab/` is git-excluded, which is why it appears in no
history): it runs after the migration, adds the control-plane `pbtb` block plus
the legacy flat `strategy_name`/`strategies` markers, overwrites `live.*`
runtime settings, and drops `metrics`/`optimize`. It does not touch `bot`.

Template key diff (`get_template_config()` at both tags, flattened):

- `live`: 47 keys at v7 vs 62 at v8. v7-only:
  `initial_entry_exec_max_market_dist_pct` (retired in v8; a positive value
  migrates to `order_replacement_churn_gate_market_dist_pct`, a null or
  non-positive one to `order_replacement_churn_gate_activation_count = 0`
  -- upstream `docs/configuration.md:520`, changelog l.806. NOT to
  `limit_order_create_max_market_dist_pct`, which this line claimed until
  2026-09-10; the live configs settle it, carrying the v7 value 0.005 in
  `order_replacement_churn_gate_market_dist_pct` and the v8 default 0.8 in
  `limit_order_create_max_market_dist_pct`). v8-only:
  `strategy_kind`, `limit_order_create_max_market_dist_pct`, the four
  `order_replacement_churn_gate_*`, `exchange_symbol_unavailable_cooldown_hours`,
  `enable_forager_ws_candles`, `forager_ws_candle_rest_audit_minutes`,
  `fee_conversion_max_age_ms`, `fee_pct_fallback`, `fee_pct_sanity_abs_max`,
  `force_cold_startup`, `hsl_accept_incomplete_history`,
  `startup_phase_budgets`, `custom_endpoints_path`. Same key, different
  default: `hsl_signal_mode` `unified` -> `coin`,
  `max_ohlcv_fetches_per_minute` 24 -> 30, `minimum_coin_age_days` 365 -> 60,
  `recv_window_ms` 10000 -> 5000. Keys the runner reads today
  (`live_value(...)` in pb7): `filter_by_min_effective_cost`,
  `pnls_max_lookback_days`, `execution_delay_seconds`, `market_orders_allowed`,
  `market_order_near_touch_threshold`, `max_realized_loss_pct`,
  `order_match_tolerance_pct`, `initial_entry_exec_max_market_dist_pct`,
  `forager_score_hysteresis_pct`, `auto_gs`; plus `hedge_mode`,
  `forced_mode_long/short`, `time_in_force`, `max_n_*_per_batch`,
  `approved_coins`/`ignored_coins`, `coin_overrides`.
- `bot.<side>`: 48 **flat** keys at v7 vs 33 grouped keys at v8. v7 has
  the strategy fields flat (`close_grid_*`, `close_trailing_*`, `entry_*`,
  `ema_span_0/1`), `forager_volatility_ema_span`, `forager_volume_ema_span`,
  `forager_volume_drop_pct`, `forager_score_weights{volume,volatility,ema_readiness}`,
  `hsl_*` (9 keys incl. `hsl_tier_ratios{yellow,orange}`), `n_positions`,
  `total_wallet_exposure_limit`, `risk_twel_enforcer_threshold`,
  `risk_wel_enforcer_threshold`, `risk_we_excess_allowance_pct`,
  `unstuck_{close_pct,ema_dist,loss_allowance_pct,threshold}`. v8 groups
  them (`risk.*`, `forager.*`, `hsl.*`, `unstuck.*`,
  `strategy.<kind>.*`) and renames: `risk_twel_enforcer_threshold ->
  risk.total_exposure_enforcer_threshold`, `risk_wel_enforcer_threshold ->
  risk.position_exposure_enforcer_threshold`, `forager_*_ema_span ->
  forager.*_ema_span_1m`, `entry_volatility_ema_span_hours ->
  strategy.trailing_grid_v7.entry.volatility_ema_span_hours`. v8-only with
  no v7 equivalent: `risk.entry_cooldown_minutes`,
  `risk.{position,total}_exposure_enforcer_enabled`,
  `risk.total_exposure_entry_gate_enabled`, `risk.total_exposure_enforcer_policy`,
  `risk.we_excess_allowance_mode`, `unstuck.enabled`,
  `unstuck.ema_gating_enabled`, `hsl.restart_after_red_policy`.
- `wallet_exposure_limit` is still written back into the config per cycle
  at v7 (pb7:6815), same convention as v8 (`bot_params.rs`).

For the runner: `ConfigView` (`crates/runner/src/bot_params.rs`) would need a
line-7 mode with flat-only lookup (no `group_path`), the 37+9-field
`_bot_params_to_rust_dict` list of pb7:9851-9958 with its two renames and
four global keys, no `strategy_params`, and `LiveConfig` gaining the v7
`live` keys above. The `coin_overrides` mechanism (`config_get` with
symbol fallback) is the same shape.

### 6.4 Exchange-side differences (Bybit)

`src/exchanges/bybit.py` differs by 69 lines between the tags; the call set
in section 3 is identical (`fetch_open_orders`/`fetch_positions` paginated
by `nextPageCursor`, UTA balance = `totalEquity - totalPerpUPL` since
v7.12.0 per its changelog, `gather_fill_events` = `fetch_my_trades` +
`fetch_positions_history` bybit.py:291-475, `_build_order_params` =
`{positionIdx: 1|2, timeInForce: postOnly|GTC, orderLinkId: custom_id}`
bybit.py:490-499, margin/leverage with 110026/110043 ignored 501-530).
v7-specific:

- Open-order position side comes from `determine_pos_side_ccxt(order)`
  (bybit.py:33-35) rather than v8's strict `positionIdx` parser
  (v8 bybit.py:34-85); the runner's strict parser is a superset for hedge
  mode accounts.
- `update_exchange_config` calls `set_position_mode(True)` without
  swallowing Bybit `110025`/"not modified" (bybit.py:532-534); v8 does.
  The Rust adapter already ignores it (P3.2), which is harmless.
- Custom id format is the same: `format_custom_id_single` pb7:11779 =
  `"0x<4-hex type id>" + uuid4().hex` cut to `custom_id_max_length = 36`
  (pb7:616), decoded by `_TYPE_MARKER_RE` (pb7:203-215). Order-type ids are
  the 27-variant v7 enum, so the id-to-snake table differs from v8 for ids
  >= 26.
- `ExecutableOrder` has no `execution_priority`, so the v8 reconcile rules
  that key on `risk_critical` do not exist at v7.
- `ccxt_bot.py` (1149 vs 1429 lines): v8 added WS order-update helpers,
  `_apply_exchange_market_options`, `_preserve_position_timing`,
  `_canonical_open_order_reduce_only`; the REST create/cancel path
  (`execute_orders` / `execute_cancellations`, `asyncio.gather`) is the same.
- Fill handling is the real delta: v7 blocks the authoritative refresh while
  close fills have pending PnL enrichment (pb7:4695-4710, `_pending_pnl_*`)
  and synthesises realized PnL from fill history with degraded provenance
  (v7.12.0 changelog); v8 uses proven-coverage lookback instead. Both matter
  only for `realized_pnl_cumsum_*` and the unstuck allowance.

### 6.5 rlib feasibility at v7.12.0

`passivbot-rust/Cargo.toml` at v7 (16 lines) has no `[features]` table,
`crate-type = ["cdylib"]`, `pyo3 = { version = "0.21.2", features =
["extension-module"] }`, `numpy = "0.21.0"`; no `abi3` feature. The v8
plumbing commit (`iengai/passivbot` e808cfd33, 5 files, +72/-38) applies
almost verbatim:

- `Cargo.toml`: add `[features] default = ["python", "extension-module"]`,
  `python = ["dep:pyo3", "dep:numpy"]`, `extension-module = ["python",
  "pyo3/extension-module"]` (no `abi3-py312` at v7 — keep the wheel build
  as it is), make `pyo3`/`numpy` optional, `crate-type = ["cdylib", "rlib"]`.
- `lib.rs` (72 lines): 13 modules to make `pub` (no `dynamic`, no
  `strategies`), gate `mod python`, the 5 `use` lines and the `#[pymodule]`
  (lib.rs:9, 15-19, 21-72). v7 has no `runtime_build_info`.
- `types.rs`: gate `HlcvsBundle` (types7:3-5, 66-95), same as v8.
- `utils.rs`: 20 `#[pyfunction]`s (v7 has no `ema_last_py`): 19 become
  `#[cfg_attr(feature = "python", pyfunction)]`, `calc_order_price_diff`
  (utils.rs:299-300, `PyResult`) is gated.
- `coin_selection.rs` is byte-identical between the tags: the v8 hunk
  applies as is.
- `python.rs`, `analysis.rs`, `backtest.rs`: 0 pyo3 refs outside `python.rs`.

Nothing else needs to change; expected effort is the same half day as
P1.1. `Cargo.lock` is not tracked at either tag; the local
`E:\projects\pb-v712\passivbot-rust\Cargo.lock` resolves `serde_json`
1.0.151, the same as the v8 worktree, so the D8 pin holds for both lines.
In pb-runner the dependency would be a second optional entry
`passivbot_rust_v7 = { git = ..., branch = "pb-runner/rlib-v7.12.0",
package = "passivbot_rust", default-features = false }` behind
`engine-v7` (`crates/runner/Cargo.toml:36`, currently `engine-v7 = []`);
cargo allows two revisions of one package name under different dependency
names, and D6 guarantees only one is compiled into a binary.

Recording infrastructure: the fake exchange exists at v7
(`src/exchanges/fake.py`, 1168 lines, `live.fake_scenario_path` at :196)
and the live call site has the same `json.dumps(input_dict)` ->
`compute_ideal_orders_json` shape (pb7:11286), so the RECORDER.md patch
transfers. Orchestrator tests that could seed a `synthetic_v7` set:
`tests/test_orchestrator_json_api.py`, `test_orchestrator_integration.py`,
`test_unstucking_safeguards.py`, `test_missing_ema_fix.py`,
`test_passivbot_balance_split.py` (15 `compute_ideal_orders` call sites).

### 6.6 Options for the v7 line

See PLAN.md P7 for the recommendation. Facts that bound the choice:

- (a) Full line-7 runner: engine rlib and P3 adapter reusable; snapshot
  builder needs the field delta of 6.1 (fewer inputs, plus the two
  Python-computed unstuck allowances and the HSL-derived `panic_close_market`)
  and a v7 re-derivation of EMA/forager-metric loading, mode overrides,
  the reconcile/execute guards of 6.2 and the pending-PnL semantics of 6.4;
  no churn gate, no barrier, no strategy params. Roughly 60% of the line-8
  P4 effort plus the same P2/P5 wall clock.
- (b) v7 configs on the v8 engine: upstream ships `trailing_grid_v7` +
  `migrate-config-v7`, but states that it "does not promise identical fills
  or performance across the complete v7 and v8 runtimes"
  (`docs/v7_to_v8_migration.md:4-6`); its controlled study found >= 98%
  event match only for single-coin cases with exposure enforcement and
  unstuck neutralised, and "risk-heavy pure-grid and forager cases diverged
  materially" (ibid., "What users should expect"). The migration tool refuses
  to write when manual-review fields remain (exit 1) and inserts v8 defaults
  (entry cooldown, enforcer enables, bounded WE excess allowance) that must
  be reviewed. Under D6 this is a new strategy, not a port.
- (c) Keep v7 bots on the Python image: D12 already routes engine key `7`
  to `passivbot-live:v7.12.0-arm64` per bot; zero pb-runner work; cost is
  the Python RSS (~430 MB vs ~20 MiB measured in P6.1) and a frozen ccxt
  against a moving Bybit API.
