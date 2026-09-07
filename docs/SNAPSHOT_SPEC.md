# Snapshot spec: how passivbot v8.1.0 builds `OrchestratorInput`

Field-by-field trace of the JSON that the Python live bot hands to
`passivbot_rust.compute_ideal_orders_json` once per cycle, so that the P4.2
Rust runner (`crates/runner`) can reproduce it. Companion to
PORT_INVENTORY section 2 and decisions D8/D9.

## 0. Conventions and sources

- Python line numbers refer to the **v8.1.0 tag** (`e808cfd33`, the commit
  HEAD of `E:\projects\passivbot-rlib-v8.1.0`). The working copy there carries
  the uncommitted recorder patch (24 lines inserted at `src/passivbot.py:26`),
  so **worktree line = tag line + 24** for everything in `passivbot.py`.
  `fill_events_manager.py` is also patched (+21 lines after its line 246);
  tag numbers are given. All other files are unpatched.
- `pb` = `src/passivbot.py`; `hsl` = `src/passivbot_hsl.py`;
  `cm` = `src/candlestick_manager.py`; `orch.rs` = `passivbot-rust/src/orchestrator.rs`;
  `types.rs` = `passivbot-rust/src/types.rs`; `python.rs` = `passivbot-rust/src/python.rs`.
- **Live entry point.** `calc_ideal_orders` (pb:16444) calls
  `calc_ideal_orders_orchestrator` (pb:19752-20020). That function is the
  builder documented here. `calc_ideal_orders_orchestrator_from_snapshot`
  (pb:17216-17499) is a replay/tooling variant that takes precomputed EMA
  maps and uses `bid = ask = last`; it is **not** the live path. The nested
  forager helpers named in the task (`_forager_score_weight`,
  `candidate_only_forager_symbol`, `fetch_cached_forager_metrics`,
  `ema_forager_lr_1m`, `dynamic_forager_*_psides`) live inside
  `_load_orchestrator_ema_bundle` (pb:17500-19750), not inside `_from_snapshot`.
- "config-derived" marks fields that are constant for a given config file
  (after the config loader's normalisation) and do not depend on exchange
  state. "state" fields read balance / positions / open orders / candles /
  fills.
- Verified against a real recording:
  `.local/fake_v8/iter7/recordings/1788783999991_e0612f917549a383.in.json`
  (10 symbols, forager long, short `graceful_stop`, one incumbent).
- Serialisation is `json.dumps(input_dict)` (pb:19994): Python floats are
  emitted with `repr` (shortest round-trip), ints as ints (`timestamp_ms`,
  `symbol_idx`, `n_positions`, the peek-hint index lists), `None` as `null`,
  bools as `true/false`. See D8 for why the runner should keep the
  text -> `serde_json::from_str` round trip.

Cycle order that matters for the builder (all in `calc_ideal_orders_orchestrator`):

1. `symbols = sorted(set(self.active_symbols or self._build_live_symbol_universe()))` (pb:19755)
2. `mode_overrides = self._build_orchestrator_mode_overrides(symbols)` (pb:19763)
3. `_apply_exchange_symbol_unavailable_planning_policy` mutates `mode_overrides` and returns cooled-down symbols (pb:19764)
4. `update_effective_min_cost()` only if the cache is empty (pb:19779)
5. `_warmup_new_forager_normal_symbols` (candle warmup side effect) (pb:19781)
6. `_load_orchestrator_ema_bundle(symbols, mode_overrides)` -> EMA maps and the `_orchestrator_*` side-state used below (pb:19791)
7. `market_snapshots = await self._get_orchestrator_market_snapshots(symbols)` (pb:19814) -> `PlanningSnapshot` -> `last_prices` (pb:19824)
8. realized pnl, `auto_unstuck_allowed`, `now_ms`, increase-fill timestamps (pb:19833-19845)
9. `input_dict` assembly (pb:19856-19993), `json.dumps`, call.

Order of steps 6 and 7 is deliberate: EMA loading is slow, so the ticker
snapshot is taken afterwards to keep it fresh.

## 1. Struct tables

### 1.1 `OrchestratorInput` (orch.rs:442-458)

| Field | Python expression (pb line) | Reads | Coercion / notes |
|---|---|---|---|
| `timestamp_ms` | `now_ms = int(self.get_exchange_time())` (19835); `get_exchange_time` = `utc_ms()` (pb:15764) | wall clock | int ms. Fake-live pins this clock. |
| `balance` | `self.get_hysteresis_snapped_balance()` = `float(self.balance or 0.0)` (11000) | balance (snapped) | see section 5 |
| `balance_raw` | `self.get_raw_balance()` = `float(self.balance_raw or 0.0)` (11004) | balance (raw) | see section 5 |
| `global` | dict, section 1.2 | | |
| `symbols` | list, one `SymbolInput` per element of `symbols`, in sorted order (19894-19993) | | `symbol_idx == list position` |
| `peek_hints` | `_build_orchestrator_runtime_hints(self, symbol_to_idx)["peek_hints"]` (19891 -> 8105-8158) | positions | always present live; section 6 |
| `forager_hysteresis` | same helper, `["forager_hysteresis"]` | open orders, positions, config | always present live; section 6 |

Not emitted live: `global.market_order_slippage_pct` (serde default 0.0,
comment at pb:19860), `global.unstuck_allowance_long/short` (default 0.0),
`next_candle` is emitted as `null`, `runtime_budget` and `is_forced_active`
are never written by Python (grep: no occurrences in pb) -> serde defaults
(`None`, `false`).

### 1.2 `OrchestratorGlobal` (orch.rs:324-371)

| Field | Python expression (pb line) | Reads | Coercion / notes |
|---|---|---|---|
| `filter_by_min_effective_cost` | `bool(self.live_value("filter_by_min_effective_cost"))` (19864) | config `live.*` | config-derived |
| `market_orders_allowed` | `bool(self.live_value("market_orders_allowed"))` (19867) | config | config-derived |
| `market_order_near_touch_threshold` | `float(self.live_value("market_order_near_touch_threshold"))` (19868) | config | config-derived |
| `market_order_slippage_pct` | **omitted** | | serde default 0.0 |
| `panic_close_market` | literal `False` (19871) | | constant; the protective-panic path (`calc_protective_panic_ideal_orders_orchestrator`, pb:16500+) is a separate input, not traced here |
| `auto_unstuck_allowed` | `self._auto_unstuck_configured_live()` = `self._pnls_manager is not None and self._unstuck_uses_realized_pnl()` (19834 -> 17196) | config + fill-manager presence | `_unstuck_uses_realized_pnl` (pb:1822): any side with `total_wallet_exposure_limit > 0` and, for the global config or any `coin_overrides` symbol, `unstuck_enabled and unstuck_loss_allowance_pct > 0 and unstuck_close_pct > 0`. Config-derived once the fill manager exists (it always does after startup). |
| `unstuck_allowance_long/short` | **omitted** | | serde default 0.0; only used by Rust when `auto_unstuck_allowed` is absent |
| `max_realized_loss_pct` | `float(Passivbot._live_max_realized_loss_pct(self))` (19845 -> 1813): `1.0 if value is None else float(value)` of `live.max_realized_loss_pct` | config | config-derived |
| `realized_pnl_cumsum_max` | `float(realized_pnl_cumsum.get("max", 0.0) or 0.0)` (19875) | fills | section 5 |
| `realized_pnl_cumsum_last` | `float(realized_pnl_cumsum.get("last", 0.0) or 0.0)` (19878) | fills | section 5 |
| `sort_global` | literal `True` (19881) | | constant |
| `global_bot_params` | `{"long": self._bot_params_to_rust_dict("long", None), "short": ...("short", None)}` (19847-19850) | config | config-derived; section 1.5 with `symbol=None` |
| `hedge_mode` | `self._config_hedge_mode and self.hedge_mode` (19853); `_config_hedge_mode = bool(get_optional_live_value(config, "hedge_mode", True))` (pb:1301), `self.hedge_mode = True` (pb:1304) unless an exchange subclass overrides | config + exchange capability | Bybit subclass does not override (`grep hedge_mode exchanges/bybit.py` is empty) -> equals `live.hedge_mode`. Recording shows `false` because that config sets it. |
| `strategy_kind` | `normalize_strategy_kind(config["live"]["strategy_kind"])` (19855 -> `config/strategy_spec.py:47`) | config | lower-cased, must be one of `pbr.get_strategy_kinds()` = `trailing_martingale` (default), `ema_anchor`, `trailing_grid_v7` |

### 1.3 `SymbolInput` (orch.rs:416-440)

Per `symbol in symbols`, `idx = symbol_to_idx[symbol]` (19894-19993).

| Field | Python expression (pb line) | Reads | Coercion / notes |
|---|---|---|---|
| `symbol_idx` | `int(idx)` | | position in sorted `symbols` |
| `order_book.bid` | `float(snap.bid) if snap is not None and snap.is_valid() else mprice` (19900) | ticker snapshot | `snap = market_snapshots.get(symbol)`; `is_valid` = bid, ask, last all finite and > 0 (`live/market_snapshot.py:23`). `mprice = float(last_prices.get(symbol, 0.0))`; raises if not finite or <= 0 (19897-19899). In practice `get_orchestrator_market_snapshots` (`live/market_data.py:574`) already raises when any symbol is missing/invalid, so the fallback is dead on the live path. |
| `order_book.ask` | `float(snap.ask) ... else mprice` (19901) | ticker snapshot | same |
| `exchange` | `Passivbot._orchestrator_exchange_params(self, symbol)` (hsl:3393) | market specs | see 1.4 |
| `tradable` | `bool(active and symbol not in ema_unavailable_symbols and not exchange_cooldown_blocks_symbol)` (19909-19913) | markets, EMA bundle state, cooldowns | `active = bool(self.markets_dict.get(symbol, {}).get("active", True))` (19903); `ema_unavailable_symbols = self._orchestrator_ema_unavailable_symbols` set by the EMA bundle (19798, section 3.6); `exchange_cooldown_blocks_symbol = symbol in exchange_unavailable_symbols and not self.has_position(symbol=symbol)` (pb:10117-10122) |
| `allow_missing_strategy_inputs` | `symbol in allow_missing_strategy_inputs_symbols` (19972) | EMA bundle state | `self._orchestrator_allow_missing_strategy_inputs_symbols` (19801); section 3.6 |
| `next_candle` | literal `None` (19974) | | always `null` live |
| `effective_min_cost` | `float(self.effective_min_cost.get(symbol, 0.0) or 0.0)`; if `<= 0.0` then `self._calc_effective_min_cost_at_price(symbol, mprice)` (19914-19918) | cached min cost (price at last `update_effective_min_cost`) | section 1.4 |
| `emas` | `{"m1": {"close": m1_close_pairs, "log_range": m1_lr_pairs, "volume": m1_volume_pairs}, "h1": {"close": [], "log_range": h1_lr_pairs, "volume": []}}` (19976-19983) | candles | section 3 |
| `forager_m1` | `{"close": [], "log_range": forager_m1_lr_pairs, "volume": m1_volume_pairs}` (19984-19988) | candles | always present live; section 3.5 |
| `long` / `short` | `side_input("long")`, `side_input("short")` (19920-19943) | | section 1.5 |

### 1.4 `ExchangeParams` (types.rs:203-211) and `effective_min_cost`

`_orchestrator_exchange_params` (hsl:3393-3403):

| Field | Expression | Notes |
|---|---|---|
| `qty_step` | `float(self.qty_steps[symbol])` | from `set_market_specific_settings` (pb:9835+) / ccxt market precision; per-exchange subclass |
| `price_step` | `float(self.price_steps[symbol])` | |
| `min_qty` | `float(self.min_qtys[symbol])` | |
| `min_cost` | `float(self.min_costs[symbol])` | |
| `c_mult` | `float(self.c_mults[symbol])` | |
| `maker_fee`, `taker_fee` | `self._get_exchange_fee_rates(symbol)` (hsl:3372): `market.get("maker_fee")` else `market.get("maker")`, same for taker; raises if missing or non-finite | ccxt `markets[symbol]["maker"/"taker"]` |

How the Bybit subclass fills `qty_steps` etc. from ccxt markets is **not traced**
here (PORT_INVENTORY section 3 covers the exchange side).

`effective_min_cost` cache: `update_effective_min_cost` (pb:16417-16442) runs
in `prepare_planning_universe` (pb:10302) every cycle for
`sorted(self.get_symbols_approved_or_has_pos())` (approved-minus-ignored |
symbols with position | `coin_overrides` symbols whose forced mode is
`normal`; pb:20580), using `_get_live_last_prices(..., max_age_ms=600_000)`.
`_calc_effective_min_cost_at_price(symbol, price)` (pb:16392-16415):

```
qty_step, min_qty, min_cost, c_mult = per-symbol specs
if min_qty <= 0 and qty_step > 0: min_qty = qty_step
min_entry_qty = pbr.calc_min_entry_qty_py(price, c_mult, qty_step, min_qty, min_cost)
return float(pbr.qty_to_cost(min_entry_qty, price, c_mult))
```

Both helpers are engine functions (`entries::calc_min_entry_qty`,
`utils::qty_to_cost`), so the runner can reproduce the value exactly given
the same price. Note the price used is the *cached* last price from the
600 s-TTL fetch, not the planning-snapshot `last`; the two can differ within
a cycle (recording: `effective_min_cost = 5.14` for `min_cost = 5.0`).

### 1.5 `SymbolSideInput` (orch.rs:383-406)

`side_input(pside)` (pb:19920-19943):

| Field | Python expression (pb line) | Reads | Coercion / notes |
|---|---|---|---|
| `mode` | `self._mode_override_to_orchestrator_mode(mode_overrides[pside].get(symbol))` (19921) | config, HSL state, runtime forced modes, markets, ineligibility | `None` -> `null` (forager-eligible default); otherwise one of `normal|panic|graceful_stop|tp_only|manual`; `tp_only_with_active_entry_cancellation` -> `tp_only`; anything else -> `manual` (pb:16931-16939). Section 2.3. |
| `position.size` | `float(pos["size"])`, `pos = self.positions.get(symbol, {}).get(pside, {"size": 0.0, "price": 0.0})` (19924-19932) | positions | signed? `self.positions[...]["size"]` is stored as returned by the exchange adapter; `has_position` tests `!= 0.0` only. Sign convention **not traced** (the fake recordings have no positions). |
| `position.price` | `float(pos["price"])` | positions | |
| `trailing` | `_orchestrator_trailing_input(self, symbol, pside)[0]` (19927 -> pb:353-370) | `self.trailing_prices[symbol][pside]` | 4 floats; default bundle = `pbr.trailing_bundle_default_py()` = `(f64::MAX, 0.0, 0.0, f64::MAX)`, serialised as `1.7976931348623157e+308`. Section 4. |
| `trailing_available` | `pside not in set(self._orchestrator_trailing_unavailable_psides.get(symbol, []))` (pb:353-357) | trailing state | Section 4.3 |
| `last_increase_fill_timestamp_ms` | `last_increase_fill_timestamps.get(symbol, {}).get(pside)` (19935) | fills, positions | `None` unless `risk_entry_cooldown_minutes > 0` for that (symbol, pside); section 4.4 |
| `bot_params` | `self._bot_params_to_rust_dict(pside, symbol)` (19936) | config | section 1.6, config-derived |
| `strategy_params` | `self._strategy_params_to_rust_dict(pside, symbol)` (19937) | config | section 1.7, config-derived |
| `runtime_budget` | **never emitted** | | serde default `None` |

### 1.6 `BotParams` (types.rs:532-628) via `_bot_params_to_rust_dict` (pb:16701-16896)

Key resolution helpers:

- `self.bot_value(pside, key)` (pb:1855-1866): reads the **global** side
  config `config["bot"][pside]` through `get_grouped_bot_value` (grouped
  `risk.*` / `forager.*` / `hsl.*` / `unstuck.*` first, then flat key; map in
  `config/shared_bot.py:8-50`); dotted keys (`hsl_tier_ratios.yellow`) index
  into the group value. Raises `KeyError` if absent.
- `self.bp(pside, key, symbol)` = `config_get(["bot", pside, key], symbol)`
  (pb:4537-4576): if `symbol in self.coin_overrides`, look up the override's
  `bot[pside]` with the same grouped/flat rule; if found return it, else fall
  back to the global config path `bot.<pside>.<key>` (raises if missing).
  With `symbol=None` (global params) only the global config is read.

Resolution order in the function (pb:16787-16836):

```
for key in fields:
    if key in global_keys:           val = self.bot_value(pside, key)          # never per-symbol
    elif key in strategy_keys:       val = strategy_cfg.get(key, 0.0)          # from _strategy_params_to_rust_dict
    else:                            val = self.bp(pside, key, symbol)         # symbol may be None
```

`global_keys` = `{n_positions, total_wallet_exposure_limit, risk_twel_enforcer_enabled, risk_twel_enforcer_policy, risk_twel_enforcer_threshold}` (pb:16704).
`strategy_keys` (pb:16722) = the 18 `close_*`/`entry_*` flat fields plus
`ema_span_0`, `ema_span_1`. Because `_strategy_params_to_rust_dict` returns
the nested v8 shape (`{"ema_span_0", "ema_span_1", "entry": {...}, "close": {...}}`),
only `ema_span_0/1` can hit; but those two are **not in `fields`** (pb:16745-16786),
so they are never emitted and Rust defaults them to 0.0. All 18 `close_*`/`entry_*`
flat fields therefore come out as `0.0` in v8 (recording confirms). They are
legacy v7 mirrors; the engine reads strategy values from `strategy_params`.

| Rust field | Source key / expression | Coercion |
|---|---|---|
| `close_grid_qty_pct` ... `entry_trailing_threshold_pct` (18 fields) | strategy_keys path -> constant `0.0` | `float(val or 0.0)` |
| `filter_volatility_ema_span_1m` | `bp(pside, "forager_volatility_ema_span_1m", symbol)`; renamed on output (pb:16814) | float |
| `filter_volume_ema_span_1m` | `bp(pside, "forager_volume_ema_span_1m", symbol)`; renamed (pb:16816) | float |
| `forager_volume_drop_pct` | `bp(pside, "forager_volume_drop_pct", symbol)` | float; config loader already clamps to [0,1] (`config/bot.py:756-767`) |
| `forager_score_weights` | `bp(pside, "forager_score_weights", symbol)` must be a dict; `{"volume": float(v["volume"]), "ema_readiness": float(...), "volatility": float(...)}` (pb:16818-16827) | The config loader normalises weights to sum 1 (`normalize_bot_forager_config`, `config/bot.py:749+`; recording shows 0.3434/0.1717/0.4848). Rust re-canonicalises anyway. |
| `risk_entry_cooldown_minutes` | `bp(..., symbol)` | float |
| `n_positions` | `bot_value(pside, "n_positions")` | `int(round(val or 0.0))` (pb:16829) |
| `total_wallet_exposure_limit` | `bot_value` | float |
| `wallet_exposure_limit` | `bp(pside, "wallet_exposure_limit", symbol)` | float. The value is **written into the config** each cycle by `set_wallet_exposure_limits` (pb:10906-10917, called from `prepare_planning_universe` pb:10300): `config["bot"][pside]["wallet_exposure_limit"] = get_wallet_exposure_limit(pside)` = `round(twel / n_positions, 8)` with `n_positions = int(round(bot_value("n_positions")))`, `0.0` if `twel <= 0` or `n_positions <= 0`; for override symbols that define `wallet_exposure_limit` the override value is kept verbatim (pb:10919-10927). Recording: global 3.35/1 = 3.35, override symbol 2.5. |
| `risk_wel_enforcer_enabled` | `bp` | `bool(val)` |
| `risk_wel_enforcer_threshold` | `bp` | float |
| `risk_twel_enforcer_enabled` | `bot_value` | bool |
| `risk_twel_enforcer_policy` | `bot_value` -> `normalize_twel_enforcer_policy` (`config/bot.py:109`): must be str, `strip().lower()`, in `{reduce_overweight, reduce_portfolio}` | str |
| `risk_twel_entry_gate_enabled` | `bp` | bool |
| `risk_twel_enforcer_threshold` | `bot_value` | float |
| `risk_we_excess_allowance_pct` | `bp` | float |
| `risk_we_excess_allowance_mode` | `bp` -> `normalize_we_excess_allowance_mode` (`risk_limits.py:16`): `None` -> `"bounded"`, else `str().strip().lower()` in `{bounded, legacy_raw}` | str |
| `unstuck_enabled`, `unstuck_ema_gating_enabled` | `bp` | bool |
| `unstuck_close_pct`, `unstuck_ema_dist`, `unstuck_loss_allowance_pct`, `unstuck_threshold` | `bp` | float |
| `hsl_enabled` | `bool(hsl_cfg["enabled"])` | see below |
| `hsl_red_threshold`, `hsl_ema_span_minutes`, `hsl_cooldown_minutes_after_red`, `hsl_no_restart_drawdown_threshold` | `float(hsl_cfg[...])` | |
| `hsl_restart_after_red_policy` | `normalize_hsl_restart_after_red_policy(hsl_cfg["restart_after_red_policy"])` (`config/coerce.py:53`): `None` -> `"threshold"`, must be in `{always, threshold, never}` | str |
| `hsl_tier_ratio_yellow`, `hsl_tier_ratio_orange` | `float(hsl_cfg["tier_ratios"]["yellow"/"orange"])` | |
| `hsl_orange_tier_mode`, `hsl_panic_close_order_type` | `str(hsl_cfg[...])` | |
| `is_forced_active`, `ema_span_0`, `ema_span_1`, `_legacy_filter_volatility_drop_pct` | not emitted | serde defaults |

`hsl_cfg` (pb:16838-16872): for `symbol is not None` it is
`self._equity_hard_stop_config(pside, symbol)` (hsl:2648-2684), which returns
the parsed global `self.hsl[pside]` (hsl:2521-2590, built once at init from
`bot_value(pside, "hsl_*")` with validation and the clamp
`no_restart_drawdown_threshold = max(no_restart_drawdown_threshold, red_threshold)`)
unless **all** of: `symbol in coin_overrides`, and
`live.hsl_signal_mode == "coin"`; only then per-symbol `bp(...)` values are
used (tier ratios = global dict updated by the override dict). For
`symbol is None` (global params) the same values are re-read through
`bot_value` (pb:16841-16872) **without** the clamp; if a config has
`hsl_no_restart_drawdown_threshold < hsl_red_threshold` the global and
per-symbol copies differ. Not observed in the recordings; flagged, not traced
further.

### 1.7 `strategy_params` (`serde_json::Value`) via `_strategy_params_to_rust_dict` (pb:16672-16690)

```
strategy_kind = normalize_strategy_kind(config["live"]["strategy_kind"])
strategy_cfg  = get_active_strategy_side(config["bot"][pside], strategy_kind)   # config["bot"][pside]["strategy"][kind] or {}
override      = coin_overrides.get(symbol, {}).get("bot", {}).get(pside, {})     # {} when symbol is None
return build_runtime_strategy_side(strategy_cfg, strategy_kind, pside, override_side=override)
```

`build_runtime_strategy_side` (`config/strategy.py:152-203`): start from
`get_strategy_defaults(kind)[pside]` (defaults come from
`pbr.get_strategy_spec(kind)`, i.e. the engine's own registry, D9), then for
every dotted key in `get_strategy_param_keys(kind)` take the override's
value if present, else the side's value if present, else keep the default.
Values are deep-copied verbatim (ints stay ints: recording shows
`"trailing_threshold_volatility_weight": 23`); the only normalisation is
`entry.ema_gate_mode` for `trailing_martingale` (lower-cased, must be in
`{disabled, all, initial, reentry}`). If `override_side` itself contains a
`"strategy"` key it is first unwrapped with `get_active_strategy_side`.
The emitted object is exactly the engine's strategy param struct for that
kind (the engine parses it with `parse_strategy_params`; unknown keys are
rejected there, not in Python).

### 1.8 `EmaBundle` / `EmaTimeframeBundle` (orch.rs:309-322)

`EmaBySpan = Vec<(f64, f64)>`, serialised as `[[span, value], ...]`.
Python builds every list as `[[float(k), float(v)] for k, v in sorted(map[symbol].items())]`
(pb:19944-19963): ascending by span, one entry per span, values are the
**latest** bias-corrected EMA (a scalar), never a series. Empty list when the
map is empty. Section 3 gives the spans and the computation.

| List | Python map | Timeframe |
|---|---|---|
| `emas.m1.close` | `m1_close_emas[symbol]` | 1m |
| `emas.m1.log_range` | `m1_log_range_emas[symbol]` | 1m |
| `emas.m1.volume` | `m1_volume_emas[symbol]` (quote volume) | 1m |
| `emas.h1.close` | `[]` literal | |
| `emas.h1.log_range` | `h1_log_range_emas[symbol]` | 1h |
| `emas.h1.volume` | `[]` literal | |
| `forager_m1.close` | `[]` literal | |
| `forager_m1.log_range` | `forager_m1_log_range_emas.get(symbol, {})` | 1m, strict |
| `forager_m1.volume` | same object as `emas.m1.volume` | 1m, strict |

Engine lookup (`ema_lookup`, orch.rs:921-934): linear scan, match when
`|k - span| <= max(1e-9, |span| * 1e-12)`. Missing span -> `MissingEma`.

### 1.9 `TrailingPriceBundle` (types.rs:699-713)

Fields `min_since_open, max_since_min, max_since_open, min_since_max`, each
`float(trailing.get(name, 0.0))` (pb:361-366). Section 4.

### 1.10 `EntryPeekHints` (orch.rs:11-19) and `ForagerHysteresisState` (orch.rs:21-28)

`_build_orchestrator_runtime_hints` (pb:8105-8158), section 6.

| Field | Expression |
|---|---|
| `expand_grid_long`, `expand_close_long` | `sorted({idx for symbol, idx in symbol_to_idx.items() if pos_size(symbol, "long") != 0.0})` |
| `expand_grid_short`, `expand_close_short` | same with `"short"` |
| `score_hysteresis_pct` | `float(self.live_value("forager_score_hysteresis_pct") or 0.0)` |
| `incumbent_long/short` | indices of symbols with a resting non-reduce-only **entry** order on that side and **no** position on that side |

`pos_size(symbol, pside) = float(self.positions.get(symbol, {}).get(pside, {}).get("size", 0.0) or 0.0)`.
Rust `HashSet<usize>` deserialises from the sorted JSON int lists.

### 1.11 `RuntimeBudgetState` (types.rs:401-407)

Never produced by the live bot (no reference in `passivbot.py`); the engine
computes runtime budgets itself. The runner must **omit** the field.

## 2. Symbol universe and ordering

### 2.1 `symbols`

`symbols = sorted(set(self.active_symbols or self._build_live_symbol_universe()))` (pb:19755-19760).
Plain string sort of ccxt unified symbols (e.g. `"1000PEPE/USDT:USDT"`),
which is also the `symbol_idx` order. Empty universe -> no engine call.

`self.active_symbols` is set in two places:

1. `prepare_planning_universe` (pb:10296-10310), every cycle before planning:
   `self.active_symbols = self._build_live_symbol_universe()`; it also inserts
   zero positions / empty open-order lists for new symbols.
2. `_apply_orchestrator_symbol_states` (pb:17036), after the engine returns:
   `sorted(set(pb_modes["long"]) | set(pb_modes["short"]) | set(self.open_orders))`
   where `pb_modes` covers every `symbol_states` row of the diagnostics plus
   all symbols with positions/open orders. Since the diagnostics contain one
   row per input symbol, this is a superset of (1). Step 1 runs first each
   cycle, so the input universe is (1).

`_build_live_symbol_universe` (pb:16917-16929):

```
symbols  = set(self.positions) | set(self.open_orders) | set(self.coin_overrides)
for pside in (long, short):
    if self._pside_blocks_new_entries(pside): continue      # forced side mode in {panic, graceful_stop, tp_only, tp_only_with_active_entry_cancellation, manual}
    for symbol in self.approved_coins_minus_ignored_coins[pside]:
        if self.is_approved(pside, symbol): symbols.add(symbol)
return sorted(symbols)
```

`is_approved` (pb:9928-9936): in `approved_coins_minus_ignored_coins[pside]`,
not in `ignored_coins[pside]`, and `is_old_enough` (pb:10263: only enforced
when `is_forager_mode(pside)` and `live.minimum_coin_age_days > 0`, using the
first-candle timestamp fetched in `update_first_timestamps`).
`approved_coins_minus_ignored_coins` is rebuilt by
`refresh_approved_ignored_coins_lists` (pb:~22190-22300, reads
`live.approved_coins`/`live.ignored_coins`, external coin lists and market
metadata; **not traced** beyond one rule: a side with
`not is_pside_enabled(pside)` — `total_wallet_exposure_limit <= 0` or
`n_positions <= 0` (pb:10932) — gets an **empty** set, pb:22201-22213). `self.positions` keeps an entry for
every symbol that ever entered the universe (zero sizes are not evicted),
which is why a forager universe stays stable across cycles.

Consequence: in forager mode the input contains **all** approved coins
(candidates), not only the selected ones; the engine does the selection.
Recording: 10 symbols, 1 tradable (the others were marked EMA-unavailable
because the fake replay had no candle cache for them yet).

### 2.2 `tradable`

`active and symbol not in ema_unavailable_symbols and not exchange_cooldown_blocks_symbol`
(pb:19909). `ema_unavailable_symbols` comes from the EMA bundle (section 3.6):
a symbol lands there only when a *required* EMA is missing **and**
`required_ema_can_mark_nontradable(symbol)` holds (flat forager-managed or
candidate-only symbol). Symbols with a position or an explicit normal mode
that lack required EMAs stay `tradable = true` with
`allow_missing_strategy_inputs = true` instead (or the cycle raises).

### 2.3 `mode` per side

`_build_orchestrator_mode_overrides` (pb:17149-17158) calls
`_orchestrator_mode_override(pside, symbol)` (pb:17091-17147) for every
symbol; then `_apply_exchange_symbol_unavailable_planning_policy`
(pb:10089-10115) overwrites entries for cooled-down symbols; then
`_mode_override_to_orchestrator_mode` normalises. Decision order in
`_orchestrator_mode_override`:

1. HSL (`_equity_hard_stop_enabled(pside)`): red latched and not halted -> `panic`; halted -> `_equity_hard_stop_halted_mode` (hsl:2924: when the symbol holds a position, `panic` if the cooldown residue is unresolved, else `panic|manual|tp_only|graceful_stop` per `live.hsl_position_during_cooldown_policy` (default `panic`; `normal` and `graceful_stop` both give `graceful_stop`); flat symbols -> `graceful_stop`); orange tier -> `hsl_orange_tier_mode` (`graceful_stop` or `tp_only_with_active_entry_cancellation`). **Modelled** (2026-09-08, D16): `hsl.rs` owns the per-side state machine (`HslState`, engine `HardStopState` + `RollingPeakTracker`, `_equity_hard_stop_check`, the RED supervisor's flat confirmations and `finalize_red_stop`, cooldown handling, start-up replay from the fill history); `HslState::modes()` -> `CycleState.hsl` -> `SnapshotBuilder::with_hsl`, which applies this step in `mode_override` and, through `side_forced_mode` (= `get_forced_PB_mode(pside)`), in `_pside_blocks_new_entries` for the universe. Note `is_forager_mode` (pb:8243) only reads the *configured* forced mode, so a red/halted side keeps its forager flag and only loses its candidates from the universe; the symbols already in `self.positions` stay (`CycleState.known_symbols`). Verified by the `grid_v7_hsl` set (README in `tests/fixtures/recordings`).
2. HSL coin mode with replay pending for `(pside, symbol)` -> configured forced mode if not normal, else `graceful_stop` (both through `_apply_entry_eligibility_mode`). **Modelled** (2026-09-08, D20): `hsl_coin.rs` owns the per-pair machine of `live.hsl_signal_mode = "coin"` (the default): one engine `HardStopState` per `(pside, symbol)` fed with `hsl_coin_drawdown_signal` (`slot_budget = balance / n_positions`, `drawdown_usd = peak_realized - (last_realized + upnl)` over the pair's fills since `max(now - lookback, pnl_reset_timestamp_ms)`; synthetic equity `max(1 - drawdown_raw, 1e-12)` against a peak of 1), the per-pair latch / halt / cooldown / flat-confirmation bookkeeping (`_equity_hard_stop_check_coin`, hsl:7409), the production coin RED supervisor (`_equity_hard_stop_run_coin_red_supervisor`, hsl:8205, one iteration per cycle) and the start-up reconstruction from the fill history with the panic markers decoded from the fills' `pb_order_type` (`_equity_hard_stop_initialize_coin_from_history`, hsl:5569, over `hsl::coin_history` = `get_balance_equity_history(coin, compact)`). In coin mode the account-level state of step 1 is never fed (step 1 and `get_forced_PB_mode(pside)` are no-ops: the universe and the forager flag are untouched); the pair modes reach the orchestrator through `HslModes.replay_pending` (this step; empty once the synchronous replay finished) and `HslModes.runtime_forced` (step 3). Per-coin `hsl_*` values come from `coin_overrides` (`HslConfig::coin_overrides`, `side_config(pside, symbol)`). Verified by the `grid_v7_hsl_coin` set (README in `tests/fixtures/recordings`).
3. `self._runtime_forced_modes[pside][symbol]` (operator runtime overrides and the coin HSL machine's forced modes: `panic` on a red pair, `tp_only_with_active_entry_cancellation` on a recovered red-seen pair or an orange pair, `graceful_stop` on a halted pair, the cooldown-policy mode on a position held during a cooldown) -> `_apply_entry_eligibility_mode(pside, symbol, mode)`. **Modelled** for the coin machine's entries (D20); operator overrides have no source in the runner.
4. `config_get(["live", f"forced_mode_{pside}"], symbol)` (per-symbol override or global `live.forced_mode_long/short`) -> `expand_PB_mode` (`config/overrides.py:798`: `gs|graceful_stop|graceful-stop`, `m|manual`, `n|normal`, `p|panic`, `t|tp|tp_only|tp-only`; anything else raises) -> `_apply_entry_eligibility_mode`.
5. `not markets_dict[symbol]["active"]` -> `tp_only`.
6. `self.ineligible_symbols.get(symbol)` (exchange eligibility, e.g. wrong quote/margin): `"not active"` -> `tp_only`, else `manual`.
7. else `_apply_entry_eligibility_mode(pside, symbol, None)`.

`_apply_entry_eligibility_mode` (pb:17176-17190): returns `mode` unchanged if
`is_approved(pside, symbol)` or `mode` is one of the stop modes; otherwise
returns `self.PB_mode_stop[pside]` = `"graceful_stop"` if `live.auto_gs`
else `"manual"` (pb:1638-1641). So an unapproved symbol with a position (or
open order, or a `coin_overrides` entry) gets `graceful_stop`/`manual`
instead of `null`. `None` means "let the engine decide" (forager
selection or forced-normal-by-config is handled in Rust via `n_positions`,
`wallet_exposure_limit` and the hysteresis state).

Exchange-unavailable cooldown (pb:10098-10114): for each cooled symbol and
each side, if the normalised mode is `None`/`normal`, or the symbol has a
position and mode is `graceful_stop`, set `tp_only` when
`has_position(symbol)` else `graceful_stop`. `has_position(symbol=...)` is
symbol-level (either side), so a held long's disabled short side goes from
`graceful_stop` to `tp_only` as well. Cooldowns are armed by
`_handle_order_write_failures` (pb:1195) through the connector's
`_classify_exchange_symbol_unavailable_error`; only `exchanges/weex.py`
overrides it at v8.1.0, the Bybit adapter never activates one. Runner:
`cooldown.rs`, `snapshot::cooldown_mode`.

Recording: `long` is `null` everywhere (forager long, no forced modes),
`short` is `graceful_stop` everywhere although `live.forced_mode_short` is
empty. Cause: `bot.short.total_wallet_exposure_limit = 0`, so
`is_pside_enabled("short")` is false and
`refresh_approved_ignored_coins_lists` sets
`approved_coins_minus_ignored_coins["short"] = set()` (pb:22201-22213);
step 7 then runs `_apply_entry_eligibility_mode("short", s, None)` with
`is_approved` false and `auto_gs = true` -> `graceful_stop`. The runner must
replicate this per-side approval check; a disabled side is never `null`.

### 2.4 `allow_missing_strategy_inputs`

Set per symbol by `load_symbol_bundle` (pb:19259-19275) when a
`MissingCloseEma` / `MissingRequiredEma` occurred and
`required_ema_can_mark_nontradable(symbol)` is false (symbol has a position,
or explicit normal mode, or is an active non-candidate). Any other exception
propagates and aborts the cycle. Recording: all `false`.

## 3. EMA inputs (`_load_orchestrator_ema_bundle`, pb:17500-19750)

### 3.1 Spans requested per symbol and side (pb:17590-17715)

For each `pside` and each `symbol`, with `sp = _strategy_params_to_rust_dict(pside, symbol)`:

- **Close spans (1m):** `span0 = sp["ema_span_0"]`, `span1 = sp["ema_span_1"]`
  (via `_positive_finite_warmup_value`: `float(v)`, non-finite raises,
  `<= 0` -> 0.0), `span2 = sqrt(span0 * span1)` if both > 0 (pb:17661).
  Each positive finite value is added to `need_close_spans[symbol]`. Spans are
  **per symbol** (overrides may change `ema_span_*`), and long and short
  spans are unioned into one set per symbol. Values are floats, not rounded.
- **m1 log-range spans:** `requirements = strategy_warmup_requirements(sp)`
  (`strategy_warmup.py:140`): `max_m1_log_range_span_minutes` = max over
  probe paths `volatility_ema_span_1m`, `entry_volatility_ema_span_1m`,
  `offset_volatility_ema_span_1m`. Added to `need_m1_lr_spans[symbol]` only
  if `> 0` and the strategy's max abs 1m volatility weight (entry or close
  paths, pb:17671-17701) is `> 0`. For `trailing_grid_v7` no 1m
  log-range probe path exists -> no required 1m log-range span (recording:
  `emas.m1.log_range == []`).
- **h1 log-range spans:** `_required_h1_log_range_span_for_strategy`
  (pb:4614-4670): `max_h1_span_hours` from probe paths
  `volatility_ema_span_1h`, `entry_volatility_ema_span_1h`,
  `offset_volatility_ema_span_1h`, `entry.volatility_ema_span_hours`; kept
  only if the max abs 1h weight (`entry.grid_spacing_volatility_weight`,
  `entry.trailing_*_volatility_weight`, `entry_weight_volatility_1h`,
  `close.*_volatility_1h_weight`, ...) is `> 0`. Added to
  `need_h1_lr_spans[symbol]`. Span unit = hours = number of 1h candles.
- **Forager spans (global per side, pb:17716-17749):**
  `vol_span_{long,short} = bot_value(pside, "forager_volume_ema_span_1m")`,
  `lr_span_{long,short} = bot_value(pside, "forager_volatility_ema_span_1m")`
  (global config only, never per-symbol).
  `m1_volume_spans = sorted({s for s in (vol_long, vol_short) if s > 0 and finite})`,
  `m1_lr_spans = sorted({lr_long, lr_short} filtered)`. Requested for **every**
  symbol (also when forager is off).
- **Required forager spans (pb:17772-17786):** only when `is_forager_mode(pside)`
  (pb:8219-8231: `twel > 0`, no `live.forced_mode_<pside>`,
  `get_max_n_positions(pside) > 0`, and `n_positions < len(approved_minus_ignored[pside])`):
  volume span required if `forager_volume_drop_pct > 0 or score_weights.volume != 0`;
  log-range span required if `score_weights.volatility != 0`. Missing required
  forager spans mark candidates nontradable (3.6) or raise for active symbols.

### 3.2 Candle range and EMA computation (candlestick_manager)

`_latest_finalized_range(span, period_ms)` (cm:9506-9515):

```
span_candles = max(1, ceil(span))
end_ts   = floor(now_ms / period_ms) * period_ms - period_ms     # last *closed* bucket
start_ts = end_ts - period_ms * (span_candles - 1)
```

`now_ms = cm._now_ms()` (cm:1391): wall clock, or the fake-live callback.
The window therefore has exactly `ceil(span)` closed candles ending at the
most recently closed minute (or hour). Note `ceil`, not `round`: span 1455.0
-> 1455 candles.

Series (cm:9143-9160 `_ema_metric_series`, same in `get_latest_ema_metrics`):

- `close`: `c`
- `qv` (quote volume): `bv * (h + l + c) / 3`
- `log_range`: `ln(max(h, 1e-12) / max(l, 1e-12))`

EMA: `_ema(values, span)` (cm:9051-9080) is pandas-style
`ewm(span, adjust=True)` restricted to finite values, i.e. with
`alpha = 2/(span+1)`:

```
num = v[first_finite]; den = 1
for v in rest (skip non-finite): num = alpha*v + (1-alpha)*num; den = alpha + (1-alpha)*den
return num/den
```

The Rust extension provides `_RUST_EMA_LAST` (`utils::ema_last_f64`) and is
used when available, so the runner can call the same engine function on the
same candle window and get bit-identical results (provided the candle
arrays are identical — see section 8).

Coverage rules before computing: `arr.size > 0`,
`_ema_window_has_required_coverage(arr, start_ts, end_ts)` (cm:9526+):
for non-WEEX exchanges = the array contains a row with `ts == end_ts`
(`_ema_window_has_expected_tail`), plus, for 1m and non-fake exchanges, full
coverage across any "unverified gap ranges"; for 1h additionally
`_candle_range_has_full_coverage` (every bucket present). Otherwise `nan`.
Missing *leading* history is tolerated (sparse-leading contract), missing
tail is not.

Readers used by the bundle (all `max_age_ms=60_000` for 1m, `600_000` for 1h,
`allow_remote_fetch = symbol not in cache_only_symbols`):

| ema_type | reader (pb line) | cm call | strictness |
|---|---|---|---|
| `m1_close` | `ema_close` (18822) | `get_latest_ema_close(symbol, span, max_age, allow_remote_fetch, allow_provisional_internal_gaps=not cache_only)` (cm:9609) | provisional internal gaps allowed for fetched symbols |
| `m1_volume` | `ema_qv` (18836) | `get_latest_ema_quote_volume(..., allow_provisional_internal_gaps=False)` | strict |
| `m1_log_range` | `ema_lr_1m` (18849) | `get_latest_ema_log_range(...)` (cm:10175), provisional default = `allow_remote_fetch` | provisional |
| `forager_m1_log_range` | `ema_forager_lr_1m` (18859) | same with `allow_provisional_internal_gaps=False` | strict, separate cache key `log_range:strict` |
| `h1_log_range` | `ema_lr_1h` (18872) | `get_latest_ema_log_range(..., tf="1h")` | |

In practice all five go through `fetch_batched_ema_values` (pb:18883-18925)
-> `cm.get_latest_ema_metric_spans` (cm:10056-10173), which loads one candle
window for the widest span and slices per span (`_slice_ts_range` to
`[end_ts - period*(ceil(span)-1), end_ts]`), applying the same coverage
checks per span; `strict = ema_type in {m1_volume, forager_m1_log_range}`.
Results and cache keys are identical to the single-span readers.

"Provisional internal gaps" = the candle manager may synthesise zero-volume
flat candles for *confirmed* internal no-trade gaps; strict readers refuse
those. This affects `close` and `log_range` values on illiquid symbols; the
runner needs the same gap policy to be bit-exact (section 8).

### 3.3 Per-symbol load (`load_symbol_bundle`, pb:19099-19340)

1. `cache_only_never_fetched` symbols -> marked unavailable, all maps empty.
2. If an open-tail projection context exists (section 3.7) load close/vol/lr1m
   through `load_projected_open_tail_bundle`; else `close = fetch_close_map(need_close_spans)`.
3. `h1 = fetch_required_map(sorted(need_h1_lr_spans[sym]), ema_lr_1h, "h1_log_range")`.
4. `vol = fetch_map(sym, m1_volume_spans, ema_qv, "m1_volume")` (optional: failures drop the span).
5. `lr1m`: `fetch_required_map(required_m1_lr_for_symbol, ema_lr_1m)` then
   optional `fetch_map` for the remaining `m1_lr_spans`; merged
   `{**optional, **required}`.
6. `forager_lr1m = fetch_map(sym, m1_lr_spans, ema_forager_lr_1m, "forager_m1_log_range")`
   when `is_forager_mode()`; otherwise a copy of `lr1m` restricted to `m1_lr_spans`.
7. Required-span bookkeeping (3.6).

`fetch_close_map` (pb:18663-18820) is the only reader with a **carry-forward
fallback**: if a close span is missing this cycle and no projection context
applies, the previous cycle's value (`self._orchestrator_prev_close_ema[symbol][span]`)
is reused if its age `<= _close_ema_fallback_max_age_ms()`
(`live.max_forager_candle_staleness_minutes` or
`inactive_coin_candle_ttl_minutes`, default 10 min, floor 60 s; pb:20220-20240).
This is cross-cycle state the runner must keep.

Values: `float(...)`; `inf` raises immediately (`RuntimeError`); `nan`
counts as missing. NaN never reaches the JSON.

### 3.4 `m1_volume_emas`, `m1_log_range_emas` vs `forager_m1_log_range_emas`

- `emas.m1.log_range` = strategy volatility EMAs (provisional-gap policy,
  required spans from strategy params plus optional global forager spans).
- `forager_m1.log_range` = the same spans read with the strict policy
  (completed candles only, no synthetic gap rows). The engine ranks forager
  candidates from `forager_m1` (orch.rs:2312-2336) and falls back to `emas.m1`
  only when `forager_m1` is absent (never live).
- `forager_m1.volume` is literally the same list object as `emas.m1.volume`
  (quote-volume EMA is always read strictly).
- When forager is off, `forager_m1.log_range` is a subset copy of `emas.m1.log_range`.

Recording: `emas.m1 = {close: [], log_range: [], volume: []}`,
`forager_m1 = {close: [], log_range: [], volume: []}` for the nine
non-tradable symbols; the tradable one carries `close` for the three spans
and `volume`/`log_range` for the forager spans. (The fake replay's candle
cache was cold for the others.)

### 3.5 What gets written into the JSON

For each symbol the five maps `{span -> value}` become sorted `[span, value]`
pairs (pb:19944-19963). Missing spans are simply absent; the engine decides
whether that is fatal (`MissingEma` is scoped by `allow_missing_strategy_inputs`,
orch.rs:2482).

### 3.6 Unavailability and `tradable` / `allow_missing_strategy_inputs`

Inside `load_symbol_bundle` an exception of type `MissingCloseEma` /
`MissingRequiredEma` is handled as (pb:19256-19300):

- `required_ema_can_mark_nontradable(sym)` (pb:17868-17876) true when the
  symbol is a flat forager-managed symbol (`flat_forager_default_normal_symbol`,
  pb:18144-18161: no position, no explicitly normal side, and some side in
  `dynamic_forager_managed_entry_psides`), or has no normal planning mode and
  is cache-only or a candidate-only forager symbol
  (`candidate_only_forager_symbol`, pb:17859-17866: forager on, no normal
  planning mode, no position and no open order) -> `mark_ema_unavailable`
  with reason `cache_only_fetch_failed` / `candidate_required_ema_unavailable`
  / `flat_active_required_ema_unavailable`; all maps empty; `tradable=false`.
- otherwise -> `_orchestrator_allow_missing_strategy_inputs_symbols.add(sym)`,
  return the partial maps; `tradable` stays true.
- Any other exception propagates (cycle aborts).

Then (pb:19303-19340): missing *required forager* spans -> `RuntimeError`
(cycle aborts) when `required_ema_can_mark_nontradable(sym)` is false, else
mark unavailable (`missing_required_forager_volume` / `..._log_range`).
Note the condition is the same predicate as for missing close/1h spans, not
"has a position or order": a flat deselected symbol with a resting entry
and no retained dynamic eligibility raises. Cache-only symbols missing any
global forager span are marked unavailable too (`missing_volume`,
`missing_log_range`) and return empty maps unless a required-forager reason
already applied. Runner: both rules in `SnapshotBuilder::build`.

Helper predicates the runner needs (all closures over `modes` and bot state):

- `normal_planning_psides(symbol)` (pb:18007-18046): side is "normal" if the
  explicit override normalises to `normal`; else if `PB_modes[pside][symbol]`
  (previous cycle's engine result) normalises to `normal` and
  `get_max_n_positions(pside) > 0`; else if no explicit override, capacity > 0,
  side not blocked, and `symbol in active_symbols`.
- `dynamic_forager_normal_psides` (pb:18082-18116) and
  `dynamic_forager_managed_entry_psides` (pb:18118-18142): forager sides that
  are currently normal, were normal last cycle (`_orchestrator_dynamic_forager_eligibility_psides_by_symbol`),
  or have a resting entry previously authorised by the bot
  (`_orchestrator_ema_entry_cancellation_order_keys`), plus bot-managed
  override modes (`graceful_stop`, `panic`, `tp_only_with_active_entry_cancellation`).

These depend on the previous cycle's `PB_modes` and two private sets, i.e.
cross-cycle state (section 8).

### 3.7 Cache-only symbols and staleness budget (forager only)

When `is_forager_mode()` (pb:18163-18240): symbols with a position, an open
order, or a normal planning mode are *priority*; the rest are *secondary* =
`cache_only_symbols` (no remote candle fetch during planning; `m1/h1
max_age = 365 d`; their candles are refreshed by the background
`update_ohlcvs`/warmup budget instead). Staleness budget
`_forager_target_staleness_ms(n_symbols, live.max_ohlcv_fetches_per_minute)`
(pb:20689-20730): `max(floor, ceil(n/max_calls) minutes)` with floor
`live.max_active_candle_tail_gap_minutes` (default 10 min, min 60 s), capped
by `live.max_forager_candle_staleness_minutes`. A secondary symbol whose
completed-candle staleness exceeds the budget, or that was never fetched, is
`cache_only_never_fetched` -> unavailable.

Open-tail projection (pb:18241-18300, cm:9221 `get_projected_open_tail_ema_metrics`):
when the latest closed minute is missing but the gap is within the budget,
EMAs are computed on the cached candles plus flat zero-volume rows at the
previous close for the missing tail. Strategy log-range is projected only
for non-cache-only symbols with required 1m spans; forager metrics are never
projected. Full projection semantics **not traced**.

Cached forager metrics (`fetch_cached_forager_metrics`, pb:18307-18370 ->
`_get_forager_cached_ema_metric_spans` pb:20287 ->
`cm.get_latest_cached_ema_metric_spans` cm:9430-9505): fallback for
`qv`/`log_range` (and `h1 log_range` for cache-only symbols) when the primary
read fails: uses the cache's last final candle as `end_ts` (not the current
minute), requires `latest_expected - last_cached <= max_staleness_ms`, full
coverage of the window, and strict gap policy. For 1h the budget is checked
against the first missing finalized hourly bucket and extended by 60 min.

## 4. Trailing bundle, `trailing_available`, `last_increase_fill_timestamp_ms`

### 4.1 Source of `self.trailing_prices` (`update_trailing_data`, pb:9407-9835)

Called at pb:7612 (`execute_to_exchange` prepare phase) and pb:10323
(`execution_cycle`, after `prepare_planning_universe` rebuilt
`active_symbols`), i.e. before every planning run.
For every `symbol` in
`set(self.trailing_prices) | set(last_position_changes) | set(self.active_symbols)`
both sides are initialised to the default bundle if absent. A side is
*required* when `has_position(pside, symbol) and is_trailing(symbol, pside)`;
`is_trailing` (pb:8612-8631) on the per-symbol strategy params:
`trailing_grid_v7` -> `entry.trailing_grid_ratio != 0 or close.trailing_grid_ratio != 0`;
`trailing_martingale` / `ema_anchor` -> `entry.retracement_base_pct > 0 or close.retracement_base_pct > 0`.

For each required side:

1. Anchor = `_get_last_position_change_anchors` (pb:9280-9300): the newest
   fill for `(symbol, pside)` from `self._pnls_manager.get_events()`
   (`_latest_fill_position_change_anchors`, pb:8862-8930; same-timestamp
   cohorts resolved by `_terminal_same_timestamp_fill_index`, not traced),
   `{"timestamp": event.timestamp, "epoch": "fill:<id...>", psize, pprice, qty, price, c_mult, side}`;
   if no fill is known, fall back to the exchange position timestamp
   (`_position_anchor_timestamp_ms`, pb:8728, epoch `"position:<ts>"`).
   No anchor -> default bundle and unavailable (`missing_position_change_anchor`).
2. If the anchor epoch changed since last cycle -> reset to default bundle
   (pb:9573-9578). A pending fill-confirmation state machine
   (`_trailing_pending_fill_confirmations`, pb:9496-9570) can also force the
   default bundle and mark the side unavailable (`position_fill_confirmation_pending`)
   after the bot itself placed an order and has not yet seen the matching fill.
3. Candles: `cm.get_candles_with_resolution_ladder(symbol, start_ts=(anchor_ts // 60000 + 1) * 60000, end_ts=None, strict=False)`
   (pb:9585-9600), i.e. from the first full minute **after** the fill.
   Older history may come from coarser timeframes (`approximate old candle prefix`, pb:9637-9660).
4. `_completed_trailing_candle_subset(arr, changed_ts)` (pb:9302-9380): rows
   with `first_eligible <= ts <= latest_finalized` (`latest_finalized = now//60000*60000 - 60000`),
   must be dense (`ts[0] == first_eligible`, consecutive minutes); a missing
   tail up to `live.max_active_candle_tail_gap_minutes` (default 10) is
   projected as flat candles at the previous close; otherwise unavailable
   (`incomplete_trailing_candle_coverage`).
5. `bundle = _trailing_bundle_from_arrays(h, l, c)` (pb:377-389) =
   `pbr.update_trailing_bundle_py(highs, lows, closes, bundle=None)`: starts
   from the default bundle and folds every candle through
   `update_trailing_bundle_with_candle` (`trailing.rs:9-32`):

   ```
   if low < min_since_open: min_since_open = low; max_since_min = close
   else:                    max_since_min = max(max_since_min, high)
   if high > max_since_open: max_since_open = high; min_since_max = close
   else:                     min_since_max = min(min_since_max, low)
   ```

   Non-finite candles are skipped. The bundle is **recomputed from scratch
   every cycle** from the post-fill candles; it is not incrementally updated
   from ticks.

Sides that are not required keep whatever the dict holds (default bundle for
flat sides). `_orchestrator_trailing_input` then emits the four floats.

### 4.2 Positions that exist but have no trailing requirement

Emit the default bundle `(f64::MAX, 0, 0, f64::MAX)` with `trailing_available = true`.

### 4.3 `trailing_available`

`pside not in self._orchestrator_trailing_unavailable_psides.get(symbol, [])`
(pb:353-357), where the dict is written at the end of `update_trailing_data`
(pb:9826-9830) from `unavailable_psides`: reasons
`position_fill_confirmation_pending`, `missing_position_change_anchor`,
`candle_fetch_failed`, `missing_exact_trailing_candles`,
`missing_trailing_candles`, `incomplete_trailing_candle_coverage`,
`bundle_compute_failed`. Only required (position + trailing strategy) sides
can become unavailable; when false the engine ignores the bundle for
trailing branches (orch.rs:388-392). Recording: all `true`.

### 4.4 `last_increase_fill_timestamp_ms` (pb:19836-19844)

```
fill_ts  = _get_last_increase_fill_timestamps(symbols, now_ms)      # pb:13718-13760
delta_ts = _update_entry_cooldown_position_delta_guard(symbols, now_ms)  # pb:13850-13898
out      = _merge_entry_cooldown_anchors(fill_ts, delta_ts)         # per side: max of the non-None candidates, else None
```

- Only `(symbol, pside)` pairs with `bp(pside, "risk_entry_cooldown_minutes", symbol) > 0`
  are considered; otherwise `None` (`null`). Recording: all `null`
  (cooldown 0).
- `fill_ts`: scan `self._pnls_manager.get_events(start_ms = now - lookback*60000)`
  newest first, `lookback = 1 if cd < 1 else ceil(cd) + 1` minutes over the
  max cooldown (pb:1999-2002); take the first event whose
  `_fill_event_increases_position(pside, side, qty)` holds
  (pb:13704-13716: `qty > 0` for long, `qty < 0` for short, falling back to
  `side == "buy"/"sell"` when `qty == 0`); value `int(event.timestamp)`.
  FillEvent `qty` sign convention: positive = buy, negative = sell
  (inferred from this predicate; **not traced** in the fill normaliser).
- `delta_ts`: cross-cycle guard. Keeps `_entry_cooldown_prev_pos_sizes[symbol][pside]`
  (abs size seen last cycle); if the current abs size exceeds it by more than
  `max(1e-12, qty_step/2)`, record `now_ms` in
  `_entry_cooldown_pos_increase_detected_ts` and emit that timestamp from then
  on (never cleared in this function). First observation only seeds the size.

## 5. Balance and account-level fields

### 5.1 `balance` vs `balance_raw` (`_prepare_balance_snapshot`, pb:16094-16173)

Every `update_balance` (pb:16327-16341) applies a snapshot:

- With `live.balance_override` set (`self.balance_override`, pb:1284-1289,
  `float` of the config value, `None` when empty): `balance_raw = balance =
  override`; the exchange value is only kept for diagnostics
  (`_exchange_reported_balance_raw`). `update_balance` does not even call
  `fetch_balance` in that case.
- Otherwise `balance_raw = float(fetch_balance())` (must be finite, else
  `RuntimeError`), and
  `balance = pbr.hysteresis(balance_raw, previous_hysteresis_balance, live.balance_hysteresis_snap_pct)`
  (`utils.rs:176`: returns `val` if `prev == 0` or
  `|val - prev| / |prev| > pct`, else `prev`; default pct 0.02, pb:1293).
  `previous_hysteresis_balance` is initialised to the first raw balance and
  thereafter set to the **snapped** value, so `balance` only moves when the
  raw balance drifts more than 2 % from the last snapped value.
- Exchange hooks `_reconcile_balance_after_*` are no-ops in the base class;
  Bybit-specific composition (`live/balance_composition.py`) affects what
  `fetch_balance` returns, **not traced**.

Initial values before the first fetch: `1e-12` (pb:1290-1291).

### 5.2 Realized pnl cumsum (`_get_realized_pnl_cumsum_stats`, pb:13686-13702)

Returns `{"max": 0.0, "last": 0.0}` when there is no fill manager or when
`not _orchestrator_uses_realized_pnl()` (pb:1843: `max_realized_loss_pct < 1.0 or _unstuck_uses_realized_pnl()`).
Otherwise:

```
events = self._pnls_manager.get_events(start_ms = lookback.event_history_start_ms(now))   # live.pnls_max_lookback_days
assert_pnl_history_safe_for_risk(events)         # raises on pending/synthetic-degraded pnl or insufficient coverage
cumsum = cumsum([ev.pnl + ev.fee_paid for ev in events])       # fill_event_net_pnl, fill_events_manager.py:664
max  = max(0.0, cumsum.max());  last = cumsum[-1]
```

Events are in chronological order (manager-owned). `fee_paid` is signed as
stored by the fill normaliser (fees negative), **not traced**. Rust
validates `max >= last` and finiteness.

### 5.3 Other account fields

- `max_realized_loss_pct`: section 1.2 (config).
- `auto_unstuck_allowed`: section 1.2 (config + manager presence).
- `panic_close_market`: constant `false`.
- `sort_global`: constant `true`.
- `hedge_mode`: `live.hedge_mode` AND exchange capability (Bybit: no override).
- `strategy_kind`: `live.strategy_kind` normalised.

## 6. Forager hysteresis and peek hints (`_build_orchestrator_runtime_hints`, pb:8105-8158)

Always emitted live, both objects; empty lists are legitimate.

- `peek_hints.expand_{grid,close}_{long,short}`: index of every symbol whose
  position size on that side is non-zero. Meaning for the engine: expand the
  full grid/close ladder for symbols with positions; for flat symbols emit
  only the next order (avoids placing whole flat entry grids live).
- `forager_hysteresis.score_hysteresis_pct = float(live.forager_score_hysteresis_pct or 0.0)`.
- `forager_hysteresis.incumbent_{long,short}`: for each `symbol, orders in self.open_orders.items()`
  with `symbol in symbol_to_idx`, for each order: `pside = (order["position_side"] or order["positionSide"]).lower()` in
  `{long, short}`; skip if `_extract_order_reduce_only(order)`; skip unless
  `"entry" in _resolve_pb_order_type(order).lower()` (decoded from the custom
  id / `clientOrderId` family, pb:8160-8180 `_decode_pb_type_from_ids`); skip
  if the symbol already has a position on that side. Remaining -> incumbent.
  Recording: `incumbent_long = [4]`, the symbol with a resting
  `entry_initial_normal_long`.

Both `_extract_order_reduce_only` and `_resolve_pb_order_type` depend on how
the exchange adapter normalises open orders and on the custom-id encoding
(`custom_id_to_snake`), **not traced** here.

## 7. Pre-compute validators (`python.rs:4240-4253`)

`compute_ideal_orders_json` parses with `serde_json::from_str` (all
`deny_unknown_fields`), then runs, in order:

| Validator (python.rs) | Rejects |
|---|---|
| `validate_orchestrator_account_risk_inputs` (2816) | `balance` not finite or `<= 0`; `balance_raw` (or `balance` if raw is non-finite) `<= 0`; `global.max_realized_loss_pct` not finite or `< 0`; `realized_pnl_cumsum_max/last` non-finite; `cumsum_max < cumsum_last`; `unstuck_allowance_long/short` not finite or `< 0` |
| `validate_forager_score_weights_pair` (2564) | any of `global_bot_params.{long,short}.forager_score_weights.{volume,ema_readiness,volatility}` non-finite or negative (`ForagerScoreWeights::canonicalize`, types.rs:496) |
| `validate_hsl_panic_close_order_type_pair` (2573) | global `hsl_panic_close_order_type` not in `{market, limit}` |
| `validate_hsl_restart_after_red_policy_pair` (2588) | global `hsl_restart_after_red_policy` not in `{always, threshold, never}` |
| `validate_hsl_risk_unstuck_orchestrator_input` (2792) -> `validate_hsl_risk_unstuck_bot_params` (2634-2790) for global and every `symbols[i].{long,short}.bot_params` | `hsl_panic_close_order_type` not in `{market, limit}`; `hsl_ema_span_minutes` non-finite or `< 1`; `hsl_red_threshold` not in `(0, 1]`; `hsl_no_restart_drawdown_threshold` not in `[0, 1]` or `< hsl_red_threshold`; `hsl_restart_after_red_policy` invalid; `hsl_cooldown_minutes_after_red < 0`; `hsl_tier_ratio_yellow/orange` not in `(0, 1]` or not `yellow < orange < 1`; `risk_entry_cooldown_minutes`, `total_wallet_exposure_limit`, `wallet_exposure_limit`, `risk_wel_enforcer_threshold`, `risk_twel_enforcer_threshold`, `risk_we_excess_allowance_pct`, `unstuck_loss_allowance_pct`, `unstuck_threshold` non-finite or `< 0`; `unstuck_close_pct` not in `[0, 1]`; `unstuck_ema_dist` non-finite, `<= -1` for long, `>= 1` for short. All are `PyValueError`s with the field path. |

`ExchangeParams::validate_required` (types.rs:227+) is applied inside
`compute_ideal_orders`, not here. Only `symbols[*]` and the global pair are
validated; a per-symbol `forager_score_weights` is canonicalised at use.

## 8. Open questions / what the runner cannot know from REST alone

1. **Cross-cycle private state** that feeds the input and has no REST source.
   The runner carries it in `snapshot::CycleState` (owned by `LiveRunner`,
   replayed by `pb-snapcheck` from the previous recording's output); status
   per item as of 2026-09-08:
   - `PB_modes` from the previous engine output (used by `normal_planning_psides`)
     and `_orchestrator_dynamic_forager_eligibility_psides_by_symbol`
     (section 3.6): **modelled** — `CycleState.pb_modes` is rebuilt after
     every engine call by `SnapshotBuilder::pb_modes_after_cycle`
     (`_python_mode_from_orchestrator_state`: explicit override, else
     `normal` when the side is active, else `PB_mode_stop`);
     `CycleState.dynamic_forager_eligibility` is rewritten by `build`.
     `normal_planning_psides`, `dynamic_forager_normal_psides`,
     `dynamic_forager_managed_entry_psides`, `flat_forager_default_normal`,
     `candidate_only` and `required_ema_can_mark_nontradable` are ported
     verbatim in `snapshot.rs` (unit test
     `pb_modes_carry_over_decides_unavailable_vs_allow_missing`). Not
     modelled: `_orchestrator_ema_entry_cancellation_order_keys` (the
     "previously authorised resting entry" branch, only populated when
     forager rank features go missing for a managed side); treated as empty.
     In every fake run an active side always had a position or an order, so
     the carry-over never changed a fixture (checked over all 600-cycle runs).
   - `_orchestrator_prev_close_ema` carry-forward with 10-minute TTL (3.3):
     **modelled** — `CycleState.prev_close_ema`, TTL from
     `close_ema_fallback_max_age_ms` (unit test
     `close_ema_carry_forward_within_ttl_then_stale`). Applied only when no
     open-tail projection context exists, like `fetch_close_map`. Never
     fires in the fake runs: the harness primes the full 1m array for every
     coin each step (`_prime_fake_candles`) and the replay timeline
     synthesises a flat candle for coins missing a step, so the last closed
     minute is always present (`pb-snapcheck` 600/600 on both full public
     runs with and without the fallback code).
   - Open-tail projection (3.7): **modelled** for the health-based context
     (`emas::open_tail_gap`, `live.max_active_candle_tail_gap_minutes`) with
     `emas::open_tail_rows` / `projected_ema` = `cm.get_projected_open_tail_ema_metrics`
     (window `min(latest_expected - (ceil(max_span)-1) min, last_cached)`,
     provisional internal gaps, flat zero-volume tail rows, per-span EMA over
     the last `ceil(span)` rows). Metric set as in
     `load_projected_open_tail_bundle`: close always; `qv` and `log_range`
     when forager is off; required strategy `log_range` only for non
     cache-only symbols when forager is on. Not modelled: the forager
     stale-tail context for cache-only symbols
     (`forager_projection_max_age_by_symbol`) and the cached forager-metric
     fallback (`fetch_cached_forager_metrics`, 3.7): the runner refreshes
     every symbol's candles each cycle, so a missing tail means the exchange
     lags, and a forager priority symbol whose required forager span is then
     missing makes the cycle fail (`bail!`) where Python would carry the
     metric forward within its staleness budget. Never fires in the fake runs
     (same reason as the carry-forward).
   - `_entry_cooldown_prev_pos_sizes` / `_entry_cooldown_pos_increase_detected_ts` (4.4):
     open.
   - Trailing epochs and the fill-confirmation state machine (4.1);
     `previous_hysteresis_balance` (5.1): `live.rs` keeps the hysteresis
     balance; the fill-confirmation machine is open.
   - Exchange-unavailable cooldowns (`_exchange_symbol_unavailable_until_ms`):
     **modelled** — `cooldown::ExchangeCooldowns` (activation with the
     `live.exchange_symbol_unavailable_cooldown_hours` validation, expiry on
     the bot clock, refresh), fed by `execute::WaveReport.write_failures`
     through `LiveRunner::note_write_failures`; the planning policy is
     `snapshot::cooldown_mode` plus the flat-symbol tradability rule in
     `build` (unit tests `cooldown_mode_table_matches_python`,
     `exchange_cooldown_blocks_flat_symbols_and_reduces_held_ones`).
     `cooldown::classify_symbol_unavailable` returns `None` for every Bybit
     error because v8.1.0 only ships a WEEX classifier (`-1058`); the state
     machine is therefore dormant on Bybit, exactly like the Python bot.
     `ineligible_symbols` (step 6 of 2.3) is still open.
   - Config-driven forced modes (2.3 step 4, `expand_PB_mode`,
     `_apply_entry_eligibility_mode`): **modelled** in `mode_override` and
     verified by the `grid_v7_forced` fixture set (per-symbol
     `coin_overrides.<coin>.live.forced_mode_long` = `gs` / `tp_only` / `m`
     on seeded positions; 400/400 identical on the full run). Operator
     runtime overrides (`_runtime_forced_modes`, step 3) have no source in
     the runner and are not modelled.
   - HSL runtime state (`_equity_hard_stop[pside]`, engine-owned state machine
     fed by Python with equity samples and fills; `_hsl_state`, hsl:2426):
     **modelled** for the account-level signal modes (`unified`, `pside`)
     in `hsl.rs` (D16). Persistence: Python writes latch files
     (`caches/equity_hard_stop/<exchange>/<user>_<pside>.json`) and a
     replay-matrix cache, but never reads a state file for a decision; at
     start-up `_equity_hard_stop_initialize_from_history` replays
     `get_balance_equity_history` (fills + 1m closes over
     `pnls_max_lookback_days`, `latch_red = False`, red-seen episodes
     flattened by an ordinary fill finalized with their cooldown) and then
     samples the present. The runner does the same in
     `LiveRunner::initialize_hsl` from its fill/closed-pnl history and 1m
     buffers (`hsl::balance_equity_timeline`), so a restart lands in the
     same state as a Python restart. Inputs per cycle: raw balance,
     realized pnl from the fill ledger (`closedPnl` on the order's last fill
     + signed fee per `live.fee_pct_fallback` / `fee_pct_sanity_abs_max`,
     `hsl::FeePolicy`), unrealized pnl from positions x planning `last`.
     Details that matter for parity (all found by the trace replay in
     `pb-snapcheck`): the candle manager stores closes as f32 and serves
     only finalized minutes, so the replay row of the current minute
     carries the previous close forward; `reset_after_restart` keeps
     `last_stop_event`; the history replay's stop-event anchor is the latest
     scope fill inside the flatten window. Coin mode (`hsl_coin.rs`, D20):
     **modelled** -- per-pair states in `HslState.coin`, the runtime forced
     modes in `HslState.runtime_forced` (`HslModes` carries both plus the
     replay-pending pairs into `mode_override` steps 2-3), the start-up
     reconstruction in `HslState::initialize_coin_from_history` over
     `hsl::coin_history` (the shared fill replay: minute grid, per-pair
     realized/unrealized series, panic flatten markers from the fills'
     `pb_order_type` with Python's `compute_psize_pprice` fallback quirk,
     `psize_after_quirk`), dense rows for held / ambiguous pairs and the
     change-point rows (`compact_sparse_replay_indices`) for the rest, the
     cooldown contract inferred from the panic fills
     (`infer_coin_replay_contract`). The replay-matrix cache is only an
     accelerator in Python ("never becomes authoritative", hsl:1874) and is
     not ported; the background/partial replay (`mark_protective_ready`) is
     not either -- the runner replays synchronously at warmup. Per cycle
     `LiveRunner` runs `check_coin`; when pairs still need panic
     supervision it runs one production supervisor iteration
     (`supervise_coin_red`: flat confirmations, sample refresh, finalization)
     and then plans the protective-panic input
     (`SnapshotBuilder::build_protective`,
     `calc_protective_panic_ideal_orders_orchestrator` pb:16516: target
     symbols holding a position, `panic` / `manual` per pside, no EMAs, no
     trailing, `auto_unstuck_allowed = false`, zero realized cumsum;
     reconciliation limited to the target pairs without mode filters,
     `PB_modes` untouched) instead of the normal one, as the production loop
     does at `execution_delay_seconds` cadence. Not modelled: operator
     runtime forced modes (the runner's map is written by the coin machine
     only), the account-level modes' protective path (unified / pside keep
     D16 item 3).
   A cold-started runner reproduces the *first* cycle of a cold-started
   Python bot; both then diverge from each other only through the items
   still marked open.
2. **Candle cache identity.** EMAs are only bit-identical if the candle
   arrays are identical: same source (REST `fetch_ohlcv` paging vs the
   candle websocket in `live/candle_ws.py`), same gap synthesis
   (`standardize_gaps`, cm:6259; provisional vs strict policy), same
   resolution ladder for old history, same disk cache. The Python bot mixes
   WS-fed and REST-fed candles; the runner will use REST only. Expect
   last-ulp differences on illiquid symbols and any difference in the tail
   minute. The diffcheck fixtures (fake exchange, REST-only) do not exercise
   this.
3. **Ticker source.** `market_snapshot_provider.get_snapshots` (fetch TTL
   `_live_market_snapshot_fetch_max_age_ms`, hard TTL 10 s) may be fed by
   WS tickers on real exchanges; the exact bid/ask/last observed at planning
   time is inherently non-reproducible. Only structural equivalence
   (`bid`, `ask`, `last` from the same ticker call) can be ported.
4. **Fill history.** `realized_pnl_cumsum_*`, `last_increase_fill_timestamp_ms`
   and trailing anchors depend on `FillEventsManager` (fill_events_manager.py,
   ~6k lines: merges `fetch_my_trades` / income endpoints, dedups, computes
   `psize/pprice` after each fill, synthetic pnl fallbacks, coverage checks).
   The runner needs at least: chronological net-pnl series over
   `pnls_max_lookback_days`, newest fill per (symbol, side) with timestamp,
   and per-fill signed qty. Bybit specifics **not traced** (PORT_INVENTORY 3).
5. **Position sign convention** in `self.positions[...]["size"]` and
   `FillEvent.qty` (4.4) not verified against the Bybit adapter.
6. **`effective_min_cost` price.** Uses the 600 s-TTL last-price cache
   rather than the planning snapshot; the runner should keep a separate cached
   price to match (1.4).
7. **Global vs per-symbol HSL defaults** (`hsl_no_restart_drawdown_threshold`
   clamp only in the parsed per-symbol path, 1.6) — decide whether to
   replicate the asymmetry or the intent; record as a decision.
8. **Not traced:** `refresh_approved_ignored_coins_lists` beyond the
   disabled-side rule (approved/ignored resolution, external lists, market
   filters, pb:~22190-22300), open-tail projection maths
   (`get_projected_open_tail_ema_metrics`), the protective-panic input path
   for the account-level modes (the coin-mode one is ported and verified,
   D20), `_terminal_same_timestamp_fill_index`, the trailing fill-confirmation
   state machine, exchange adapter normalisation of open orders
   (`position_side`, `reduceOnly`, custom-id decoding) and market specs
   (`set_market_specific_settings` per exchange).
