# RECONCILE_SPEC — how passivbot v8.1.0 turns orchestrator output into exchange actions

Scope: the path from `pbr.compute_ideal_orders_json(...)` returning to the point
where ccxt `create_order` / `cancel_order` is called, in passivbot v8.1.0
(Python), so that pb-runner phases P4.3 (reconcile) and P4.4 (execute) can
reproduce it. Bybit is the reference exchange.

Sources are under `E:\projects\passivbot-rlib-v8.1.0\` (tag v8.1.0). Line
numbers for `src/passivbot.py` are as read in this checkout, i.e. **tag line +
24** because of the uncommitted recorder patch at the top of the file. All
other files are unpatched. Abbreviations: `pb.py` = `src/passivbot.py`,
`rec.py` = `src/live/reconciler.py`, `exe.py` = `src/live/executor.py`,
`churn.py` = `src/live/order_churn_gate.py`, `md.py` = `src/live/market_data.py`,
`orch.rs` = `passivbot-rust/src/orchestrator.rs`, `types.rs` =
`passivbot-rust/src/types.rs`.

Config defaults quoted below come from `src/config/schema.py:400-450`
(`hedge_mode=False`, `limit_order_create_max_market_dist_pct=0.8`,
`market_order_near_touch_threshold=0.001`, `market_orders_allowed=False`,
`max_n_cancellations_per_batch=5`, `max_n_creations_per_batch=3`,
`max_n_restarts_per_day=10`, `order_match_tolerance_pct=0.0002`,
`order_replacement_churn_gate_activation_count=10`,
`..._market_dist_pct=0.005`, `..._stability_minutes=2.0`,
`..._window_minutes=10.0`, `time_in_force="good_till_cancelled"`), plus
`execution_delay_seconds=2`, `auto_gs=true` from `configs/examples/btc_long.json`.

---

## 0. Pipeline overview (one execution cycle)

```
run_execution_loop (pb.py:6284)
  refresh_authoritative_state -> barrier check (pb.py:11528) -> prepare_planning_universe
  -> execute_to_exchange (exe.py:555)
       calc_orders_to_cancel_and_create (rec.py:939)
         calc_ideal_orders  == calc_ideal_orders_orchestrator (pb.py:19776)
           build input -> pbr.compute_ideal_orders_json (pb.py:20041)
           parse_and_validate_rust_orchestrator_output (rec.py:2860)
           _apply_orchestrator_symbol_states -> PB_modes/active_symbols (pb.py:16978)
           tuples -> to_executable_orders (rec.py:3862) -> finalize_reduce_only_orders (rec.py:3952)
         validate_rust_ideal_orders (rec.py:971)
         prepare_order_churn_evidence (rec.py:2964)           [supported connectors]
         calc_orders_to_cancel_and_create_from_ideal (rec.py:3049)
           snapshot_actual_orders (rec.py:3258)
           per symbol: filter_orders (pure_funcs.py:86) -> apply_mode_filters (rec.py:3799)
                       -> annotate_order_deltas (rec.py:3592) -> apply_order_match_tolerance (rec.py:3680)
           _sort_orders_by_market_diff (pb.py:20489) for both lists
       execute_order_plan (exe.py:567)
         low-balance filter -> HSL replay filter
         execute_cancellations_parent (exe.py:1255)  -> exchange
         cancel-first barrier (exe.py:691-760)       [supported connectors]
         recent-execution guard -> state-change guard -> exchange-config guard
         filter_fresh_market_snapshot_creations (md.py:152) incl. limit distance filter
         _apply_order_churn_admission (exe.py:261) -> _apply_creation_batch_capacity (exe.py:223)
         execute_orders_parent (exe.py:1017)         -> exchange
  sleep execution_delay_seconds, then wait up to 30 s for execution_scheduled (pb.py:6644-6656)
```

"Supported connector" means `connector_supports_order_churn_gate(bot)`
(`churn.py:28`), true for `binance, bitget, bitunix, bybit, fake, gateio,
hyperliquid, kucoin, okx, weex` (`churn.py:9-21`). Bybit is supported; the
"legacy" branches described below apply only to other exchanges and can be
ignored by the port.

---

## 1. Engine output -> order dicts

### 1.1 Rust output shape

`OrchestratorOutput { orders: Vec<ExecutableOrder>, diagnostics }`
(`orch.rs:280`). Each `ExecutableOrder` (`orch.rs:132-140`):

| field | type | notes |
|---|---|---|
| `symbol_idx` | usize | index into the input `symbols` array |
| `pside` | `"long"`/`"short"` | |
| `qty` | f64, signed | **buy > 0, sell < 0** (`orch.rs:124` doc comment) |
| `price` | f64 | already rounded to the exchange price step by Rust |
| `order_type` | snake string, e.g. `close_grid_long` | `types.rs:733` |
| `execution_type` | `"limit"`/`"market"` | decided in Rust (`orch.rs:872-880`) |
| `execution_priority` | `"ordinary"`/`"risk_critical"` | decided in Rust (`orch.rs:881-887`) |

Recorded example (`tests/fixtures/recordings/fake_v8/grid_v7_seeded/*.out.json`):
`{"symbol_idx":0,"pside":"long","qty":-233.0,"price":0.6614,"order_type":"close_grid_long","execution_type":"limit","execution_priority":"ordinary"}`
and `{"symbol_idx":2,"pside":"long","qty":71.0,"price":0.19325,"order_type":"entry_initial_normal_long",...}`.

`diagnostics` (`orch.rs:266-278`): `warnings`, `loss_gate_blocks`
(`orch.rs:171`: symbol_idx, pside, order_type, qty, ...), `symbol_states`
(`orch.rs:296`: per symbol `{long: {input_mode, effective_mode, active,
allow_initial}, short: {...}}`), `min_effective_cost_blocks`,
`forager_selections`.

Order type ids (`types.rs:733-772`, `id() = self as u16`, `types.rs:775`):

```
 0 entry_initial_normal_long     11 entry_initial_normal_short   22 close_panic_long
 1 entry_initial_partial_long    12 entry_initial_partial_short  23 close_panic_short
 2 entry_trailing_normal_long    13 entry_trailing_normal_short  24 close_auto_reduce_wel_long
 3 entry_trailing_cropped_long   14 entry_trailing_cropped_short 25 close_auto_reduce_wel_short
 4 entry_grid_normal_long        15 entry_grid_normal_short      26 entry_ema_anchor_long
 5 entry_grid_cropped_long       16 entry_grid_cropped_short     27 close_ema_anchor_long
 6 entry_grid_inflated_long      17 entry_grid_inflated_short    28 entry_ema_anchor_short
 7 close_grid_long               18 close_grid_short             29 close_ema_anchor_short
 8 close_trailing_long           19 close_trailing_short       65535 empty
 9 close_unstuck_long            20 close_unstuck_short
10 close_auto_reduce_twel_long   21 close_auto_reduce_twel_short
```

Python round-trips them with `pbr.order_type_snake_to_id` /
`pbr.order_type_id_to_snake` (`pb.py:20149`, `rec.py:1924-1926`). Ids 6 and
17 are "legacy unemittable" and are rejected if seen (`rec.py:1050-1055`,
`rec.py:1928`).

### 1.2 Field derivation (Python)

Step A, `pb.py:20146-20166`: each Rust order becomes the 6-tuple
`(qty, price, order_type, order_type_id, execution_type, execution_priority)`
under `ideal_orders[symbol]`, where `symbol = idx_to_symbol[symbol_idx]`.
Missing `execution_type` or an `execution_priority` not in
`{"ordinary","risk_critical"}` raises `ValueError`.

Step B, `to_executable_orders` (`rec.py:3862-3949`), per symbol, iterating the
tuples sorted by `order_market_diff(side, price, last_price)` ascending
(`rec.py:3881-3883`; the sort only affects list order, see 2.5):

| dict key | derivation | ref |
|---|---|---|
| `symbol` | the mapping key | `rec.py:3932` |
| `side` | `determine_side_from_order_tuple`: `"long"` in type and `"entry"` -> `buy`; long+close -> `sell`; short+entry -> `sell`; short+close -> `buy` | `pure_funcs.py:232-243` |
| `position_side` | `"long" if "long" in order[2] else "short"` | `rec.py:3888` |
| `qty` | `abs(order[0])` | `rec.py:3935` |
| `price` | `order[1]` unchanged (no re-rounding in Python) | `rec.py:3936` |
| `reduce_only` | `"close" in order[2]` | `rec.py:3937` |
| `custom_id` | `bot.format_custom_id_single(order[3])` | `rec.py:3938`, see 1.4 |
| `type` | `execution_type` lower-cased, must be `limit`/`market` | `rec.py:3907-3913` |
| `pb_order_type` | `snake_of(order[3])` == the snake string | `rec.py:3900` |
| `execution_priority` | validated `ordinary`/`risk_critical` | `rec.py:3914-3924` |

Skips and hard failures inside Step B:
- `order[0] == 0.0` -> silently skipped (`rec.py:3889-3892`).
- Two orders with the same conversion identity
  `(symbol, abs(qty), price, order_type)` -> `FatalBotException`
  (`rec.py:3893-3899`, `rust_order_conversion_identity` `rec.py:1816`).
- `_validate_intrinsic_rust_execution_priority` (`rec.py:1091`): families
  `close_panic`, `close_auto_reduce_twel`, `close_auto_reduce_wel`,
  `close_unstuck` (`rec.py:1033-1040`) must be `risk_critical`; `entry_*` must
  be `ordinary`; other closes may be either (graceful_stop closes are
  risk_critical, `orch.rs:881-883`).

Step C, `finalize_reduce_only_orders` (`rec.py:3952-4020`) — called twice
(inside `to_executable_orders` at `rec.py:3947` and again at `pb.py:20194`;
idempotent): for each reduce-only order, `qty = min(qty, abs(position.size))`
with a warning log; then, per pside, if the *sum* of reduce-only qtys exceeds
`abs(pos.size)` (beyond `4*eps*max(|a|,|b|)`), trim the excess starting from
the order that sorts highest under key
`(0 if protective_reducer else 1, order_market_diff)` **reversed**, i.e.
ordinary closes farthest from market are trimmed first, protective reducers
last; orders whose qty reaches 0 are dropped. Quantities are
`round(new_qty, 12)`.

### 1.3 Limit vs market

The decision is made **in Rust**; Python only re-derives it to validate.
`should_use_market_execution` (`orch.rs:~755-782`) and its Python mirror
`_expected_rust_execution_type` (`rec.py:1418-1445`):

1. `close_panic_*`: market iff `global.panic_close_market` (Python always
   sends `False`, `pb.py:19894`) **or** the pside's HSL is enabled with
   `panic_close_order_type == "market"`; otherwise limit
   (`rec.py:1399-1415`).
2. Otherwise, if `not market_orders_allowed` -> limit.
3. `market_price = (bid + ask) / 2`; if not finite or <= 0 -> limit.
4. Crossing: `qty > 0 and price >= market_price` or `qty < 0 and price <=
   market_price` -> market.
5. Near touch: `abs(price/market_price - 1) <= max(market_order_near_touch_threshold, 0)` -> market, else limit.

`market_orders_allowed` / `market_order_near_touch_threshold` are copied
from live config into the Rust `global` input (`pb.py:19891-19893`). With the
default `market_orders_allowed=false` every non-panic order is a limit order.
Python's `_live_market_orders_allowed` (`pb.py:7989`) is only used for a
startup warning.

### 1.4 `custom_id`

`format_custom_id_single` (`pb.py:20568-20571`):

```python
token = type_token(order_type_id, with_marker=True)   # "0x" + f"{id:04x}"
return (token + uuid4().hex)[: self.custom_id_max_length]   # 36 (pb.py:1294)
```

So the id is `"0x"` + 4 lowercase hex digits of the u16 type id + the first
30 hex chars of a random UUID4 = 36 chars. Live example
`0x00048ead933b53284cf7b9836e62b694cf` = type `0x0004` =
`entry_grid_normal_long` + `8ead933b53284cf7b9836e62b694cf`. Uniqueness is
probabilistic (120 random bits); nothing checks collisions. Bybit sends it as
`orderLinkId` (`exchanges/bybit.py:550`); the generic ccxt path sends
`clientOrderId` (`exchanges/ccxt_bot.py:1381`). Exchanges with their own
prefix rules override `format_custom_id_single` (binance, bitget,
hyperliquid, okx, weex) — not Bybit.

Decoding (`pb.py:274-350`): `_TYPE_MARKER_RE = r"0x([0-9a-fA-F]{4})"` searched
anywhere in the string; fallback `_LEADING_HEX4_RE = r"^(?:0x)?([0-9a-fA-F]{4})"`.
`custom_id_has_explicit_passivbot_marker` (`pb.py:330`) requires the `0x`
marker; only ids with the explicit marker are trusted as bot orders when
snapshotting open orders (`rec.py:3395-3401`). `canonical_passivbot_custom_id`
(`rec.py:356`) strips any broker prefix before the marker.

### 1.5 Validation that can reject a whole cycle

`parse_and_validate_rust_orchestrator_output` (`rec.py:2860`) parses JSON
with duplicate-key / NaN / Inf rejection, then `validate_rust_orchestrator_output`
(`rec.py:1834-2836`) raises `FatalBotException` (bot **stops**, no auto
restart, see 3.6) if any invariant fails. The checks that can plausibly fire
against a correct engine are:

- per order (`rec.py:1888-1940`): `symbol_idx` known, `pside` valid, `qty`
  finite and != 0, `price` finite and > 0, `order_type` round-trips through
  the id table, `order_type.endswith("_"+pside)`, and
  `(side == "buy") == (qty > 0)`.
- execution_type must equal the Python mirror in 1.3 (checked further down
  in the same function); execution_priority must equal
  `_expected_rust_execution_priority` (`rec.py:1083`).
- mode/eligibility consistency: no orders for a globally disabled pside, no
  entries while an entry cooldown is active, at most one `entry_*` per pair
  when cooldown is configured, no `entry_*` for symbols the input marked
  ineligible, exactly one protective reducer per pair, panic-mode pairs with
  a position must contain a full-size `close_panic_*`.
- exchange constraints: close qty <= position, price within order-book
  tolerance for panic limits, min qty/cost respected.
- `validate_rust_ideal_orders` (`rec.py:971`) after conversion: every dict
  must satisfy `normalize_ideal_orders` (`churn.py:106`), i.e. finite
  positive qty/price and a complete cohort (symbol, pside, side, bool
  reduce_only, limit/market type, known pb_order_type).

A port that consumes its own engine output directly needs none of these; they
are guards against a broken FFI boundary. Keep the qty-sign/side check and
the reduce-only trim.

### 1.6 `PB_modes` and `active_symbols` from diagnostics

`_apply_orchestrator_symbol_states` (`pb.py:16978-17060`): for every
`symbol_states` row and each pside,
`PB_modes[pside][symbol] = explicit_override or ("normal" if active else PB_mode_stop[pside])`
(`pb.py:16965-16976`), where `PB_mode_stop = "graceful_stop" if auto_gs else "manual"`
(`pb.py:1662-1665`) and `explicit_override` comes from
`_build_orchestrator_mode_overrides` (`pb.py:17173`; HSL panic/halt tiers,
runtime forced modes, forced_mode_* config). Symbols with a position or open
orders but no diagnostics row get the override or `PB_mode_stop`. Then
`active_symbols = sorted(PB_modes.long ∪ PB_modes.short ∪ open_orders.keys())`
(`pb.py:17055-17057`). `PB_modes` drive `apply_mode_filters` (2.4); they must
persist across cycles because the next cycle's `symbols` list starts from
`active_symbols` (`pb.py:19779-19784`).

---

## 2. `calc_orders_to_cancel_and_create`

### 2.1 Open-order snapshot (`snapshot_actual_orders`, `rec.py:3258-3470`)

Input: `bot.open_orders: {symbol: [ccxt order dict + position_side + qty]}`
as normalised by the exchange class (Bybit: `position_side` from
`info.positionIdx` 1/2, `qty = amount`, `exchanges/bybit.py:49-66,115-121`).
Symbols considered: `active_symbols ∪ ideal_orders.keys() ∪ open_orders.keys()
∪ positions.keys()` (`rec.py:3063-3068`); every open-orders bucket is
validated even if not requested.

Each open order is normalised to
`{symbol, side, position_side, qty, price, reduce_only, type, pb_order_type, id, custom_id}`:

- `qty` = authoritative remaining qty via `extract_order_remaining_qty`
  (`rec.py:392`: `remaining`, else `amount - filled`, consistency-checked),
  must be > 0 (`rec.py:3339-3350`).
- `reduce_only` = `_canonical_open_order_reduce_only` (Bybit: hedge mode ->
  `(long and sell) or (short and buy)`, one-way -> exchange `reduceOnly`
  flag, `exchanges/bybit.py:68-85`).
- `type` = `"limit"` if the ccxt `type` is `limit`, else `"unknown"`
  (`rec.py:3385-3390`).
- `pb_order_type` = `custom_id_to_snake(custom_id)` only if the id has the
  explicit `0x` marker, else `"unknown"` (`rec.py:3391-3401`). A `close_*`
  type with `reduce_only=False` (or `entry_*` with `True`) is malformed.
- Missing exchange id, non-finite numbers, invalid side/position_side ->
  the order is **malformed**.

Any malformed order marks its symbol in `_malformed_actual_order_symbols`
and calls `mark_account_critical_state_dirty` (`rec.py:891`), which requests
a full authoritative refresh, sets `execution_scheduled=True` and adds the
symbol to `state_change_detected_by_symbol`. On supported connectors one
malformed symbol blocks **all** cancels and creates for the cycle
(`rec.py:3168-3187`).

### 2.2 Exact matching (`filter_orders`, `pure_funcs.py:86-108`)

Per symbol, `filter_orders(actual, ideal, keys)`. Keys on supported
connectors (`rec.py:3085-3097`):

```
("symbol","side","position_side","reduce_only","type","pb_order_type","qty","price")
```

(legacy connectors: `("symbol","side","position_side","qty","price")`).
An ideal order is "already resting" iff some actual order has **exactly
equal** values for all keys (dict equality: floats compared with `==`, no
rounding; `type` must be `"limit"` on both sides; `pb_order_type` must be
the same snake name). Each actual order can satisfy at most one ideal
(first match wins, in ideal-list order). Result: `to_cancel = actual minus
matched`, `to_create = ideal minus matched`.

Consequences of the keys:
- A market ideal order (`type="market"`) never matches an open order, so it
  is always created.
- Open orders without a passivbot marker (`pb_order_type="unknown"`) never
  match, so **manual orders on a managed symbol are cancelled** unless the
  pside is in `manual` mode (2.4). This is standard passivbot behaviour.
- An ideal `entry_grid_normal_long` and a resting `entry_initial_normal_long`
  at the same price/qty are *different* orders -> cancel + create.
- Same price/qty but different `pb_order_type` between two consecutive Rust
  outputs (e.g. `close_grid` -> `close_trailing`) causes a replace.

### 2.3 Tolerance matching (`apply_order_match_tolerance`, `rec.py:3680-3797`)

Runs after mode filters and delta annotation, on the residual
`(to_cancel, to_create)` of each symbol. `tolerance =
live.order_match_tolerance_pct` (a **fraction**, default 0.0002 = 0.02 %;
`<= 0` disables). Supported-connector branch:

1. `current = normalize_ideal_orders(to_create)` (fails open: if any create is
   malformed, return unchanged).
2. `previous` = those `to_cancel` entries that normalise (needs a known
   `pb_order_type`; manual/unknown orders can never be tolerance-matched).
3. `deterministic_one_to_one_matches(current, previous, tol)`
   (`churn.py:134-190`): candidate pair iff same **cohort**
   `(symbol, position_side, side, reduce_only, execution_type, pb_order_type)`
   (`churn.py:39-46`, `churn.py:77`) and
   `abs(prev.price - cur.price)/cur.price <= tol` and
   `abs(prev.qty - cur.qty)/cur.qty <= tol` (relative to the **new** value,
   `churn.py:128-131`). Candidates are sorted by `(price_diff + qty_diff,
   cur.stable_key, prev.stable_key)` and a maximum-cardinality bipartite
   matching (augmenting paths) is computed, so as many resting orders as
   possible are kept.
4. Matched pairs are removed from both lists (`skipped += 1`, DEBUG log
   `skipped_recreate | ...`).

Legacy branch (`rec.py:3705-3745`) uses `orders_matching` (`pb.py:525`) with
the same tolerance for price and qty relative to the *create* order, first
match wins.

Net effect: a resting order is kept if its price and qty are each within
0.02 % of the new ideal and the cohort is identical.

### 2.4 Mode filters (`apply_mode_filters`, `rec.py:3799-3859`)

Applied per symbol right after `filter_orders`, using `bot.PB_modes[pside][symbol]`:

| mode | cancels for that pside | creates for that pside |
|---|---|---|
| `manual` | dropped (exception: an entry whose exchange/client id is in `_orchestrator_ema_entry_cancellation_order_keys`, the forager EMA-unavailable case) | dropped |
| `tp_only` | keep only `reduce_only` | keep only `reduce_only` |
| `tp_only_with_active_entry_cancellation` | unchanged | keep only `reduce_only` |
| `normal`, `graceful_stop`, `panic`, anything else | unchanged | unchanged |

`graceful_stop` and `panic` are enforced by Rust (no entries / full close),
not here. Nothing else exempts an open order from cancellation.

### 2.5 Ordering, delta annotation, summary

- `annotate_order_deltas` (`rec.py:3592-3677`) pairs each cancel with the
  closest-priced create of the same symbol/side/position_side and stores
  `_delta`, `_context="replace"`, `_reason="price"/"qty"/"price+qty"`; unpaired
  creates get `_context="new"`, `_reason="fresh"`. Logging only.
- After all symbols: `to_cancel` and `to_create` are each sorted by
  `order_market_diff(side, price, last_price)` ascending
  (`pb.py:20489-20517`), where `order_market_diff` = Rust
  `calc_order_price_diff` (`utils.rs:398`): buy -> `1 - price/market`, sell
  -> `price/market - 1`. Closest-to-market (and crossing, negative) first.
  Prices come from `_get_live_last_prices(max_age_ms=10_000)`; if any symbol
  has no price the original order is preserved (`pb.py:20502-20510`).
- Summary log `[order] order plan summary | cancel a->b | create c->d | skipped=n`
  (`rec.py:3230-3253`), INFO only when "interesting".

### 2.6 Batch limits

- Cancels: `execute_cancellations_parent` (`exe.py:1255-1290`) keeps at most
  `max_n_cancellations_per_batch` (default 5); when over capacity the list is
  re-ordered `reduce_only first, then the rest`, each group in market-diff
  order, then truncated.
- Creates: `_apply_creation_batch_capacity` (`exe.py:223-259`) — stable sort
  `risk_critical` first, then truncate to `max_n_creations_per_batch`
  (default 3); `execute_orders_parent` truncates again (`exe.py:1021`).
  Deferred orders are simply re-planned next cycle.

### 2.7 Cancel-first barrier (`exe.py:574-577`, `exe.py:691-760`)

Active when `to_cancel` is non-empty, not debug mode, and the connector is
supported. Procedure:

1. Cancels are sent first (`exe.py:673-690`). In the `finally`, a full
   authoritative confirmation is requested for
   `{"balance","positions","open_orders","fills"}` (`pb.py:11116`,
   `pb.py:11482`), i.e. `_authoritative_pending_confirmations[surface] =
   ledger.epoch + 1`. This happens even if the cancel call raised
   (`tests/test_order_churn_cancel_first.py:222`).
2. Scope of each cancel: `_cancel_first_scope` (`exe.py:151-167`) =
   `(symbol, position_side)` when `_config_hedge_mode and hedge_mode`, else
   `(symbol, "")`; `None` if symbol missing (an unscoped cancel defers
   everything).
3. A create is deferred iff `has_unscoped_cancel or its scope is None or its
   scope ∈ cancel_scopes`, **unless** it is a "dedicated protective market
   panic" (`exe.py:120-128`: `configure_creations=False` (protective panic
   supervisor path) and panic and reduce_only and market). Deferred creates
   are dropped from this wave (`to_create = bypass_creates`, `exe.py:760`) and
   re-planned next cycle.
4. Next cycle: `_authoritative_execution_barrier_state` (`pb.py:11528-11553`)
   blocks execution until every required surface is fresh at an epoch >= the
   pending one; while blocked the loop sleeps
   `_authoritative_confirmation_retry_delay_seconds` (`pb.py:11580`: 1 s if
   only `open_orders` is required, else `execution_delay_seconds`).

Tests: same (symbol, pside) deferred (`test_...:109`), other pside / other
symbol allowed in hedge mode (`:136`), opposite pside deferred in one-way
mode (`:168`), unscoped cancel defers all (`:198`), only reduce-only market
panics bypass (`:235`, `:279`), legacy connector places creates in the same
wave (`:298`).

### 2.8 Recently-executed / recently-cancelled guards

State: `bot.recent_order_executions`, `bot.recent_order_cancellations`
(lists of order dicts + `execution_timestamp`, `pb.py:1386-1387`).

- Every cancel that is *submitted* is appended to
  `recent_order_cancellations` before the exchange call
  (`exe.py:1292`, `rec.py:156`). Every *acknowledged* create is appended to
  `recent_order_executions` (`exe.py:1220`, `rec.py:199`).
- `order_was_recently_updated(order, 15_000 ms)` (`rec.py:261`) uses
  `order_has_match` with the **default** tolerances `qty 1 %, price 0.2 %`
  (`pb.py:545`) against recent executions. In `execute_order_plan`
  (`exe.py:775-800`) a create matching a create sent < 15 s ago is deferred
  (`[order] recent execution found; delaying ...`). Deferred creates consume
  no churn allowance (`test_...:314`).
- `order_was_recently_cancelled(order, 15_000)` (`rec.py:163`, exact match)
  and `order_matches_bot_cancellation(order, 180_000)` (`rec.py:181`) are
  **not** consulted when planning cancels; they are used by the open-orders
  refresh (`pb.py:16245-16260`) to classify a disappeared order as
  "removed by bot" vs "missing" (unexpected -> schedules a position refresh)
  and by the WS self-echo detector (`pb.py:12215`). Same for
  `order_matches_recent_execution` on added orders (`pb.py:16272`).
- `local_order_open_orders_confirmed` (`rec.py:222`) — true when no recent
  cancel is still visible and every recent create is visible in
  `open_orders`; used to decide whether an `open_orders`-only refresh is
  sufficient (`pb.py:11600`).

There is **no** guard that blocks cancelling the same order twice within a
window; the planner may re-cancel an order that is still visible after a
failed/unconfirmed cancel, subject to the barrier in 2.7.

### 2.9 Order churn gate

Two halves, both only on supported connectors and only when
`order_replacement_churn_gate_activation_count > 0` (default 10).

**Evidence** (`prepare_order_churn_evidence`, `rec.py:2964-3047`, called
right after the ideal orders are produced, before reconciliation):
`OrderChurnGateState` (`churn.py:294`) keeps per-symbol deques of
`IdealSnapshot(monotonic_seconds, observations)` bounded by
`window_minutes*60`. `evaluate_and_record` (`churn.py:392-560`) compares the
current ideals with history per cohort and assigns each ideal order a
`ChurnDecision(churn_evidenced, reason)`:

- track = current order + the same-index order of each preceding snapshot
  of the same cohort with the same cardinality, as long as consecutive
  snapshots are <= `max(10, 3*(execution_delay_seconds+30))` s apart
  (`rec.py:48-52`).
- `stable_tight_prefix` (False): >= 2 history points within `tolerance` of
  the current price and qty, spanning >= `stability_minutes*60`.
- `intermittent_cohort_reappearance` (**True**): the symbol's snapshots each
  contain exactly one cohort and the current cohort has appeared in >= 3
  distinct runs, the last two >= `stability_seconds` apart
  (`churn.py:236-292`).
- `continuous_price_drift` / `continuous_qty_drift` / `..._price_qty_drift`
  (**True**): a monotonic run of >= 2 changes whose total relative change
  exceeds `tolerance`, lasting >= `stability_seconds`
  (`_continuous_drift_start_index`, `churn.py:207-233`).
- everything else (`no_history`, `history_short`, `drift_run_short`,
  `no_continuous_drift`, `intermittent_run_short`) -> False.
- Orders whose `(symbol, pside)` is in `_order_churn_risk_active_pairs`
  (Rust orders with `risk_critical` priority plus `diagnostics.loss_gate_blocks`,
  `rec.py:2881-2928`) are forced to `False, "rust_risk_phase_active"`.

Results are written on the order dicts as `_churn_evidence` /
`_churn_reason`. History is cleared on planning failure and for symbols
that left the universe. Tolerance is the same `order_match_tolerance_pct`.

**Admission** (`_apply_order_churn_admission`, `exe.py:261-447`, applied to
the create list just before capacity):

```
rolling = number of create attempts recorded in the last window_minutes
for each create (in list order):
    if market order or risk_critical or not _churn_evidence: admit ("ready", exempt)
    elif _churn_gate_market_distance missing/non-finite: defer ("market_distance_unavailable")
    elif distance <= order_replacement_churn_gate_market_dist_pct: admit (exempt)
    elif rolling + 1 > activation_count: defer ("allowance_exhausted")
    else: admit; rolling += 1        # only churn-evidenced far orders consume
```

`_churn_gate_market_distance` is the signed `order_market_diff` set by the
limit-distance filter (`md.py:294`). Attempts are recorded by
`_record_order_churn_allowance_attempts` for **every** submitted create
(`exe.py:1063-1065`, `pb.py:1798-1830`), so exempt orders also count toward
the rolling usage. Cancels never consume allowance and are never gated. The
gate therefore only delays *far* (> 0.5 % from market), *unstable* limit
creates once more than 10 creates were sent in 10 minutes; it never
prevents a cancel and never keeps a stale order alive
(`tests/test_order_reconciliation_contract.py:1171`).

---

## 3. Execution

### 3.1 Per-cycle sequence in `execute_order_plan` (`exe.py:567-1014`)

1. `_begin_order_wave` (`pb.py:4297`) timing record (None if both lists empty).
2. Low balance: if `raw_balance < balance_threshold` (1.0 quote,
   `pb.py:1381`), keep only reduce-only creates (`exe.py:606-650`).
3. `_filter_hsl_replay_pending_creates` (`exe.py:170`): drop
   exposure-increasing creates for pairs awaiting HSL replay.
4. **Cancels**: `execute_cancellations_parent(to_cancel)` (3.2). Then the
   cancel-first barrier (2.7) may drop creates.
5. Recent-execution guard (2.8).
6. `state_change_detected_by_symbol` (`exe.py:808-850`): creates for symbols
   flagged dirty during this cycle (by `mark_account_critical_state_dirty`,
   by a cancel that was not acknowledged, or by a cancel response length
   mismatch) are skipped, except dedicated market panics. The set is reset
   at the start of every loop iteration (`pb.py:6322`).
7. Exchange config (`exe.py:861-950`): if `configure_creations`,
   `update_exchange_configs(symbols)` (leverage / margin mode / hedge mode
   per symbol, Bybit `exchanges/bybit.py:553-590`); creates for symbols whose
   config is still pending are skipped (protective creates bypass through
   `_order_requires_exchange_config_before_create`, which returns True in the
   base class, `pb.py:10242`).
8. `_filter_fresh_market_snapshot_creations` (`md.py:152-250`): refresh
   ticker snapshots for the create symbols with
   `max_age_ms=_live_market_snapshot_max_age_ms()`; if the refresh fails or
   any snapshot is stale **all creates are skipped** this cycle. Then
   `_filter_limit_order_creations_by_market_distance` (`md.py:271-320`):
   limit orders with `order_market_diff > limit_order_create_max_market_dist_pct`
   (default 0.8, i.e. buys below 0.2x market or sells above 1.8x) are
   skipped; market orders and symbols without a valid snapshot pass.
9. Churn admission (2.9), then batch capacity (2.6).
10. **Creates**: `execute_orders_parent(to_create_mod)` (3.3). A non-restart
    exception here is swallowed after `restart_bot_on_too_many_errors()`.
11. `execution_scheduled = True` if anything was planned (`exe.py:1002`),
    which shortens the 30 s scheduled wait to zero; wave summary logged.

Cancels and creates are never interleaved: all cancels of the wave complete
(gather) before any create is sent.

### 3.2 Cancellation path

`execute_cancellations_parent` (`exe.py:1255-1476`): capacity (2.6);
`add_to_recent_order_cancellations` for each; DEBUG `log_order_action(...,
"cancelling order")`; INFO per-symbol summary `[order] cancel COIN | sell
long 233@0.6614 close_grid_long [replace price ...]`
(`_log_order_action_summary`, `pb.py:8030-8093`, repeats suppressed); then
`bot.execute_cancellations(orders)`.

`execute_cancellations` (ccxt: `exchanges/ccxt_bot.py:1414-1428`) runs
`execute_cancellation` for all orders concurrently with
`asyncio.gather(return_exceptions=True)`, passes exceptions to
`_handle_order_write_failures`, returns the raw result list (results or
exceptions, positionally aligned). Base-class `execute_multiple`
(`pb.py:22009-22037`) is equivalent (tasks created in order, awaited in order).

`execute_cancellation` (`pb.py:22373-22408`):
`cca.cancel_order(order["id"], symbol=order["symbol"])`. On exception, if the
lowercase message contains any of
`"100004","110001","order not exists","order does not exist","order not found",
"too late to cancel","already filled","already cancelled","already canceled",
"-2011","51400","order_not_found"` the cancel is treated as **already gone**:
INFO `[order] cancel skipped: ...` and return
`_ambiguous_cancel_success_result(order)` (`pb.py:11472`) =
`{"status":"success","_passivbot_cancel_requires_full_authoritative_confirmation":True, id, symbol, side, position_side, qty, price, reduce_only}`.
Any other exception propagates into the gather result.

Result handling (`exe.py:1345-1476`):
- `len(res) != len(orders)` -> every symbol marked dirty, `execution_scheduled`,
  return `[]`.
- `did_cancel_order(ex)` (`pb.py:8233`): `ex` is a dict with a non-None `id`
  (a 1-element list is unwrapped). Exceptions fail this -> symbol added to
  `state_change_detected_by_symbol`, warning `[order] cancel not acknowledged`.
- Acknowledged: missing/None keys are back-filled from the request; the
  "already gone" marker adds the symbol to `state_change_detected_by_symbol`
  and upgrades the confirmation request to all four surfaces; otherwise only
  `open_orders` is requested. `remove_order` is a no-op (`pb.py:12158`) —
  the local `open_orders` are only changed by the next REST refresh.

### 3.3 Creation path

`execute_orders_parent` (`exe.py:1017-1253`): slice to
`max_n_creations_per_batch`; `_record_emitted_order_custom_id(status="submitted")`
(foreign-order detection bookkeeping); DEBUG `posting order`; INFO
`[order] MARKET order submission | ...` for market orders (`pb.py:8009`);
INFO per-symbol summary `[order] post COIN | buy long 71@0.19325
entry_initial_normal_long [new]`; churn attempts recorded; then
`bot.execute_orders(orders)` (ccxt: gather, `exchanges/ccxt_bot.py:1392-1412`).

`execute_order` (`pb.py:22351-22367`):
```python
cca.create_order(symbol=order["symbol"], type=order.get("type","limit"), side=order["side"],
                 amount=abs(order["qty"]), price=order["price"], params=self._build_order_params(order))
```
Bybit params (`exchanges/bybit.py:542-551`):
`{"positionIdx": 1 if long else 2, "timeInForce": "postOnly" if time_in_force=="post_only" else "GTC", "orderLinkId": custom_id}`.
Note `price` is passed even for market orders (ccxt/Bybit ignore it for
market type). Generic ccxt params (`ccxt_bot.py:1363-1390`):
`positionSide=LONG/SHORT`, `clientOrderId`, and for limit orders
`postOnly=True` or `timeInForce="GTC"`.

Result handling (`exe.py:1082-1252`): empty response or length mismatch ->
all orders remembered as ambiguous, return `[]`. Per order,
`did_create_order(ex)` (`pb.py:8211-8231`): dict with non-empty `id` and
status not in `{rejected, canceled, cancelled, expired, failed}`. Failures are
logged `[order] create not acknowledged | ... reason=...`; exceptions in the
result list are "ambiguous" (order may exist). Acknowledged responses are
back-filled from the request, appended to `recent_order_executions`, and an
`open_orders` confirmation is requested. `add_new_order` is a no-op
(`pb.py:12154`).

### 3.4 Feedback into the next cycle

- Local `open_orders` / `positions` / balance change **only** through the
  authoritative REST refresh at the top of the next loop iteration
  (`refresh_authoritative_state`, `pb.py:6329`), gated by the pending
  confirmation epochs (2.7). WS order updates only mark state dirty /
  schedule execution (`handle_order_update`, `pb.py:12240`).
- `execution_scheduled=True` skips the 30 s idle wait
  (`EXECUTION_SCHEDULED_WAIT_SECONDS`, `pb.py:193`, loop `pb.py:6650-6656`);
  `execution_delay_seconds` (default 2) is always slept first.
- Fills are learned via the fills surface / fill-events manager, not from
  create responses.

### 3.5 Error classification, retry, restart

- `_handle_order_write_failures` (`pb.py:1195-1220`): for each failed write
  `_activate_exchange_symbol_unavailable_cooldown(symbol, error)`
  (`pb.py:9989`; symbol-unavailable style errors put the symbol on a
  `exchange_symbol_unavailable_cooldown_hours` cooldown) and
  `[order] write failed | action=... error_type=... status=... code=...`;
  then `restart_bot_on_too_many_errors()`.
- `restart_bot_on_too_many_errors` (`pb.py:20544-20566`): appends `now` to
  `error_counts`, prunes to the last hour, logs `[health] error_budget
  count=%d limit=10`, and when `count >= 10` calls `restart_bot()` which
  raises `RestartBotException` (`pb.py:20622-20633`). Note: it is invoked
  **once per failed batch**, not per failed order.
- There is **no retry** of a failed create or cancel within a cycle; the
  next planning cycle simply recomputes the diff. `tests/test_ccxt_retry_policy.py`
  concerns `CandlestickManager.fetch_ohlcv` retries only (Bybit 7 attempts,
  others 5, page failures raise) and is unrelated to order writes.
- Loop-level exceptions (`pb.py:6657-6760`): `RestartBotException` /
  `FatalBotException` propagate; `RateLimitExceeded` -> error budget + 5 s
  sleep; `FillHistoryCoverageUnavailable` -> request fills refresh + 1 s;
  any other `Exception` -> `_handle_execution_loop_failure` (error log,
  optional time-sync recovery, error budget, 1 s sleep, continue).
- Exchange-config writes use their own backoff
  `min(base * 2**(attempt-1), 60) + jitter`, base 5 s on Bybit (`pb.py:10256`).
- `main()` (`pb.py:22675-22736`): `FatalBotException` -> log and **exit the
  process loop** (no restart); any other exception (incl.
  `RestartBotException`) -> cleanup, 60 s countdown, restart; restarts in
  the last 24 h are counted and the loop exits when
  `len(restarts) > max_n_restarts_per_day` (default 10).

What is fatal (stops without restart): malformed Rust output / ideal orders
(1.5), malformed JSON from Rust, contradictory reducer/priority invariants,
foreign passivbot orders detected on the account (`rec.py:784`).

### 3.6 State that must persist across cycles (in-process only)

| state | owner | used by |
|---|---|---|
| `open_orders`, `positions`, balance | REST refresh | snapshot, reduce-only trim |
| `PB_modes`, `active_symbols` | `_apply_orchestrator_symbol_states` | mode filters, next symbol universe |
| `recent_order_cancellations` (15 s / 180 s pruning) | cancel path | open-order diff classification |
| `recent_order_executions` | create path | create dedup guard (15 s), diff classification |
| `_authoritative_pending_confirmations` (+ freshness ledger epoch) | cancel-first barrier, dirty marking | execution barrier |
| `state_change_detected_by_symbol` | reset each iteration | create skip |
| `_order_churn_gate_state` (snapshot history, attempt timestamps) | churn gate | evidence + admission |
| `error_counts` (1 h) | write failures | restart at 10/h |
| `_forager_new_normal_warmup_symbols`, HSL state, cooldowns | planning | out of scope here |

Nothing in this pipeline is persisted to disk; a restart starts with empty
guards and rebuilds from REST.

---

## 4. Minimal faithful subset for the Rust port

Goal: in steady state (no exchange errors, `market_orders_allowed=false`,
hedge mode as configured), place and cancel the same orders as the Python
bot given the same engine output and the same open-orders snapshot.

### 4.1 Must have (P4.3 reconcile)

1. **Tuple -> order conversion** exactly as 1.2: side from order type,
   `qty = |qty|`, `reduce_only = "close" in type`, `type = execution_type`,
   `pb_order_type` snake, `custom_id = "0x%04x" + 30 random hex`
   (36 chars). Drop zero-qty orders. Reduce-only trimming (1.2 step C)
   including the aggregate cap — it changes quantities whenever the position
   shrank between engine input and execution.
2. **Open-order normalisation** (2.1): remaining qty, `reduce_only` from
   `positionIdx`+side, `pb_order_type` from `orderLinkId` only with the
   `0x` marker, `type` = limit/unknown. Malformed entry -> block the symbol
   (blocking the whole account is the Python behaviour on Bybit; a port may
   start with per-symbol blocking but must not act on a bucket it could not
   parse).
3. **Exact matching** with the 8-key tuple (2.2), first-match-wins
   semantics, float equality.
4. **Mode filters** (2.4) driven by `PB_modes` derived from
   `diagnostics.symbol_states` (1.6) — without them a `manual` pside would
   have its operator orders cancelled and `tp_only` would create entries.
5. **Tolerance matching** (2.3) with the cohort key and relative-to-new
   tolerance, maximum-cardinality matching. Without it the port would churn
   on every sub-tick engine wobble; with a different matching rule it would
   diverge from Python on which of two near-identical orders survives.
6. **Sorting** both lists by `order_market_diff` ascending (2.5) and the
   **batch limits** with their priority rules (2.6). These decide *which*
   orders go out when more than 5 cancels / 3 creates are planned, which is
   the common case right after a fill or a config change.
7. **Cancel-first barrier** (2.7) with (symbol, pside) scoping in hedge mode
   and (symbol) scoping otherwise, plus the rule that the next cycle may not
   execute until open orders, positions, balance and fills were refreshed.
   Without it the port would place a replacement before the exchange
   confirms the cancel, which Python never does on Bybit.

### 4.2 Must have (P4.4 execute)

8. Cancels before creates, cancels gathered concurrently, creates gathered
   concurrently, no interleaving.
9. `create_order` params: `positionIdx`, `timeInForce` GTC/postOnly,
   `orderLinkId`; `amount = |qty|`; `type` limit/market.
10. `cancel_order(id, symbol)` with the **already-gone** classification
    (3.2) treated as success + full confirmation request; other errors mark
    the symbol dirty for the cycle.
11. Acknowledgement predicates `did_create_order` / `did_cancel_order` and
    the length-mismatch rule (drop the whole batch result).
12. `recent_order_executions` guard (15 s, 1 %/0.2 % tolerances) on creates —
    cheap and prevents duplicate placement while `open_orders` lags.
13. Error budget: 10 write failures per hour -> restart; `max_n_restarts_per_day`
    in the supervisor; fatal vs restartable distinction.
14. Loop timing: `execution_delay_seconds` sleep, 30 s idle wait cut short
    by `execution_scheduled`.

### 4.3 Safety rails that can follow later

- **Order churn gate** (2.9). It only *defers* far, unstable, non-critical
  limit creates after >10 creates in 10 min. In steady state with a grid
  config it is inactive (evidence is `no_history`/`stable_tight_prefix`).
  Add it once the port runs for hours against a live market; its state
  machine is ~500 lines and has its own tests (`tests/test_order_churn_gate.py`).
- **Pre-create market snapshot freshness** and the
  `limit_order_create_max_market_dist_pct` filter (3.1 step 8). Needed
  before real money (protects against stale tickers producing absurd limit
  prices) but not for steady-state parity, since a fresh engine input
  already used the same ticker.
- **Low-balance filter**, **HSL replay filter**, **exchange-config
  gate** (steps 2, 3, 7): the port sets leverage/margin once at startup
  (P5) and does not implement HSL yet.
- **Foreign passivbot detection** and emitted-custom-id ledger
  (`rec.py:513-890`): operational safety, no effect on order content.
- **Rust output validation** (1.5) beyond sign/side consistency: the port
  owns the engine; keep only cheap sanity checks plus fatal-on-invariant.
- **Live event pipeline / structured console** (`_emit_*`): logging only.
  Reproduce the INFO summaries (`[order] post COIN | ...`, `[order] cancel
  COIN | ...`, plan summary) for diff-ability with recorded Python logs.

Reasoning: items 1-7 determine the *content* of the cancel/create lists and
items 8-14 determine *what reaches the exchange and when*; every other
component either only removes orders under abnormal conditions or only
affects logs. Removing an order under abnormal conditions is conservative, so
omitting those rails cannot produce an order Python would not have placed —
except for the churn gate and distance filter, which can delay orders
Python would also delay; that delay is at most one cycle in practice.

---

## 5. Open questions / not traced

1. `_orchestrator_ema_entry_cancellation_order_keys` (the manual-mode
   exception in 2.4) — where it is populated in the forager EMA-unavailable
   path was not traced; the port can treat it as empty until forager mode is
   ported.
2. `refresh_authoritative_state` / `FreshnessLedger` epoch mechanics
   (`pb.py:11116-11600`) were only read as far as the barrier predicate;
   the exact conditions under which `open_orders` is considered fresh after a
   partial refresh (`_finalize_authoritative_refresh_consistency`,
   `pb.py:11590`) are not documented here.
3. `_filter_fresh_market_snapshot_creations` max-age value
   (`_live_market_snapshot_max_age_ms`, `md.py:656`) and the ticker strategy
   per exchange were not read.
4. Bybit's `did_create_order`/`did_cancel_order` are the base-class
   versions (no override found in `exchanges/bybit.py` or `ccxt_bot.py`);
   whether ccxt's Bybit `cancel_order` returns an `id` on every success path
   (v5 API returns `orderId`) was not verified against ccxt source.
5. `update_exchange_configs` eligibility/backoff bookkeeping
   (`pb.py:10242-10340`) — which symbols are considered "configured" across
   cycles — is not traced; the port does this once at startup.
6. `_detect_foreign_passivbot_orders` (`rec.py:816`) can stop the bot when
   unknown `0x....` ids appear; the precise matching against the emitted-id
   ledger and its TTL were not traced.
7. The protective panic supervisor path
   (`calc_protective_panic_orders_to_cancel_and_create`, `pb.py:20406`,
   `configure_creations=False`) is only mentioned where it changes barrier
   behaviour; its planner inputs are out of scope.
8. Whether `order_match_tolerance_pct` is ever interpreted as a percentage
   anywhere else: in the code read it is a fraction (0.0002) and logs
   multiply by 100.
9. Rust `should_use_market_execution` lines were read around
   `orch.rs:755-782`; the exact start line of the function was not
   captured.
