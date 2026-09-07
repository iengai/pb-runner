# MOCK_EXCHANGE — the runner's fake exchange and the closed-loop parity check (P5.1)

`crates/runner/src/mock_exchange.rs` is an `ExchangeClient` that reproduces
passivbot's fake exchange, `src/exchanges/fake.py` (`FakeCCXTClient`) at
v8.1.0, so the runner's real `--live` path (`LiveRunner` + `Executor`) can be
driven through the same scenario a Python bot ran under
`src/tools/run_fake_live.py`. `pb-mockrun` (`crates/runner/src/bin/mockrun.rs`)
does the driving and the comparison. Line numbers below are `fake.py` at the
`E:\projects\passivbot-rlib-v8.1.0` checkout (unpatched file).

```bash
cargo run -p pb-runner --bin pb-mockrun -- --run .local/fake_v8_public/grid_v7 [--diff-inputs] [--gate-clock wall|cycle] [--no-harness-compat]
```

## 1. What is mirrored, line by line

| Behaviour | `fake.py` | Mock |
|---|---|---|
| Scenario: `tick_interval_seconds`, `start_time`, `boot_index`, `account.{balance,positions,fills,open_orders}`, `symbols`, `timeline` or `replay` | 89-175 | `Scenario::from_value` |
| Timestamps: numbers < 1e11 are seconds, strings are integers or ISO-8601 | 17-42 | `parse_time_to_ms` |
| Markets: `qty_step` (default 0.001), `price_step` (0.1), `min_qty` (= qty_step), `min_cost` (5.0), `contractSize` (1.0), `maker_fee` (0.0002), `taker_fee` (0.00055); `symbols = sorted(markets)` | 205-236, 135 | `SymbolMeta`, `BTreeMap` |
| Scripted timeline: prices carry forward, candle `open = previous close`, `high/low = max/min(open, close)`, `volume = row.volume`, `timestamp = start + t * tick` | 246-298 | `build_timeline` |
| Replay: per-symbol rows from inline `candles`, `file`, `files`, `glob` (`.npy` only in the mock), deduplicated by timestamp (last wins), filtered by `start_time`/`end_time`; one step per distinct timestamp; a symbol without a row at a step gets a flat candle at its last close | 300-427 | `build_replay_timeline`, `load_replay_rows` |
| Boot positions: `size = abs(qty)`, `entry_price = price` | 468-478 | `MockExchange::new` |
| Boot fills: trade id and `order_id` bump the counters (`max(next, id + 1)`), `pnl`/fee join the realized totals but not the balance, `liquidity = "historical"` | 511-587 | `MockExchange::new` |
| Boot orders: id bumps the counter, `amount = abs`, `type` defaults to limit | 480-509 | `MockExchange::new` |
| Boot: resting orders are processed against the boot candle once | 175 | `MockExchange::new` |
| `fetch_balance`: `total[quote] = balance_total`, `free = balance_free` | 599-608 | `Balance { total_usdt, available_usdt }` |
| `fetch_positions`: non-zero sizes only, `contracts`, `entryPrice`, `side` | 610-626 | `Position` (no leverage / margin / timestamp) |
| `fetch_open_orders`: sorted by `(timestamp, id)` with the id as a **string** | 628-635 | `fetch_open_orders` |
| Tickers: `bid = ask = last = step close` | 637-658 | `fetch_tickers` (`quote_volume_24h = 0`) |
| `fetch_ohlcv`: rows up to and including the current step (the open minute), `1h` aggregated by `ts // 1h`: first open, max high, min low, last close, summed volume | 660-692, 1038-1061 | `fetch_ohlcv_sync`, `aggregate` — paging differs, see section 2 |
| `fetch_my_trades`: filter by symbol / `since` / `until`, sorted by `(timestamp, id)` | 694-727 | `fetch_fills` |
| `create_order`: `positionSide` from params, reduce-only validation (would increase the position; amount > position + 1e-12) **before** the id is taken, `order_id = next` (string), `amount = abs`, limit price required, market fills at the step price as taker, a limit already crossed by the step price (`buy: last <= price`, `sell: last >= price`) fills at once **at the order price** as maker, otherwise it rests | 729-822, 1031-1036 | `create_one` (one order at a time, in slice order) |
| `cancel_order`: the order is popped **before** the symbol check; unknown id raises; symbol mismatch raises after removal | 824-842 | `cancel_one` (`ExchangeError::Rejected`) |
| `advance_time`: `current_index += 1`, `now_ms`, step actions, then resting orders | 878-888 | `advance` |
| Step actions `manual_fill` (market taker fill, id `manual_<n>`) and `cancel_open_orders` (filters: symbol, side, position side, reduce-only, client id) | 890-959 | `apply_action` |
| Resting fills: iterate the book in insertion order, a buy fills when the step candle's `low <= price`, a sell when `high >= price`, each at the order price as maker | 1015-1029 | `process_resting_orders` |
| Fill model: increasing side `new_size = size + qty`, `entry = (entry * size + fill * qty) / new_size`; reducing side `close_qty = min(size, qty)`, `pnl = (fill - entry) * close_qty * c_mult` (short: `(entry - fill)`), `size = max(0, size - close_qty)`, entry reset at 0; **never flips**; `fee = fill * qty * c_mult * fee_rate`; `realized_pnl += pnl`, `realized_fees += fee`, `balance_total += pnl - fee`, `balance_free = balance_total`; trade `id = next_trade_id`, `timestamp = now_ms` | 1063-1140 | `fill_order` |
| Fee rate: `maker` for maker liquidity, else `taker` | 1165-1167 | `fee_rate` |
| `set_position_mode`, `set_leverage`, `set_margin_mode` are recorded, nothing else | 848-873 | `set_hedge_mode`, `configure_symbol` |
| Request log with `timestamp`, `step_index`, method and payload | 177-191 | `RequestRecord` (`Create` / `Cancel` / `Other`) |

Floating-point operations are done in the same order as the Python code, so a
fill sequence produces bit-identical balances, entry prices and pnl.

## 2. Deliberate differences

1. **`fetch_ohlcv` paging.** The fake returns the *newest* `limit` rows
   (`fake.py:681-682`); ccxt and the runner's Bybit client return the
   oldest rows from `since`. The Python harness was recorded through
   `tools/fake_live_clock.py`, which restores ccxt semantics, so the mock
   implements the Bybit client's contract (`bybit/mod.rs::fetch_ohlcv`):
   `since` floored to the bucket, up to 5 pages of `limit` (max 1000), each
   page starting at the last row of the previous one; without `since` the
   newest `limit` rows. Timeframes other than `1m`/`1h` are `NotSupported`.
2. **Market orders** (since 2026-09-08, REVIEW finding 3): `NewOrder`
   carries the engine's execution type; the mock prices a market order at
   the step's last price and fills it immediately as taker exactly as
   `fake.py:790-808` does (`create_one`, unit test
   `market_order_fills_at_the_step_price_as_taker`). None of the recorded
   runs emits one (`market_orders_allowed=false`, no HSL panic), so their
   parity is unaffected. Market fills also exist through the `manual_fill`
   scenario action.
3. **Closed pnl.** The fake has no closed-pnl endpoint (Python reads
   `realizedPnl` per fill). `fetch_closed_pnl` returns one record per
   position-reducing fill with that fill's pnl, which gives the runner's
   `realized_pnl_cumsum` the same series.
4. **Errors** are `ExchangeError::Rejected { code: "fake_*" }` instead of
   Python exceptions. None occurs in the recorded runs (every cancel found
   its order, no reduce-only rejection).
5. Replay files must be `.npy` (the dev box's candle cache); the fake also
   reads CSV through `load_ohlcv_data`.

## 3. `pb-mockrun`: the closed loop and what is compared

Per recorded run directory (`tools/record_fake_v8.py` output):
`config.json`, `scenario.json`, `recordings/` (one engine call per step),
`artifacts/<stamp>_<name>/` (`remote_calls.json`, `step_summaries.json`,
`fills.json`, `fake_exchange_state.json`).

Loop, exactly `run_fake_live._run_fake_bot`: warmup + exchange configuration
at the boot step, then per step one `LiveRunner::plan` + `Executor::execute`
wave and one `MockExchange::advance` (the harness calls `advance_time` then
one cycle). Clocks: the runner's wall clock is the mock's `now_ms` (the
harness pins `utc_ms` to scenario time); the churn gate's monotonic clock is
the recording stem of the step (`<wall_ms>_<hash>.in.json`, D13), the same
pacing `pb-plancheck` uses; `--gate-clock cycle` uses scenario time instead.

Compared per step:

- create requests: multiset of `(symbol, side, pside, qty, price,
  reduce_only, pb_order_type)`; the ordered `(order_id, key)` sequence as a
  secondary count (ids are deterministic on both sides: `1`, `2`, ... after
  the boot fills' order ids);
- cancel requests: multiset of the cancelled orders' `(symbol, side, pside,
  qty, price)`; the cancel id set as a secondary count;
- account state after the wave: the open-order set (content and ids)
  against the Python book replayed from `remote_calls.json` minus
  `fills.json`; positions against `step_summaries[k].positions`; balance
  against `scenario.balance + sum(pnl - fee)` over the non-historical
  Python fills up to the step; the fill count against
  `step_summaries[k].fills`;
- final state against `fake_exchange_state.json` (balance, open-order and
  fill counts) when the whole run is replayed;
- `--diff-inputs`: the runner's engine input against the recording of the
  step, field path by field path.

## 4. Results (2026-09-08, `--diff-inputs`)

| run | steps | requests identical | open-order set / ids | positions | balance | fills | engine inputs |
|---|---|---|---|---|---|---|---|
| public grid_v7 (both artifact dirs) | 600 | 600/600 | 600/600 / 600/600 | 600/600 | 600/600 | 600/600 | 600/600 |
| public tm | 600 | 600/600 | 600/600 / 600/600 | 600/600 | 600/600 | 600/600 | 600/600 |
| seeded2 grid_v7 | 400 | 400/400 | 400/400 / 400/400 | 400/400 | 400/400 | 400/400 | 0/400 (*) |
| seeded2 tm | 400 | 400/400 | 400/400 / 400/400 | 400/400 | 400/400 | 400/400 | 0/400 (*) |
| seeded2 tm8 | 400 | 400/400 | 400/400 / 400/400 | 400/400 | 400/400 | 400/400 | 0/400 (*) |
| seeded2 iter7 | 400 | 400/400 | 400/400 / 400/400 | 400/400 | 400/400 | 400/400 | 0/400 (*) |

Create order/id sequence and cancel id set: 0 differing steps in every run.
Final state identical in every run. tm8 places no order in 400 steps
(Python neither).

(*) The only differing field is `global.realized_pnl_cumsum_last`
(`0.0` vs recorded `-0.0530015256`, every step): the seeded boot fills carry
`fee.cost = 0.0`, and Python's fill-event manager charges
`live.fee_pct_fallback` (0.0002) on fills without a fee, the runner does
not. `uses_realized_pnl` is on in these configs, but a 0.053 USDT offset of
the cumsum did not change any order in 1600 steps. Bybit fills always carry
a fee, so the fallback never fires on the live account; not ported.

Control runs (both expected to fail):

- `--no-harness-compat` on public grid_v7: 267/600 requests identical,
  open-order set 43/600. Without the harness quirk the runner keeps every
  forager candidate's candles fresh and rotates entries (ADA replaces HBAR
  from step 17); the Python bot under the harness could not (D17).
- `--gate-clock cycle` on iter7: 398/400 requests, open-order set 367/400,
  the same 33 cycles `pb-plancheck --gate-clock cycle` reported (D13).

**Limit of the evidence.** None of the six runs produced a live fill
(prices never reached a resting order in 400-600 minutes; `fills.json`
holds only the seeded boot fills), so balances and positions are constant
and the fill model is verified by the unit tests in `mock_exchange.rs`
(fill on candle range, fill at creation when crossed, entry averaging, pnl
and clamp on closes, short mirror, reduce-only rejection, id counters,
cancel semantics, book ordering, `fetch_ohlcv` paging and aggregation,
scenario actions) and by the extra run in section 5.

## 5. Extra run with fills

`.local/fake_v8_fills/grid_v7` (not committed): `grid_v7.json`, 150 steps,
`--seed-positions 3 --seed-entry-offset -0.03` (positions 3 % in profit so
the close grid crosses at creation). Python: 3 live maker fills at step 1
(ADA 233 @ 0.6425 pnl 4.423272 fee 0.01497025, BTC 0.001 @ 110050 pnl
3.31217 fee 0.011005, DOGE 769 @ 0.19482 pnl 4.434823 fee 0.014981658),
positions flat, final balance 1012.129308092, 2 open orders at the end.
`pb-mockrun --diff-inputs`: 150/150 identical requests (order/id sequences
included), open-order sets and ids, positions, fill counts, balances
(bit-identical every step), final state identical. Engine inputs differ
only in `realized_pnl_cumsum_{last,max}`: the recording keeps
`-0.079479763` / `0.0` at every step (the boot-fill fee fallback, and
**no live fill ever enters Python's series**: the harness primes the fill
cache once at boot and the bot never calls `fetch_my_trades` in any run,
`remote_calls.json` has zero such calls), while the runner refetches fills
each cycle and its series reaches `12.129308092`. Neither value changed an
order here (positions were flat, no unstuck). A real Python bot polls
fills, so this is a harness limit, not a runner deviation (D17.4).

## 6. Harness quirks the runner reproduces only under `pb-mockrun` (D17)

1. **Forager cache-only symbols are never refreshed.** `run_fake_live.py`
   runs no background `update_ohlcvs`; `_prime_fake_candles` writes the
   candle cache directly without touching the fetch bookkeeping, so from the
   second cycle every secondary (cache-only) symbol is
   `cache_only_never_fetched` (pb:18244-18247) and non-tradable; the
   forager can never rotate to a coin without a position or order.
   `LiveRunner::set_harness_secondary_never_fetched(true)` reports every
   symbol as never fetched (`SymbolState.candles_available = false`), which
   the snapshot builder only reads for cache-only symbols. On a real
   exchange the runner refreshes every universe symbol each cycle and the
   flag stays off.
2. **Trailing unavailable until a full minute closed after the last fill**
   (not a quirk: real Python behaviour, now ported in `live.rs`): the
   trailing candle fetch starts at the first full minute after the fill and
   ends at the latest finalized minute (cm:7540-7545); an empty result is
   `missing_exact_trailing_candles` (pb:9651). The seeded runs' boot fills
   are one minute before boot, so step 0 has `trailing_available = false`
   and the engine emits nothing; the runner used to return the default
   bundle as available there.
