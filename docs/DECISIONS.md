# Decisions

Append-only. A later entry may supersede an earlier one; say so explicitly.

## D1 (2026-09-07) Separate repository, not inside pbtb-rust or the passivbot fork

- pbtb-rust is the control plane (bot lifecycle); pb-runner is the data plane
  (one process that trades). Different release cadence: pb-runner versions
  follow passivbot tags, pbtb-rust follows ops needs.
- The ccxt Rust port is a large transpiled crate (bybit alone ~730 KB of
  source); compiling it inside pbtb-rust's clippy/test gate would slow every
  PR there.
- The passivbot checkout moves by tags only; active development inside it
  would turn every tag move into a merge.
- Integration with pbtb-rust is via the container contract (docs/CONTRACT.md),
  not shared code.

## D2 (2026-09-07) Strategy engine = pinned upstream `passivbot_rust`, zero logic fork

- The engine crate (`passivbot-rust/`) already contains grid/entries/closes/
  trailing/HSL/unstuck/forager scoring and the orchestrator
  (`compute_ideal_orders`, pure function over `OrchestratorInput`).
- The only change we make is build plumbing: `crate-type = ["cdylib","rlib"]`
  and a cargo feature gating pyo3/numpy (`python.rs`, `lib.rs`, the `_py`
  functions in `coin_selection.rs`, `utils.rs`, `types.rs`). `orchestrator.rs`
  and the strategy modules have zero pyo3 references.
- Lives on branch `pb-runner/rlib-<tag>` of the `iengai/passivbot` fork; an
  upstream PR is offered but not required.
- Consequence: backtest engine == live engine by construction. What must be
  ported faithfully is the *input construction* (Python side), see
  docs/PORT_INVENTORY.md.

## D3 (2026-09-07) Exchange I/O = ccxt official Rust port, pinned by commit

- ccxt merged its Rust port on 2026-09-03 (PR ccxt/ccxt#28627): transpiled
  from the same TypeScript source as the Python ccxt passivbot uses, 105
  exchanges including bybit, REST in `ccxt`/`ccxt-base`, WebSocket in
  `ccxt-pro`. Not on crates.io yet; git dependency only. Status "preview".
- Chosen over hand-written Bybit SDKs (rs-bybit: 1 star, unmaintained;
  Praying/ccxt-rust: 5 exchanges, single author) because behaviour parity
  with the Python bot's ccxt is the property we care about most.
- Pin a commit, never a branch: `rust/` receives daily automated regeneration
  commits.
- Fallback if the port proves unusable for private endpoints: implement the
  ~10 Bybit v5 endpoints in `pb-exchange-bybit` directly (signing scheme is
  simple HMAC-SHA256; rs-bybit shows the shape).

## D4 (2026-09-07) No barter-rs engine

- barter-execution ships only a mock client and a Binance placeholder; Bybit
  execution would have to be written by us anyway.
- barter's event-driven Strategy model does not match passivbot's
  "snapshot -> target order set -> reconcile" loop; the Engine would only be
  used as a state container.
- A tokio loop plus explicit state structs is ~1-2k lines and matches the
  reconciliation model exactly. barter-data's public Bybit WS may be reused
  later for tickers/trades if REST polling proves insufficient (optional).

## D5 (2026-09-07) diffcheck first, runner second

- Phase order is fixed: recorder patch -> diffcheck (engine replay must match
  recorded outputs 100%) -> exchange adapter -> runner loop -> shadow run.
- Rationale: the orchestrator is a pure function, so "is the engine wired
  correctly" is provable in a day; "is the snapshot built correctly" is then
  testable by comparing our snapshot builder's output with recorded inputs on
  the same market state (P4 acceptance).

## D6 (2026-09-07) One engine line per binary/image; build matrix over lines

- User requirement: v8.1.0 rejects some older configs, and a migrated config
  is not the same strategy. Legacy configs (the 18 `cap-v712` S3 templates,
  the live XRP abot config) must keep running on a v7.12.0 engine.
- Therefore pb-runner is built once per engine line (cargo features
  `engine-v7`, `engine-v8`), each pinned to its own passivbot tag
  (v7.12.0 commit `fc6b9e016`, v8.1.0 commit `7af64f3e9`), producing images
  tagged `pb-runner:<line>-<tag>-arm64`. A binary refuses configs of another
  line (`crates/runner/src/config.rs`), mirroring pbtb-rust's routing rule.
- The snapshot-construction code differs per line where the Python side
  differs (v7.12.0 `passivbot.py` is 13.5k lines, v8.1.0 is 22.7k). Shared
  code is line-agnostic; line-specific parts live behind the same features.
  Do the v8 line first (that is where new strategies go), then v7.
- pbtb-rust already routes by `config_version` major; the remaining question
  is how it chooses Python image vs pb-runner image *within* a line (D7 -> D12).

## D7 (open, resolved by D12) Runtime selection in pbtb-rust

Options, to be decided at P6 with the user:
1. New engine keys in `passivbot_engines` (e.g. `"8rs"`) plus a per-bot
   `runtime` attribute in DynamoDB, default `py`.
2. Replace the image of an engine line wholesale once the shadow run passes.
Option 1 is safer (per-bot opt-in, instant rollback by flipping the
attribute). Nothing in this repo depends on the choice.

## D8 (2026-09-07) Recordings are compared as JSON *text*; engine inputs are parsed from raw text; `serde_json` pinned to the wheel's version, no `float_roundtrip`

- Finding (P1.2): `serde_json`'s default float parsing is best-effort, not
  correctly rounded. Literals such as `929.9999999999999` or
  `-1.0010000000000001` parse one ulp off (to `930.0` / `-1.001`). The first
  diffcheck run reported 7/153 "mismatches" that were entirely this parser
  artefact: the engine output was byte-identical to the recording.
- Serialisation is exact: `serde_json::to_string` uses ryu (shortest
  round-trip), so identical f64s give identical bytes when the same
  serde_json version serialises the same struct.
- Therefore diffcheck (a) parses the engine input with
  `serde_json::from_str(&input_text)`, the same call `compute_ideal_orders_json`
  makes in `python.rs`, so the engine sees bit-identical inputs, and
  (b) compares `serde_json::to_string(&output)` with the recorded `.out.json`
  bytes. Value-level diffs are only used to *explain* a failure.
- Rule: pb-runner's `Cargo.lock` keeps `serde_json` at the version in the
  pinned tag's `passivbot-rust/Cargo.lock` (1.0.151 at v8.1.0) and never
  enables `float_roundtrip` or `arbitrary_precision` (cargo feature
  unification would silently change the engine's parsing). Checked in the
  upgrade procedure.
- Consequence for P4: the Python bot's engine sees values that went
  Python float -> `json.dumps` -> best-effort parse. To be bit-faithful the
  runner should feed the engine through the same round trip
  (`to_string` -> `from_str`) rather than constructing `OrchestratorInput`
  natively; decide at P4.2 with a measurement (cost is a few hundred us).
- Also found: the input validators that `compute_ideal_orders_json` runs
  before `compute_ideal_orders` (`validate_orchestrator_account_risk_inputs`,
  `validate_forager_score_weights_pair`, `validate_hsl_*`) live in
  `python.rs` behind the `python` feature. They only reject inputs, so
  diffcheck is unaffected (recordings with an `.out.json` passed them). The
  runner must replicate them (P4.3), or a later revision of the rlib branch
  moves them out of `python.rs` (moving code is build plumbing, allowed by D2).

## D9 (2026-09-07) P1.3: `passivbot_rust` exposes the target types, not the config mapping

- Exposed (serde, `deny_unknown_fields`): `types::BotParams`,
  `BotParamsPair`, `ExchangeParams`, the strategy param structs, and
  `strategies::registry::strategy_spec(kind)` (per-strategy param spec with
  defaults). `OrchestratorInput` round-trips through JSON unchanged.
- Not exposed: the resolution from a live config file to those structs.
  In v8.1.0 that is Python: `Passivbot._bot_params_to_rust_dict` (40 fields;
  global-vs-per-symbol lookup through `bot_value`/`bp` with `coin_overrides`;
  renames `forager_*_ema_span_1m` -> `filter_*_ema_span_1m`; int/bool/string
  coercions; `normalize_twel_enforcer_policy`,
  `normalize_we_excess_allowance_mode`), `_strategy_params_to_rust_dict`
  (`get_active_strategy_side` + `build_runtime_strategy_side`),
  `_equity_hard_stop_config`, and the defaults/template in
  `src/config/schema.py` (588 lines).
- Decision: port only the *resolved-value mapping* into `crates/runner`
  (module `bot_params`), targeting the Rust types directly. Do not port
  schema migrations: the runner refuses configs of another line (D6) and
  configs arrive already migrated from the control plane.
- Verification: record `_bot_params_to_rust_dict` / `global` outputs for each
  committed config with the plugin approach of RECORDER.md section C and
  compare field-by-field; this folds into the P4.2 acceptance. Budget
  1-2 days inside P4.


## D10 (2026-09-07) Committed fake_v8 fixtures come from public configs; private-config recordings stay local

- The repository is public (`iengai/pb-runner`, user decision 2026-09-07).
  A recorded `OrchestratorInput` embeds the full per-coin `bot_params`, so a
  recording made with one of the `strategy_lab` configs would publish the
  user's optimized strategy parameters. Those configs are gitignored research
  in the passivbot checkout for the same reason.
- Therefore the committed set `tests/fixtures/recordings/fake_v8/` is produced
  from `tests/fixtures/configs/fake_v8/{grid_v7,tm}.json`, derived by
  `tools/make_public_configs.py` from passivbot's own example configs with
  upstream default strategy parameters (trailing_grid_v7 with forager on and
  two `coin_overrides`; trailing_martingale with forager on). Exercised
  engine paths are the same as for the private configs (same strategy kinds,
  forager selection, overrides), only the numbers differ.
- The three private cap1000 configs named in PLAN P2.2 are still recorded
  (`tools/record_fake_v8.py`, output under the gitignored `.local/`) and
  diffcheck is run on them before every engine-facing change; results are
  reported in STATUS, the files are never committed.
- Supersedes the P2.2 wording "commit them under fake_v8/" for the private
  configs.

## D11 (2026-09-07) P3.1: hand-written Bybit v5 client; the ccxt Rust port is not used (supersedes D3)

Adjudicated by an independent same-tier review (brief: scratchpad
`p3_brief.md`; facts in STATUS 2026-09-07 session 3). Verdict B, reasons in
rank order:

1. `ccxt-base` declares `serde_json = { features = ["preserve_order",
   "float_roundtrip"] }`. Cargo feature unification would enable
   `float_roundtrip` for the whole binary, changing how
   `compute_ideal_orders_json` parses engine input: exactly the artefact D8
   forbids. A vendored slice would have to fork generated code to remove it.
2. Parity is weaker than it looks: typed methods take `&mut self` (Python
   gathers N `create_order` calls on one instance), and the fields the bot
   actually reads (balance from `info.result.list[0]`, position side from
   `info.positionIdx`) are raw Bybit payload, not unified fields. A small
   client reproduces the request shapes line for line from `bybit.ts`.
3. Transpiled bodies signal errors by `panic!` caught with `catch_unwind`
   (74 `panic!` in bybit.rs); a `panic = "abort"` release profile would turn
   every exchange error into process death.
4. Build cost: 7 min 22 s dev for the three ccxt crates on 32 threads,
   2.5 GB rlib, 12 GB target, all 209 exchanges compiled (no per-exchange
   feature); with thin LTO + cgu 1 and two engine lines (D6) this is tens of
   minutes per PR.
5. ~10 endpoints / ~600 lines vs 870 KB generated source + 23k-line runtime
   regenerated daily.

Consequences:
- `crates/exchange-bybit` implements Bybit v5 (`category=linear`) directly:
  public `GET /v5/market/instruments-info`, `/v5/market/tickers`,
  `/v5/market/kline`; private (HMAC-SHA256 over
  `timestamp + apiKey + recvWindow + query|body`) `GET /v5/account/wallet-balance`
  (`accountType=UNIFIED`), `GET /v5/position/list`, `GET /v5/order/realtime`,
  `POST /v5/order/create`, `POST /v5/order/cancel`,
  `POST /v5/position/set-leverage`, `POST /v5/position/switch-mode`,
  `POST /v5/position/switch-isolated`; for P4.1 fills
  `GET /v5/execution/list`, `GET /v5/position/closed-pnl`.
- Behaviours reproduced verbatim from ccxt/the Python adapter (list in
  PORT_INVENTORY section 3 and the verdict): unified symbol mapping
  `BASE/USDT:USDT` <-> `BASEUSDT` (linear USDT-settled only), market fields
  (`qtyStep`, `tickSize`, `minOrderQty`, `minNotionalValue`, `maxLeverage`,
  contract size 1), UTA balance formula, `positionIdx` side mapping, ignored
  error codes 110025/110026/110043, "already gone" cancel codes (110001 and
  message patterns), order params (`positionIdx`, `timeInForce`
  `PostOnly|GTC`, `orderLinkId`, `reduceOnly`, `orderType Limit`, qty/price
  formatted with market precision), cursor pagination limits (200 / 50),
  kline `limit=1000` with at most 5 forward pages.
- Numbers are parsed from Bybit's strings with `str::parse::<f64>`
  (correctly rounded, same as Python `float()`), never via serde_json floats.
- Private WebSocket (P3.3) starts as REST polling; WS only if measured
  latency requires it.
- Reversal condition: ccxt ships per-exchange features, drops
  `float_roundtrip` from defaults, `&self` typed clients, and a release build
  under ~5 min incremental in CI; all four together.
- Parity check for P3.4: record the Python bot's ccxt requests/responses for
  the read-only abot account and compare against this client's requests.

## D12 (2026-09-07) pbtb-rust selects the image per bot: engine key `<major>[rs]` + bot attribute `runtime`

Resolves D7 with option 1. Implemented on `iengai/pbtb-rust` branch
`feat/pb-runner-runtime` (commit c56f53c; PR from that branch, not merged or
applied by this repo's sessions).

- Engine table `APP__ECS__TD_PASSIVBOT_BY_ENGINE` keys become
  `<major>[py|rs]` (`7=arn,8=arn,8rs=arn`). A bare major or `py` is the
  Python passivbot image; `rs` is the pb-runner image of the same line. Any
  other suffix fails boot, so a typo can never register a line.
- The bot row gets an optional string attribute `runtime` (`py`|`rs`); absent
  reads as `py`, so every existing bot keeps launching exactly as before.
  Unknown values are a corrupt-record error, not a fallback.
- Both launchers (Telegram Run and the Lambda auto-restart) resolve on
  `(config line, bot.runtime)`. A bot set to `rs` whose line has no `rs`
  image is refused with a user-facing message at `/runtime`, at "Choose
  config" and at Run; it never silently launches the Python image.
- Telegram: `/runtime <bot_id> [py|rs]`; State and the `/start` summary show
  the runtime. Applies on the next Run (same convention as the other per-bot
  setters); rollback is flipping it back and restarting.
- Terraform: `passivbot_engines` entries gain `image_repo` (default the
  passivbot-live repo) and `command` (null keeps the image entrypoint; the
  `8rs` entry passes `["--live"]` because pb-runner is dry-run by default,
  CONTRACT.md). New ECR repo `pb-runner` (`module.ecr` key `pb_runner`,
  `force_delete=false`). The `8rs` tfvars entry stays commented out until the
  image exists; the task-definition module emits a byte-identical definition
  for `command=null`, so the existing `7`/`8` families show no diff.
- Rollout order (RUNBOOK "pb-runner runtime"): build+push image from this
  repo's CodeBuild project -> uncomment `8rs` -> scoped apply (task def,
  lambda, telebot base env) -> telebot-deploy (lambda and telebot must both
  carry the new parser before an `8rs` key appears) -> move one bot with
  `/runtime <bot_id> rs` + Stop/Run.
- Why not option 2 (swap the line's image wholesale): no per-bot opt-in, no
  instant rollback, and the Python and Rust runners would share a task-def
  family and memory limit although their RSS differs by an order of magnitude.

## D13 (2026-09-07) Churn gate clock: caller-supplied monotonic seconds; plancheck replays the recorder's wall clock

Facts: `live/order_churn_gate.py`, `prepare_order_churn_evidence` and
`_apply_order_churn_admission` read `time.monotonic()`. `tools/fake_live_clock.py`
pins `utc_ms` / `_utc_now_ms` but not `time.monotonic`, so in every recorded
fake run the evidence window (10 min), the stability span (2 min), the
sample gap limit (96 s) and the create-allowance window were wall-clock
quantities while scenario time advanced 60 s per step (about 0.8 s of wall
time). Evidence: `live_events.json` `monotonic_ms` deltas of ~780 ms per
cycle, `rolling_usage=90` in iter7's `run.log` (impossible with 60 s steps
and 3 creates per batch), and `pb-plancheck --gate-clock cycle` giving the
pre-port 367/400 on iter7 while `--gate-clock wall` gives 400/400.

Decision: `ChurnGate` takes seconds on a monotonic clock chosen by the
caller. The live runner passes `churn::monotonic_seconds()` (an `Instant`
since process start, the Rust equivalent of `time.monotonic()`); plancheck
passes the recording stem `<utc_ms>_<hash>` (written by the recorder patch
with `time.time()` at the engine call: the same clock as the bot's
`time.monotonic()` up to a constant offset, and the only per-cycle wall
clock in the artifacts since `live_events.json` keeps 2000 events).

Consequences: parity on fake runs depends on the recorder's stem; a harness
that pins `time.monotonic` as well would need `--gate-clock cycle`. On a
real exchange both bots see the same clock, so nothing changes there.

## D14 (2026-09-08) Pre-create market gate sits between `reconcile()` and `admit_and_cap`; freshness = local receive time, constant 10 s

Facts: `execute_order_plan` runs `_filter_fresh_market_snapshot_creations`
after the cancel-first barrier and the recent-execution / state-change /
exchange-config guards and *before* `_apply_order_churn_admission` and
`_apply_creation_batch_capacity` (`exe.py:958-975`); the admission reads
`_churn_gate_market_distance`, which only that filter sets (`md.py:294`,
from the pre-create snapshot's `last`). `_live_market_snapshot_max_age_ms`
is the constant 10 000 ms (`md.py:656`); staleness compares `utc_ms()` at
the check with `MarketSnapshot.fetched_ms`, the `utc_ms()` taken after the
ticker request returned (`market_snapshot.py:98`). The Bybit connector's
`_normalize_tickers` (`ccxt_bot.py:1219`) discards the ccxt ticker
`timestamp`, so `exchange_timestamp_ms` is `None` on Bybit and no freshness
check uses it anywhere. Python's `fetch_tickers(symbols)` retry for symbols
missing from the bulk response is, on Bybit, the same
`/v5/market/tickers?category=linear` request.

Decision: `reconcile()` ends at the recent-execution guard; the async market
gate (`market_filter.rs`) runs on `plan.creates`; `reconcile::admit_and_cap`
then applies churn admission (market distance from `OrderRec.market_distance`,
unset -> deferred as `market_distance_unavailable`), the create capacity and
the attempt bookkeeping. Freshness uses the runner's wall clock against the
local receive time of the bulk `fetch_tickers`; the Rust `Ticker` gets no
timestamp field. The planning tickers go through the same cache with the
5 s fetch TTL, so the engine's `last` and the pre-create `last` coincide
unless the cycle took more than 10 s (then a refetch, exactly as Python).
The retry is one more bulk fetch. Surface epochs of the freshness ledger
are not modelled (every surface is refreshed at the top of each cycle).

Consequences: any caller of `reconcile()` that wants the churn gate must
call `admit_and_cap` (live loop and plancheck do); a cycle whose ticker
refresh fails or returns a stale/missing snapshot places no orders at all,
market orders included, and logs Python's `[market] skipping order creation`
line; a fake-harness scenario cannot exercise the stale path because its
clock is pinned (plancheck reports the skip count instead).

## D15 (2026-09-08) Snapshot cross-cycle state: `CycleState` carried by the runner; cooldowns dormant on Bybit; cached forager metrics not ported

Facts (SNAPSHOT_SPEC 8, traced in `src/passivbot.py` at e808cfd33):

- The tradable/unavailable decision for a symbol with missing required EMAs
  depends on the previous cycle's `PB_modes` and
  `_orchestrator_dynamic_forager_eligibility_psides_by_symbol`; missing
  required *forager* spans raise (cycle aborts) whenever
  `required_ema_can_mark_nontradable` is false (pb:19340), not merely when
  the symbol is a priority symbol. The builder used the latter until
  2026-09-08; no fixture distinguished the two.
- `fetch_close_map` reuses the previous cycle's close EMA for at most
  `_close_ema_fallback_max_age_ms` (10 min by default) and only when no
  open-tail projection context exists; the projection
  (`cm.get_projected_open_tail_ema_metrics`) is stateless.
- `_activate_exchange_symbol_unavailable_cooldown` needs a connector
  classifier; only `exchanges/weex.py` has one (`-1058`). On Bybit the
  cooldown never arms.
- `fetch_cached_forager_metrics` (forager `qv`/`log_range` computed on a
  window ending at the last cached candle, within a staleness budget) is
  the Python bot's way to keep ranking candidates while candles lag. The
  runner fetches every universe symbol's candles each cycle, so candle lag
  only occurs when the exchange itself is behind.
- The fake harness primes the full 1m array for every coin each step, so
  neither the carry-forward nor the projection ever fires in a fake run:
  `pb-snapcheck` is 600/600 on both full public runs with the new code, the
  same as before it. The new behaviour is covered by unit tests only.

Decision:

1. The builder takes `&mut CycleState` (`pb_modes`,
   `dynamic_forager_eligibility`, `prev_close_ema`, `exchange_unavailable`)
   instead of reading bot-private attributes. `LiveRunner` owns it;
   `pb-snapcheck` derives `pb_modes` from the previous recording's output,
   which is the previous cycle only for unsubsampled sets (subsampled sets
   were verified not to depend on it: no fixture has an active side without
   a position or order).
2. Port the cooldown state machine and planning policy faithfully
   (`cooldown.rs`, `snapshot::cooldown_mode`) but keep
   `classify_symbol_unavailable` returning `None` for Bybit, so the runner
   matches the Python Bybit bot; a future exact-code classifier is a
   one-function change with the state machine already tested.
3. Port the health-based open-tail projection (close; `qv`/`log_range`
   when forager is off; required strategy `log_range` when on) and the
   close-EMA carry-forward. Do not port the cached forager-metric fallback
   or the forager stale-tail context: on a lagging exchange a forager
   priority symbol with a missing required forager span makes the runner
   skip the cycle with an error (Python would rank on stale metrics). Revisit
   if the shadow run (P5.2) shows Bybit candle lag beyond one minute.
4. `_orchestrator_ema_entry_cancellation_order_keys` (resting entries
   authorised while forager rank features were missing) is treated as
   empty; it only widens the set of symbols the bot may mark nontradable.
5. Runtime operator forced modes (`_runtime_forced_modes`) have no source in
   the runner and are not modelled; config forced modes (global and
   `coin_overrides.<coin>.live.forced_mode_*`) are, verified by the
   `grid_v7_forced` fixture set. HSL modes remain a separate task.

## D17 (2026-09-08) Mock exchange = `fake.py` semantics behind the Bybit client's contract; two harness facts the runner reproduces only under `pb-mockrun`

Facts (P5.1 closed loop, docs/MOCK_EXCHANGE.md):

- The Python fake exchange (`src/exchanges/fake.py`) fills a resting limit
  order when the *next* step candle's low/high reaches its price and a new
  limit order immediately when the step's last price already crosses it,
  both at the order price as maker; positions net per pside with an
  average entry price and never flip; `balance += pnl - fee` per fill;
  order and trade ids are consecutive integers seeded by the boot fills'
  ids (`fake.py:517-525`). Its `fetch_ohlcv` returns the *newest* `limit`
  rows, which `tools/fake_live_clock.py` overrides to ccxt semantics for
  the Python harness (RECORDER.md A.3).
- Under `run_fake_live.py` no background candle refresh runs and
  `_prime_fake_candles` bypasses the candle manager's fetch bookkeeping, so
  from the second cycle every forager cache-only symbol is
  `cache_only_never_fetched` (pb:18244-18247) and non-tradable: the Python
  bot could never rotate to a coin without a position or order. The
  recordings show it (`tradable: false`, empty EMAs for those symbols;
  `pb-snapcheck` models it as `candles_available = fi == 0 || has_pos ||
  has_order`). The live runner refreshes every universe symbol each cycle,
  which is what the Python bot does on a real exchange through
  `update_ohlcvs`.
- Python's trailing candle window starts at the first full minute after
  the last fill and ends at the latest finalized minute (cm:7540-7545); an
  empty window is `missing_exact_trailing_candles` (pb:9651) and the side is
  unavailable for that cycle. The seeded scenarios' boot fills are one
  minute before boot, so step 0 has `trailing_available = false` and the
  engine emits no orders; the runner returned the default bundle as
  available there.
- The churn gate ran on wall-clock time in the harness (D13); the
  recording stems are the only per-step wall clock.
- `NewOrder` carries no order type: the Bybit client hard-codes
  `orderType: Limit`.

Decision:

1. `mock_exchange.rs` mirrors `fake.py` operation for operation (line
   references in the code and in MOCK_EXCHANGE.md section 1), except that
   `fetch_ohlcv` implements the Bybit client's paging contract (oldest rows
   from `since`, 5 pages), every created order is a limit order, closed pnl
   is derived per position-reducing fill, and errors are
   `ExchangeError::Rejected { code: "fake_*" }`.
2. `LiveRunner` takes injected clocks (`with_clocks`: wall = scenario time,
   monotonic = recording stem) and a harness-only flag
   `set_harness_secondary_never_fetched` that reports every symbol as never
   fetched by a background refresh (`candles_available = false`, read by
   the snapshot builder for cache-only symbols only). `pb-mockrun` sets it;
   `pb-runner` never does. Without it the public forager runs diverge from
   step 17 (267/600), which is the runner behaving like the Python bot on a
   real exchange, not a bug.
3. The first-minute trailing rule is ported into `live.rs` for the live
   path too (it is Python's real behaviour after any fill), guarded by
   `latest_finalized >= first_full_minute_after(anchor)`.
4. Not ported: `live.fee_pct_fallback` on fills without a fee (Python's
   fill-event manager charges 0.02 % on the seeded boot fills, hence
   `realized_pnl_cumsum_last = -0.053` in the seeded recordings vs `0.0`
   in the runner). Bybit fills always carry a fee; revisit if a real
   recording shows a fee-less fill. Related harness limit: the fill cache
   is primed once at boot and the harnessed bot never calls
   `fetch_my_trades` (zero calls in every `remote_calls.json`), so live
   fills never enter Python's realized-pnl series in a fake run; the
   runner refetches fills every cycle, as the Python bot does on Bybit.
   The `fake_v8_fills` run shows the resulting `realized_pnl_cumsum_*`
   difference without any order changing.

Consequences: `pb-mockrun` is the P5.1 acceptance tool (six runs identical
in requests and account state, MOCK_EXCHANGE.md section 4); the mock cannot
exercise market orders, partial fills, or exchange errors, none of which
the fake exchange models either. Fills are covered by unit tests and the
extra `fake_v8_fills` run, not by the six original runs (no price ever
reached a resting order there).

## D18 (2026-09-08) P7 (v7.12.0 line) deferred: v7 bots stay on the Python image via D12 routing

Adjudicated by a same-tier subagent survey (PORT_INVENTORY section 6, PLAN
P7 recommendation block). Option (c): do not build the `engine-v7` runner
now. pbtb-rust routes engine key `7` to the frozen Python image per bot
(D12), so nothing is blocked; the only measured cost is RSS (~430 MB vs
~20 MiB per bot).

- Option (b) (v7 configs through the v8 `trailing_grid_v7` compatibility
  strategy) is not a port under D6: upstream's `docs/v7_to_v8_migration.md`
  disclaims runtime identity and measured divergence for forager and
  exposure-enforced configs. It remains a re-validation path that turns a
  legacy config into an ordinary line-8 bot.
- Option (a) (full v7 runner line) is feasible: v7.12.0 already has
  `compute_ideal_orders` with an input schema that is a strict subset of
  v8's plus two Python-computed unstuck allowances; the rlib plumbing is a
  verbatim re-apply of e808cfd33; the Bybit adapter and recording tooling
  are reusable. But EMA/forager loading, mode overrides, the initial-entry
  distance gate, freshness guardrails and pending-PnL blocking have
  v7-specific semantics needing their own spec derivation: ~60% of the
  line-8 P4 effort plus 1-2 weeks of shadow, for a line that gets no new
  strategies.
- Revisit triggers: Bybit API drift breaking the frozen v7 image; the v7
  bot count after the user reviews the 18 `cap-v712` templates; an
  operational need for the pb-runner contract on v7 bots. The order of
  work if (a) starts is written under PLAN P7.
