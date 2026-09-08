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

## D16 (2026-09-08) HSL: account-level state machine ported in `hsl.rs`, reconstructed from the fill history at start; coin mode refused; verified by trace replay

Facts (`src/passivbot_hsl.py` at e808cfd33, `passivbot-rust/src/equity_hard_stop_loss.rs`):

- The per-side HSL state (`_equity_hard_stop_make_state`, hsl:2430) wraps
  the engine's `HardStopState` (`EquityHardStopRuntime`) and
  `RollingPeakTracker`; Python adds halted / cooldown / no-restart /
  flat-confirmation bookkeeping, the per-minute sample cache and the stop
  event. `get_forced_PB_mode(pside)` returns `panic` (red, not halted) or
  `graceful_stop` (halted); `_orchestrator_mode_override` step 1 turns that
  into per-symbol modes (`_equity_hard_stop_halted_mode`). `is_forager_mode`
  ignores HSL; `_pside_blocks_new_entries` does not.
- Python persists latch payloads
  (`caches/equity_hard_stop/<exchange>/<user>_<pside>.json`) and a
  replay-matrix cache, both write-only or accelerators: on start it always
  replays `get_balance_equity_history` (fills + 1m closes over
  `pnls_max_lookback_days`) with `latch_red = False`, finalizes red-seen
  episodes that an ordinary fill flattened, and samples the present.
- `live.hsl_signal_mode` defaults to `coin` (per-symbol state, replay
  matrices, panic markers from `pb_order_type`); `unified` / `pside` are the
  account-level modes.
- The fake harness runs red supervision through the normal planning path
  with `panic` overrides (`_run_fake_red_supervisor_step`, two flat
  confirmations, `_finalize_fake_terminal_red_if_sync_flat`); production
  uses the protective-panic input path (hsl:8068) and anchors the stop
  event at the fill that flattened the scope. The harness never refreshes
  its fill ledger after boot (Python's `realized_pnl` stays at the boot
  value), the production bot does (`update_pnls`).
- Parity details found by replaying the Python trace: closes are f32 in the
  candle manager, only finalized minutes are served (the current minute
  carries the previous close), zero fees fall back to
  `live.fee_pct_fallback` x notional (`_normalize_fee_paid_from_payload`),
  `reset_after_restart` keeps `last_stop_event`, the 1h EMA map is
  all-or-nothing (`fetch_required_map` raises, `h1 = {}`).

Decision:

1. `hsl.rs` ports the account-level machine (`unified`, `pside`) verbatim on
   top of the engine crate: `HslState::{initialize_from_history, check,
   supervise_red, sync_flat_finalize, modes}`; `LiveRunner` owns it,
   initializes it at warmup from its own fill/closed-pnl history and 1m
   buffers (`balance_equity_timeline`), feeds it every cycle before the
   snapshot and passes `HslState::modes()` through `CycleState.hsl` to
   `SnapshotBuilder::with_hsl`. No state file is read or written: a runner
   restart reconstructs the same state a Python restart would.
2. `hsl_signal_mode = "coin"` with HSL enabled is refused at config load
   (`HslConfig::from_config`). Coin mode needs the per-symbol replay
   matrices and panic-marker reconstruction and has no fixture; a config
   that wants HSL on the runner sets `unified` or `pside`.
3. Red supervision runs through the normal planning path with `panic`
   overrides (the harness structure), not the protective-panic input;
   `Supervision::Production` anchors the stop event at the latest flattening
   fill as hsl:8068 does, `Supervision::FakeHarness` reproduces the harness
   for the replay check. Operator runtime forced modes are not carried.
4. Verification is a trace replay, not a state dump: `tools/fake_live_clock.py`
   writes `hsl_trace.jsonl` (inputs and states around every check,
   supervisor step, finalization and `compute`); `pb-snapcheck` drives
   `HslState` with the traced inputs, recomputes the start-up replay from
   `fills.json` + candles, cross-checks the derived pnl inputs (deviations
   explained by the harness's stale ledger are counted, not failed) and
   asserts every traced state (floats within 1e-9 relative; 10822/10835
   bit-exact on `grid_v7_hsl`, the rest 2.7e-16 from summation order).
5. Fees: `hsl::FeePolicy` reproduces the fill manager's normalisation for
   the HSL ledger. `live::realized_pnl_cumsum` (SPEC 5.2) still negates the
   reported fee only; zero-fee fills there should get the same fallback
   (follow-up; no fixture distinguishes it because the recorded stats come
   from the recording itself).

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
4. Not ported at the time: `live.fee_pct_fallback` on fills without a fee
   (Python's fill-event manager charges 0.02 % on the seeded boot fills,
   hence `realized_pnl_cumsum_last = -0.053` in the seeded recordings vs
   `0.0` in the runner). Ported 2026-09-08 (D20 item 6): the seeded2 runs
   are now identical in every engine-input field. Related harness limit: the fill cache
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

## D19 (2026-09-08) Pre-live review fixes: fill windows, per-symbol degradation, in-process restart loop, market orders

Context: docs/REVIEW_2026-09-08.md findings 1-8, fixed in one pass against
`src/exchanges/bybit.py`, `src/passivbot.py` and ccxt `bybit.py` (line
references in the code). The non-obvious choices:

1. **Fill / closed-pnl windows walk the whole range.** The client sends
   explicit `[startTime, endTime]` 7-day windows newest-first
   (`weekly_windows`, the arithmetic of `fetch_pnls_sub` bybit.py:200-225
   and of Bybit's implicit `[endTime - 7d, endTime]` that `fetch_fills`
   bybit.py:279-308 relies on). Python's `fetch_fills` stops at the first
   week that returns no fills, which would truncate a 30-day lookback
   behind a quiet week; the runner walks every window (5 requests for 30
   days, bounded like Python at 100 / 52 windows). Verified equal counts
   on the abot account (1391 fills / 514 closed-pnl rows over 30 days).
   The incremental refresh refetches from one hour before the last sync
   (Python's `start_time - 1h` overlap) up to now, every cycle, so it
   reaches the present on an empty account too; both buffers are pruned to
   `pnls_max_lookback_days`.
2. **Missing ticker / market / candles degrade per symbol, not per
   cycle.** Python raises the whole cycle when any planning ticker is
   missing (`market_data.py:600-640`) and when the EMA bundle fails for a
   symbol with a position (`required_ema_can_mark_nontradable` false);
   for flat candidates it marks the symbol non-tradable and continues. The
   runner drops a symbol with no ticker / no market / no candles from the
   cycle (no orders planned, its open orders left alone so they are not
   cancelled as unwanted), plans the others, and charges the shared error
   budget once per cycle only when a dropped symbol has a position or open
   orders -- the case where Python would have aborted the cycle -- so a
   persistent fault still reaches the restart -> `init_markets` path at
   Python's pace. A candle refresh failure with cached candles plans on
   them (Python's candle manager serves its cache); with no candles and
   exposure the cycle fails as Python's bundle raises. Inactive markets
   (status != Trading) are kept with `MarketSpec.active = false` and flow
   into `tradable` (pb:19903); only approved coins require an active market.
3. **Error budget = one counter, in-process restarts.** Planning failures
   and write failures feed the same `Executor::note_error`
   (`restart_bot_on_too_many_errors`, 10 per hour). Hourly market reload
   failures and dropped-symbol cycles are charged through
   `LiveRunner::take_budget_errors`. On a trip the process does what
   Python's `main()` does: tear down, sleep 60 s, rebuild the bot with a
   fresh budget, count restarts over 24 h, and exit (30) once they exceed
   `live.max_n_restarts_per_day`. pbtb-rust's task-state-change lambda
   restarts a task only on a memory-related stop (`exit_code == 137`, not
   `UserInitiated`; `usecase/reconcile_stopped_task.rs`), so a non-OOM exit
   leaves the bot stopped until the user starts it -- identical to the
   Python image, whose process also leaves its loop after the cap. The
   daily cap is therefore in-process only and not persisted (CONTRACT.md
   section 2).
4. **Market orders.** `NewOrder.order_type` carries the engine's
   execution type; the body is `orderType: Market`, no `price`,
   `timeInForce: GTC` (ccxt sends `price` only for limit orders and
   `handle_post_only` refuses `postOnly` on market orders -- with
   `time_in_force = post_only` Python's ccxt call would raise before the
   request; the runner sends GTC instead, since a refused panic close is
   the worse outcome and the guard is ccxt's, not strategy). Reconcile is
   unchanged: a market ideal never matches a resting order (Python's exact
   key includes `type`) and, once executed, is not emitted again.
5. **Lazy per-symbol exchange configuration** (`exchange_config.rs`) as
   `update_exchange_configs`: only the wave's create symbols, done-set,
   exponential backoff, rate-limit stop, 0.2 s pause, creates on pending
   symbols skipped without a budget charge. On a unified account the
   margin mode goes through the account-wide
   `/v5/account/set-margin-mode` once per symbol exactly as ccxt's
   `set_margin_mode` does for Python (tolerating 110026 / not modified);
   the classic path keeps `switch-isolated`. Hedge mode is asserted at
   startup and re-asserted hourly regardless of `live.hedge_mode`, as
   `init_markets` -> `update_exchange_config` -> `set_position_mode(True)`.
   Margin mode is always cross on Bybit (`_resolve_margin_policy_for_symbol`
   ignores the `isolated` preference unless the market is isolated-only).
6. **Ordering of the wave filters.** Python applies the dirty-symbol skip
   (3.1 step 6) and the exchange-config skip (step 7) before the market
   snapshot filter, churn admission and create capacity (steps 8-9). The
   runner's steps 8-9 run inside `plan()` before execution, so steps 6-7
   are applied by the executor after them: a wave can carry fewer creates
   than Python's (never more, never different ones), and the difference
   is re-planned next cycle.

Not fixed (REVIEW finding 8): `normalize_open_order` still derives
reduce-only from the config's `hedge_mode` rather than the order's
`positionIdx`; the recent-execution guard still stamps creates with the
loop-start time.

## D20 (2026-09-08) HSL coin mode ported in `hsl_coin.rs` (amends D16 item 2): per-pair machine, coin RED supervisor + protective planning per cycle, start-up reconstruction from the fills' panic markers

Facts (`src/passivbot_hsl.py` at e808cfd33, `src/passivbot.py`,
`passivbot-rust/src/equity_hard_stop_loss.rs`):

- `live.hsl_signal_mode = "coin"` (the default) keeps one
  `EquityHardStopRuntime` per `(pside, symbol)` (`_hsl_coin_state`,
  hsl:2460: the account-level state dict plus `pnl_reset_timestamp_ms`) fed
  with `hsl_coin_drawdown_signal(balance, n_positions, peak_realized,
  last_realized, upnl)`: `slot_budget = balance / round(n_positions)`,
  `drawdown_raw = max(0, peak - (last + upnl)) / slot_budget`, synthetic
  equity `max(1 - drawdown_raw, 1e-12)` against a constant peak of 1. Peak
  and last realized are the running sum of the pair's `pnl + fee_paid` over
  the fills at or after `max(ts - lookback, pnl_reset_timestamp_ms)`
  (hsl:3485); a finalized episode sets `pnl_reset_timestamp_ms = stop_ts + 1`.
  TWEL is not an input; `_coin_active_pside` only requires the side enabled,
  `n_positions > 0` and `twel > 0`. Per-coin `hsl_*` values apply only to
  `coin_overrides` coins (`_equity_hard_stop_config(pside, symbol)`).
- The account-level state is never fed in coin mode: `_orchestrator_mode_override`
  step 1, `get_forced_PB_mode(pside)`, `_apply_equity_hard_stop_orange_overlay`
  and `_refresh_halted_runtime_forced_modes` are no-ops. The pair modes
  travel through `_runtime_forced_modes[pside][symbol]` (step 3) and the
  replay-pending set (step 2); the universe and the forager flag are not
  affected by a red / halted pair.
- Production red supervision (`_equity_hard_stop_run_coin_red_supervisor`,
  hsl:8205) loops while a pair needs panic supervision (latched, not halted,
  and the current sample red or absent; or halted with a repanic reset
  pending): protective refresh (balance / positions / open orders only),
  per pair flat -> flatten-fill lookup since `pending_red_since_ms` (the
  ledger refreshed through `update_pnls` when the fill is missing) ->
  pending stop event + confirmation, else a sample refresh whose recovery
  pauses panic (`tp_only_with_active_entry_cancellation`) without ending the
  episode; two confirmations finalize (halt, cooldown, `graceful_stop`); the
  remaining pairs are planned with the protective-panic input
  (`calc_protective_panic_ideal_orders_orchestrator`, pb:16516: target
  symbols holding a position, `panic` on the target psides, `manual`
  elsewhere, no EMAs / trailing / fill timestamps, `auto_unstuck_allowed`
  false, zero realized cumsum), executed without mode filters against the
  target pairs' open orders, then `execution_delay_seconds` of sleep. The
  fake harness runs this production loop in coin mode (unlike the unified
  fake step of D16), all iterations at the same scenario minute.
- Start-up (`_equity_hard_stop_initialize_coin_from_history`, hsl:5569):
  `get_balance_equity_history(hsl_replay_signal_mode="coin",
  hsl_coin_compact_replay=True)` returns the minute grid with per-pair
  realized / unrealized series and the panic flatten markers (a `panic`
  fill after which the pair is flat by `compute_psize_pprice`'s fallback --
  the `final_state=` keyword raises `TypeError`, so a long's psize never
  returns to zero and a short's stays zero --, by the authoritative flat
  position, or by the replay slot). Held and ambiguous pairs walk every row,
  the others the change-point rows (`_hsl_compact_sparse_replay_indices`);
  a panic marker on a red-confirming row (tier red or score >= red - 1e-12)
  or a red-seen zero crossing finalizes the episode (halted / cooldown /
  no-restart), a panic fill inside its cooldown without a reconstructed stop
  halts by contract (`_equity_hard_stop_infer_coin_replay_contract`), an
  elapsed cooldown resets, and the present sample can re-activate red. The
  replay-matrix cache reuse "never becomes authoritative" (hsl:1874): it
  hands back a history equivalent to the full replay and changes no
  decision.
- `_equity_hard_stop_refresh_coin_cooldown_after_repanic` (hsl:4776) is not
  bound on `Passivbot` in the checkout (the call sites hsl:4958 / hsl:8261
  would raise `AttributeError` on a repanic reset with the `panic` cooldown
  policy).

Decision:

1. `hsl_coin.rs` ports the per-pair machine on top of the engine crate:
   `CoinState` / `CoinMetrics` / `CoinStopEvent`, `HslState::{check_coin,
   supervise_coin_red, initialize_coin_from_history, coin_panic_pairs,
   coin_red_active}`, the flatten-fill lookup per pair
   (`hsl::latest_flatten_fill_timestamp(symbol)`), `CoinEnv` for what the
   owner supplies (`_calc_upnl_sum_strict`, blocking-order counts, an
   optional traced realized peak/last), `HslConfig::coin_overrides` /
   `side_config` / `coin_active_pside`. `HslModes` carries `coin_enabled`,
   `replay_pending` and `runtime_forced` into `SnapshotBuilder::mode_override`
   steps 2-3. `HslConfig::from_config` accepts coin mode (D16 item 2
   lifted); `unified` / `pside` are unchanged.
2. Start-up reconstruction = `hsl::coin_history` (the timeline replay
   generalised: minute grid, per-pair series with `NaN` for absent values,
   panic markers with `psize_after_quirk`) + `initialize_coin_from_history`
   (dense / sparse rows, `RealizedWindow` = `rolling_realized_at`, contract
   inference, present sample). No cache, no background replay: the runner
   replays synchronously at warmup, so `replay_pending` is empty afterwards
   and step 2 never fires in the runner; it is kept in `HslModes` for the
   trace replay and for parity of the builder.
3. `LiveRunner` runs `check_coin` every cycle; when pairs still need panic
   supervision it runs one `supervise_coin_red` iteration and plans the
   protective-panic input (`SnapshotBuilder::build_protective`) instead of
   the normal one: reconciliation limited to the target pairs, no mode
   filters, `PB_modes` untouched, the cycle sleep standing in for the loop's
   `execution_delay_seconds`. This is the production shape at the cycle
   cadence (D16 item 3 stays for the account-level modes). The repanic
   cooldown refresh is ported as written (hsl:4776) although Python cannot
   reach it.
4. Verification (same standard as D16 item 4): `tools/fake_live_clock.py`
   traces the coin machine (`coin_*` records, RECORDER A), `pb-snapcheck`
   drives `HslState` with the traced per-pair inputs (realized peak / last,
   unrealized pnl, blocking counts; the ledger-derived values are
   cross-checked with the stale-ledger classification, flatten lookups
   too), recomputes the start-up replay from `fills.json` + candles, asserts
   every pair state and forced mode after every check / supervisor
   iteration, and rebuilds the protective recordings with
   `build_protective` for the traced targets. Fixture `grid_v7_hsl_coin`
   (config: `grid_v7_hsl` in coin mode with `coin_overrides.BTC` red 0.3).
5. Not ported: the replay-matrix cache, the background / partial replay
   (`mark_protective_ready`), latch files and events, operator runtime
   forced modes (the runner's `runtime_forced` map is written only by the
   coin machine), coin overrides of `n_positions` (Python reads the global
   `bot_value`).
6. `live::realized_pnl_cumsum` (SPEC 5.2) takes the fill manager's fee
   normalisation (`hsl::FeePolicy::signed_fee_paid`: zero / missing fee ->
   `live.fee_pct_fallback` x notional, outliers beyond
   `fee_pct_sanity_abs_max` replaced) so the realized-pnl series and the
   HSL ledger agree with Python's `fill_event_net_pnl` (closes D16 item 5
   and D17 item 4; `pb-mockrun` seeded2 engine inputs 0/400 -> 400/400).

## D21 (2026-09-08) Trailing candles are fetched from the position anchor, not read out of the warmup buffer

The two live `8rs` bots (paper2 `467146583`, `452425891`) cancelled every
resting order on their XRP position and then planned nothing at all for 330 /
51 cycles: `ideal=0 cancels=4 creates=0 warnings=1`, an unmanaged position with
no close order.

1. Cause. `live.rs` folded the trailing bundle out of the warmup 1m buffer.
   `trailing_bundle` requires the window to start exactly at the first full
   minute after the position-change anchor (the newest fill of that side, else
   the exchange position timestamp), so an anchor older than the buffer yields
   `None` -> `trailing_available = false`. The engine turns that into
   `StrategyInputUnavailable` (`orchestrator.rs:2482-2497`) and emits NO orders
   for the side; the reconciler then cancels everything, and nothing ever
   refetches the missing history, so the state never heals. Both bots were past
   the edge: last fill 3070 min old against a 2872-minute warmup, and 5621 min
   against 2340.
2. Python (`passivbot.py:9588-9610`, SNAPSHOT_SPEC 4.1 step 3) instead issues
   one `get_candles_with_resolution_ladder(symbol, start_ts=anchor + 1m,
   end_ts=None, strict=False)` per symbol that needs trailing, however old the
   anchor is. The spec recorded this; the implementation had substituted the
   warmup buffer, and the substitution was invisible while the anchor happened
   to fall inside it.
3. Decision: `LiveRunner::ensure_trailing_candles` runs each cycle before the
   snapshot is built and backfills 1m candles from the anchor for every side
   that needs trailing (`is_trailing` and a non-zero position), widening that
   symbol's buffer via `trailing_floor_ms` / `m1_keep` so later refreshes do
   not trim the history away again, capped by
   `live.max_memory_candles_per_symbol`. A failed or short backfill is
   non-fatal: the side stays unavailable exactly as before, and now says so in
   the log.
4. Not ported: Python's coarse-resolution prefix for very old anchors
   (`approximate old candle prefix`, pb:9637-9660). Exact 1m is fetched for as
   far back as 40 pages reach (~200k candles); beyond that the side degrades to
   unavailable with a warning instead of silently.
5. Why the harness missed it: plancheck / snapcheck / mockrun replay recorded
   inputs, so they pin computation, not input acquisition, and every recording
   started flat -- `tools/record_fake_v8.py` stamped each seeded entry fill at
   `boot_ts - 60_000`, one minute before boot, so the anchor was always inside
   the warmup window. `--seed-fill-age-minutes` (default 1, unchanged) now sets
   that age, and fixture `grid_v7_old_anchor` records the case.
6. Observability, which is what made a 20-minute live idle undiagnosable: the
   cycle line logged only the warning COUNT, and balance, positions,
   tradability and candle depth were never logged. Now the engine's warnings
   are named, an empty plan dumps balance / positions / `tradable` /
   `effective_min_cost` / `symbol_states` / `loss_gate_blocks`, warmup logs the
   candle counts it actually got, and the missing-strategy-input fallback names
   the symbol.

## D22 (2026-09-08) The exchange clock is synced at startup and hourly, and once more after any timestamp rejection

The D21 soak did not survive the night: `pbr-soak` exited `exit=30` at 04:08
UTC after `10002` rejections exhausted its 10/h error budget across 11
restarts. The same error ended the local dry-run against paper2 -- the dev
host's clock was +1021 ms.

1. Cause. Bybit rejects a signed request whose timestamp is more than 1 s
   AHEAD of its server clock; `recv_window` widens only the LATE side, so
   raising it fixes nothing. The runner signed with the raw host clock, so a
   host running a second fast fails EVERY private call -- balance, positions,
   orders. That is not a degraded mode: every cycle becomes an error and the
   runner restarts itself out of its budget and exits.
2. Python never had the problem because ccxt does it: `_build_ccxt_options`
   sets `adjustForTimeDifference` (passivbot.py:2512), which makes ccxt call
   `load_time_difference` and add `options.timeDifference` to every signed
   timestamp; `_maybe_recover_exchange_time_sync` (2580-2615) re-runs it
   whenever `_is_exchange_time_sync_error` recognises the failure. This is the
   fourth gap of the same shape as D21: a behaviour that lived in the library
   Python leaned on, so no spec line ever named it.
3. Decision. `BybitClient` keeps a `time_offset_ms` (`server - local`),
   measured by `sync_time()` against the PUBLIC `/v5/market/time` -- public so
   it still works while the clock is too skewed for a signed call -- taken at
   the midpoint of the round trip so latency does not land in the offset.
   Every signed timestamp, and every `now_ms()` the client uses for request
   windows, goes through it.
4. Retry, once, and only for this error: `signed_with_time_resync` re-syncs
   and repeats the attempt when `is_timestamp_error` matches (`10002`, which
   ccxt maps to `InvalidNonce` and Python matches by type, plus the
   recv-window wordings). Any other rejection must NOT be retried -- a create
   would be sent twice.
5. Cadence: `LiveRunner::sync_exchange_time` runs at startup before the first
   signed call and on the hourly maintenance cycle before the hedge-mode
   re-assert, not per cycle (that would be one wasted request every 2.5 s).
   Failure is non-fatal and not charged to the error budget: the previous
   offset stays.
6. `|offset| >= 500 ms` logs a warning naming the host clock, because only the
   client's REQUESTS are corrected -- `LiveRunner`'s own `wall` clock, which
   decides candle bucketing and cycle timing, still reads the host. The offset
   is a guard against rejection, not a substitute for NTP on the host.
7. Scope, so this is not read as a live outage it was not: the LIVE bots have
   never hit this. Three days of `10002` / `recv_window` across all three
   passivbot log groups: zero. Every occurrence was on the dev box, where the
   Windows Time service is not even running (`w32tm /query /status` ->
   `0x80070426`, service not started), so nothing corrects the clock and it
   free-runs; it was measured at +1021 ms during the paper2 dry-run and ~380
   ms ahead again hours after a manual `w32tm /resync`. The ECS host is an
   EC2 instance whose ECS-optimized AMI ships chrony against the Amazon Time
   Sync Service, and a container shares the host kernel's clock, so the live
   bots inherit a sub-millisecond clock. (That is how the AMI ships, not
   something measured on this host -- `chronyc tracking` over SSM would
   confirm it, and that is a command on the trading host.) D22 is therefore
   insurance: it keeps signing correct if that clock ever does slip (NTP
   down, instance migration, host resume), and it turns a `10002` into one
   re-sync and one retry instead of a cycle error. The reason insurance is
   worth it here is the failure SHAPE, not its likelihood -- a clock problem
   fails every private call at once, so the 10/h error budget empties in
   minutes and the bot exits rather than degrading.

## D23 (2026-09-08) The `8rs` task definition points at a moving version-line tag, so shipping a runner fix needs no terraform

Every pb-runner build so far was rolled out by editing `image_tag` in the
pbtb-rust tfvars, a scoped apply, `telebot-deploy`, and a restart. That
cadence was inherited from the Python image, where it fits: passivbot images
are cut once per upstream release, so pinning each one in terraform records
real history. pb-runner is our own code -- D21 and D22 were both same-day
fixes -- and the pin turned every fix into an infrastructure change.

1. Decision: the image carries a MOVING tag named after the passivbot
   version line it serves (`v810` for v8.1.0), and `passivbot_engines["8rs"]`
   points at that tag. The `image-build` workflow re-points it after each
   build (`promote` job, default on), and ECS re-pulls the tag on every task
   start -- the agent's default `ECS_IMAGE_PULL_BEHAVIOR`, unset on the
   cluster host -- so a fix ships as a build plus a bot restart. Terraform
   keeps what it is actually good at: adding a version LINE (a v8.2.0 or
   v7.1.2 runner gets its own tag and its own engine entry), and owning
   memory, command and family.
2. Provenance, which the pin used to provide for free: `docker/Dockerfile`
   bakes the commit into `PB_RUNNER_BUILD`, and the binary logs
   `pb-runner starting version=… engine_line=… build=<git sha>` before
   anything that can fail. Once a tag moves, that log line is the ONLY record
   of which build a container ran -- the task definition, the ECS console and
   the ECR tag all describe the present, not the task's past.
3. Rollback is re-pointing the tag, so every build also keeps an immutable
   `<git sha>` tag to point back at (RUNBOOK "Shipping a pb-runner fix"). The
   repo's `keep_last_images = 20` bounds how far back that reaches; older
   than that, rebuild from the commit.
4. Accepted cost: an auto-restart after a crash resolves the tag afresh, so a
   bot that dies after a build comes back on the NEW build rather than the
   one it was running. Builds are manual and deliberate, which makes that
   acceptable; `promote: false` builds without making the result deployable
   when it is not.
5. The `promote` job is separate from `build` so it also runs when the build
   was skipped as already-built (unchanged source), and it retags with
   `docker buildx imagetools create` -- a manifest copy inside the registry,
   no pull, no push, no rebuild. NOT `aws ecr batch-get-image | put-image`:
   the manifest that comes back is not byte-identical to what was pushed, so
   ECR stores a SECOND image index under its own digest. The image runs, but
   the moving tag then shares no digest with any `<git sha>` tag, and every
   promote leaves another near-duplicate counting against
   `keep_last_images`. The job asserts the two manifests match before it
   reports success. Corollary: a commit that
   changes nothing under `crates/`, `Cargo.*` or the Dockerfile promotes the
   EARLIER image, so `build=<sha>` names that build, not the commit the
   workflow ran on. That is accurate -- the binary really is the earlier
   build -- but it is not the head commit.
6. One drift check is now VOID, and it is one people were using. Comparing
   the tfvars `image_tag` against the digest running in ECS used to detect a
   build that had been deployed without a matching commit; today both sides
   read `v810` no matter what is running, so the comparison is an identity
   and proves nothing. A second agent had already reached for it on this
   line, correctly refusing to apply because ITS checkout was behind -- the
   right call then, but the criterion it used will now report "aligned"
   forever. What replaces it:
   - Is the deployed build the intended one? The container's first log line,
     `build=<git sha>`, compared with what the last `image-build` run
     promoted. Nothing else can answer this.
   - Has the terraform config drifted from state? `terraform plan` on the
     targets, as for anything else. Note that
     `-target=aws_ssm_parameter.telebot_base_env` always drags
     `module.passivbot_task` in with it, because
     `local.telebot_base_env` embeds `local.passivbot_families`, which is
     derived from that module -- so the smallest honest scope for this line
     is the three targets the RUNBOOK already lists, not the SSM parameter
     alone.
