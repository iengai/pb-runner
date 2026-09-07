# Status

Newest entry first. Each entry: what changed, what was verified, next action.

## 2026-09-08 (worktree agent) — P5.1 closed: mock exchange + `pb-mockrun`, six runs identical in requests and account state

**Changed:** new `crates/runner/src/mock_exchange.rs`: `Scenario` (scripted
`timeline` rows or candle `replay` from `.npy` day files / inline candles,
boot positions / fills / orders, ISO or epoch timestamps) and
`MockExchange: ExchangeClient` mirroring `src/exchanges/fake.py` operation
for operation (fill on the next candle's range or at creation when the
step price crosses, fees, balance, position netting per pside, order and
trade ids seeded by the boot fills, `fetch_open_orders` `(timestamp, id)`
string order, tickers = step price, `manual_fill` / `cancel_open_orders`
actions, request log); `fetch_ohlcv` follows the Bybit client's paging
contract instead of the fake's newest-`limit` quirk. 13 unit tests on the
fill model and the API. New `pb-mockrun` (`bin/mockrun.rs`): `LiveRunner`
+ `Executor` (the `--live` path) drive the mock through a
`tools/record_fake_v8.py` run directory with the harness pacing (one wave
per step, then `advance`; wall clock = scenario time, churn clock = the
recording stem per D13), comparing per step the create/cancel requests
(content, order/id sequence), the open-order set (content and ids),
positions, balance and fill count against `remote_calls.json`,
`step_summaries.json`, `fills.json`, and the final
`fake_exchange_state.json`; `--diff-inputs` diffs the engine input against
the recording. `live.rs` (minimal, separated): `LiveRunner::with_clocks`
(injected wall/monotonic clocks, `new` unchanged in behaviour),
`set_harness_secondary_never_fetched` (harness-only flag feeding
`candles_available`), and the first-minute trailing rule (side
unavailable until a full minute closed after the last fill, Python's
`missing_exact_trailing_candles`). Docs: MOCK_EXCHANGE.md (line-by-line
map to `fake.py`, deviations, results), D17, PLAN P5.1 ticked, README.

**Verified:** `pb-mockrun --diff-inputs`: public grid_v7 600/600 (both
artifact dirs), tm 600/600, seeded2 grid_v7 400/400, tm 400/400, tm8
400/400 (no order in either bot), iter7 400/400 — identical create and
cancel request sets, identical create order/id sequences and cancel id
sets, identical open-order sets and ids, positions, balances and fill
counts at every step, final state identical, 0 planning errors, 0 write
failures. Engine inputs identical 600/600 on both public runs; on the
four seeded runs the only differing field is
`global.realized_pnl_cumsum_last` (`0.0` vs `-0.0530015256`: Python's
`fee_pct_fallback` on the fee-less seeded boot fills, D17.4, no order
affected). Control runs fail as they should: `--no-harness-compat` on
public grid_v7 267/600 requests, open-order set 43/600 (the runner rotates
forager entries the harnessed Python bot could not, D17); `--gate-clock
cycle` on iter7 398/400 requests, open-order set 367/400 (D13).
Extra run with fills (`.local/fake_v8_fills/grid_v7`, grid_v7, 150 steps, `--seed-positions 3 --seed-entry-offset -0.03`, positions 3 % in profit): the close-grid orders cross at creation at step 1 (3 maker fills: ADA 233 @ 0.6425 pnl 4.423272, BTC 0.001 @ 110050 pnl 3.31217, DOGE 769 @ 0.19482 pnl 4.434823, fees 0.0150/0.0110/0.0150), positions go flat, fresh initial entries follow; `pb-mockrun --diff-inputs` 150/150 identical requests, open-order sets and ids, positions, fill counts and balances (final 1012.129308092 bit-identical), final state identical; engine inputs differ only in `realized_pnl_cumsum_{last,max}`: Python's series stays at the boot-fill fee fallback (-0.079479763 / 0.0) because the harness primes the fill cache once and the bot never calls `fetch_my_trades` (zero such calls in every run's `remote_calls.json`), while the runner refetches fills and its series reaches 12.129308092; no order affected (flat positions, no unstuck).
`cargo fmt --all --check`, `cargo clippy --workspace --all-targets -D
warnings`, `cargo test --workspace` (98 tests; new: 13 mock_exchange).

**Limits:** none of the six recorded runs produced a live fill, so their
balance/position parity is trivial; the fill model rests on the unit tests
and the extra fills run above. The mock cannot exercise market orders,
partial fills or exchange errors (the fake exchange does not model them
either). Not ported: `fee_pct_fallback` on fee-less fills.

**Next action:** HSL equity state machine (in flight in a parallel
worktree, `snapshot.rs`); P5.2 shadow run against the abot account; P6.

## 2026-09-08 (worktree agent) — SNAPSHOT_SPEC 8 gaps closed except HSL

**Changed:** `crates/runner/src/snapshot.rs`: `CycleState` (previous
`PB_modes`, dynamic forager eligibility, close-EMA carry-forward cache,
cooled symbols) passed as `&mut` to `build`; `Snapshot.mode_overrides` and
`pb_modes_after_cycle` (`_python_mode_from_orchestrator_state`); verbatim
ports of `normal_planning_psides`, `dynamic_forager_normal_psides`,
`dynamic_forager_managed_entry_psides`, `flat_forager_default_normal`,
`candidate_only`, `required_ema_can_mark_nontradable`; the missing
required-forager rule now raises on `!can_mark_nontradable` (was
"priority") plus the cache-only rule; exchange-cooldown planning policy
(`cooldown_mode`) and flat-symbol tradability; close-EMA carry-forward
(`close_ema_fallback_max_age_ms`) and open-tail projection wiring.
`crates/runner/src/emas.rs`: `open_tail_gap`, `open_tail_rows`,
`projected_ema` (= `cm.get_projected_open_tail_ema_metrics`).
New `crates/runner/src/cooldown.rs` (`ExchangeCooldowns`, config
validation, Bybit classifier = `None` as in v8.1.0). `execute.rs`
`WaveReport.write_failures`; `live.rs` owns `CycleState` +
`ExchangeCooldowns`, `note_write_failures` (called from `main.rs`);
`pb-snapcheck` replays `PB_modes` from the previous recording's output.
Tools: `record_fake_v8.py` decodes the harness output as UTF-8 (cp932
crash after a complete run, RECORDER pitfall 6). New fixture set
`tests/fixtures/recordings/fake_v8/grid_v7_forced` (30 cycles) from
`tests/fixtures/configs/fake_v8/grid_v7_forced.json` (grid_v7 +
`coin_overrides.{ADA,BTC,DOGE}.live.forced_mode_long` = gs / tp_only / m,
`--seed-positions 3`). Docs: SNAPSHOT_SPEC 2.3, 3.6, 8; PLAN P4.1/P4.2;
RECORDER section A; D15.

**Verified:** `pb-snapcheck` identical on grid_v7 30/30, grid_v7_seeded
30/30, tm 30/30, tm_seeded 30/30, grid_v7_forced 30/30 (full local run
400/400: ADA graceful_stop = 399 closes + 376 grid re-entries and no
initials, BTC tp_only = closes only, DOGE manual = no orders), and on the
full 600-cycle public runs grid_v7 600/600, tm 600/600. `pb-plancheck`
unchanged: public grid_v7 600/600, tm 600/600; seeded grid_v7, tm, tm8,
iter7 400/400 each. `cargo fmt --check`, clippy `-D warnings`,
`cargo test --workspace` (71 tests; new: 3 cooldown, 2 emas, 7 snapshot).
Evidence for (d): the fake harness primes every coin's full 1m array each
step, so the last closed minute is never missing; neither the carry-forward
nor the projection fires in any fake run (600/600 with and without the
code). Their behaviour is covered by unit tests mirroring pb:18687-18830
and cm:9221-9400.

**Not modelled (D15):** HSL modes (separate task), runtime operator forced
modes, `ineligible_symbols`, cached forager-metric fallback and forager
stale-tail context (runner skips the cycle with an error where Python
would rank on stale metrics), EMA-entry-cancellation order keys,
entry-cooldown position-delta guard.

**Next action:** HSL equity state machine (SPEC 2.3 steps 1-2, D15 item 5),
then the P5.2 shadow run; the market-distance filter is in flight in a
parallel worktree (`reconcile.rs`/`live.rs`).

## 2026-09-08 (worktree agent) — pre-create market snapshot gate + distance filter ported (RECONCILE_SPEC 2.10)

**Changed:** new `crates/runner/src/market_filter.rs`: `MarketSnapshot`
(bid/ask/last + local `fetched_ms`), `SnapshotProvider` (Python
`MarketSnapshotProvider`, bulk strategy: cache within `max_age_ms`, one bulk
`fetch_tickers`, one retry fetch, `Incomplete`/`Fetch` errors),
`planning_snapshot_invalid`, `snapshot_signature_invalid`,
`MarketFilter::{from_config, filter_by_market_distance, filter_fresh_creations}`
with Python's log lines and the hourly INFO throttle; 13 unit tests
mirroring `tests/test_passivbot_balance_split.py` and
`tests/test_fresh_entry_eligibility_integration.py`. `OrderRec.market_distance`
(`_churn_gate_market_distance`). `reconcile()` lost its churn argument and
ends at the recent-execution guard; new `reconcile::admit_and_cap` = churn
admission (reading `market_distance`) + create capacity + attempt
bookkeeping. `LiveRunner` owns the snapshot cache: planning tickers come
from `SnapshotProvider::get_snapshots` with the 5 s fetch TTL, the
pre-create gate re-reads it with the 10 s hard TTL, then `admit_and_cap`.
`pb-plancheck` applies the gate + distance filter per step on the recorded
price and prints the skip count. `Plan.skipped_market_snapshot` /
`skipped_market_distance`, logged by `pb-runner` as `skipped`.
SPEC: new 2.10, 4.3 item moved to "ported", section 5 item 3 resolved;
PLAN P4.3 note; D14.

**Facts:** max age is the constant 10 000 ms (`md.py:656`), not config;
freshness compares `utc_ms()` at the check against the *local receive time*
of the fetch; the Bybit connector drops the ccxt ticker `timestamp`
(`ccxt_bot.py:1219`), so no ticker timestamp is needed in the Rust client.
Python's call order is barrier/guards -> market filter -> churn admission ->
capacity (`exe.py:958-975`); the churn admission takes its market distance
from the filter. Whole-cycle skips drop market orders too; the distance
filter exempts them and symbols without a valid snapshot; `t == 0` disables
the skip but still annotates.

**Verified:** `pb-plancheck` grid_v7 600/600 (both artifact dirs), tm
600/600, seeded2 grid_v7 400/400, tm 400/400, tm8 400/400, iter7 400/400,
all with 0 market-filter skips (the fake ticker is `bid=ask=last=price`
and `utc_ms` is pinned, so snapshots are never stale; no
`far-from-market` / `skipping order creation` line in any `fake_live.log`,
no `create_skipped` event in any `live_events.json`). `cargo fmt --check`,
clippy `-D warnings`, `cargo test --workspace`. Not exercised: a real stale
or failed ticker refresh on the live account (dry-run only).

**Next action:** unchanged: (2) HSL / cooldown / runtime-forced modes with
a seeded fake run; (3) long local dry-run of the container against the abot
account; P5.2 shadow run, P6. Optional: a fake scenario with a price jump
> 80 % between steps to exercise the distance skip end to end.

## 2026-09-07 (worktree agent) — order churn gate ported (RECONCILE_SPEC 2.9)

**Changed:** `crates/runner/src/churn.rs` (`ChurnParams::from_config`,
`ChurnGate`: evidence `evaluate`, admission `admit`, `record_attempts`,
`monotonic_seconds`; 20 unit tests mirroring `tests/test_order_churn_gate.py`
and the admission arithmetic). `OrderRec.churn_evidenced`; `reconcile()`
takes `Option<(&mut ChurnGate, f64)>`: admission runs after the
recent-execution guard and before the creation capacity, and the creates
left in the plan are recorded as attempts (exempt ones too, as Python does
for every submitted create; `execute.rs` submits `plan.creates` as-is).
`LiveRunner` owns the gate, evaluates the executable ideals before
reconciliation and derives the risk-phase pairs (`risk_active_pairs`:
risk-critical orders + `loss_gate_blocks`). `pb-plancheck` runs the gate per
step in order (`--gate-clock wall|cycle`, `--churn-trace`).

**Verified:** `pb-plancheck` grid_v7 600/600, tm 600/600, seeded grid_v7
400/400, tm 400/400, tm8 400/400, iter7 400/400 (was 367/400). Python's
own `order.churn_evidence` reason counts and `order.churn_admission`
rolling counts (the last 2000 events each run kept) equal the Rust trace
cycle for cycle: iter7 50/50 evidence + 36/36 rolling, tm8 60/60,
grid_v7 52/52 + 14/14. `cargo fmt`, clippy `-D warnings`,
`cargo test --workspace`.

**Fact (D13):** the fake harness does not pin `time.monotonic()`, so the
Python gate ran on wall-clock time in every recorded run (~0.8 s per step,
`rolling_usage=90` at the first deferral). plancheck feeds the recording
stem's wall-clock ms as the gate clock; `--gate-clock cycle` (60 s per step)
gives the old 367/400 on iter7.

**Next action:** unchanged (P5.2 shadow run, P6); the market-distance
filter (SPEC 4.3) is still open.

## 2026-09-07 (session 3) — P2 recordings, Bybit client, snapshot builder, reconcile, dry-run loop, image

Long entry; the "Next action" block at its end is the resume point.

**Mandate (user, this session):** advance until a pure-Rust bot can be
deployed on pbtb-rust; open decisions go to a same-tier subagent for review;
no upstream PR for the rlib plumbing; repo is public and stays so
(memory: pb-runner-mandate). History was rewritten to remove the company
account name; origin/master = 26b3789 + this session's commits.

**Done so far:**
- Recorder patch applied (uncommitted) to `E:\projects\passivbot-rlib-v8.1.0\src\passivbot.py`.
- Tools: `tools/record_fake_v8.py` (replay scenario + harness driver + MANIFEST,
  `--seed-positions`), `tools/fake_live_clock.py` (pins all clocks to fake
  time, ccxt-like `fetch_ohlcv` paging, vectorised candle priming),
  `tools/select_fixtures.py` (subsample), `tools/make_public_configs.py`
  -> `tests/fixtures/configs/fake_v8/{grid_v7,tm}.json`. Details and the
  five pitfalls in RECORDER.md section A. Decision D10 (public configs for
  committed fixtures; private recordings stay in gitignored `.local/`).
- Private recordings (600 cycles each, 2025-08-01..10-28 replay, boot day 84):
  iter7, iter12, tm8 -> diffcheck 1800/1800 ok. Only
  `entry_initial_normal_long` orders appeared (no fills in 10 h of replay).
- Detached jobs running (`.local/fake_v8/run_*.sh|log`): public configs
  (grid_v7, tm; 600 cycles) then seeded batch (grid_v7, tm, iter7, tm8;
  400 cycles, 2 seeded long positions 2% under water).

**Next:** when jobs finish: diffcheck each set; `select_fixtures.py` public
sets (stride 20, max 60) + seeded public sets into
`tests/fixtures/recordings/fake_v8/{grid_v7,tm,grid_v7_seeded,tm_seeded}`;
diffcheck committed sets; tick P1.2/P2.1/P2.2; commit+push. Then P3.1:
ccxt HEAD on 2026-09-07 = `11f45ee2bf0d2f809c318761c717415268da27c0`;
rust/ tree = workspace {ccxt, ccxt-base, ccxt-pro, ccxt-prediction, tests}, 56 MB.
P3.1 facts: `transpiled-base` compiles all 209 exchanges (no per-exchange
feature); dev build of ccxt-base+ccxt+ccxt-pro = 7 min 22 s on 32 threads,
debug target 12 GB, ccxt-base rlib 2.5 GB. Typed Bybit API covers every call
the Python bot uses (`&mut self`, `crate::Result<T>`; set_leverage /
set_position_mode / set_margin_mode are untyped core calls; errors carry the
ccxt kind string). Python-side semantics recorded in PORT_INVENTORY section 3.
Adjudication A (full ccxt dep) / B (hand-written v5 client) / C (vendored
bybit slice) delegated to a subagent (brief in scratchpad `p3_brief.md`);
verdict recorded as D11 (hand-written client).
P3.2 done: `crates/exchange-bybit` = signing, envelope/error classes,
parsers (numbers via `str::parse`), `ExchangeClient` impl; 15 unit tests.
P3.4 done: `examples/readonly_probe.rs` and `tools/probe_python_ccxt.py`
(ccxt 4.5.66, the version passivbot pins) agree on every stable field for
the abot account (`tools/compare_probes.py`). Two parity facts learned and
encoded: ccxt 4.5.66 gives `limits.cost.min = None` for Bybit linear so
passivbot's `min_cost` is always 0.1 (`or 0.1`); fee rates are ccxt's
describe defaults 0.0001 / 0.0006, not 0.0002 / 0.00055. Rust probe ~0.9 s
vs Python ~3.7 s for the same 7 calls. P3.3 (fills) is implemented as
`fetch_fills` (`/v5/execution/list`); private WS deferred.
`docs/SNAPSHOT_SPEC.md` (893 lines, subagent) is the P4.2 field-by-field spec.
P4.2 started: `crates/runner/src/bot_params.rs` (`ConfigView`) reproduces
`global_bot_params`, per-symbol `bot_params` and `strategy_params` exactly
(JSON Value equality, int vs float preserved) for every committed grid_v7
and tm fixture recording. Facts encoded: hjson parses integral literals as
ints (`0.0` -> `0`), the loader fills missing keys from the v8.1.0 template
(`crates/runner/assets/template_v8.1.0.json`), forager weights are
normalised at load, `wallet_exposure_limit = round(twel/n_positions, 8)`.
P4.2 snapshot builder done for the fake sets: `emas.rs` (window = ceil(span)
closed buckets, candle fields rounded to float32 like `CANDLE_DTYPE`, engine
`ema_last_f64`, provisional/strict gap policies, 1h aggregation) and
`snapshot.rs` (universe, modes steps 4-7, spans per strategy, tradability
with the forager cache-only rule, peek hints, incumbents, global). Acceptance
tool `pb-snapcheck` (parses recordings with correctly-rounded floats, D8;
replays the fake exchange's timeline gap fill): grid_v7 30/30, tm 30/30,
grid_v7_seeded 30/30 identical. Gaps to close before P5 (SPEC section 8):
HSL/cooldown/runtime-forced modes, `PB_modes` carry-over for tradability,
close-EMA 10-min carry-forward, open-tail projection, trailing from real
fills, entry-cooldown fill timestamps, realized-pnl cumsum from fills.
Fixture sets now: grid_v7, tm (unseeded), grid_v7_seeded, tm_seeded (boot
positions + boot fills: closes, grid entries, cropped entries); snapcheck
identical on all 120 recordings (trailing bundles from fill anchors with
float32 candles, `is_trailing` rule). Live loop skeleton `live.rs` +
`pb-runner --dry-run --once` verified on the abot account (read-only key):
one cycle 4.3 s. `docs/RECONCILE_SPEC.md` (819 lines, subagent) written;
P4.3 `reconcile.rs` and P4.4 `execute.rs` implement its "minimal faithful
subset"; `live.rs` keeps `PB_modes`, closed-pnl history, realized-pnl
cumsum and entry-cooldown timestamps; `startup.rs` downloads config/keys
from S3 with the contract's exit codes. `pb-runner --dry-run --once` on the
abot account now prints the reconciled plan (cancels of the live v7 bot's
XRP orders + 2 entries, expected with a different config). All four
seeded-with-fills private sets: 400/400 diffcheck.
`pb-plancheck` (P5.1-lite): the reconcile plan equals the Python bot's
actual create/cancel requests on 2400/2400 cycles of five full runs; the
sixth (seeded iter7) differs on 33/400 cycles where Python's order churn
gate deferred far grid entries. `jsonexact` module = exact float parser for
recordings. Churn gate ported by a subagent and merged (entry above, D13):
all six local runs 2800/2800 identical plans.
P6: `docker/Dockerfile` + `deploy/buildspec.yml`; the same Dockerfile built
locally for linux/amd64 (63.2 MB image, 1m49s cold build); dry-run loop in
the container against the abot account, grid_v7 config, 10 symbols, 22
cycles: 19-20 MiB RSS (Python bot ~430 MB). Release binary 16.9 MB on
Windows. pbtb-rust: D7 resolved by D12 (subagent-written branch
`feat/pb-runner-runtime`, PR https://github.com/iengai/pbtb-rust/pull/35,
reviewed here: engine keys `<major>[rs]`, bot attribute `runtime`,
`/runtime` command, ECR repo `pb-runner`, `8rs` entry commented out until
the image exists; not merged, nothing applied).

**Not done / gaps:** market-distance filter (RECONCILE_SPEC 4.3); HSL,
cooldown and runtime-forced modes, close-EMA carry-forward, open-tail
projection (SNAPSHOT_SPEC 8); live execution never exercised with a trading
key; arm64 image never built (CodeBuild); P5.2 shadow run in ECS; P6.4
write-back check; P7 (v7 line). The passivbot worktree
`E:\projects\passivbot-rlib-v8.1.0` still carries the uncommitted recorder
patch and the Windows fcntl patch (keep them out of the fork branch).

**Next action (autonomous):** P5.2-prep and remaining SNAPSHOT_SPEC gaps
in this order: (1) market-distance filter + pre-create snapshot freshness
(SPEC 3.1 step 8) so the runner is safe with real money; (2) HSL /
cooldown / runtime-forced modes with a seeded fake run that exercises
them; (3) a long local dry-run of the container against the abot account
(hours) to catch drift, error-budget and reconnect behaviour.
**Needs the user:** merge PR #35 and apply Terraform (ECR repo, later the
`8rs` task definition); create the pb-runner CodeBuild project and run the
first arm64 build (P6.2); approve the ECS shadow task (P2.4/P5.2) and the
small-capital live run on a separate sub-account (P5.3).

## 2026-09-07 (session 2) — P1 done except the P2-gated box

**Remote (added later the same day):** `origin` = public repo
`https://github.com/iengai/pb-runner`, branch `master`. All commits are
authored as the private identity (AGENTS.md "Git identity").

**State:** P1.1 done and pushed (`iengai/passivbot` branch
`pb-runner/rlib-v8.1.0`, commit `e808cfd33` = tag v8.1.0 + build plumbing).
P1.2 implemented; the `passivbot_rust` git dependency is enabled in
`Cargo.toml` (locked to `e808cfd33`), `diffcheck --features engine` replays
recordings. P1.3 decided (D9). 153 synthetic recordings committed under
`tests/fixtures/recordings/synthetic_v8/` with `MANIFEST.json`.

**Verified this session:**
- Engine crate on the branch: `cargo build --no-default-features`,
  `cargo test --no-default-features` (254 passed), `cargo build` (default),
  `cargo fmt --check`, `pip wheel . --no-deps`, and
  `pytest tests/test_coin_filtering.py tests/test_candle_interval.py`
  (29 passed with the rebuilt extension; the same two files show 3
  "extension appears stale" failures in the *main* checkout because its root
  `passivbot_rust.pyd` has no fingerprint stamp: pre-existing, not ours).
- pb-runner gates: fmt, clippy `-D warnings` (default and `--features engine`),
  `cargo test --workspace`.
- `diffcheck --features engine --dir tests/fixtures/recordings/synthetic_v8`:
  153 ok / 0 failed, identical in dev and release (lto thin, cgu 1) profiles.
  Negative test: corrupting a recorded float by one ulp is reported with the
  differing byte offset.

**Facts gathered (see D8, D9, RECORDER.md "Pitfalls"):**
- serde_json best-effort float parsing caused 7 false mismatches; fixed by
  byte-exact text comparison. Rule: serde_json stays at the wheel's version
  (1.0.151), no `float_roundtrip`.
- The five pre-compute input validators live in `python.rs` (feature-gated);
  the runner must replicate them at P4.3.
- Dev-box layout: passivbot venv is Python 3.12 at
  `E:\projects\passivbot\.venv` (system `python` is 3.14, unusable for pyo3
  0.21); the site-packages `passivbot_rust` there is a stale build. The P1.1
  worktree `E:\projects\passivbot-rlib-v8.1.0` has the fresh extension in
  `src/` plus the uncommitted Windows `fcntl` patch (keep it uncommitted).
  Remote `iengai` was added to `E:\projects\passivbot`.

**Next action:** P2.1/P2.2: produce fake-exchange recordings from the
worktree (apply the RECORDER.md patch to its `src/passivbot.py` locally, or
reuse the plugin's wrapper), for the three configs named in PLAN P2.2, into
`tests/fixtures/recordings/fake_v8/`; run diffcheck on them; then tick P1.2
and P2.2. Probe done: the fake exchange is selected by
`live.fake_scenario_path` (`src/exchanges/fake.py:196`); scenario examples
are in `tests/test_run_fake_live.py` and `tests/test_fake_exchange.py`; the
configs are in the main checkout `E:\projects\passivbot\strategy_lab\configs\`
(`cap1000_iter7_highreturn.json`, `cap1000_iter12_alt_balanced.json`, and
`cap1000_iter8_tm_regime26.json` for trailing_martingale), not in the
worktree.

**Open questions for the user:** unchanged (D7; approval for P2.4 and P5.3).
Optional: open the upstream PR for the rlib plumbing (P1.1 last bullet).

## 2026-09-07 — P0 skeleton created

**State:** P0 done. No upstream dependency is enabled yet (both `passivbot_rust`
and `ccxt` git deps are commented in `Cargo.toml`). Nothing has been pushed;
the repo is local at `E:\projects\pb-runner` with one commit.

**Verified:** `cargo build --workspace`, `cargo test --workspace`,
`cargo run -p pb-runner -- <v8 config>` accepts a v8.1.0 config and refuses a
v7 one; `diffcheck` exits 2 on the empty fixtures dir.

**Facts gathered this session (sources in DECISIONS/CONTRACT):**
- pyo3 coupling in the engine crate is confined to `python.rs` (106 refs),
  `coin_selection.rs` (16), `utils.rs` (5), `lib.rs` (5), `types.rs` (4);
  `orchestrator.rs` and all strategy modules have none.
- Both v7.12.0 and v8.1.0 expose `compute_ideal_orders_json` /
  `OrchestratorInput`.
- ccxt official Rust port merged 2026-09-03 (bybit included, git-dep only).
- barter-rs has no Bybit execution client (mock + Binance placeholder only).
- pbtb-rust container contract: env `BUCKET/USER_ID/BOT_ID`, entrypoint
  downloads config + api-keys from S3, exec `python src/main.py configs/$BOT_ID.json`.

**Next action:** P1.1 — create the rlib feature branch of `passivbot-rust`
at tag v8.1.0 in the `iengai/passivbot` fork (see PLAN.md for the exact
edits and acceptance checks). Do it in a separate worktree of
`E:\projects\passivbot`, not on `v8.1.0-eval`.

**Open questions for the user (not blocking P1-P2):**
- D7 runtime selection in pbtb-rust (needed at P6).
- Approval for P2.4 shadow recording task and P5.3 small-capital run.
