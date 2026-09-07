# Status

Newest entry first. Each entry: what changed, what was verified, next action.

## 2026-09-07 (session 3, IN PROGRESS) — P2 recordings, P3.1 started

Working notes so a fresh context can resume; finalize at session end.

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
fills, entry-cooldown fill timestamps, realized-pnl cumsum from fills. Detached recording jobs relaunched after a
process restart: `.local/fake_v8/run_rest.sh|log` (public grid_v7, tm; then
seeded grid_v7, tm, iter7, tm8).

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
