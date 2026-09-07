# Status

Newest entry first. Each entry: what changed, what was verified, next action.

## 2026-09-07 (session 2) — P1 done except the P2-gated box

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
