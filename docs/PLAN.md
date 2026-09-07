# Plan

Phases are sequential; each has acceptance criteria that must be verified,
not assumed. Tick a box only after running the check. Line 8 (v8.1.0) is
done first end-to-end; line 7 (v7.12.0) follows the same steps afterwards
(D6).

Effort guesses are for orientation only.

## P0 — Skeleton (done 2026-09-07)

- [x] Workspace with `snapshot`, `exchange-bybit`, `diffcheck`, `runner` crates; builds and tests pass.
- [x] Docs: README, AGENTS, DECISIONS D1-D7, CONTRACT, RECORDER, PORT_INVENTORY, STATUS.
- [x] Runner refuses configs of another engine line; feature flags `engine-v8` / `engine-v7`.

## P1 — Engine as a library (line 8)  ~1 day

- [x] **P1.1 rlib branch.** (done 2026-09-07: commit `e808cfd33` on
      `iengai/passivbot`, branch pushed; upstream PR not opened yet.) In the `iengai/passivbot` fork (check it exists:
      `gh repo view iengai/passivbot`; fork if not), branch
      `pb-runner/rlib-v8.1.0` from tag `v8.1.0`. Changes in `passivbot-rust/`:
      - `Cargo.toml`: `crate-type = ["cdylib", "rlib"]`; make `pyo3`, `numpy`
        optional; feature `python = ["dep:pyo3", "dep:numpy"]`; keep
        `default = ["python", "extension-module", "abi3-py312"]` so the Python
        wheel build is unchanged.
      - `lib.rs`: `#[cfg(feature = "python")] mod python;` and the `#[pymodule]`;
        make `orchestrator`, `types`, `strategies`, `risk`, `constants`,
        `utils`, `coin_selection`, `equity_hard_stop_loss` `pub mod`.
      - `coin_selection.rs` (16 pyo3 refs), `utils.rs` (5), `types.rs` (4):
        gate the `_py` wrappers / `#[pyclass]` derives behind the feature.
      - Acceptance: `cargo build --no-default-features` and
        `cargo build` (default) both succeed in `passivbot-rust/`;
        `pip wheel . --no-deps` still builds; `pytest tests/test_coin_filtering.py tests/test_candle_interval.py -q` passes in the checkout.
      - Push the branch (account `iengai`). Optionally open an upstream PR
        titled "passivbot-rust: optional pyo3 feature + rlib crate-type".
- [ ] **P1.2 diffcheck engine replay.** (implemented 2026-09-07; 0 failures on
      `tests/fixtures/recordings/synthetic_v8` = 153 calls recorded from
      passivbot's own orchestrator tests, RECORDER.md section C. Comparison is
      byte-exact on the output JSON text, see D8. Box stays open until the P2
      fixtures exist.) Enable the commented `passivbot_rust`
      git dependency in `Cargo.toml` (branch `pb-runner/rlib-v8.1.0`,
      `default-features = false`), implement `replay()` under feature
      `engine` in `crates/diffcheck/src/main.rs`: deserialize
      `OrchestratorInput`, run `compute_ideal_orders`, compare `orders`
      (exact on symbol_idx/pside/order_type/execution_type, qty/price bitwise
      equal), print the first differing order.
      - Acceptance: `cargo run -p pb-diffcheck --features engine -- --dir <recordings>`
        reports 0 failures on recordings produced by P2.
- [x] **P1.3 config types.** (2026-09-07: D9 - the Rust crate exposes the
      target types; the config -> `BotParams` resolution is Python and must be
      ported inside P4.) Decide whether `passivbot_rust` exposes enough
      (`BotParams`, `BotParamsPair`, `ExchangeParams`, strategy params) to
      parse the live config without re-implementing `config/schema.py`.
      Record the finding in DECISIONS (D8).

## P2 — Recordings  ~0.5 day

- [ ] **P2.1** Apply the recorder patch (docs/RECORDER.md) to the v8.1.0
      checkout locally (do not commit it there).
- [ ] **P2.2** Produce fake-exchange recordings for at least: one
      trailing_grid_v7 config with forager on (cap1000_iter7_highreturn),
      one with coin_overrides (cap1000_iter12_alt_balanced), one
      trailing_martingale config (cap1000_iter8_tm_regime26). Per D10 the
      private configs are recorded into the gitignored `.local/` only; the
      committed set under `tests/fixtures/recordings/fake_v8/` is recorded
      from the public configs in `tests/fixtures/configs/fake_v8/`
      (trailing_grid_v7 + forager + coin_overrides, trailing_martingale +
      forager), unseeded and with seeded open positions
      (`tools/record_fake_v8.py --seed-positions`).
- [ ] **P2.3** `tools/scrub.py` for real recordings (balance normalisation).
- [ ] **P2.4** (needs user approval) real-market recordings via a shadow ECS
      task with the patch; keep private; run diffcheck on them.
      - Acceptance for P2: diffcheck 0 failures on both fixture sets.

## P3 — Exchange adapter (Bybit v5 linear, hand-written; D11)  ~2-3 days

- [x] **P3.1** Evaluate the ccxt Rust port: pinned candidate
      `11f45ee2bf0d2f809c318761c717415268da27c0`, measured build cost, API
      shape, `float_roundtrip` conflict with D8. Verdict (independent review,
      D11): hand-written client; ccxt deps stay out of `Cargo.toml`.
- [x] **P3.2** (done 2026-09-07: 15 unit tests, clippy clean) Implement `ExchangeClient` for Bybit v5 in
      `crates/exchange-bybit`: instruments-info -> `MarketSpec` (USDT linear
      only), wallet-balance (UNIFIED formula), position/list (cursor, 200),
      order/realtime (cursor, 50, `positionIdx` side), tickers, kline 1m
      (limit 1000, <= 5 pages), order/create (one request per order,
      `PostOnly|GTC`, `orderLinkId`, `reduceOnly`), order/cancel ("already
      gone" codes), set-leverage / switch-mode / switch-isolated (ignore
      110025/110026/110043). HMAC-SHA256 signing, recv_window, numbers parsed
      from strings. Error classification into `ExchangeError` from `retCode`.
      - Acceptance: unit tests with recorded JSON fixtures for every parser and
        signing test vectors; `cargo clippy -D warnings`.
- [ ] **P3.3** Order/position/fill updates: REST polling (`execution/list`,
      `closed-pnl` for P4.1 fills). Private WS deferred until a measured need.
- [x] **P3.4** (done 2026-09-07: `examples/readonly_probe.rs` vs `tools/probe_python_ccxt.py`, `tools/compare_probes.py` = 0 stable-field differences on the abot account: 751 markets, BTC spec incl. min_cost/fees, XRP position, 3 open orders) Read-only integration test against Bybit using the abot read-only key from the dev box (`E:\projects\passivbot\api-keys.json` entry `415196485`): markets/balance/positions/open orders/tickers/ohlcv. No order placement.
      - Acceptance: test passes; field mapping cross-checked against what `exchanges/bybit.py` produces for the same account (dump both, diff).

## P4 — Runner loop  ~1 week

- [ ] **P4.1** (partial 2026-09-07: `crates/runner/src/live.rs` keeps hysteresis balance, 1m/1h candle buffers, fills since `pnls_max_lookback_days`, trailing anchors. 2026-09-08: previous-cycle engine states now carried as `snapshot::CycleState` (`PB_modes`, dynamic forager eligibility, close-EMA carry-forward cache) plus `cooldown::ExchangeCooldowns` fed by `WaveReport.write_failures`. Missing: entry-cooldown position-delta guard, fill-confirmation state, forager cached-metric cache.) State model: balance (with hysteresis), positions, open orders, per-symbol candle buffers, fills since start, forager metrics cache.
- [x] **P4.2** (2026-09-07: `crates/runner/src/{bot_params,emas,snapshot}.rs`; `pb-snapcheck` rebuilds every committed fake_v8 recording identically: grid_v7 30/30, tm 30/30, grid_v7_seeded 30/30. 2026-09-08: SNAPSHOT_SPEC 8 gaps closed except HSL — config forced modes verified by the new `grid_v7_forced` set (per-symbol `gs`/`tp_only`/`m` on seeded positions, 30/30 committed, 400/400 full run), exchange-unavailable cooldowns (`cooldown.rs`, dormant on Bybit), `PB_modes` carry-over for tradability, close-EMA carry-forward and open-tail projection (`emas.rs`), all with unit tests; 120/120 committed + 600/600 on both full public runs. Still open: HSL modes, runtime forced modes, cached forager-metric fallback, entry-cancellation keys.) Snapshot builder: port section 2 of PORT_INVENTORY.md into `crates/runner/src/snapshot.rs`, producing `OrchestratorInput`.
      - Acceptance (the key test): for each real recording, feed the runner the same market state (positions/balance/open orders/candles reconstructed from the recording's timestamp) and assert the built `OrchestratorInput` equals the recorded input field-by-field. Start with the fake-exchange set.
- [x] **P4.3** (2026-09-07: `crates/runner/src/reconcile.rs` = RECONCILE_SPEC subset 4.1: conversion + reduce-only trim, open-order normalisation, exact 8-key match, PB_modes filters, tolerance max-matching, market-distance sort, batch limits, cancel-first barrier, recent-execution guard; 6 unit tests. Churn gate ported 2026-09-07: `churn.rs`, 20 unit tests mirroring `tests/test_order_churn_gate.py`, clock per D13. Pre-create market snapshot gate + `limit_order_create_max_market_dist_pct` filter ported 2026-09-08: `market_filter.rs` (SPEC 2.10, D14), 13 unit tests; `reconcile()` now ends at the recent-execution guard and `admit_and_cap` applies churn admission + create capacity after the filter, as in `exe.py:958-975`; plancheck unchanged, 0 skips.) Post-processing + reconciliation: port `parse_and_validate_rust_orchestrator_output`, `calc_orders_to_cancel_and_create`, recently-cancelled guard, order churn gate.
- [x] **P4.4** (2026-09-07: `crates/runner/src/execute.rs` = cancels then creates, gathered, already-gone cancels ok, 10/h error budget -> exit 30 for the supervisor; not yet exercised with a trading key) Execution: cancels then creates, batch sizes, retry policy mirroring `tests/test_ccxt_retry_policy.py`.
- [x] **P4.5** (2026-09-07: `startup.rs` S3 download via aws-sdk-s3 with exit codes 10/20/21/22 when BUCKET/USER_ID/BOT_ID are set, tracing logs, ctrl-c shutdown, exchange configuration at start; toolchain pinned to 1.95 for the AWS SDK) Startup: S3 download of config/keys per CONTRACT.md, exit codes, structured logging, graceful shutdown on SIGTERM (ECS stop).
- [x] **P4.6** (2026-09-07: `--check-only` opt-in, `--dry-run` default with `--once`; verified against the abot account: 10 symbols, warmup 2957 x 1m + 2482 x 1h candles, 43 fills, one planning cycle in 4.3 s producing 2 entry orders) `--check-only` becomes opt-in; `--dry-run` flag (plan and log orders, never send) added.

## P5 — Paper and shadow  ~1-2 weeks wall clock

- [ ] **P5.1** (partial 2026-09-07: `pb-plancheck` replays a full fake-exchange run's request log and compares the runner's cancel/create plan with the Python bot's actual requests per cycle: grid_v7 600/600, tm 600/600, seeded grid_v7 400/400, tm 400/400, tm8 400/400 identical; seeded iter7 400/400 once the churn gate was ported (367/400 before: the 33 differing cycles were far grid entries Python's gate deferred). Full mock-exchange loop still open.) Mock `ExchangeClient` driven by the passivbot fake scenarios; runner runs the same scenario as the Python bot; compare order streams cycle by cycle.
- [ ] **P5.2** Shadow run: pb-runner in `--dry-run` against the live account (read-only key) next to the live Python bot for the same config; log planned vs actual orders per cycle; target >= 99% identical order sets over 7 days, every difference explained.
- [ ] **P5.3** Small-capital live run on a separate sub-account (user decision) for 1-2 weeks; compare fills and PnL with the Python bot on the same config.

## P6 — Image and control-plane integration

- [ ] **P6.1** (2026-09-07: `docker/Dockerfile` = rust:1.95 builder + distroless cc nonroot, engine line via `--build-arg ENGINE`; release binary 16.9 MB on Windows x64 (thin LTO, cgu 1); local linux/amd64 image built with the same Dockerfile: 63.2 MB image, 1m49s cold cargo build; dry-run loop against the abot account with the grid_v7 config (10 symbols, 22 cycles) holds at 19-20 MiB RSS via docker stats, vs ~430 MB for the Python bot. Still unverified: the arm64 build itself, which CodeBuild does (P6.2, user-triggered).) `docker/Dockerfile`: multi-stage, arm64, static or distroless; measure RSS.
- [ ] **P6.2** (2026-09-07: `deploy/buildspec.yml` written, mirrors pbtb-rust's; ECR repo `pb_runner` added by the pbtb-rust branch of P6.3; CodeBuild project creation and the first build are user actions.) ECR repo + build pipeline (reuse pbtb-rust CodeBuild pattern; user triggers builds).
- [x] **P6.3** (2026-09-07: D7 resolved by D12, option 1. Branch `feat/pb-runner-runtime` on `iengai/pbtb-rust`, PR https://github.com/iengai/pbtb-rust/pull/35: engine keys `<major>[rs]`, bot attribute `runtime`, `/runtime` command, ECR repo `pb-runner`, `8rs` task-def entry commented out until the image exists. Merge, apply and deploy are user actions.) pbtb-rust: decide D7, add task-def family/families, log groups; PR from account `iengai`.
- [x] **P6.4** (2026-09-08: verified. `pbtb-rust/deploy/passivbot-image/entrypoint.sh` only runs two `aws s3 cp` downloads (exit 20/21/22) then `exec python src/main.py configs/$BOT_ID.json`; passivbot v8.1.0 `src/` has no boto3/S3 client apart from the unrelated `tools/hyperliquid_s3_fetcher`. Nothing is uploaded; the bot writes only under its own working dir.) Verify "write-back: none" assumption in CONTRACT.md.

## P7 — Line 7 (v7.12.0)

- [ ] Repeat P1-P5 with `engine-v7` (branch `pb-runner/rlib-v7.12.0`), PORT_INVENTORY section 6 filled in first. Recordings from the abot account (read-only key, RECORDER.md option B.2) are the natural real-data source.

## Upgrade procedure (engine tag bump)

1. New branch `pb-runner/rlib-<newtag>` in the fork; rebase the rlib plumbing.
2. Re-record fake-exchange fixtures with the new Python version; run diffcheck.
3. Diff `OrchestratorInput`/`OrchestratorOutput` and PORT_INVENTORY section 2 between tags; port the delta in the snapshot builder; P4.2 acceptance must pass again.
4. Shadow run (P5.2) for at least 3 days.
5. Check `serde_json` version/features against the new tag's
   `passivbot-rust/Cargo.lock` (D8); re-pin with
   `cargo update -p serde_json --precise <ver>` if it moved.
6. Bump version `0.x.y+pb<newtag>`, new image tag, DECISIONS entry.
Skipping any step is a decision to record, not an oversight.
