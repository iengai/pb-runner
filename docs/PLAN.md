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

- [ ] **P1.1 rlib branch.** In the `iengai/passivbot` fork (check it exists:
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
- [ ] **P1.2 diffcheck engine replay.** Enable the commented `passivbot_rust`
      git dependency in `Cargo.toml` (branch `pb-runner/rlib-v8.1.0`,
      `default-features = false`), implement `replay()` under feature
      `engine` in `crates/diffcheck/src/main.rs`: deserialize
      `OrchestratorInput`, run `compute_ideal_orders`, compare `orders`
      (exact on symbol_idx/pside/order_type/execution_type, qty/price bitwise
      equal), print the first differing order.
      - Acceptance: `cargo run -p pb-diffcheck --features engine -- --dir <recordings>`
        reports 0 failures on recordings produced by P2.
- [ ] **P1.3 config types.** Decide whether `passivbot_rust` exposes enough
      (`BotParams`, `BotParamsPair`, `ExchangeParams`, strategy params) to
      parse the live config without re-implementing `config/schema.py`.
      Record the finding in DECISIONS (D8).

## P2 — Recordings  ~0.5 day

- [ ] **P2.1** Apply the recorder patch (docs/RECORDER.md) to the v8.1.0
      checkout locally (do not commit it there).
- [ ] **P2.2** Produce fake-exchange recordings for at least: one
      trailing_grid_v7 config with forager on (cap1000_iter7_highreturn),
      one with coin_overrides (cap1000_iter12_alt_balanced), one
      trailing_martingale config. Commit them under
      `tests/fixtures/recordings/fake_v8/`.
- [ ] **P2.3** `tools/scrub.py` for real recordings (balance normalisation).
- [ ] **P2.4** (needs user approval) real-market recordings via a shadow ECS
      task with the patch; keep private; run diffcheck on them.
      - Acceptance for P2: diffcheck 0 failures on both fixture sets.

## P3 — Exchange adapter (Bybit linear, via ccxt Rust port)  ~2-3 days

- [ ] **P3.1** Pin a ccxt commit (`git ls-remote https://github.com/ccxt/ccxt HEAD` at the time; record it in CONTRACT.md) and enable the `ccxt`/`ccxt-pro` git deps. Confirm `cargo build` time and binary size; if the `transpiled-base` feature is too heavy, evaluate `--no-default-features` + only the bybit module, or fall back per D3.
- [ ] **P3.2** Implement `ExchangeClient` for Bybit: markets, balance (UNIFIED), positions (hedge/one-way), open orders, tickers, 1m OHLCV, batch create (postOnly/reduceOnly, client order ids), batch cancel, set leverage. Error classification into `ExchangeError`.
- [ ] **P3.3** Private WS (orders/positions/executions) via `ccxt-pro` or REST polling fallback with the same interface.
- [ ] **P3.4** Read-only integration test against Bybit using the abot read-only key from the dev box (`E:\projects\passivbot\api-keys.json` entry `415196485`): markets/balance/positions/open orders/tickers/ohlcv. No order placement.
      - Acceptance: test passes; field mapping cross-checked against what `exchanges/bybit.py` produces for the same account (dump both, diff).

## P4 — Runner loop  ~1 week

- [ ] **P4.1** State model: balance (with hysteresis), positions, open orders, per-symbol candle buffers, fills since start, forager metrics cache.
- [ ] **P4.2** Snapshot builder: port section 2 of PORT_INVENTORY.md into `crates/runner/src/snapshot_builder.rs`, producing `OrchestratorInput`.
      - Acceptance (the key test): for each real recording, feed the runner the same market state (positions/balance/open orders/candles reconstructed from the recording's timestamp) and assert the built `OrchestratorInput` equals the recorded input field-by-field. Start with the fake-exchange set.
- [ ] **P4.3** Post-processing + reconciliation: port `parse_and_validate_rust_orchestrator_output`, `calc_orders_to_cancel_and_create`, recently-cancelled guard, order churn gate.
- [ ] **P4.4** Execution: cancels then creates, batch sizes, retry policy mirroring `tests/test_ccxt_retry_policy.py`.
- [ ] **P4.5** Startup: S3 download of config/keys per CONTRACT.md, exit codes, structured logging, graceful shutdown on SIGTERM (ECS stop).
- [ ] **P4.6** `--check-only` becomes opt-in; `--dry-run` flag (plan and log orders, never send) added.

## P5 — Paper and shadow  ~1-2 weeks wall clock

- [ ] **P5.1** Mock `ExchangeClient` driven by the passivbot fake scenarios; runner runs the same scenario as the Python bot; compare order streams cycle by cycle.
- [ ] **P5.2** Shadow run: pb-runner in `--dry-run` against the live account (read-only key) next to the live Python bot for the same config; log planned vs actual orders per cycle; target >= 99% identical order sets over 7 days, every difference explained.
- [ ] **P5.3** Small-capital live run on a separate sub-account (user decision) for 1-2 weeks; compare fills and PnL with the Python bot on the same config.

## P6 — Image and control-plane integration

- [ ] **P6.1** `docker/Dockerfile`: multi-stage, arm64, static or distroless; measure RSS.
- [ ] **P6.2** ECR repo + build pipeline (reuse pbtb-rust CodeBuild pattern; user triggers builds).
- [ ] **P6.3** pbtb-rust: decide D7, add task-def family/families, log groups; PR from account `iengai`.
- [ ] **P6.4** Verify "write-back: none" assumption in CONTRACT.md.

## P7 — Line 7 (v7.12.0)

- [ ] Repeat P1-P5 with `engine-v7` (branch `pb-runner/rlib-v7.12.0`), PORT_INVENTORY section 6 filled in first. Recordings from the abot account (read-only key, RECORDER.md option B.2) are the natural real-data source.

## Upgrade procedure (engine tag bump)

1. New branch `pb-runner/rlib-<newtag>` in the fork; rebase the rlib plumbing.
2. Re-record fake-exchange fixtures with the new Python version; run diffcheck.
3. Diff `OrchestratorInput`/`OrchestratorOutput` and PORT_INVENTORY section 2 between tags; port the delta in the snapshot builder; P4.2 acceptance must pass again.
4. Shadow run (P5.2) for at least 3 days.
5. Bump version `0.x.y+pb<newtag>`, new image tag, DECISIONS entry.
Skipping any step is a decision to record, not an oversight.
