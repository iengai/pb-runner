# pb-runner

A Rust live runner ("shell") for passivbot strategies on Bybit USDT-linear
perpetuals. It replaces the Python half of passivbot (`src/passivbot.py`,
ccxt, numpy/pandas) with a small tokio process, while the strategy math keeps
coming from the **pinned, unmodified** `passivbot_rust` engine crate.

Goal: one bot from ~430 MB RSS down to tens of MB, so one ECS host holds many
more bots, with behaviour that is provably identical to the Python bot
(differential testing against recorded orchestrator calls, and a snapshot
builder that reproduces the recorded engine inputs byte for byte).

**Not** a strategy fork. **Not** the control plane (that is
[pbtb-rust](https://github.com/iengai/pbtb-rust): Telegram bot, Lambda, Terraform).

## Read this first (new session)

1. `docs/STATUS.md` — where the project is right now and the next action.
2. `docs/PLAN.md` — phased plan with acceptance criteria; tick boxes as you go.
3. `AGENTS.md` — working rules and the map of sibling repos.
4. `docs/DECISIONS.md` — why things are the way they are (do not re-litigate
   without new facts).
5. `docs/SNAPSHOT_SPEC.md`, `docs/RECONCILE_SPEC.md` — field-by-field
   descriptions of what the Python bot does (the port's reference).

## Layout

```
Cargo.toml                 workspace; passivbot_rust pinned to the fork's rlib branch
crates/snapshot/           on-disk format of recorded orchestrator calls
crates/diffcheck/          bin pb-diffcheck: replay recordings through the pinned engine
crates/exchange-bybit/     ExchangeClient trait + hand-written Bybit v5 client (D11)
crates/runner/             lib + bins: pb-runner (live loop), pb-snapcheck (P4.2 acceptance)
  src/bot_params.rs        config -> engine BotParams / strategy params (D9)
  src/emas.rs, snapshot.rs OrchestratorInput builder (SNAPSHOT_SPEC)
  src/reconcile.rs         cancel/create plan (RECONCILE_SPEC)
  src/execute.rs, live.rs  order waves, state, the loop; startup.rs = S3 contract
docker/, deploy/           arm64 image + CodeBuild spec, same container contract as the Python image
tools/                     recorder / fixture tooling (Python), read-only Bybit probes
tests/fixtures/configs/    public configs the committed recordings were made with
tests/fixtures/recordings/ synthetic_v8 (engine tests) and fake_v8 (fake-exchange replays)
docs/                      PLAN, CONTRACT, DECISIONS, PORT_INVENTORY, RECORDER, *_SPEC, STATUS
```

## Engine lines

One binary/image per passivbot engine line (cargo feature `engine-v8` /
`engine-v7`). v8.1.0 broke the v7 config schema, and a strategy is only
proven on the engine it was validated on, so legacy v7.12.0 configs get their
own runner build. See `docs/DECISIONS.md` D6 and `docs/CONTRACT.md`.

## Build and check

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings

# engine replay must report 0 failures on every committed fixture set
cargo run -p pb-diffcheck --features engine -- --dir tests/fixtures/recordings/fake_v8/grid_v7

# snapshot builder must rebuild every fake_v8 recording identically (needs the
# dev box's Bybit 1m candle cache, see docs/RECORDER.md)
cargo run -p pb-runner --bin pb-snapcheck -- --config tests/fixtures/configs/fake_v8/grid_v7.json \
  --recordings tests/fixtures/recordings/fake_v8/grid_v7 \
  --candles E:/projects/passivbot/historical_data/ohlcvs_bybit --dates 2025-08-01:2025-10-28

# plan against a live account with a read-only key, never sends anything
cargo run -p pb-runner -- config.json --once --api-keys api-keys.json
cargo run -p pb-runner -- config.json --check-only
```

Toolchain is pinned by `rust-toolchain.toml` (1.95; the AWS SDK needs 1.94+).
`--live` is the only way to send orders; the container starts in dry-run.
