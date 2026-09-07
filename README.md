# pb-runner

A Rust live runner ("shell") for passivbot strategies on Bybit USDT-linear
perpetuals. It replaces the Python half of passivbot (`src/passivbot.py`,
ccxt, numpy/pandas) with a small tokio process, while the strategy math keeps
coming from the **pinned, unmodified** `passivbot_rust` engine crate.

Goal: one bot from ~430 MB RSS down to tens of MB, so one ECS host holds many
more bots, with behaviour that is provably identical to the Python bot
(differential testing against recorded orchestrator calls).

**Not** a strategy fork. **Not** the control plane (that is
[pbtb-rust](https://github.com/iengai/pbtb-rust): Telegram bot, Lambda, Terraform).

## Read this first (new session)

1. `docs/STATUS.md` — where the project is right now and the next action.
2. `docs/PLAN.md` — phased plan with acceptance criteria; tick boxes as you go.
3. `AGENTS.md` — working rules and the map of sibling repos.
4. `docs/DECISIONS.md` — why things are the way they are (do not re-litigate
   without new facts).

## Layout

```
Cargo.toml                 workspace; pinned upstream deps are declared (commented) here
crates/snapshot/           on-disk format of recorded orchestrator calls
crates/diffcheck/          bin: replay recordings through the pinned engine, compare orders
crates/exchange-bybit/     ExchangeClient trait + Bybit impl (ccxt Rust port, P3)
crates/runner/             bin: the live loop (P4)
docker/                    arm64 image, same container contract as the Python image
docs/                      PLAN, CONTRACT, DECISIONS, PORT_INVENTORY, RECORDER, STATUS
tests/fixtures/recordings/ recorded calls (gitignored unless scrubbed / fake-exchange)
```

## Engine lines

One binary/image per passivbot engine line (cargo feature `engine-v8` /
`engine-v7`). v8.1.0 broke the v7 config schema, and a strategy is only
proven on the engine it was validated on, so legacy v7.12.0 configs get their
own runner build. See `docs/DECISIONS.md` D6 and `docs/CONTRACT.md`.

## Build

```bash
cargo build --workspace
cargo test --workspace
cargo run -p pb-runner -- path/to/config.json      # check-only until P4
cargo run -p pb-diffcheck -- --dir tests/fixtures/recordings
```

MSRV 1.85. Host toolchain on the dev machine is 1.90.
