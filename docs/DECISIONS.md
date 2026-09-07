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
  is how it chooses Python image vs pb-runner image *within* a line (D7, open).

## D7 (open) Runtime selection in pbtb-rust

Options, to be decided at P6 with the user:
1. New engine keys in `passivbot_engines` (e.g. `"8rs"`) plus a per-bot
   `runtime` attribute in DynamoDB, default `py`.
2. Replace the image of an engine line wholesale once the shadow run passes.
Option 1 is safer (per-bot opt-in, instant rollback by flipping the
attribute). Nothing in this repo depends on the choice.
