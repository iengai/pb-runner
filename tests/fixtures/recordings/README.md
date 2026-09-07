Recorded orchestrator calls (see docs/RECORDER.md) go here as
`<utc_ms>_<input_hash>.in.json` / `.out.json` pairs.

The directory is gitignored by default because a recording from a real
account contains its balance and positions. Two sub-directories are
un-ignored and committed:

- `synthetic_v8/` - produced by passivbot's own orchestrator tests through
  `tools/pb_recorder_plugin.py` (RECORDER.md section C); `MANIFEST.json`
  names the passivbot commit and extension fingerprint.
- `fake_v8/<name>/` - produced by passivbot's fake exchange replaying real
  Bybit 1m candles (RECORDER.md section A) with the *public* configs in
  `tests/fixtures/configs/fake_v8/` (D10), subsampled by
  `tools/select_fixtures.py`; each `MANIFEST.json` records the passivbot
  commit, extension fingerprint, config sha256, overrides, scenario and
  selection parameters. Sets: `grid_v7`, `tm` (unseeded), `grid_v7_seeded`,
  `tm_seeded` (two seeded long positions), `grid_v7_forced` (three seeded
  positions under per-symbol `live.forced_mode_long` = gs / tp_only / m),
  `grid_v7_hsl` (three seeded positions, long-side equity hard stop
  enabled, booted into the 2025-10-10 crash; carries `hsl_trace.jsonl`,
  `fills.json` and a trimmed `scenario.json`, see below), `grid_v7_hsl_coin`
  (the same run in `live.hsl_signal_mode = coin` with a BTC override, D20).

Anything else must be scrubbed with `tools/scrub.py` (P2.3) before it is
un-ignored.

Check: `cargo run -p pb-diffcheck --features engine -- --dir tests/fixtures/recordings/synthetic_v8`

Snapshot parity (P4.2) per fake_v8 set, with the dev box's candle cache:

```bash
./target/debug/pb-snapcheck --config tests/fixtures/configs/fake_v8/<config>.json \
  --recordings tests/fixtures/recordings/fake_v8/<name> \
  --candles E:/projects/passivbot/historical_data/ohlcvs_bybit --dates 2025-08-01:2025-10-28 \
  [--scenario .local/<run>/<name>/scenario.json]   # seeded sets: boot fills -> trailing anchors
```

`<config>` is `grid_v7` for `grid_v7` / `grid_v7_seeded`, `tm` for `tm` /
`tm_seeded`, `grid_v7_forced` for `grid_v7_forced`, `grid_v7_hsl` for
`grid_v7_hsl`, `grid_v7_hsl_coin` for `grid_v7_hsl_coin`; the scenario
files of the seeded runs live in the gitignored `.local/` of the recording
box, except `grid_v7_hsl/scenario.json` and `grid_v7_hsl_coin/scenario.json`,
which are committed (boot fills and market steps, candle file list removed).

`grid_v7_hsl` (27 of 398 cycles; cycle numbers are the full run's, the
kept files are in the same order): the HSL config is enabled on the long
side only and `pb-snapcheck` replays `hsl_trace.jsonl` through `hsl.rs`
(the trace must stay complete, every cycle's check is replayed). Modes by
cycle: 0-58 green (kept 0, 1, 20, 40), 59-65 yellow (kept 59, 60), 66-72
orange -> every long `tp_only` (kept 66), 73-74 red latched -> `panic`
closes on ADA/BTC/DOGE (kept 73, 74), 75-191 halted after the RED stop was
finalized at 21:16 UTC with a 120-minute cooldown -> flat symbols
`graceful_stop` (kept 75, 80, 100, 120, 140, 160, 180), 192-397 reset after
the cooldown (kept 192, 200, ..., 380). No position is re-entered after the
reset within the run. Expected output: `398 recordings, 398 identical` on
the full run, `27 recordings, 27 identical` here, `every traced state
matches`, plus the "stale fill ledger" count (the harness never refreshes
fills after boot, RECORDER A).

`grid_v7_hsl_coin` (25 of 400 cycles, same boot and seeding as
`grid_v7_hsl`; config `grid_v7_hsl` in `live.hsl_signal_mode = coin` with
`coin_overrides.BTC.bot.long.hsl.red_threshold = 0.3`, so the per-coin
machine (`hsl_coin.rs`, D20) sees one coin stay green while two latch red
on a 333 USDT slot budget). Per-coin modes by cycle (long side; kept files
0, 1, 20, 40, 60, 68, 69, 70, 71, 80, 100, 120, ..., 380):
0-57 all green; 58-60 ADA and DOGE yellow (no mode); 61 ADA orange ->
`tp_only` (`tp_only_with_active_entry_cancellation`), 62-67 ADA and DOGE
orange; 68 ADA red latched -> `panic`: the production coin RED supervisor
runs four iterations inside the cycle (two protective-panic recordings
with ADA alone as `symbols` and `long.mode = panic`, the panic limit close
fills at the fake exchange, two flat confirmations at the fill 21:08 UTC,
finalized with a 120-minute cooldown -> ADA `graceful_stop`); 69 DOGE still
orange; 70 DOGE red latched -> the same supervisor sequence (recording with
DOGE alone, finalized at 21:10 UTC) -> DOGE `graceful_stop`; 71-187 ADA and
DOGE halted (`graceful_stop`), BTC green with no override throughout
(drawdown score ~0.03 against its 0.3 threshold); 188 ADA reset after its
cooldown (green, no override), 190 DOGE reset; 191-399 all green, no
re-entry on ADA / DOGE within the run. The trace is compacted
(`select_fixtures.py`): every `coin_check_begin` keeps its per-pair inputs,
the pair states are asserted at the kept and transition cycles, at every
supervisor iteration and at the finalizations. Expected output: `400
recordings, 400 identical`, `every traced state matches`, 87479/87479
floats bit-exact on the full run; `25 recordings, 25 identical` here (the
two protective recordings are among them), 8 realized-pnl inputs and no
flatten lookup explained by the stale ledger.
