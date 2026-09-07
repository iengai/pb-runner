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
  `fills.json` and a trimmed `scenario.json`, see below).

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
`grid_v7_hsl`; the scenario files of the seeded runs live in the gitignored
`.local/` of the recording box, except `grid_v7_hsl/scenario.json`, which
is committed (boot fills and market steps, candle file list removed).

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
