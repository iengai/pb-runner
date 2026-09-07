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
  positions under per-symbol `live.forced_mode_long` = gs / tp_only / m).

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
`tm_seeded`, `grid_v7_forced` for `grid_v7_forced`; the scenario files of
the seeded runs live in the gitignored `.local/` of the recording box.
