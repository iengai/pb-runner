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
  selection parameters.

Anything else must be scrubbed with `tools/scrub.py` (P2.3) before it is
un-ignored.

Check: `cargo run -p pb-diffcheck --features engine -- --dir tests/fixtures/recordings/synthetic_v8`
