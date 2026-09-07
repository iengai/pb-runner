Recorded orchestrator calls (see docs/RECORDER.md) go here as
`<utc_ms>_<input_hash>.in.json` / `.out.json` pairs.

They are gitignored: a recording contains the real account's balance and
positions. Commit only fixtures that were produced by the fake exchange
(`PASSIVBOT_FAKE_EXCHANGE`) or that were scrubbed with `tools/scrub.py`
(to be written, P2.3).
