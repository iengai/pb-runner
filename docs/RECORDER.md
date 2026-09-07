# Recorder: capturing orchestrator calls from the Python bot

The Python bot funnels every planning cycle through one function:
`passivbot_rust.compute_ideal_orders_json(input_json) -> output_json`
(`E:\projects\passivbot\src\passivbot.py:25` imports it as `pbr`; call sites
at lines ~16638, ~17398, ~20016 in v8.1.0; v7.12.0 has the same API).

Wrapping that one function at import time records every call regardless of
which call site is used. This is a **dev-only monkeypatch**; it is never
committed to the passivbot checkout's tracked files.

## Patch (apply to `src/passivbot.py` right after `import passivbot_rust as pbr`)

```python
# --- pb-runner recorder (dev only; remove before any image build) ---
import os as _pbr_os
_pbr_rec_dir = _pbr_os.environ.get("PB_RUNNER_RECORD_DIR")
if _pbr_rec_dir:
    import hashlib as _pbr_hl
    import time as _pbr_time

    _pbr_orig = pbr.compute_ideal_orders_json
    _pbr_os.makedirs(_pbr_rec_dir, exist_ok=True)

    def _pbr_recording_compute_ideal_orders_json(input_json: str) -> str:
        stem = "%d_%s" % (
            int(_pbr_time.time() * 1000),
            _pbr_hl.sha256(input_json.encode("utf-8")).hexdigest()[:16],
        )
        with open(_pbr_os.path.join(_pbr_rec_dir, stem + ".in.json"), "w", encoding="utf-8") as f:
            f.write(input_json)
        out = _pbr_orig(input_json)
        with open(_pbr_os.path.join(_pbr_rec_dir, stem + ".out.json"), "w", encoding="utf-8") as f:
            f.write(out)
        return out

    pbr.compute_ideal_orders_json = _pbr_recording_compute_ideal_orders_json
# --- end recorder ---
```

File naming matches `crates/snapshot` (`<utc_ms>_<sha256[:16]>.in.json` /
`.out.json`). If the engine raises, only the `.in.json` exists; diffcheck
reports it.

## Producing recordings

### A. Fake exchange (offline, committable)

passivbot ships a scenario-driven fake exchange
(`src/exchanges/fake.py`; selected when the config has
`live.fake_scenario_path`). Recordings from it contain no real account data
and may be committed to `tests/fixtures/recordings/`.

```bash
cd E:/projects/passivbot
PB_RUNNER_RECORD_DIR=E:/projects/pb-runner/tests/fixtures/recordings/fake_v8 \
  .venv/Scripts/python.exe src/main.py <config-with-fake_scenario_path>.json
```

Look at `tests/test_fake_exchange.py` and `tests/fixtures/` in the passivbot
checkout for scenario file examples. Use a real strategy config
(e.g. `strategy_lab/configs/cap1000_iter7_highreturn.json`, v8) with the
fake scenario added.

### B. Real market data (local, NOT committable)

Running the real bot locally needs a trading key, which we do not have on the
dev box (AGENTS.md). Two workable options:

1. Enable the patch inside a **shadow** ECS task (a copy of a live task-def
   with `PB_RUNNER_RECORD_DIR=/tmp/rec` and a sidecar/cron `aws s3 sync` to a
   private prefix). Requires user approval and a temporary image with the
   patch; the recordings contain real balances/positions -> stay private.
2. Run locally with the read-only abot key: planning cycles complete (the
   orchestrator is called before any order placement), order placement then
   fails with a permission error. Noisy but gives real snapshots for the
   abot (v7, XRP) account only.

Prefer A for correctness fixtures and B.1 for a final parity check before
P5.

## Scrubbing

`tools/scrub.py` (to write, P2.3) replaces `balance`/`balance_raw` with a
fixed value and rescales `position.size` proportionally so the recording
stays internally consistent but reveals no account size. Only scrubbed or
fake-exchange recordings are committed.
