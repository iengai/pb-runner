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
`live.fake_scenario_path`) and a harness that drives the bot through it one
planning cycle per candle (`src/tools/run_fake_live.py`). Recordings from it
contain no real account data (balance is the scenario's, positions are
fake fills).

`tools/record_fake_v8.py` (this repo) does the whole thing: builds a
`replay` scenario for the config's approved coins from the dev box's cached
Bybit 1m candles (`E:\projects\passivbot\historical_data\ohlcvs_bybit`,
daily `.npy`), writes a config copy with two overrides
(`live.minimum_coin_age_days = 0`, `live.user = fake_<name>`), runs the
harness through `tools/fake_live_clock.py`, dedupes identical inputs and
writes `MANIFEST.json`. `tools/select_fixtures.py` subsamples the result
(order-set transitions + every n-th cycle) into `tests/fixtures/recordings/fake_v8/<name>/`.

Which configs (D10): the committed set is recorded from the public configs
in `tests/fixtures/configs/fake_v8/` (`tools/make_public_configs.py`,
upstream default parameters). The private `strategy_lab` configs are
recorded the same way into the gitignored `.local/` and only their diffcheck
result is reported.

```bash
python tools/make_public_configs.py --checkout E:/projects/passivbot-rlib-v8.1.0
python tools/record_fake_v8.py --checkout E:/projects/passivbot-rlib-v8.1.0 \
  --python E:/projects/passivbot/.venv/Scripts/python.exe \
  --candles E:/projects/passivbot/historical_data/ohlcvs_bybit \
  --config grid_v7=tests/fixtures/configs/fake_v8/grid_v7.json \
  --config tm=tests/fixtures/configs/fake_v8/tm.json \
  --dates 2025-08-01:2025-10-28 --boot-index 120960 --max-steps 600 --out .local/fake_v8_public
python tools/select_fixtures.py --src .local/fake_v8_public/grid_v7/recordings \
  --dst tests/fixtures/recordings/fake_v8/grid_v7 --stride 20 --max 60
cargo run -p pb-diffcheck --features engine -- --dir tests/fixtures/recordings/fake_v8/grid_v7
```

Things learned on 2026-09-07 (all handled by the two tools; none of them
changes tracked passivbot files):

1. **Clock.** The harness redirects `bot.get_exchange_time` and the candle
   manager's `_now_ms_callback` to scenario time, but `utils.utc_ms` and
   `candlestick_manager._utc_now_ms` are still wall clock and
   `get_completed_candle_health` is often called without `now_ms`, so a
   replay older than the EMA windows is judged stale and every coin becomes
   non-tradable. `fake_live_clock.py` rebinds those names in every imported
   module to the fake client's `now_ms`.
2. **History length.** Hourly EMAs (`volatility_ema_span_hours` = 1467 h in
   the cap1000 configs) require the full window *before* the boot candle;
   `warmup_ratio` does not relax this. Use ~89 days of candles with
   `boot_index` at day 84 (`--dates 2025-08-01:2025-10-28 --boot-index 120960`).
3. **Fake `fetch_ohlcv` paging.** With `since` + `limit` the fake returns
   the *newest* `limit` rows, ccxt returns the *oldest*; the manager pages
   forward, so windows longer than 1000 hourly candles kept a hole at the
   start. The wrapper restores ccxt semantics.
4. **Coin age.** `is_old_enough` compares the first replay candle with "now";
   with the clock pinned that is ~84 days, below `minimum_coin_age_days`
   (365), hence the config override.
5. **Speed.** `_prime_fake_candles` rebuilds the 1m array row by row per
   step; with 120k rows x 10 coins that is >1M Python iterations per cycle.
   The wrapper swaps in a vectorised version (same output).
6. **Console codepage (2026-09-08).** The harness prints UTF-8; on a
   Japanese Windows box `subprocess.run(text=True)` decoded it as cp932 and
   raised after a complete 400-cycle run, losing `run.log`, the dedupe and
   `MANIFEST.json` (the recordings themselves were intact). The tool now
   decodes with `encoding="utf-8", errors="replace"`.

Forced-mode set (2026-09-08): `tests/fixtures/configs/fake_v8/grid_v7_forced.json`
is `grid_v7.json` plus `coin_overrides.{ADA,BTC,DOGE}.live.forced_mode_long`
= `gs` / `tp_only` / `m`, recorded with `--seed-positions 3` (positions on
exactly those coins) and 400 steps into `.local/fake_v8_forced`, subsampled
to `tests/fixtures/recordings/fake_v8/grid_v7_forced` (stride 20, max 30).
It exercises `_apply_entry_eligibility_mode` with per-symbol config modes on
held positions: graceful_stop (closes + grid re-entries, no initials),
tp_only (closes only), manual (no orders).

HSL set (2026-09-08): `tests/fixtures/configs/fake_v8/grid_v7_hsl.json` is
`grid_v7.json` with `bot.long.hsl.enabled = true`, `red_threshold 0.06`,
`ema_span_minutes 15`, `cooldown_minutes_after_red 120`,
`no_restart_drawdown_threshold 1`, `live.hsl_signal_mode = unified`,
`live.hsl_position_during_cooldown_policy = panic`, recorded with
`--boot-index 102000 --max-steps 400 --seed-positions 3 --seed-we 0.3` into
`.local/fake_v8_hsl` (boot at 2025-10-10 20:00 UTC, right before the
Oct 10 crash: ADA 0.767 -> 0.585, DOGE 0.232 -> 0.181 within two hours).
`tools/fake_live_clock.py` writes `hsl_trace.jsonl` next to the recordings
when `PB_RUNNER_HSL_TRACE` is set (`record_fake_v8.py` sets it): one JSON
line per HSL event (`init`, `check_begin`/`check_end`, `sample`,
`supervisor_begin`/`counts`/`sync_flat`/`supervisor_end`, `finalize`,
`reset`, `cooldown_handle`, `compute` with the input hash and the symbol
list) with the Python inputs and side states; `fills.json` is the fake
exchange's fill ledger. `select_fixtures.py` keeps both (trace compacted:
`sample` records and `before` states dropped) plus a `scenario.json`
without the candle file list, treats HSL state changes as transitions, and
`pb-snapcheck` replays the trace through `hsl.rs` (D16). Two harness
properties to know: the fill ledger is never refreshed after boot (Python's
`realized_pnl` stays at the boot value; the runner reads every fill, the
check reports those deviations as "stale ledger"), and the boot index
leaves only 1690 h of history, so the 1909 h log-range span is short and
every 1h EMA map is empty (Python's all-or-nothing rule, now ported).

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

### C. Synthetic: passivbot's own orchestrator tests (offline, committed)

`tools/pb_recorder_plugin.py` is a pytest plugin that wraps
`compute_ideal_orders_json` (same wrapper as the patch above) and discards
calls that raise. Run it from the checkout that has the freshly built
extension in `src/` (P1.1 worktree `E:\projects\passivbot-rlib-v8.1.0`):

```bash
cd E:/projects/passivbot-rlib-v8.1.0
PYTHONPATH=E:/projects/pb-runner/tools \
PB_RUNNER_RECORD_DIR=E:/projects/pb-runner/tests/fixtures/recordings/synthetic_v8 \
  E:/projects/passivbot/.venv/Scripts/python.exe -m pytest -p pb_recorder_plugin \
  tests/test_orchestrator_json_api.py tests/test_orchestrator_integration.py \
  tests/test_unstucking_safeguards.py tests/test_missing_ema_fix.py \
  tests/test_order_churn_gate.py tests/test_passivbot_balance_split.py -q
```

Result on 2026-09-07: 154 calls recorded (153 unique), 50 discarded. The
plugin also writes `MANIFEST.json` (passivbot commit, extension path and
source fingerprint). These inputs are hand-built by the tests (no account
data) and are committed. They exercise the JSON schema and many strategy
branches but are not a substitute for A/B recordings.

## Pitfalls (learned the hard way)

1. **Stale extension.** `import passivbot_rust` resolves to
   `.venv/Lib/site-packages` unless `src/` is first on `sys.path`; that copy
   can be an older build (its `runtime_build_info()["source_fingerprint"]`
   differed from `rust_utils.source_fingerprint()` on 2026-09-07). Always
   check the fingerprint in `MANIFEST.json` against the checkout, and never
   trust `tests/conftest.py` to fix the path for a plugin loaded with `-p`.
2. **Float text, not float values.** Compare recordings as bytes; parse the
   input with `serde_json::from_str`. See DECISIONS D8.
3. **Building the wheel.** `pip wheel . --no-deps` works on Windows with
   `PYO3_PYTHON=<venv>/Scripts/python.exe` and
   `PASSIVBOT_RUST_SOURCE_FINGERPRINT=$(python -c 'from rust_utils import source_fingerprint; print(source_fingerprint())')`
   (run with `PYTHONPATH=src`). Copy the `.pyd` out of the wheel into `src/`
   and write `<pyd>.rust-src-sha256` with the fingerprint, otherwise
   `verify_loaded_runtime_extension` raises "appears stale" in some tests.

## Scrubbing

`tools/scrub.py` (to write, P2.3) replaces `balance`/`balance_raw` with a
fixed value and rescales `position.size` proportionally so the recording
stays internally consistent but reveals no account size. Only scrubbed or
fake-exchange recordings are committed.
