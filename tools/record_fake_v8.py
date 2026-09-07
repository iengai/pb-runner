"""Produce fake-exchange recordings for pb-diffcheck (docs/RECORDER.md, section A).

Drives passivbot's own fake-exchange harness (`src/tools/run_fake_live.py` in
the checkout, through `tools/fake_live_clock.py` which pins every clock to
the scenario time) with a real v8 strategy config and a replay scenario built
from locally cached Bybit 1m candles, while the recorder monkeypatch in the
checkout's `src/passivbot.py` (RECORDER.md) writes every
`compute_ideal_orders_json` call to `PB_RUNNER_RECORD_DIR`.

History rule: the boot candle must lie at least the longest hourly EMA window
(e.g. `volatility_ema_span_hours` = 1467 h ~ 61 days for the cap1000
configs) after the first replay candle, or every coin is marked
non-tradable. 89 days of candles with boot at day 84 satisfies this.

Example (dev box):

    python tools/record_fake_v8.py \
        --checkout E:/projects/passivbot-rlib-v8.1.0 \
        --python E:/projects/passivbot/.venv/Scripts/python.exe \
        --candles E:/projects/passivbot/historical_data/ohlcvs_bybit \
        --config iter7=E:/projects/passivbot/strategy_lab/configs/cap1000_iter7_highreturn.json \
        --dates 2025-08-01:2025-10-28 --boot-index 120960 --max-steps 600 \
        --out .local/fake_v8

Per config `<name>` the script writes `<out>/<name>/{config.json,scenario.json}`,
the harness artifacts under `<out>/<name>/artifacts/`, and the recordings plus
a `MANIFEST.json` under `<out>/<name>/recordings/`. The recordings directory is
what `pb-diffcheck --dir` consumes.

Config overrides applied (recorded in MANIFEST.json):
- `live.minimum_coin_age_days = 0`: the fake exchange derives a coin's first
  timestamp from the first replay candle, and `is_old_enough` compares it with
  wall-clock time, so a replay a few months old would exclude every coin from
  forager selection.
- `live.user = fake_<name>`: isolates the harness' per-user caches.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

# Bybit USDT-linear market steps, fetched via public ccxt on 2026-09-07
# (see docs/STATUS.md). Only what the fake exchange's `symbols` section needs.
BYBIT_MARKETS = {
    "BTC": {"qty_step": 0.001, "price_step": 0.1, "min_qty": 0.001},
    "ETH": {"qty_step": 0.01, "price_step": 0.01, "min_qty": 0.01},
    "SOL": {"qty_step": 0.1, "price_step": 0.01, "min_qty": 0.1},
    "XRP": {"qty_step": 0.1, "price_step": 0.0001, "min_qty": 0.1},
    "DOGE": {"qty_step": 1.0, "price_step": 0.00001, "min_qty": 1.0},
    "ADA": {"qty_step": 1.0, "price_step": 0.0001, "min_qty": 1.0},
    "TRX": {"qty_step": 1.0, "price_step": 0.0001, "min_qty": 1.0},
    "XLM": {"qty_step": 1.0, "price_step": 0.0001, "min_qty": 1.0},
    "HBAR": {"qty_step": 1.0, "price_step": 0.00001, "min_qty": 1.0},
    "DOT": {"qty_step": 0.1, "price_step": 0.0001, "min_qty": 0.1},
}
COMMON_MARKET = {"min_cost": 5.0, "contractSize": 1.0, "maker_fee": 0.0001, "taker_fee": 0.0006}


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def date_range(spec: str) -> list[str]:
    start_s, end_s = spec.split(":")
    start = dt.date.fromisoformat(start_s)
    end = dt.date.fromisoformat(end_s)
    if end < start:
        raise SystemExit(f"bad --dates {spec}")
    out = []
    while start <= end:
        out.append(start.isoformat())
        start += dt.timedelta(days=1)
    return out


def approved_coins(config: dict) -> list[str]:
    approved = config["live"]["approved_coins"]
    if isinstance(approved, dict):
        coins = set(approved.get("long") or []) | set(approved.get("short") or [])
    else:
        coins = set(approved)
    return sorted(coins)


def boot_candle(candles_dir: Path, coin: str, dates: list[str], boot_index: int) -> tuple[int, float]:
    """(timestamp_ms, close) of the 1m candle at `boot_index` (files are one UTC day = 1440 rows).

    Reads the `.npy` directly (float64, C order, shape (1440, 6)) so the script
    runs under any Python, numpy or not.
    """
    import ast
    import struct

    day, minute = divmod(boot_index, 1440)
    path = candles_dir / coin / f"{dates[day]}.npy"
    with path.open("rb") as f:
        if f.read(6) != b"\x93NUMPY":
            raise SystemExit(f"{path} is not a .npy file")
        major = f.read(1)[0]
        f.read(1)
        header_len = struct.unpack("<H", f.read(2))[0] if major == 1 else struct.unpack("<I", f.read(4))[0]
        header = ast.literal_eval(f.read(header_len).decode("latin1"))
        if header["descr"] not in ("<f8", "float64") or header["fortran_order"] or header["shape"][1] != 6:
            raise SystemExit(f"{path}: unexpected layout {header}")
        f.seek(minute * 6 * 8, 1)
        row = struct.unpack("<6d", f.read(6 * 8))
    return int(row[0]), float(row[4])


def seed_positions(coins: list[str], candles_dir: Path, dates: list[str], boot_index: int,
                   balance: float, n: int, wallet_exposure: float, entry_offset: float) -> tuple[list[dict], list[dict]]:
    """Long positions on the first `n` coins, sized to `wallet_exposure` of the
    balance at the boot price, with the entry price `entry_offset` above it so the
    position starts under water (exercises closes/grid/unstuck paths at once).
    Each position also gets one boot fill one minute before boot, otherwise the
    bot keeps the position in `position_fill_confirmation_pending` and marks its
    strategy inputs unavailable (no closes or grid entries are planned)."""
    out, fills = [], []
    for coin in coins[:n]:
        step = BYBIT_MARKETS[coin]["qty_step"]
        boot_ts, price = boot_candle(candles_dir, coin, dates, boot_index)
        qty = int((balance * wallet_exposure / price) / step) * step
        qty = max(qty, BYBIT_MARKETS[coin]["min_qty"])
        out.append({
            "symbol": f"{coin}/USDT:USDT",
            "position_side": "long",
            "qty": round(qty, 10),
            "price": round(price * (1.0 + entry_offset), 10),
        })
        fills.append({
            "id": str(len(fills) + 1),
            "order_id": str(1000 + len(fills)),
            "symbol": f"{coin}/USDT:USDT",
            "side": "buy",
            "position_side": "long",
            "amount": round(qty, 10),
            "price": round(price * (1.0 + entry_offset), 10),
            "timestamp": boot_ts - 60_000,
        })
    return out, fills


def build_scenario(name: str, coins: list[str], candles_dir: Path, dates: list[str],
                   boot_index: int, balance: float, positions: list[dict] | None = None,
                   fills: list[dict] | None = None) -> dict:
    symbols = {}
    replay = {}
    for coin in coins:
        if coin not in BYBIT_MARKETS:
            raise SystemExit(f"no market steps for {coin}; extend BYBIT_MARKETS")
        symbol = f"{coin}/USDT:USDT"
        symbols[symbol] = {**BYBIT_MARKETS[coin], **COMMON_MARKET}
        files = []
        for day in dates:
            path = candles_dir / coin / f"{day}.npy"
            if not path.is_file():
                raise SystemExit(f"missing candle file {path}")
            files.append(str(path).replace("\\", "/"))
        replay[symbol] = {"files": files}
    return {
        "name": name,
        "tick_interval_seconds": 60,
        "boot_index": boot_index,
        "account": {"balance": balance, "positions": positions or [], "fills": fills or []},
        "symbols": symbols,
        "replay": {"symbols": replay},
    }


def clean_fake_user_state(checkout: Path, user: str) -> None:
    shutil.rmtree(checkout / "caches" / "fill_events" / "fake" / user, ignore_errors=True)
    # HSL latch files and the replay-matrix cache (hsl:1361): a stale cache
    # would let the start-up replay reuse a previous run's equity series.
    shutil.rmtree(checkout / "caches" / "equity_hard_stop" / "fake" / "replay_matrix" / user,
                  ignore_errors=True)
    for pside in ("long", "short"):
        latch = checkout / "caches" / "equity_hard_stop" / "fake" / f"{user}_{pside}.json"
        if latch.exists():
            latch.unlink()


def dedupe_recordings(rec_dir: Path) -> tuple[int, int]:
    """Keep the first recording per input hash (the stem suffix); drop later duplicates."""
    seen: set[str] = set()
    kept = dropped = 0
    for out_path in rec_dir.glob("*.out.json"):
        if not out_path.with_name(out_path.name.replace(".out.json", ".in.json")).exists():
            out_path.unlink()
            dropped += 1
    for in_path in sorted(rec_dir.glob("*.in.json")):
        digest = in_path.name.split("_", 1)[1].split(".", 1)[0]
        out_path = in_path.with_name(in_path.name.replace(".in.json", ".out.json"))
        if digest in seen or not out_path.exists():
            in_path.unlink()
            if out_path.exists():
                out_path.unlink()
            dropped += 1
            continue
        seen.add(digest)
        kept += 1
    return kept, dropped


def run_one(args, name: str, config_path: Path) -> dict:
    checkout = Path(args.checkout).resolve()
    out_dir = Path(args.out).resolve() / name
    rec_dir = out_dir / "recordings"
    if rec_dir.exists():
        shutil.rmtree(rec_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    original = json.loads(config_path.read_text(encoding="utf-8"))
    config = json.loads(json.dumps(original))
    user = f"fake_{name}"
    overrides = {"live.minimum_coin_age_days": 0, "live.user": user}
    config["live"]["minimum_coin_age_days"] = 0
    config["live"]["user"] = user
    cfg_out = out_dir / "config.json"
    cfg_out.write_text(json.dumps(config, indent=2), encoding="utf-8")

    coins = approved_coins(config)
    dates = date_range(args.dates)
    candles_dir = Path(args.candles).resolve()
    positions, fills = [], []
    if args.seed_positions > 0:
        positions, fills = seed_positions(coins, candles_dir, dates, args.boot_index, args.balance,
                                          args.seed_positions, args.seed_we, args.seed_entry_offset)
    scenario = build_scenario(name, coins, candles_dir, dates,
                              args.boot_index, args.balance, positions, fills)
    scn_out = out_dir / "scenario.json"
    scn_out.write_text(json.dumps(scenario, indent=2), encoding="utf-8")

    clean_fake_user_state(checkout, user)
    artifacts = out_dir / "artifacts"
    if artifacts.exists():
        shutil.rmtree(artifacts)
    rec_dir.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env["PYTHONPATH"] = str(checkout / "src")
    env["PB_RUNNER_RECORD_DIR"] = str(rec_dir)
    # HSL state-machine trace (fake_live_clock.py) next to the recordings;
    # empty when HSL is disabled, consumed by `pb-snapcheck --hsl-trace`.
    env["PB_RUNNER_HSL_TRACE"] = str(rec_dir / "hsl_trace.jsonl")
    wrapper = Path(__file__).resolve().with_name("fake_live_clock.py")
    cmd = [args.python, str(wrapper), str(cfg_out), str(scn_out),
           "--max-steps", str(args.max_steps), "--output-dir", str(artifacts),
           "--log-level", str(args.log_level)]
    print(f"[{name}] running: {' '.join(cmd)}", flush=True)
    t0 = time.time()
    # The harness prints UTF-8 (symbol names, box drawing); decoding with the
    # console codepage (cp932 on a Japanese Windows box) raised after a
    # complete 400-cycle run on 2026-09-08 and lost the MANIFEST.
    proc = subprocess.run(cmd, cwd=str(checkout), env=env, text=True,
                          encoding="utf-8", errors="replace",
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    elapsed = time.time() - t0
    (out_dir / "run.log").write_text(proc.stdout, encoding="utf-8")
    if proc.returncode != 0:
        print(proc.stdout[-4000:])
        raise SystemExit(f"[{name}] harness failed with exit code {proc.returncode}")
    recorded = len(list(rec_dir.glob("*.in.json"))) if rec_dir.exists() else 0
    kept, dropped = dedupe_recordings(rec_dir) if rec_dir.exists() else (0, 0)
    # The fake exchange's fill ledger (harness artifact) is the fill history
    # `pb-snapcheck` derives the HSL realized pnl from; keep it with the
    # recordings so the committed fixture is self-contained.
    hsl_trace = rec_dir / "hsl_trace.jsonl"
    hsl_trace_lines = sum(1 for _ in hsl_trace.open(encoding="utf-8")) if hsl_trace.exists() else 0
    if hsl_trace_lines == 0 and hsl_trace.exists():
        hsl_trace.unlink()
    for fills_path in artifacts.glob("*/fills.json"):
        shutil.copyfile(fills_path, rec_dir / "fills.json")

    fingerprint = None
    for stamp in (checkout / "src").glob("passivbot_rust*.rust-src-sha256"):
        fingerprint = stamp.read_text(encoding="utf-8").strip()
    manifest = {
        "producer": "tools/record_fake_v8.py + passivbot src/tools/run_fake_live.py (fake exchange, replay)",
        "passivbot_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=checkout, text=True).strip(),
        "passivbot_describe": subprocess.check_output(["git", "describe", "--tags", "--always"], cwd=checkout, text=True).strip(),
        "extension_source_fingerprint": fingerprint,
        "python": args.python,
        "config_name": name,
        "config_source": str(config_path),
        "config_source_sha256": sha256_file(config_path),
        "config_overrides": overrides,
        "scenario": {
            "coins": coins,
            "candle_dates": [dates[0], dates[-1]],
            "boot_index": args.boot_index,
            "balance": args.balance,
            "max_steps": args.max_steps,
            "seeded_positions": positions,
        },
        "calls_recorded": recorded,
        "unique_inputs_kept": kept,
        "duplicates_dropped": dropped,
        "hsl_trace_lines": hsl_trace_lines,
        "harness_seconds": round(elapsed, 1),
        "recorded_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    (rec_dir if rec_dir.exists() else out_dir).joinpath("MANIFEST.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[{name}] done in {elapsed:.0f}s: recorded={recorded} kept={kept} dropped={dropped}", flush=True)
    return manifest


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--checkout", required=True, help="passivbot checkout with the recorder patch and fresh extension in src/")
    ap.add_argument("--python", required=True, help="python.exe of the passivbot venv (3.12)")
    ap.add_argument("--candles", required=True, help="dir with <COIN>/<YYYY-MM-DD>.npy 1m candles")
    ap.add_argument("--config", action="append", required=True, metavar="NAME=PATH")
    ap.add_argument("--dates", required=True, help="YYYY-MM-DD:YYYY-MM-DD inclusive")
    ap.add_argument("--boot-index", type=int, default=4320)
    ap.add_argument("--max-steps", type=int, default=400)
    ap.add_argument("--balance", type=float, default=1000.0)
    ap.add_argument("--seed-positions", type=int, default=0,
                    help="open long positions on the first N approved coins at boot")
    ap.add_argument("--seed-we", type=float, default=0.15, help="wallet exposure per seeded position")
    ap.add_argument("--seed-entry-offset", type=float, default=0.02,
                    help="seeded entry price relative to boot close (0.02 = 2%% above, under water)")
    ap.add_argument("--log-level", type=int, default=1)
    ap.add_argument("--out", default=".local/fake_v8")
    args = ap.parse_args()

    for spec in args.config:
        name, _, path = spec.partition("=")
        if not path:
            raise SystemExit(f"--config expects NAME=PATH, got {spec}")
        run_one(args, name, Path(path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
