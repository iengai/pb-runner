"""Subsample a recordings directory into a committable fixture set.

Full fake-exchange runs produce one ~70 KB input per planning cycle; 600
cycles x 3 configs would be >100 MB. This keeps the informative cycles:

- every cycle whose output order set differs from the previous kept cycle
  (state transitions: new entries, fills, closes, trailing updates),
- every cycle where the HSL side state (tier / red latch / halted) changed,
  when the source carries an `hsl_trace.jsonl` (coin mode: any pair's
  tier / red latch / halted or a runtime forced mode), and
- every `--stride`-th cycle regardless,

capped at `--max` per source (transitions first, then evenly spaced
regulars). `MANIFEST.json` from the source is copied with the selection
parameters appended. An HSL run also gets its full `fills.json` and a
compacted `hsl_trace.jsonl` (the `sample` records and the `before` states,
which `pb-snapcheck` does not read, are dropped; in coin mode also the
account-level `check_*` records and the pair states of the checks between
kept cycles that changed nothing, see `compact_trace`): the trace must stay
complete in its inputs because the Rust state machine is replayed through
every cycle, not only the kept ones.

    python tools/select_fixtures.py --src .local/fake_v8/iter7/recordings \
        --dst tests/fixtures/recordings/fake_v8/iter7 --stride 20 --max 60
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path


def order_key(out_path: Path) -> str:
    data = json.loads(out_path.read_text(encoding="utf-8"))
    orders = data.get("orders") or []
    return json.dumps(
        sorted(
            (o.get("symbol_idx"), o.get("pside"), o.get("order_type"), o.get("qty"), o.get("price"))
            for o in orders
        ),
        sort_keys=True,
    )


def coin_mode_key(rec: dict) -> str:
    """Per-pair `tier/red_latched/halted` plus the runtime forced modes of a
    coin-mode trace record (`after` = `_hsl_coin_states`, `modes` =
    `_hsl_coin_modes`)."""
    parts = []
    for pside in ("long", "short"):
        for symbol, st in sorted((rec.get("after") or {}).get(pside, {}).items()):
            parts.append("%s:%s=%s/%s/%s" % (pside, symbol.split("/")[0], st.get("tier"),
                                             st.get("red_latched"), st.get("halted")))
        forced = ((rec.get("modes") or {}).get("forced") or {}).get(pside, {})
        for symbol, mode in sorted(forced.items()):
            parts.append("%s:%s!%s" % (pside, symbol.split("/")[0], mode))
    return " ".join(parts)


def hsl_mode_keys(src: Path) -> dict[str, list[str]]:
    """`compute` input hash -> HSL side-state keys at that compute (in order)."""
    trace = src / "hsl_trace.jsonl"
    out: dict[str, list[str]] = {}
    if not trace.exists():
        return out
    last = None
    for line in trace.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        kind = rec.get("kind")
        if kind in ("coin_init", "coin_check_end", "coin_iter_end", "coin_supervisor_end"):
            last = coin_mode_key(rec)
        elif kind in ("init", "check_end", "supervisor_end", "sync_flat", "finalize"):
            after = rec.get("after") or {}
            sides = after if "long" in after or "short" in after else {rec.get("pside", "long"): after}
            parts = []
            for pside in ("long", "short"):
                st = sides.get(pside)
                if st is None and last is not None:
                    parts.append(last_parts.get(pside, ""))
                    continue
                if st is None:
                    parts.append("")
                    continue
                parts.append("%s:%s/%s/%s" % (pside, st.get("tier"), st.get("red_latched"), st.get("halted")))
            last_parts = dict(zip(("long", "short"), parts))
            last = " ".join(parts)
        elif kind == "compute":
            out.setdefault(rec["hash"], []).append(last or "")
    return out


def compact_trace(src: Path, dst: Path, kept_hashes: set[str]) -> int:
    """Coin mode carries ten pair states per check (~13 KB): the account-level
    `check_*` records (unused there) and the `after` states of
    `coin_cooldown_handle` (not compared) are dropped, and `coin_check_end`
    keeps its `after` states only for the kept cycles and the cycles where a
    pair state or forced mode changed; every check is still replayed from
    its `coin_check_begin` inputs, the states are asserted where kept."""
    lines = [l for l in (src / "hsl_trace.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
    recs = [json.loads(l) for l in lines]
    coin = any(r.get("kind", "").startswith("coin_") for r in recs)
    # cycle index -> kept (a kept recording's compute belongs to the cycle)
    kept_cycles: set[int] = set()
    cycle = -1
    for rec in recs:
        if rec.get("kind") == "coin_check_begin":
            cycle += 1
        elif rec.get("kind") == "compute" and rec.get("hash") in kept_hashes:
            kept_cycles.add(cycle)
    n = 0
    cycle = -1
    last_key = None
    with (dst / "hsl_trace.jsonl").open("w", encoding="utf-8", newline="\n") as f:
        for rec in recs:
            kind = rec.get("kind")
            if kind in ("sample", "coin_sample"):
                continue
            if coin and kind in ("check_begin", "check_end"):
                continue
            rec.pop("before", None)
            if kind == "coin_check_begin":
                cycle += 1
            elif kind == "coin_cooldown_handle":
                rec.pop("after", None)
            elif kind == "coin_check_end":
                key = coin_mode_key(rec)
                if cycle not in kept_cycles and key == last_key:
                    rec.pop("after", None)
                last_key = key
            elif kind in ("coin_init", "coin_iter_end", "coin_supervisor_end"):
                last_key = coin_mode_key(rec)
            f.write(json.dumps(rec, sort_keys=True, separators=(",", ":")) + "\n")
            n += 1
    return n


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", required=True)
    ap.add_argument("--dst", required=True)
    ap.add_argument("--stride", type=int, default=20)
    ap.add_argument("--max", type=int, default=60)
    args = ap.parse_args()

    src = Path(args.src)
    dst = Path(args.dst)
    ins = sorted(src.glob("*.in.json"))
    if not ins:
        raise SystemExit(f"no recordings in {src}")

    hsl_keys = hsl_mode_keys(src)
    hsl_seen: dict[str, int] = {}
    transitions: list[Path] = []
    regulars: list[Path] = []
    prev_key = None
    for idx, in_path in enumerate(ins):
        out_path = in_path.with_name(in_path.name.replace(".in.json", ".out.json"))
        key = order_key(out_path)
        stem_hash = in_path.name.split("_", 1)[1].split(".")[0]
        keys = hsl_keys.get(stem_hash, [])
        pos = hsl_seen.get(stem_hash, 0)
        hsl_seen[stem_hash] = pos + 1
        key = (key, keys[pos] if pos < len(keys) else (keys[-1] if keys else ""))
        if key != prev_key:
            transitions.append(in_path)
        elif idx % args.stride == 0:
            regulars.append(in_path)
        prev_key = key

    selected = transitions[: args.max]
    room = args.max - len(selected)
    if room > 0 and regulars:
        step = max(1, len(regulars) // room)
        selected += regulars[::step][:room]
    selected = sorted(set(selected))

    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True)
    for in_path in selected:
        out_path = in_path.with_name(in_path.name.replace(".in.json", ".out.json"))
        shutil.copy2(in_path, dst / in_path.name)
        shutil.copy2(out_path, dst / out_path.name)

    extras = {}
    if (src / "hsl_trace.jsonl").exists():
        kept_hashes = {p.name.split("_", 1)[1].split(".")[0] for p in selected}
        extras["hsl_trace_lines"] = compact_trace(src, dst, kept_hashes)
        shutil.copy2(src / "fills.json", dst / "fills.json")
        extras["fills"] = len(json.loads((src / "fills.json").read_text(encoding="utf-8")))

    manifest = {}
    src_manifest = src / "MANIFEST.json"
    if src_manifest.exists():
        manifest = json.loads(src_manifest.read_text(encoding="utf-8"))
    manifest["selection"] = {
        "source_recordings": len(ins),
        "transitions_available": len(transitions),
        "stride": args.stride,
        "max": args.max,
        "kept": len(selected),
        "kept_transitions": len([p for p in selected if p in set(transitions)]),
        **extras,
    }
    (dst / "MANIFEST.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    total = sum(p.stat().st_size for p in dst.glob("*.json"))
    print(f"{src} -> {dst}: kept {len(selected)}/{len(ins)} "
          f"({len(transitions)} transitions available), {total/1e6:.1f} MB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
