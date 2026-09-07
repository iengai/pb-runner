"""Subsample a recordings directory into a committable fixture set.

Full fake-exchange runs produce one ~70 KB input per planning cycle; 600
cycles x 3 configs would be >100 MB. This keeps the informative cycles:

- every cycle whose output order set differs from the previous kept cycle
  (state transitions: new entries, fills, closes, trailing updates),
- every cycle where the HSL side state (tier / red latch / halted) changed,
  when the source carries an `hsl_trace.jsonl`, and
- every `--stride`-th cycle regardless,

capped at `--max` per source (transitions first, then evenly spaced
regulars). `MANIFEST.json` from the source is copied with the selection
parameters appended. An HSL run also gets its full `fills.json` and a
compacted `hsl_trace.jsonl` (the `sample` records and the `before` states,
which `pb-snapcheck` does not read, are dropped): the trace must stay
complete because the Rust state machine is replayed through every cycle,
not only the kept ones.

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
        if kind in ("init", "check_end", "supervisor_end", "sync_flat", "finalize"):
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


def compact_trace(src: Path, dst: Path) -> int:
    n = 0
    with (dst / "hsl_trace.jsonl").open("w", encoding="utf-8", newline="\n") as f:
        for line in (src / "hsl_trace.jsonl").read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            if rec.get("kind") == "sample":
                continue
            rec.pop("before", None)
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
        extras["hsl_trace_lines"] = compact_trace(src, dst)
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
