"""Subsample a recordings directory into a committable fixture set.

Full fake-exchange runs produce one ~70 KB input per planning cycle; 600
cycles x 3 configs would be >100 MB. This keeps the informative cycles:

- every cycle whose output order set differs from the previous kept cycle
  (state transitions: new entries, fills, closes, trailing updates), and
- every `--stride`-th cycle regardless,

capped at `--max` per source (transitions first, then evenly spaced
regulars). `MANIFEST.json` from the source is copied with the selection
parameters appended.

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

    transitions: list[Path] = []
    regulars: list[Path] = []
    prev_key = None
    for idx, in_path in enumerate(ins):
        out_path = in_path.with_name(in_path.name.replace(".in.json", ".out.json"))
        key = order_key(out_path)
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
    }
    (dst / "MANIFEST.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    total = sum(p.stat().st_size for p in dst.glob("*.json"))
    print(f"{src} -> {dst}: kept {len(selected)}/{len(ins)} "
          f"({len(transitions)} transitions available), {total/1e6:.1f} MB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
