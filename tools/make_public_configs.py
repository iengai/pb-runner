"""Derive public v8 configs for the committed fake_v8 fixtures.

The committed fake-exchange recordings must not embed anyone's optimized
strategy parameters (this repository is public), so the fixture configs are
derived from passivbot's own example configs and the engine's default
strategy parameters:

- `grid_v7.json`: `configs/examples/default_trailing_martingale_long.json`
  switched to `trailing_grid_v7` with upstream defaults (filled in by
  passivbot's config loader from the strategy spec), approved coins limited
  to the 10 Bybit coins cached on the dev box, `n_positions` = 3 so forager
  mode is on, and two `coin_overrides` (a wallet-exposure override and a
  strategy-parameter override).
- `tm.json`: `configs/examples/BTC_ETH_XRP_SOL_ADA_long.json` verbatim
  (`trailing_martingale`, 4 positions over 5 coins, forager on).

    python tools/make_public_configs.py --checkout E:/projects/passivbot-rlib-v8.1.0
"""

from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path

COINS = ["ADA", "BTC", "DOGE", "DOT", "ETH", "HBAR", "SOL", "TRX", "XLM", "XRP"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--checkout", required=True)
    ap.add_argument("--out", default="tests/fixtures/configs/fake_v8")
    args = ap.parse_args()
    examples = Path(args.checkout) / "configs" / "examples"
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    base = json.loads((examples / "default_trailing_martingale_long.json").read_text(encoding="utf-8"))
    grid = copy.deepcopy(base)
    grid["live"]["strategy_kind"] = "trailing_grid_v7"
    grid["live"]["approved_coins"] = {"long": list(COINS), "short": list(COINS)}
    grid["live"]["ignored_coins"] = {"long": [], "short": []}
    for pside in ("long", "short"):
        grid["bot"][pside]["strategy"] = {"trailing_grid_v7": {}}
        risk = grid["bot"][pside].setdefault("risk", {})
        risk["n_positions"] = 3
    grid["coin_overrides"] = {
        "XRP": {"bot": {"long": {"wallet_exposure_limit": 0.5}}},
        "DOGE": {"bot": {"long": {"strategy": {"trailing_grid_v7": {"entry": {"initial_qty_pct": 0.02}}}}}},
    }
    grid["_pb_runner_provenance"] = (
        "derived by tools/make_public_configs.py from passivbot "
        "configs/examples/default_trailing_martingale_long.json (tag v8.1.0); "
        "strategy parameters are upstream defaults"
    )
    (out / "grid_v7.json").write_text(json.dumps(grid, indent=2) + "\n", encoding="utf-8")

    tm = json.loads((examples / "BTC_ETH_XRP_SOL_ADA_long.json").read_text(encoding="utf-8"))
    tm["_pb_runner_provenance"] = (
        "copy of passivbot configs/examples/BTC_ETH_XRP_SOL_ADA_long.json (tag v8.1.0)"
    )
    (out / "tm.json").write_text(json.dumps(tm, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {out / 'grid_v7.json'} and {out / 'tm.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
