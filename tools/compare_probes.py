"""P3.4 acceptance: diff the Rust and Python views of the same Bybit account.

    python tools/compare_probes.py .local/probe_rs.json .local/probe_py.json

Stable fields must match exactly; balance and tickers/candles are printed for
eyeballing because they move between the two calls (equity follows mark price).
Exit code 1 on any stable-field difference.
"""

from __future__ import annotations

import json
import sys


def main() -> int:
    rs = json.load(open(sys.argv[1], encoding="utf-8"))
    py = json.load(open(sys.argv[2], encoding="utf-8"))
    bad = 0

    def cmp(name, a, b):
        nonlocal bad
        ok = a == b
        bad += 0 if ok else 1
        print(("OK  " if ok else "DIFF"), name, "" if ok else f"rs={a!r} py={b!r}")

    cmp("markets.count", rs["markets"]["count"], py["markets"]["count"])
    for k in ["symbol", "id", "qty_step", "price_step", "min_qty", "min_cost", "contract_size", "max_leverage", "maker_fee", "taker_fee"]:
        cmp("markets.btc." + k, rs["markets"]["btc"][k], py["markets"]["btc"][k])
    cmp("balance.account_type", rs["balance"]["account_type"], py["balance"]["account_type"])
    rp = [{k: p[k] for k in ["symbol", "pside", "size", "entry_price", "leverage"]} for p in rs["positions"]]
    cmp("positions", rp, py["positions"])
    ro = [{k: o[k] for k in ["id", "client_id", "symbol", "side", "pside", "qty", "price", "reduce_only", "created_ms"]} for o in rs["open_orders"]]
    cmp("open_orders", ro, py["open_orders"])
    cmp("tickers.count", rs["tickers"]["count"], py["tickers"]["count"])
    cmp("ohlcv.symbol", rs["ohlcv_1m"]["symbol"], py["ohlcv_1m"]["symbol"])
    print("timing-dependent: balance rs/py", rs["balance"]["total_usdt"], py["balance"]["total_usdt"],
          "| btc ticker rs/py", rs["tickers"]["btc"], py["tickers"]["btc"],
          "| ohlcv count rs/py", rs["ohlcv_1m"]["count"], py["ohlcv_1m"]["count"],
          "| elapsed ms rs/py", rs["elapsed_ms"], py["elapsed_ms"], "| ccxt", py.get("ccxt_version"))
    print("stable-field differences:", bad)
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
