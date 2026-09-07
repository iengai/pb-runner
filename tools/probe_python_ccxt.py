"""P3.4 cross-check: the Python bot's view of a Bybit account, via ccxt.

Reproduces exactly the calls and field extractions of passivbot v8.1.0
`exchanges/bybit.py` + `exchanges/ccxt_bot.py` (PORT_INVENTORY section 3)
and writes a summary in the same shape as
`crates/exchange-bybit/examples/readonly_probe.rs`, so the two can be diffed.
Read-only: never places or cancels orders. Secrets are never printed.

    E:/projects/passivbot/.venv/Scripts/python.exe tools/probe_python_ccxt.py \
        --keys E:/projects/passivbot/api-keys.json --user 415196485 --out .local/probe_py.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time

import ccxt.async_support as ccxt_async


def get_balance(fetched: dict) -> float:
    """exchanges/bybit.py::_get_balance"""
    balinfo = fetched["info"]["result"]["list"][0]
    if balinfo["accountType"] == "UNIFIED":
        if "totalEquity" in balinfo and "totalPerpUPL" in balinfo:
            return float(balinfo["totalEquity"]) - float(balinfo["totalPerpUPL"])
        balance = 0.0
        used = False
        for elm in balinfo["coin"]:
            mc = str(elm["marginCollateral"]).lower() in {"true", "1"}
            cs = str(elm["collateralSwitch"]).lower() in {"true", "1"}
            if mc and cs:
                used = True
                balance += float(elm["usdValue"]) - float(elm["unrealisedPnl"])
        if not used:
            raise KeyError("no enabled collateral coins")
        return balance
    return float(fetched["total"]["USDT"])


async def fetch_positions_paginated(cca) -> list:
    """exchanges/bybit.py::_do_fetch_positions_paginated"""
    positions, seen = [], set()
    limit = 200
    fetched = await cca.fetch_positions(params={"limit": limit})
    while True:
        if all(e["symbol"] + e["side"] in seen for e in fetched):
            break
        cursor = None
        for e in fetched:
            key = e["symbol"] + e["side"]
            if key in seen:
                continue
            seen.add(key)
            positions.append(e)
            if "nextPageCursor" in e.get("info", {}):
                cursor = e["info"]["nextPageCursor"]
        if len(fetched) < limit or cursor is None:
            break
        fetched = await cca.fetch_positions(params={"cursor": cursor, "limit": limit})
    return positions


async def fetch_open_orders_paginated(cca) -> list:
    """exchanges/bybit.py::_do_fetch_open_orders"""
    orders, seen = [], set()
    limit = 50
    fetched = await cca.fetch_open_orders(symbol=None, limit=limit)
    while True:
        if all(e["id"] in seen for e in fetched):
            break
        cursor = None
        for e in fetched:
            if e["id"] in seen:
                continue
            seen.add(e["id"])
            orders.append(e)
            if "nextPageCursor" in e.get("info", {}):
                cursor = e["info"]["nextPageCursor"]
        if len(fetched) < limit or cursor is None:
            break
        fetched = await cca.fetch_open_orders(symbol=None, limit=limit, params={"cursor": cursor})
    return orders


def order_pside(o: dict) -> str:
    """exchanges/bybit.py::_get_position_side_for_order (positionIdx path)"""
    idx = int(o.get("info", {}).get("positionIdx"))
    if idx == 1:
        return "long"
    if idx == 2:
        return "short"
    side, ro = o["side"], bool(o.get("reduceOnly"))
    return "long" if (side == "buy") != ro else "short"


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keys", required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    entry = json.load(open(args.keys, encoding="utf-8"))[args.user]
    assert entry["exchange"] == "bybit"
    cca = ccxt_async.bybit({"apiKey": entry["key"], "secret": entry["secret"], "enableRateLimit": True, "timeout": 30000})
    cca.options["defaultType"] = "swap"
    t0 = time.time()
    try:
        markets = await cca.load_markets()
        t_markets = int((time.time() - t0) * 1000)
        linear = {s: m for s, m in markets.items() if m.get("linear") and m.get("swap") and m.get("settle") == "USDT" and m.get("active")}
        fetched_balance = await cca.fetch_balance()
        balance = get_balance(fetched_balance)
        acct = fetched_balance["info"]["result"]["list"][0]
        positions = [
            {"symbol": e["symbol"], "pside": e["side"].lower(), "size": float(e["contracts"]), "entry_price": float(e["entryPrice"]),
             "leverage": float(e["leverage"]) if e.get("leverage") is not None else None}
            for e in await fetch_positions_paginated(cca)
            if float(e.get("contracts") or 0) != 0
        ]
        open_orders = sorted(
            [{"id": o["id"], "client_id": o.get("clientOrderId") or None, "symbol": o["symbol"], "side": o["side"],
              "pside": order_pside(o), "qty": float(o["amount"]), "price": float(o["price"]), "reduce_only": bool(o.get("reduceOnly")),
              "created_ms": o.get("timestamp")}
             for o in await fetch_open_orders_paginated(cca)],
            key=lambda x: x["created_ms"] or 0,
        )
        tickers_raw = await cca.fetch_tickers()
        tickers = {s: {"bid": float(d.get("bid") or 0), "ask": float(d.get("ask") or 0), "last": float(d.get("last") or d.get("bid") or 0)}
                   for s, d in tickers_raw.items() if s in linear}
        probe_symbol = positions[0]["symbol"] if positions else "XRP/USDT:USDT"
        since = int((time.time() * 1000 - 2 * 3600 * 1000) // 60000 * 60000)
        candles = await cca.fetch_ohlcv(probe_symbol, timeframe="1m", since=since, limit=1000)
        btc = linear.get("BTC/USDT:USDT")
        summary = {
            "user": args.user,
            "markets": {"count": len(linear), "load_ms": t_markets, "btc": None if btc is None else {
                "symbol": btc["symbol"], "id": btc["id"], "qty_step": btc["precision"]["amount"], "price_step": btc["precision"]["price"],
                "min_qty": btc["limits"]["amount"]["min"] or btc["precision"]["amount"],
                # ccxt_bot.py::set_market_specific_settings: `limits.cost.min or 0.1`
                "min_cost": btc["limits"]["cost"]["min"] or 0.1, "contract_size": btc.get("contractSize", 1),
                "max_leverage": btc["limits"]["leverage"]["max"], "maker_fee": btc["maker"], "taker_fee": btc["taker"]}},
            "balance": {"account_type": acct["accountType"], "total_usdt": balance,
                        "available_usdt": float(acct.get("totalAvailableBalance") or 0)},
            "positions": positions,
            "open_orders": open_orders,
            "tickers": {"count": len(tickers), "btc": tickers.get("BTC/USDT:USDT")},
            "ohlcv_1m": {"symbol": probe_symbol, "count": len(candles), "first_ts": candles[0][0] if candles else None,
                         "last_ts": candles[-1][0] if candles else None, "last": candles[-1] if candles else None},
            "elapsed_ms": int((time.time() - t0) * 1000),
            "ccxt_version": ccxt_async.__version__,
        }
    finally:
        await cca.close()
    text = json.dumps(summary, indent=2)
    print(text)
    if args.out:
        open(args.out, "w", encoding="utf-8").write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
