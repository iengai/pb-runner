#!/usr/bin/env python3
"""Read-only daily realized PnL and balance for one or more bots.

Built to answer one question: paper2 and abot run byte-identical configs
(232 keys, the only difference being `live.user`) on two accounts, one moved
to the Rust runtime and one still on Python. That makes them a controlled A/B
of the runtime itself, and this reports the outcome side by side.

Read-only: `GET /v5/position/closed-pnl` and `GET /v5/account/wallet-balance`,
nothing else. No orders, no cancels, no config writes.

WHERE: on the NAT instance -- the per-bot keys are IP-whitelisted to its
egress address, and AGENTS.md keeps them off the dev box.

    python3 bybit_account_report.py --days 14 \\
        --bot 5351347639:415196485:abot-py \\
        --bot 5351347639:467146583:paper2-rs
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import hmac
import json
import subprocess
import time
import urllib.parse
import urllib.request

BASE = "https://api.bybit.com"
RECV_WINDOW = "5000"
WEEK_MS = 7 * 24 * 60 * 60 * 1000


def signed_get(key: str, secret: str, path: str, params: dict) -> dict:
    query = urllib.parse.urlencode(params)
    ts = str(int(time.time() * 1000))
    sign = hmac.new(
        secret.encode(), (ts + key + RECV_WINDOW + query).encode(), hashlib.sha256
    ).hexdigest()
    req = urllib.request.Request(f"{BASE}{path}?{query}")
    for k, v in {
        "X-BAPI-API-KEY": key,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "X-BAPI-SIGN": sign,
        "X-BAPI-SIGN-TYPE": "2",
    }.items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = json.loads(resp.read())
    if body.get("retCode") != 0:
        raise RuntimeError(f"{path}: {body.get('retCode')} {body.get('retMsg')}")
    return body["result"]


def load_key(bucket: str, user_id: str, bot_id: str) -> tuple[str, str]:
    out = subprocess.run(
        ["aws", "s3", "cp", f"s3://{bucket}/{user_id}/{bot_id}/api-keys.json", "-"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    entry = json.loads(out)[bot_id]
    return entry.get("key") or entry["apiKey"], entry["secret"]


def closed_pnl(key: str, secret: str, start_ms: int, end_ms: int) -> list[dict]:
    """Every closed-pnl record in the range. Bybit caps a request at 7 days,
    so walk explicit windows and follow the cursor inside each -- the same
    shape `exchanges/bybit.py::fetch_pnls_sub` uses."""
    rows, seen = [], set()
    ws = start_ms
    while ws < end_ms:
        we = min(ws + WEEK_MS, end_ms)
        cursor = None
        while True:
            params = {"category": "linear", "limit": 100, "startTime": ws, "endTime": we}
            if cursor:
                params["cursor"] = cursor
            result = signed_get(key, secret, "/v5/position/closed-pnl", params)
            page = result.get("list") or []
            for r in page:
                rid = (r.get("orderId"), r.get("updatedTime"))
                if rid in seen:
                    continue
                seen.add(rid)
                rows.append(r)
            cursor = result.get("nextPageCursor") or None
            if not cursor or len(page) < 100:
                break
        ws = we
    return rows


def balance(key: str, secret: str) -> tuple[float, float]:
    """UTA equity excluding perp UPNL -- `_get_balance` in exchanges/bybit.py."""
    acct = signed_get(key, secret, "/v5/account/wallet-balance", {"accountType": "UNIFIED"})[
        "list"
    ][0]
    equity = float(acct["totalEquity"])
    upl = float(acct["totalPerpUPL"])
    return equity - upl, upl


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bot", action="append", required=True, help="user_id:bot_id:label")
    ap.add_argument("--bucket", default="scalable-cluster-dev-bot-configs")
    ap.add_argument("--days", type=int, default=14)
    args = ap.parse_args()

    now = int(time.time() * 1000)
    start = now - args.days * 24 * 60 * 60 * 1000
    per_bot = {}
    for spec in args.bot:
        user_id, bot_id, label = spec.split(":", 2)
        key, secret = load_key(args.bucket, user_id, bot_id)
        rows = closed_pnl(key, secret, start, now)
        wallet, upl = balance(key, secret)
        daily: dict[str, float] = {}
        for r in rows:
            day = dt.datetime.fromtimestamp(
                int(r["updatedTime"]) / 1000, dt.timezone.utc
            ).strftime("%m-%d")
            daily[day] = daily.get(day, 0.0) + float(r["closedPnl"])
        per_bot[label] = {
            "rows": len(rows),
            "daily": daily,
            "wallet": wallet,
            "upl": upl,
            "total": sum(float(r["closedPnl"]) for r in rows),
        }

    labels = list(per_bot)
    print(f"realized pnl by day (UTC), last {args.days}d\n")
    print("day     " + "".join(f"{l:>18}" for l in labels))
    days = sorted({d for b in per_bot.values() for d in b["daily"]})
    for d in days:
        print(f"{d:8}" + "".join(f"{per_bot[l]['daily'].get(d, 0.0):>18.4f}" for l in labels))
    print("-" * (8 + 18 * len(labels)))
    print("total   " + "".join(f"{per_bot[l]['total']:>18.4f}" for l in labels))
    print("closes  " + "".join(f"{per_bot[l]['rows']:>18}" for l in labels))
    print("wallet  " + "".join(f"{per_bot[l]['wallet']:>18.4f}" for l in labels))
    print("upnl    " + "".join(f"{per_bot[l]['upl']:>18.4f}" for l in labels))
    print(
        "\nreturn% " + "".join(
            f"{100 * per_bot[l]['total'] / max(per_bot[l]['wallet'] - per_bot[l]['total'], 1e-9):>17.2f}%"
            for l in labels
        )
    )


if __name__ == "__main__":
    main()
