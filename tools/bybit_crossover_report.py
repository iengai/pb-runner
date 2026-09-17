#!/usr/bin/env python3
"""Mark-to-market returns per period, for scoring a runtime swap.

On 2026-09-11 15:00 UTC abot (415196485) moved Python -> Rust and paper2
(467146583) moved Rust -> Python, on byte-identical configs. That is a 2x2
crossover: each account ran both runtimes and each runtime ran in both
periods, so an account effect and a period effect both cancel in the
average. Realized PnL alone cannot score it -- a position open across a
boundary carries money from one period into the next -- so this rebuilds
EQUITY (cash + unrealized) at every boundary.

How a past boundary is recovered, without needing a flat starting point:
cash walks backward from today's USDT wallet through every transaction-log
change after t; each position walks backward from today's position through
every trade execution after t (an execution's `closedSize` says how much of
it closed the opposite exposure; the closing order's closed-pnl record gives
the entry price that a full close erases). The position is then marked at
the mark-price 1m close before t.

Calibration printed with every boundary: the backward-walked cash against
the `cashBalance` Bybit itself recorded on the last transaction at or before
t. They should agree to the cent. If they do not, nothing else here counts.

Read-only: account/wallet-balance, position/list, execution/list,
position/closed-pnl, account/transaction-log, market/mark-price-kline.

WHERE: on the NAT instance -- the per-bot keys are IP-whitelisted to its
egress address, and AGENTS.md keeps them off the dev box.

    python3 bybit_crossover_report.py \\
        --bot 5351347639:415196485:abot --bot 5351347639:467146583:paper2 \\
        --t 2026-09-08T16:00:00Z --t 2026-09-11T15:00:00Z

Each --t is a boundary; the last period runs to now.
"""

from __future__ import annotations

import argparse
import collections
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
EPS = 1e-9


def api(path, params, key=None, secret=None):
    query = urllib.parse.urlencode(params)
    for attempt in range(5):
        req = urllib.request.Request(f"{BASE}{path}?{query}")
        if key:
            ts = str(int(time.time() * 1000))
            sign = hmac.new(
                secret.encode(), (ts + key + RECV_WINDOW + query).encode(), hashlib.sha256
            ).hexdigest()
            req.add_header("X-BAPI-API-KEY", key)
            req.add_header("X-BAPI-TIMESTAMP", ts)
            req.add_header("X-BAPI-RECV-WINDOW", RECV_WINDOW)
            req.add_header("X-BAPI-SIGN", sign)
            req.add_header("X-BAPI-SIGN-TYPE", "2")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read())
        except Exception:
            if attempt == 4:
                raise
            time.sleep(1 + attempt)
            continue
        if body.get("retCode") == 10006:  # rate limited
            time.sleep(2 + attempt)
            continue
        if body.get("retCode") != 0:
            raise RuntimeError("%s: %s %s" % (path, body.get("retCode"), body.get("retMsg")))
        return body["result"]
    raise RuntimeError(path + ": rate limited")


def windowed(path, base, key, secret, start_ms, end_ms, limit, id_field):
    """Every record in [start, end]: explicit 7-day windows, cursor inside each."""
    rows, seen = [], set()
    ws = start_ms
    while ws <= end_ms:
        we = min(ws + WEEK_MS - 1, end_ms)
        cursor = None
        while True:
            params = dict(base, startTime=ws, endTime=we, limit=limit)
            if cursor:
                params["cursor"] = cursor
            result = api(path, params, key, secret)
            page = result.get("list") or []
            for r in page:
                rid = r.get(id_field)
                if rid in seen:
                    continue
                seen.add(rid)
                rows.append(r)
            cursor = result.get("nextPageCursor") or None
            if not cursor or not page:
                break
        ws = we + 1
    return rows


def load_key(bucket, user_id, bot_id):
    out = subprocess.run(
        ["aws", "s3", "cp", "s3://%s/%s/%s/api-keys.json" % (bucket, user_id, bot_id), "-"],
        capture_output=True, text=True, check=True,
    ).stdout
    entry = json.loads(out)[bot_id]
    return entry.get("key") or entry["apiKey"], entry["secret"]


def mark_close_before(symbol, t_ms):
    """Close of the last completed 1m mark-price candle before t."""
    minute = (t_ms // 60000) * 60000
    r = api("/v5/market/mark-price-kline", {
        "category": "linear", "symbol": symbol, "interval": "1",
        "start": minute - 5 * 60000, "end": minute - 1, "limit": 10,
    })
    candles = sorted(r.get("list") or [], key=lambda c: int(c[0]))
    done = [c for c in candles if int(c[0]) + 60000 <= t_ms]
    if not done:
        raise RuntimeError("no mark candle for %s before %d" % (symbol, t_ms))
    return float(done[-1][4])


def iso(ms):
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime("%m-%d %H:%M")


def parse_t(s):
    return int(dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)


def report(user_id, bot_id, label, bucket, boundaries):
    key, secret = load_key(bucket, user_id, bot_id)
    now = int(time.time() * 1000)
    start = boundaries[0]

    wallet = api("/v5/account/wallet-balance", {"accountType": "UNIFIED", "coin": "USDT"}, key, secret)
    coin = [c for c in wallet["list"][0]["coin"] if c["coin"] == "USDT"][0]
    cash_now = float(coin["walletBalance"])

    positions = api("/v5/position/list",
                    {"category": "linear", "settleCoin": "USDT", "limit": 200}, key, secret)["list"]
    # state[(symbol, "L"|"S")] = [qty, avg entry]
    state = collections.defaultdict(lambda: [0.0, 0.0])
    upnl_now = 0.0
    for p in positions:
        size = float(p.get("size") or 0)
        if size <= EPS:
            continue
        leg = "L" if p["side"] == "Buy" else "S"
        state[(p["symbol"], leg)] = [size, float(p["avgPrice"])]
        upnl_now += float(p.get("unrealisedPnl") or 0)

    txns = windowed("/v5/account/transaction-log", {"accountType": "UNIFIED", "currency": "USDT"},
                    key, secret, start - WEEK_MS, now, 50, "id")
    execs = [e for e in windowed("/v5/execution/list", {"category": "linear"},
                                 key, secret, start, now, 100, "execId")
             if e.get("execType") == "Trade"]
    closed = windowed("/v5/position/closed-pnl", {"category": "linear"},
                      key, secret, start, now, 100, "orderId")
    entry_by_order = {c["orderId"]: float(c["avgEntryPrice"]) for c in closed}

    txns.sort(key=lambda r: int(r["transactionTime"]))
    execs.sort(key=lambda e: int(e["execTime"]), reverse=True)

    # Walk positions backward, snapshotting at each boundary (latest first).
    snaps = {}
    unknown_entry = 0
    ei = 0
    for t in sorted(boundaries, reverse=True):
        while ei < len(execs) and int(execs[ei]["execTime"]) > t:
            e = execs[ei]
            ei += 1
            sym, qty, px = e["symbol"], float(e["execQty"]), float(e["execPrice"])
            closed_sz = float(e.get("closedSize") or 0)
            open_sz = max(qty - closed_sz, 0.0)
            buy = e["side"] == "Buy"
            if open_sz > EPS:  # undo an open: Buy opened long, Sell opened short
                leg = state[(sym, "L" if buy else "S")]
                q_before = leg[0] - open_sz
                if q_before <= EPS:
                    leg[0], leg[1] = 0.0, 0.0
                else:
                    leg[1] = (leg[0] * leg[1] - open_sz * px) / q_before
                    leg[0] = q_before
            if closed_sz > EPS:  # undo a close: Buy closed short, Sell closed long
                leg = state[(sym, "S" if buy else "L")]
                if leg[0] <= EPS:
                    entry = entry_by_order.get(e["orderId"])
                    if entry is None:
                        unknown_entry += 1
                        entry = px
                    leg[1] = entry
                leg[0] += closed_sz
        snaps[t] = dict((k, tuple(v)) for k, v in state.items() if v[0] > EPS)

    def cash_at(t):
        return cash_now - sum(float(r["change"]) for r in txns if int(r["transactionTime"]) > t)

    def recorded_cash_at(t):
        before = [r for r in txns if int(r["transactionTime"]) <= t]
        return float(before[-1]["cashBalance"]) if before else None

    def equity_at(t):
        upnl = 0.0
        for (sym, leg), (q, avg) in snaps[t].items():
            m = mark_close_before(sym, t)
            upnl += q * (m - avg) if leg == "L" else q * (avg - m)
        return cash_at(t) + upnl, upnl

    points = [(t,) + equity_at(t) for t in boundaries] + [(now, cash_now + upnl_now, upnl_now)]

    print("\n=== %s (%s)  cash_now=%.4f upnl_now=%.4f  execs=%d closes=%d txns=%d unknown_entry=%d"
          % (label, bot_id, cash_now, upnl_now, len(execs), len(closed), len(txns), unknown_entry))
    for t in boundaries:
        rec = recorded_cash_at(t)
        diff = "n/a" if rec is None else "%+.6f" % (cash_at(t) - rec)
        pos = ", ".join("%s%s=%g@%.5g" % (s.replace("USDT", ""), l, q, a)
                        for (s, l), (q, a) in sorted(snaps[t].items())) or "flat"
        print("  @%s  cash=%.4f (backward minus recorded: %s)  pos: %s" % (iso(t), cash_at(t), diff, pos))
    print("  %-25s %5s %10s %10s %9s %9s %8s %8s %9s %8s %8s %6s" % (
        "period", "days", "eq_start", "eq_end", "transfer", "pnl", "%", "%/day",
        "trade", "funding", "dUPNL", "fills"))
    for (t0, e0, u0), (t1, e1, u1) in zip(points, points[1:]):
        by_type = collections.Counter()
        for r in txns:
            if t0 < int(r["transactionTime"]) <= t1:
                by_type[r["type"]] += float(r["change"])
        transfer = sum(v for k, v in by_type.items()
                       if "TRANSFER" in k or "DEPOSIT" in k or "WITHDRAW" in k)
        trade = by_type.get("TRADE", 0.0)
        funding = by_type.get("SETTLEMENT", 0.0)
        pnl = e1 - e0 - transfer
        days = (t1 - t0) / 86400000.0
        fills = sum(1 for e in execs if t0 < int(e["execTime"]) <= t1)
        print("  %-25s %5.2f %10.4f %10.4f %9.4f %9.4f %7.3f%% %7.3f%% %9.4f %8.4f %8.4f %6d" % (
            iso(t0) + " -> " + iso(t1), days, e0, e1, transfer, pnl,
            100 * pnl / e0, 100 * pnl / e0 / days, trade, funding, u1 - u0, fills))
        other = dict((k, round(v, 4)) for k, v in by_type.items()
                     if k not in ("TRADE", "SETTLEMENT") and "TRANSFER" not in k)
        if other:
            print("      other txn types: %s" % other)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bot", action="append", required=True, help="user_id:bot_id:label")
    ap.add_argument("--t", action="append", required=True, help="boundary, ISO UTC")
    ap.add_argument("--bucket", default="scalable-cluster-dev-bot-configs")
    args = ap.parse_args()
    boundaries = sorted(parse_t(s) for s in args.t)
    for spec in args.bot:
        user_id, bot_id, label = spec.split(":", 2)
        report(user_id, bot_id, label, args.bucket, boundaries)


if __name__ == "__main__":
    main()
