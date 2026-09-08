#!/usr/bin/env python3
"""Ask Bybit the same question twice, changing exactly one thing.

D24 concluded that `reduceOnly` was why sub-5-USDT entries came back
`110094`. It was wrong, and the evidence that convinced me had this shape:
same account, one variable changed, two observations twelve minutes apart.
D25 then reached the same conclusion about the broker `Referer` header on
evidence of exactly the same shape -- thirteen minutes apart -- and happened
to be right. A structure that produced one wrong answer does not become sound
because the next answer it produced was correct.

This closes that gap. It sends one order body twice, seconds apart, differing
only in the header under test, and prints both `retCode`s side by side. That
is a controlled A/B rather than a before-and-after.

The order is designed not to trade: `minOrderQty` at a price well below the
mark (~0.1 USDT of notional), and anything the exchange accepts is cancelled
immediately. It is still a real order on a real account -- run it deliberately.

WHERE: on the NAT instance. The per-bot keys are IP-whitelisted to its egress
address and will not authenticate from anywhere else, and AGENTS.md keeps them
off the dev box.

    python3 bybit_request_probe.py --user-id 5351347639 --bot-id 467146583

Add `--dry-run` to print both signed requests without sending them; that part
is safe anywhere, except that the key fetch still needs S3 access.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import subprocess
import time
import urllib.parse
import urllib.request

BASE = "https://api.bybit.com"
BROKER_ID = "passivbotbybit"  # broker_codes.hjson, via ccxt options["brokerId"]
RECV_WINDOW = "5000"


def http(method: str, path: str, *, query=None, body=None, headers=None) -> dict:
    url = BASE + path
    if query:
        url += "?" + urllib.parse.urlencode(query)
    data = body.encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read())


def signed_headers(key: str, secret: str, payload: str, *, referer: bool) -> dict:
    ts = str(int(time.time() * 1000))
    raw = ts + key + RECV_WINDOW + payload
    sign = hmac.new(secret.encode(), raw.encode(), hashlib.sha256).hexdigest()
    h = {
        "X-BAPI-API-KEY": key,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "X-BAPI-SIGN": sign,
        "X-BAPI-SIGN-TYPE": "2",
        "Content-Type": "application/json",
    }
    if referer:
        # ccxt bybit.py:9424-9427 -- POST only, from options["brokerId"], which
        # exchanges/bybit.py::create_ccxt_sessions refuses to leave unset.
        h["Referer"] = BROKER_ID
    return h


def load_key(bucket: str, user_id: str, bot_id: str) -> tuple[str, str]:
    """The same object the runner downloads, read the same way (live.rs:59)."""
    out = subprocess.run(
        ["aws", "s3", "cp", f"s3://{bucket}/{user_id}/{bot_id}/api-keys.json", "-"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    entry = json.loads(out)[bot_id]
    return entry.get("key") or entry["apiKey"], entry["secret"]


def market(symbol: str) -> tuple[float, float, float]:
    info = http(
        "GET", "/v5/market/instruments-info", query={"category": "linear", "symbol": symbol}
    )["result"]["list"][0]
    tick = float(info["priceFilter"]["tickSize"])
    min_qty = float(info["lotSizeFilter"]["minOrderQty"])
    mark = float(
        http("GET", "/v5/market/tickers", query={"category": "linear", "symbol": symbol})[
            "result"
        ]["list"][0]["markPrice"]
    )
    return mark, tick, min_qty


def fmt_step(value: float, step: float) -> str:
    """Same spelling the client uses: rounded to the step, no padding (D24)."""
    decimals = max(0, len(f"{step:.10f}".rstrip("0").split(".")[1]))
    s = f"{value:.{decimals}f}"
    return s.rstrip("0").rstrip(".") if "." in s else s


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--user-id", required=True)
    ap.add_argument("--bot-id", required=True)
    ap.add_argument("--bucket", default="scalable-cluster-dev-bot-configs")
    ap.add_argument("--symbol", default="XRPUSDT")
    ap.add_argument(
        "--price-frac",
        type=float,
        default=0.9,
        help="limit price as a fraction of the mark. Two failure modes bound "
        "this: too near and the order could trade, too far and Bybit rejects "
        "it for leaving the permissible price band -- and a band rejection "
        "would confound the test it exists to run. 10%% under the mark is "
        "outside any spread and inside any band; the order rests for about a "
        "second.",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    mark, tick, min_qty = market(args.symbol)
    price = fmt_step(mark * args.price_frac, tick)
    qty = fmt_step(min_qty, min_qty)
    notional = float(price) * float(qty)
    print(
        f"{args.symbol}: mark={mark} -> probe {qty} @ {price} "
        f"= {notional:.4f} USDT notional (minimum enforced without the header is 5)"
    )

    key, secret = load_key(args.bucket, args.user_id, args.bot_id)
    results = {}
    for referer in (False, True):
        label = "with Referer" if referer else "without Referer"
        body = json.dumps(
            {
                "category": "linear",
                "symbol": args.symbol,
                "side": "Buy",
                "orderType": "Limit",
                "qty": qty,
                "timeInForce": "GTC",
                "positionIdx": 1,
                "orderLinkId": f"0x0000probe{'R' if referer else 'N'}{int(time.time())}",
                "price": price,
            },
            separators=(",", ":"),
        )
        headers = signed_headers(key, secret, body, referer=referer)
        if args.dry_run:
            shown = {k: ("<redacted>" if k.startswith("X-BAPI") else v) for k, v in headers.items()}
            print(f"\n{label}\n  body    {body}\n  headers {shown}")
            continue
        resp = http("POST", "/v5/order/create", body=body, headers=headers)
        code, msg = resp.get("retCode"), resp.get("retMsg")
        results[label] = (code, msg)
        print(f"{label:16} retCode={code} retMsg={msg}")
        if code == 0:
            oid = resp["result"]["orderId"]
            cancel = json.dumps(
                {"category": "linear", "symbol": args.symbol, "orderId": oid},
                separators=(",", ":"),
            )
            done = http(
                "POST",
                "/v5/order/cancel",
                body=cancel,
                headers=signed_headers(key, secret, cancel, referer=True),
            )
            print(f"{'':16} accepted as {oid}; cancel retCode={done.get('retCode')}")
        time.sleep(1)

    if args.dry_run:
        return
    no, yes = results["without Referer"][0], results["with Referer"][0]
    print()
    if no == 110094 and yes == 0:
        print("CONFIRMED: the broker Referer is what waives the per-symbol minimum.")
    elif no == yes:
        print(
            "NOT the header: both calls got the same answer. Whatever separates "
            "the runtimes is not in the request -- compare account state next "
            "(leverage, margin mode, position mode, UTA tier), taken from both "
            "at the same moment."
        )
    else:
        print("Unexpected pair; read the two retMsgs above before concluding anything.")


if __name__ == "__main__":
    main()
