"""Record the exact HTTP requests ccxt sends for every call passivbot makes.

D11 chose a hand-written Bybit v5 client over the ccxt Rust port and wrote
down the check that was supposed to keep it honest: "record the Python bot's
ccxt requests/responses for the read-only abot account and compare against
this client's requests". That check was never built, and D24 is what it would
have caught -- a `reduceOnly` field we sent on every order that the Python bot
has never sent on any.

This is the recording half. It drives ccxt exactly as passivbot v8.1.0 drives
it (call sites in `exchanges/bybit.py` and `exchanges/ccxt_bot.py`, cited per
entry below), intercepts at `Exchange.fetch`, and writes what would have gone
on the wire to `tests/fixtures/ccxt_requests.json`. The comparing half is
`crates/exchange-bybit/tests/ccxt_request_parity.rs`.

Nothing is sent for the private calls: the key and secret are dummies and
every private response is canned, so no order is ever placed and no account is
touched. The one real network call is the public `instruments-info` that
`load_markets` needs; without markets, ccxt cannot build a symbol's request at
all.

    E:/projects/passivbot/.venv/Scripts/python.exe tools/ccxt_request_fixtures.py

Re-run it after bumping the pinned ccxt, and commit the diff: a change in this
file is a change in what the Python bot puts on the wire.
"""

from __future__ import annotations

import argparse
import json
import types
import urllib.parse
from pathlib import Path

import ccxt

# Canned bodies for the private endpoints. Only the shape matters: ccxt has to
# get far enough to have built the request we are recording, and whether it
# then parses an empty list is irrelevant.
CANNED = {
    "/v5/user/query-api": {
        "retCode": 0,
        "retMsg": "OK",
        "result": {"isMaster": True, "uta": 1, "unified": 1, "userID": 1},
    },
    "_default": {
        "retCode": 0,
        "retMsg": "OK",
        "result": {"list": [], "category": "linear", "nextPageCursor": ""},
    },
}


class Recorder:
    """Stands in for `Exchange.fetch` and keeps what each call would send."""

    def __init__(self, exchange):
        self.exchange = exchange
        self.real_fetch = exchange.fetch
        self.captured: list[dict] = []
        self.label: str | None = None

    def install(self):
        rec = self

        def fetch(self, url, method="GET", headers=None, body=None):  # noqa: ANN001
            parsed = urllib.parse.urlparse(url)
            query = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
            if rec.label is not None:
                rec.captured.append(
                    {
                        "call": rec.label,
                        "method": method,
                        "path": parsed.path,
                        "query": query,
                        "body": json.loads(body) if body else None,
                    }
                )
            # instruments-info is the only request allowed out: load_markets
            # has to succeed or nothing downstream can be built.
            if parsed.path == "/v5/market/instruments-info":
                return rec.real_fetch(url, method, headers, body)
            return CANNED.get(parsed.path, CANNED["_default"])

        self.exchange.fetch = types.MethodType(fetch, self.exchange)

    def record(self, label, fn):
        """Run one passivbot call site and keep whatever it puts on the wire."""
        self.label = label
        try:
            fn()
        except Exception as exc:  # canned responses make most parsers unhappy
            print(f"  {label}: {type(exc).__name__} (expected, request captured)")
        finally:
            self.label = None


def build_exchange() -> ccxt.bybit:
    """A bybit client configured the way `ccxt_bot.py` configures it."""
    return ccxt.bybit(
        {
            "apiKey": "dummy-key-never-sent",
            "secret": "dummy-secret-never-sent",
            "enableRateLimit": False,
            "options": {"defaultType": "swap"},
        }
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--out",
        default=str(Path(__file__).resolve().parents[1] / "tests/fixtures/ccxt_requests.json"),
    )
    args = ap.parse_args()

    ex = build_exchange()
    rec = Recorder(ex)
    # Installed before load_markets: that call reaches for private endpoints
    # too (`fetch_currencies`), which must be canned rather than sent with a
    # dummy key. Nothing is captured until a label is set.
    rec.install()
    ex.load_markets()

    symbol = "XRP/USDT:USDT"
    # passivbot.py::execute_order -> exchanges/bybit.py::_build_order_params.
    # Those three keys are the whole of it; `reduceOnly` is deliberately absent
    # even for closes, which are expressed by positionIdx alone (D24).
    entry_params = {"positionIdx": 1, "timeInForce": "GTC", "orderLinkId": "0x0007pb-entry"}
    close_params = {"positionIdx": 1, "timeInForce": "GTC", "orderLinkId": "0x0007pb-close"}
    post_only_params = {
        "positionIdx": 1,
        "timeInForce": "postOnly",
        "orderLinkId": "0x0007pb-postonly",
    }

    calls = [
        # --- write path (passivbot.py:22327 execute_order / 22380 execute_cancellation)
        (
            "create_order.entry_limit_gtc",
            lambda: ex.create_order(
                symbol=symbol, type="limit", side="buy", amount=1.6, price=1.4077,
                params=dict(entry_params),
            ),
        ),
        (
            "create_order.entry_limit_post_only",
            lambda: ex.create_order(
                symbol=symbol, type="limit", side="buy", amount=1.6, price=1.4077,
                params=dict(post_only_params),
            ),
        ),
        (
            "create_order.close_limit_gtc",
            lambda: ex.create_order(
                symbol=symbol, type="limit", side="sell", amount=1.6, price=1.5,
                params=dict(close_params),
            ),
        ),
        (
            "create_order.close_market",
            lambda: ex.create_order(
                symbol=symbol, type="market", side="sell", amount=1.6, price=None,
                params=dict(close_params),
            ),
        ),
        ("cancel_order", lambda: ex.cancel_order("1234567890", symbol=symbol)),
        # --- exchange config (exchanges/bybit.py:553-590)
        ("set_position_mode", lambda: ex.set_position_mode(True)),
        ("set_leverage", lambda: ex.set_leverage(10, symbol=symbol)),
        (
            "set_margin_mode",
            lambda: ex.set_margin_mode("cross", symbol=symbol, params={"leverage": 10}),
        ),
        # --- read path (ccxt_bot.py and exchanges/bybit.py call sites)
        ("fetch_balance", lambda: ex.fetch_balance()),
        ("fetch_positions", lambda: ex.fetch_positions(params={"limit": 200})),
        ("fetch_open_orders", lambda: ex.fetch_open_orders(symbol=None, limit=50)),
        ("fetch_tickers", lambda: ex.fetch_tickers()),
        (
            "fetch_ohlcv",
            lambda: ex.fetch_ohlcv(symbol, timeframe="1m", since=1788000000000, limit=1000),
        ),
        ("fetch_my_trades", lambda: ex.fetch_my_trades()),
        (
            "closed_pnl",
            lambda: ex.private_get_v5_position_closed_pnl(
                {"category": "linear", "limit": 100}
            ),
        ),
        ("load_markets", lambda: ex.fetch_markets()),
    ]

    for label, fn in calls:
        rec.record(label, fn)

    out = {
        "ccxt_version": ccxt.__version__,
        "note": (
            "Requests ccxt would send for the calls passivbot v8.1.0 makes. "
            "Generated by tools/ccxt_request_fixtures.py; compared by "
            "crates/exchange-bybit/tests/ccxt_request_parity.rs."
        ),
        "requests": rec.captured,
    }
    path = Path(args.out)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    print(f"wrote {len(rec.captured)} requests to {path}")


if __name__ == "__main__":
    main()
