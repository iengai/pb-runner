"""Run passivbot's fake-live harness with *every* clock following the fake exchange.

`src/tools/run_fake_live.py` only redirects `bot.get_exchange_time` and the
candlestick manager's `_now_ms_callback` to the scenario clock. Several
paths still read wall-clock time directly (`utils.utc_ms`,
`candlestick_manager._utc_now_ms`, e.g. `get_completed_candle_health`
called without `now_ms`), so a replay of candles older than the strategy's
EMA windows is judged "stale" and every coin is marked non-tradable.

This wrapper rebinds those names in every already-imported module to a
function that returns the active `FakeCCXTClient.now_ms`, then hands over
to `run_fake_live.main()`. Dev-only; nothing here touches the checkout.

Usage (cwd = passivbot checkout, PYTHONPATH=src):

    python E:/projects/pb-runner/tools/fake_live_clock.py <config> <scenario> [run_fake_live args]
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.getcwd(), "src"))

import numpy as np  # noqa: E402

import candlestick_manager  # noqa: E402
import utils  # noqa: E402
from candlestick_manager import CANDLE_DTYPE  # noqa: E402
from exchanges.fake import FakeCCXTClient  # noqa: E402
import tools.run_fake_live as run_fake_live  # noqa: E402  (imports passivbot & friends)

_clients: list[FakeCCXTClient] = []
_orig_init = FakeCCXTClient.__init__


def _tracking_init(self, *args, **kwargs):
    _orig_init(self, *args, **kwargs)
    _clients.append(self)


FakeCCXTClient.__init__ = _tracking_init

_wall_utc_ms = utils.utc_ms
_wall_cm_now = candlestick_manager._utc_now_ms


def fake_utc_ms() -> float:
    if _clients:
        return float(_clients[-1].now_ms)
    return _wall_utc_ms()


def fake_cm_now_ms() -> int:
    return int(fake_utc_ms())


def install() -> int:
    patched = 0
    for module in list(sys.modules.values()):
        if module is None:
            continue
        for name in ("utc_ms", "_utc_now_ms"):
            current = getattr(module, name, None)
            if current is _wall_utc_ms:
                setattr(module, name, fake_utc_ms)
                patched += 1
            elif current is _wall_cm_now:
                setattr(module, name, fake_cm_now_ms)
                patched += 1
    return patched


_orig_fetch_ohlcv = FakeCCXTClient.fetch_ohlcv


async def ccxt_like_fetch_ohlcv(self, symbol, timeframe="1m", since=None, limit=None, params=None):
    """`since` + `limit` must return the OLDEST `limit` candles from `since` (ccxt
    semantics, what the candlestick manager pages on). The fake exchange slices
    `rows[-limit:]` instead, so a window longer than one page (1000 hourly
    candles) keeps a permanent hole at its start and the hourly EMAs never
    become available. Without `since` the newest-`limit` behaviour is kept.
    """
    if since is None or limit is None:
        return await _orig_fetch_ohlcv(self, symbol, timeframe, since, limit, params)
    rows = await _orig_fetch_ohlcv(self, symbol, timeframe, since, None, params)
    return rows[: int(limit)]


FakeCCXTClient.fetch_ohlcv = ccxt_like_fetch_ohlcv

_np_candles: dict[tuple[int, str], "np.ndarray"] = {}


def fast_prime_fake_candles(bot, fake_client) -> None:
    """Vectorised replacement for run_fake_live._prime_fake_candles.

    The original rebuilds the candle array row by row in Python on every step;
    with the ~60 days of 1m history that hourly EMA windows need (>100k rows
    per symbol) that is >1M iterations per step. Same result, built once.
    """
    if not hasattr(bot, "cm"):
        return
    provider = getattr(bot, "market_snapshot_provider", None)
    if provider is not None and hasattr(provider, "_cache"):
        provider._cache.clear()
    n = int(fake_client.current_index) + 1
    for symbol in fake_client.symbols:
        key = (id(fake_client), symbol)
        full = _np_candles.get(key)
        if full is None:
            raw = np.asarray(fake_client._candles_by_symbol.get(symbol, []), dtype=float).reshape(-1, 6)
            full = np.zeros(len(raw), dtype=CANDLE_DTYPE)
            if len(raw):
                full["ts"] = raw[:, 0].astype(np.int64)
                full["o"] = raw[:, 1]
                full["h"] = raw[:, 2]
                full["l"] = raw[:, 3]
                full["c"] = raw[:, 4]
                full["bv"] = raw[:, 5]
            _np_candles[key] = full
        bot.cm._cache[symbol] = full[:n].copy()
        bot.cm._ema_cache.pop(symbol, None)
        bot.cm._current_close_cache.pop(symbol, None)
        bot.cm._tf_range_cache.pop(symbol, None)


if __name__ == "__main__":
    n = install()
    run_fake_live._prime_fake_candles = fast_prime_fake_candles
    print(f"[fake_live_clock] patched {n} clock bindings + fast candle priming", flush=True)
    sys.argv = [sys.argv[0], *sys.argv[1:]]
    raise SystemExit(run_fake_live.main())
