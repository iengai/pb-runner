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


# --- HSL trace (pb-runner P4 HSL parity; env PB_RUNNER_HSL_TRACE) ---------
#
# Logs, as JSON lines, every input the account-level HSL state machine
# consumes and the state it leaves behind (docs/RECORDER.md section A):
# `init` (start-up replay result), `check_begin`/`check_end`
# (`_equity_hard_stop_check` inputs and state), `sample`
# (`_equity_hard_stop_apply_sample` outside the start-up replay),
# `supervisor_begin`/`supervisor_end`, `counts` (position/order counts the
# supervisor saw after its authoritative refresh), `sync_flat`, `finalize`,
# `cooldown_handle`, `reset` and `compute` (one per
# `compute_ideal_orders_json` call, keyed by the recording stem hash).
# `pb-snapcheck --hsl-trace` replays the inputs through the Rust machine and
# asserts the states.
#
# Coin mode (`live.hsl_signal_mode = "coin"`, D20) adds `coin_init`,
# `coin_check_begin`/`coin_check_end` (`_equity_hard_stop_check_coin`: the
# per-pair realized peak/last, unrealized pnl and blocking-order counts, the
# pair states, the runtime forced modes and the replay-pending pairs),
# `coin_sample`, `coin_flatten` (flatten-fill lookups), `coin_finalize`,
# `coin_reset`, `coin_cooldown_handle`, `coin_repanic_refresh`,
# `coin_supervisor_begin`/`coin_supervisor_end`
# (`_equity_hard_stop_run_coin_red_supervisor`), `coin_iter_begin` (after
# each `refresh_protective_authoritative_state`) and `coin_iter_end` (the
# states when the protective planning starts, with its target pairs); the
# `compute` records of the protective input carry `protective: true` and
# the input's symbol order.

_hsl_trace_path = os.environ.get("PB_RUNNER_HSL_TRACE")
_hsl_trace_seq = [0]
_hsl_bot = [None]


def _hsl_trace_write(record: dict) -> None:
    import json

    _hsl_trace_seq[0] += 1
    record = {"seq": _hsl_trace_seq[0], **record}
    with open(_hsl_trace_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, sort_keys=True) + "\n")


def _hsl_state_summary(bot, pside: str) -> dict:
    state = bot._hsl_state(pside)
    rt = state["runtime"]
    return {
        "initialized": bool(rt.initialized()),
        "red_latched": bool(rt.red_latched()),
        "red_seen_in_episode": bool(rt.red_seen_in_episode()),
        "tier": str(rt.tier()),
        "peak_strategy_equity": float(rt.peak_strategy_equity()),
        "drawdown_ema": float(rt.drawdown_ema()),
        "rolling_peak_strategy_equity": float(rt.rolling_peak_strategy_equity()),
        "halted": bool(state["halted"]),
        "no_restart_latched": bool(state["no_restart_latched"]),
        "no_restart_peak_strategy_equity": float(state["no_restart_peak_strategy_equity"]),
        "cooldown_until_ms": state["cooldown_until_ms"],
        "pending_red_since_ms": state["pending_red_since_ms"],
        "red_flat_confirmations": int(state["red_flat_confirmations"]),
        "cooldown_intervention_active": bool(state["cooldown_intervention_active"]),
        "cooldown_repanic_reset_pending": bool(state["cooldown_repanic_reset_pending"]),
        "cooldown_repanic_since_ms": state["cooldown_repanic_since_ms"],
        "cooldown_unresolved_residue": bool(state["cooldown_unresolved_residue"]),
        "last_metrics": state["last_metrics"],
        "pending_stop_event": state["pending_stop_event"],
        "last_stop_event": state["last_stop_event"],
    }


def _hsl_states(bot) -> dict:
    return {pside: _hsl_state_summary(bot, pside) for pside in bot._hsl_psides()}


def _hsl_positions(bot) -> list:
    out = []
    for symbol, sides in (getattr(bot, "positions", {}) or {}).items():
        for pside in ("long", "short"):
            pos = sides.get(pside, {}) if isinstance(sides, dict) else {}
            size = float(pos.get("size", 0.0) or 0.0)
            if size != 0.0:
                out.append({"symbol": symbol, "pside": pside, "size": size,
                            "price": float(pos.get("price", 0.0) or 0.0)})
    return out


async def _hsl_inputs(bot) -> dict:
    out = {
        "ts": int(bot.get_exchange_time()),
        "balance": float(bot.get_raw_balance()),
        "realized_pnl_total": float(bot._equity_hard_stop_realized_pnl_now()),
        "positions": _hsl_positions(bot),
    }
    for pside in bot._hsl_psides():
        out[f"realized_pnl_{pside}"] = float(bot._equity_hard_stop_realized_pnl_now(pside))
        out[f"unrealized_pnl_{pside}"] = float(await bot._calc_upnl_sum_strict(pside))
    return out


def _hsl_counts(bot, psides) -> dict:
    out = {}
    for pside in psides:
        entry_orders, nonpanic = bot._equity_hard_stop_count_blocking_open_orders(pside)
        out[pside] = {
            "n_positions": int(bot._equity_hard_stop_count_open_positions(pside)),
            "entry_orders": int(entry_orders),
            "nonpanic_close_orders": int(nonpanic),
        }
    return out


def _hsl_coin_state_summary(state: dict) -> dict:
    rt = state["runtime"]
    return {
        "initialized": bool(rt.initialized()),
        "red_latched": bool(rt.red_latched()),
        "red_seen_in_episode": bool(rt.red_seen_in_episode()),
        "tier": str(rt.tier()),
        "peak_strategy_equity": float(rt.peak_strategy_equity()),
        "drawdown_ema": float(rt.drawdown_ema()),
        "rolling_peak_strategy_equity": float(rt.rolling_peak_strategy_equity()),
        "halted": bool(state["halted"]),
        "no_restart_latched": bool(state["no_restart_latched"]),
        "no_restart_peak_strategy_equity": float(state["no_restart_peak_strategy_equity"]),
        "cooldown_until_ms": state["cooldown_until_ms"],
        "pending_red_since_ms": state["pending_red_since_ms"],
        "red_flat_confirmations": int(state["red_flat_confirmations"]),
        "cooldown_intervention_active": bool(state["cooldown_intervention_active"]),
        "cooldown_repanic_reset_pending": bool(state["cooldown_repanic_reset_pending"]),
        "cooldown_repanic_since_ms": state["cooldown_repanic_since_ms"],
        "cooldown_repanic_start_sizes": state.get("cooldown_repanic_start_sizes"),
        "cooldown_unresolved_residue": bool(state["cooldown_unresolved_residue"]),
        "pnl_reset_timestamp_ms": state.get("pnl_reset_timestamp_ms"),
        "last_metrics": state["last_metrics"],
        "pending_stop_event": state["pending_stop_event"],
        "last_stop_event": state["last_stop_event"],
    }


def _hsl_coin_states(bot) -> dict:
    out = {}
    for pside, states in (getattr(bot, "_equity_hard_stop_coin", None) or {}).items():
        out[pside] = {symbol: _hsl_coin_state_summary(st) for symbol, st in sorted(states.items())}
    return out


def _hsl_coin_modes(bot) -> dict:
    forced = getattr(bot, "_runtime_forced_modes", None) or {}
    pending = getattr(bot, "_equity_hard_stop_coin_replay_pending_pairs", None) or set()
    return {
        "forced": {pside: dict(sorted(forced.get(pside, {}).items())) for pside in ("long", "short")},
        "replay_pending": sorted([list(p) for p in pending]),
        "initialized": bool(getattr(bot, "_equity_hard_stop_coin_initialized", False)),
    }


def _hsl_coin_active_pairs(bot) -> list:
    out = []
    for pside, states in (getattr(bot, "_equity_hard_stop_coin", None) or {}).items():
        for symbol, st in states.items():
            if bot._equity_hard_stop_coin_needs_panic_supervision(pside, symbol, st):
                out.append([pside, symbol])
    return out


async def _hsl_coin_inputs(bot, pairs=None) -> dict:
    ts = int(bot.get_exchange_time())
    symbols = sorted(bot._equity_hard_stop_coin_symbols())
    states = getattr(bot, "_equity_hard_stop_coin", None) or {}
    events = bot._pnls_manager.get_events() if bot._pnls_manager is not None else []
    out = {
        "ts": ts,
        "balance": float(bot.get_raw_balance()),
        "positions": _hsl_positions(bot),
        "symbols": symbols,
        "ledger": {"events": len(events),
                   "last_ts": int(events[-1].timestamp) if events else None},
        "pairs": {},
    }
    if pairs is None:
        pairs = []
        for pside in bot._hsl_psides():
            if not bot._equity_hard_stop_coin_active_pside(pside):
                continue
            for symbol in symbols:
                if bot._equity_hard_stop_coin_active_pside(pside, symbol):
                    pairs.append((pside, symbol))
    for pside, symbol in pairs:
        st = states.get(pside, {}).get(symbol)
        reset_ts = st.get("pnl_reset_timestamp_ms") if st is not None else None
        peak, last = bot._equity_hard_stop_coin_realized_pnl_peak_last(pside, symbol, ts, reset_ts)
        entry_orders, nonpanic = bot._equity_hard_stop_count_blocking_open_orders_symbol(pside, symbol)
        out["pairs"][f"{pside}:{symbol}"] = {
            "peak_realized": float(peak), "last_realized": float(last),
            "unrealized_pnl": float(await bot._calc_upnl_sum_strict(pside, symbol)),
            "reset_ts": reset_ts,
            "entry_orders": int(entry_orders), "nonpanic_close_orders": int(nonpanic),
        }
    return out


def install_hsl_trace() -> None:
    import hashlib
    import passivbot
    import passivbot_rust as pbr

    if os.path.exists(_hsl_trace_path):
        os.remove(_hsl_trace_path)
    Bot = passivbot.Passivbot
    install_hsl_coin_trace(Bot)

    orig_apply = Bot._equity_hard_stop_apply_sample

    def apply_sample(self, pside, timestamp_ms, balance, realized_pnl_total, realized_pnl_pside,
                     unrealized_pnl_pside, unrealized_pnl_total=None, *, latch_red=True):
        metrics = orig_apply(self, pside, timestamp_ms, balance, realized_pnl_total,
                             realized_pnl_pside, unrealized_pnl_pside,
                             unrealized_pnl_total=unrealized_pnl_total, latch_red=latch_red)
        if not getattr(self, "_pbr_hsl_in_init", False):
            _hsl_trace_write({
                "kind": "sample", "pside": pside, "ts": int(timestamp_ms),
                "balance": float(balance), "realized_pnl_total": float(realized_pnl_total),
                "realized_pnl_pside": float(realized_pnl_pside),
                "unrealized_pnl_pside": float(unrealized_pnl_pside),
                "unrealized_pnl_total": unrealized_pnl_total, "latch_red": bool(latch_red),
                "metrics": metrics,
            })
        return metrics

    Bot._equity_hard_stop_apply_sample = apply_sample

    orig_init = Bot._equity_hard_stop_initialize_from_history

    async def init_from_history(self):
        _hsl_bot[0] = self
        self._pbr_hsl_in_init = True
        try:
            await orig_init(self)
        finally:
            self._pbr_hsl_in_init = False
        rec = await _hsl_inputs(self)
        rec.update({"kind": "init", "after": _hsl_states(self)})
        _hsl_trace_write(rec)

    Bot._equity_hard_stop_initialize_from_history = init_from_history

    orig_check = Bot._equity_hard_stop_check

    async def check(self):
        _hsl_bot[0] = self
        rec = await _hsl_inputs(self)
        rec.update({"kind": "check_begin", "before": _hsl_states(self)})
        _hsl_trace_write(rec)
        out = await orig_check(self)
        _hsl_trace_write({"kind": "check_end", "ts": int(self.get_exchange_time()),
                          "after": _hsl_states(self)})
        return out

    Bot._equity_hard_stop_check = check

    orig_finalize = Bot._equity_hard_stop_finalize_red_stop

    async def finalize(self, pside, stop_event, **kwargs):
        await orig_finalize(self, pside, stop_event, **kwargs)
        _hsl_trace_write({"kind": "finalize", "pside": pside, "ts": int(self.get_exchange_time()),
                          "stop_event": stop_event, "after": _hsl_state_summary(self, pside)})

    Bot._equity_hard_stop_finalize_red_stop = finalize

    orig_reset = Bot._equity_hard_stop_reset_after_restart

    def reset_after_restart(self, pside):
        orig_reset(self, pside)
        if not getattr(self, "_pbr_hsl_in_init", False):
            _hsl_trace_write({"kind": "reset", "pside": pside, "ts": int(self.get_exchange_time())})

    Bot._equity_hard_stop_reset_after_restart = reset_after_restart

    orig_handle = Bot._equity_hard_stop_handle_position_during_cooldown

    async def handle_cooldown(self, pside, now_ms):
        changed = await orig_handle(self, pside, now_ms)
        _hsl_trace_write({"kind": "cooldown_handle", "pside": pside, "ts": int(now_ms),
                          "positions": _hsl_positions(self), "changed": bool(changed),
                          "after": _hsl_state_summary(self, pside)})
        return changed

    Bot._equity_hard_stop_handle_position_during_cooldown = handle_cooldown

    orig_step = run_fake_live._run_fake_red_supervisor_step

    async def supervisor_step(bot):
        active = run_fake_live._fake_active_red_psides(bot)
        rec = await _hsl_inputs(bot)
        rec.update({"kind": "supervisor_begin", "psides": active})
        _hsl_trace_write(rec)
        bot._pbr_hsl_trace_counts = True
        try:
            result = await orig_step(bot)
        finally:
            bot._pbr_hsl_trace_counts = False
        _hsl_trace_write({"kind": "supervisor_end", "ts": int(bot.get_exchange_time()),
                          "result": result, "after": _hsl_states(bot)})
        return result

    run_fake_live._run_fake_red_supervisor_step = supervisor_step

    orig_refresh = Bot.refresh_authoritative_state

    async def refresh_authoritative_state(self):
        ok = await orig_refresh(self)
        if getattr(self, "_pbr_hsl_trace_counts", False):
            _hsl_trace_write({"kind": "counts", "ts": int(self.get_exchange_time()), "ok": bool(ok),
                              "counts": _hsl_counts(self, self._hsl_psides()),
                              "positions": _hsl_positions(self)})
        return ok

    Bot.refresh_authoritative_state = refresh_authoritative_state

    orig_sync = run_fake_live._finalize_fake_terminal_red_if_sync_flat

    async def sync_flat(bot, active_red_psides):
        result = await orig_sync(bot, active_red_psides)
        rec = await _hsl_inputs(bot)
        rec.update({"kind": "sync_flat", "psides": list(active_red_psides), "result": bool(result),
                    "counts": _hsl_counts(bot, bot._hsl_psides()), "after": _hsl_states(bot)})
        _hsl_trace_write(rec)
        return result

    run_fake_live._finalize_fake_terminal_red_if_sync_flat = sync_flat

    orig_compute = pbr.compute_ideal_orders_json

    def compute(input_json: str) -> str:
        bot = _hsl_bot[0]
        # The input's `symbol_idx` order (SPEC 2.1) so the replay can name
        # the recorded symbols even when the universe shrank.
        protective = bot is not None and getattr(bot, "_pbr_hsl_protective", None) is not None
        if protective:
            symbols = list(bot._pbr_hsl_protective)
        else:
            symbols = (
                sorted(set(bot.active_symbols or bot._build_live_symbol_universe()))
                if bot is not None
                else None
            )
        rec = {"kind": "compute", "symbols": symbols,
               "hash": hashlib.sha256(input_json.encode("utf-8")).hexdigest()[:16]}
        if protective:
            rec["protective"] = True
        _hsl_trace_write(rec)
        return orig_compute(input_json)

    pbr.compute_ideal_orders_json = compute


def install_hsl_coin_trace(Bot) -> None:
    orig_coin_init = Bot._equity_hard_stop_initialize_coin_from_history

    async def coin_init(self):
        _hsl_bot[0] = self
        self._pbr_hsl_in_init = True
        try:
            await orig_coin_init(self)
        finally:
            self._pbr_hsl_in_init = False
        if self._equity_hard_stop_signal_mode() != "coin" or not self._equity_hard_stop_enabled():
            return
        rec = await _hsl_coin_inputs(self)
        rec.update({"kind": "coin_init", "after": _hsl_coin_states(self),
                    "modes": _hsl_coin_modes(self)})
        _hsl_trace_write(rec)

    Bot._equity_hard_stop_initialize_coin_from_history = coin_init

    orig_coin_check = Bot._equity_hard_stop_check_coin

    async def coin_check(self):
        _hsl_bot[0] = self
        rec = await _hsl_coin_inputs(self)
        rec.update({"kind": "coin_check_begin", "before": _hsl_coin_states(self),
                    "modes": _hsl_coin_modes(self)})
        _hsl_trace_write(rec)
        out = await orig_coin_check(self)
        _hsl_trace_write({"kind": "coin_check_end", "ts": int(self.get_exchange_time()),
                          "after": _hsl_coin_states(self), "modes": _hsl_coin_modes(self)})
        return out

    Bot._equity_hard_stop_check_coin = coin_check

    orig_coin_metrics = Bot._equity_hard_stop_apply_coin_metrics_sample

    def coin_metrics_sample(self, pside, symbol, timestamp_ms, balance, peak_realized,
                            last_realized, current_upnl, *, latch_red=True):
        metrics = orig_coin_metrics(self, pside, symbol, timestamp_ms, balance, peak_realized,
                                    last_realized, current_upnl, latch_red=latch_red)
        if not getattr(self, "_pbr_hsl_in_init", False):
            _hsl_trace_write({
                "kind": "coin_sample", "pside": pside, "symbol": symbol,
                "ts": int(timestamp_ms), "balance": float(balance),
                "peak_realized": float(peak_realized), "last_realized": float(last_realized),
                "unrealized_pnl": float(current_upnl), "latch_red": bool(latch_red),
                "metrics": metrics,
            })
        return metrics

    Bot._equity_hard_stop_apply_coin_metrics_sample = coin_metrics_sample

    orig_coin_finalize = Bot._equity_hard_stop_finalize_coin_red_stop

    async def coin_finalize(self, pside, symbol, stop_event, **kwargs):
        await orig_coin_finalize(self, pside, symbol, stop_event, **kwargs)
        _hsl_trace_write({"kind": "coin_finalize", "pside": pside, "symbol": symbol,
                          "ts": int(self.get_exchange_time()), "stop_event": stop_event,
                          "after": _hsl_coin_state_summary(self._hsl_coin_state(pside, symbol)),
                          "modes": _hsl_coin_modes(self)})

    Bot._equity_hard_stop_finalize_coin_red_stop = coin_finalize

    orig_coin_reset = Bot._equity_hard_stop_reset_coin_after_restart

    def coin_reset(self, pside, symbol):
        orig_coin_reset(self, pside, symbol)
        if not getattr(self, "_pbr_hsl_in_init", False):
            _hsl_trace_write({"kind": "coin_reset", "pside": pside, "symbol": symbol,
                              "ts": int(self.get_exchange_time())})

    Bot._equity_hard_stop_reset_coin_after_restart = coin_reset

    orig_coin_handle = Bot._equity_hard_stop_handle_coin_position_during_cooldown

    async def coin_handle_cooldown(self, pside, symbol, now_ms):
        changed = await orig_coin_handle(self, pside, symbol, now_ms)
        _hsl_trace_write({"kind": "coin_cooldown_handle", "pside": pside, "symbol": symbol,
                          "ts": int(now_ms), "positions": _hsl_positions(self),
                          "changed": bool(changed),
                          "after": _hsl_coin_state_summary(self._hsl_coin_state(pside, symbol)),
                          "modes": _hsl_coin_modes(self)})
        return changed

    Bot._equity_hard_stop_handle_coin_position_during_cooldown = coin_handle_cooldown

    # Not bound on `Passivbot` in the v8.1.0 checkout (hsl:4776 exists as a
    # module function only; hsl:4958 / hsl:8261 would raise AttributeError).
    orig_coin_repanic = getattr(Bot, "_equity_hard_stop_refresh_coin_cooldown_after_repanic", None)

    async def coin_repanic(self, pside, symbol, now_ms):
        result = await orig_coin_repanic(self, pside, symbol, now_ms)
        _hsl_trace_write({"kind": "coin_repanic_refresh", "pside": pside, "symbol": symbol,
                          "ts": int(now_ms), "result": bool(result),
                          "after": _hsl_coin_state_summary(self._hsl_coin_state(pside, symbol)),
                          "modes": _hsl_coin_modes(self)})
        return result

    if orig_coin_repanic is not None:
        Bot._equity_hard_stop_refresh_coin_cooldown_after_repanic = coin_repanic

    orig_flatten = Bot._equity_hard_stop_flatten_fill_timestamp_with_refresh

    async def flatten_with_refresh(self, pside, now_ms, *, symbol=None, since_ms,
                                   replay_start_sizes=None):
        result = await orig_flatten(self, pside, now_ms, symbol=symbol, since_ms=since_ms,
                                    replay_start_sizes=replay_start_sizes)
        if symbol is not None:
            events = self._pnls_manager.get_events() if self._pnls_manager is not None else []
            _hsl_trace_write({"kind": "coin_flatten", "pside": pside, "symbol": symbol,
                              "ts": int(now_ms), "since_ms": since_ms,
                              "replay_start_sizes": replay_start_sizes, "result": result,
                              "ledger": {"events": len(events),
                                         "last_ts": int(events[-1].timestamp) if events else None}})
        return result

    Bot._equity_hard_stop_flatten_fill_timestamp_with_refresh = flatten_with_refresh

    orig_coin_supervisor = Bot._equity_hard_stop_run_coin_red_supervisor

    async def coin_supervisor(self):
        _hsl_bot[0] = self
        rec = await _hsl_coin_inputs(self)
        rec.update({"kind": "coin_supervisor_begin", "active": _hsl_coin_active_pairs(self),
                    "before": _hsl_coin_states(self), "modes": _hsl_coin_modes(self)})
        _hsl_trace_write(rec)
        self._pbr_hsl_coin_iter = 0
        try:
            await orig_coin_supervisor(self)
        finally:
            iterations = self._pbr_hsl_coin_iter
            self._pbr_hsl_coin_iter = None
        _hsl_trace_write({"kind": "coin_supervisor_end", "ts": int(self.get_exchange_time()),
                          "iterations": iterations, "active": _hsl_coin_active_pairs(self),
                          "after": _hsl_coin_states(self), "modes": _hsl_coin_modes(self)})

    Bot._equity_hard_stop_run_coin_red_supervisor = coin_supervisor

    orig_protective_refresh = Bot.refresh_protective_authoritative_state

    async def protective_refresh(self):
        ok = await orig_protective_refresh(self)
        if getattr(self, "_pbr_hsl_coin_iter", None) is not None:
            self._pbr_hsl_coin_iter += 1
            active = _hsl_coin_active_pairs(self)
            rec = await _hsl_coin_inputs(self, pairs=[tuple(p) for p in active])
            rec.update({"kind": "coin_iter_begin", "ok": bool(ok), "iteration": self._pbr_hsl_coin_iter,
                        "active": active, "before": _hsl_coin_states(self),
                        "modes": _hsl_coin_modes(self)})
            _hsl_trace_write(rec)
        return ok

    Bot.refresh_protective_authoritative_state = protective_refresh

    orig_protective_plan = Bot.calc_protective_panic_orders_to_cancel_and_create

    async def protective_plan(self):
        targets = {s: sorted(p) for s, p in self._protective_panic_target_psides_by_symbol().items()}
        if getattr(self, "_pbr_hsl_coin_iter", None) is not None:
            _hsl_trace_write({"kind": "coin_iter_end", "ts": int(self.get_exchange_time()),
                              "iteration": self._pbr_hsl_coin_iter, "targets": targets,
                              "active": _hsl_coin_active_pairs(self),
                              "positions": _hsl_positions(self),
                              "after": _hsl_coin_states(self), "modes": _hsl_coin_modes(self)})
        position_symbols = []
        for symbol, psides in targets.items():
            pos = (getattr(self, "positions", {}) or {}).get(symbol, {})
            if any(float((pos.get(pside) or {}).get("size", 0.0) or 0.0) != 0.0 for pside in psides):
                position_symbols.append(symbol)
        self._pbr_hsl_protective = sorted(position_symbols)
        try:
            return await orig_protective_plan(self)
        finally:
            self._pbr_hsl_protective = None

    Bot.calc_protective_panic_orders_to_cancel_and_create = protective_plan


if __name__ == "__main__":
    n = install()
    run_fake_live._prime_fake_candles = fast_prime_fake_candles
    if _hsl_trace_path:
        install_hsl_trace()
    print(f"[fake_live_clock] patched {n} clock bindings + fast candle priming", flush=True)
    sys.argv = [sys.argv[0], *sys.argv[1:]]
    raise SystemExit(run_fake_live.main())
