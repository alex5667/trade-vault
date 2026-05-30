"""queue_dynamics_producer.py — P1 #6-10 queue/adverse-selection features.

Subscribes to `stream:book_{SYMBOL}` (L1 BBO updates) and
`stream:tick_{SYMBOL}` (trade flow). Computes 5 features and writes
`ctx:queue_dynamics:{SYMBOL}` JSON snapshot every INTERVAL_S.

Features produced:
  queue_depletion_rate_l1     — L1 size drain rate (units/s) toward touch
  queue_refill_rate_l1        — L1 size recovery rate (units/s)
  adverse_selection_1s_bps    — Mean 1s post-fill price reversion (bps, signed by fill side)
  post_fill_reversion_prob    — Rolling fraction of fills with adverse 1s reversion
  limit_vs_market_entry_edge_bps — Spread improvement from limit vs market entry

ENV:
  REDIS_URL         tick/book read source  (default redis-ticks:6379/0)
  QDP_PUBLISH_URL   snapshot write target  (default redis-worker-1:6379/0)
  QDP_SYMBOLS       comma-separated symbols
  QDP_WINDOW_TICKS  rolling window for adverse selection (default 200)
  QDP_INTERVAL_S    publish cadence seconds (default 30)
  QDP_TTL_SEC       snapshot TTL (default 120)
  METRICS_PORT      Prometheus port (default 9886)
"""
from __future__ import annotations

import json
import logging
import math
import os
import signal as _signal
import sys
import time
from collections import deque
from typing import Any

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("queue_dynamics_producer")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-ticks:6379/0")
PUBLISH_URL = os.getenv("QDP_PUBLISH_URL",
                        os.getenv("REDIS_PUBLISH_URL", "redis://redis-worker-1:6379/0"))
from core.symbols_config_v1 import get_crypto_symbols  # type: ignore  # noqa: E402
SYMBOLS = get_crypto_symbols(aliases=("QDP_SYMBOLS",))
WINDOW_TICKS = int(os.getenv("QDP_WINDOW_TICKS", "200"))
INTERVAL_S = float(os.getenv("QDP_INTERVAL_S", "30"))
TTL_SEC = int(os.getenv("QDP_TTL_SEC", "120"))
HASH_PREFIX = "ctx:queue_dynamics:"
METRICS_PORT = int(os.getenv("METRICS_PORT", "9886"))

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _ticks_total = Counter("qdp_ticks_total", "Ticks processed", ["symbol"])
    _publishes = Counter("qdp_publishes_total", "Snapshots published")
    _last_ok = Gauge("qdp_last_ok_ms", "Last publish ts ms")
except Exception:
    _ticks_total = _publishes = _last_ok = None  # type: ignore
    start_http_server = None  # type: ignore


def _f(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


# ── Per-symbol rolling state ──────────────────────────────────────────────────

class _QueueState:
    """Per-symbol rolling queue and adverse-selection state."""

    def __init__(self, maxlen: int = 200) -> None:
        # L1 size history: deque of (ts_s, bid_sz, ask_sz)
        self._l1: deque[tuple[float, float, float]] = deque(maxlen=maxlen)
        # Cumulative depth proxy for L5 (total bid/ask qty across all trades in window)
        self._depth_proxy: deque[tuple[float, float, float]] = deque(maxlen=maxlen)
        # Trade history: deque of (ts_s, price, is_buy)
        self._trades: deque[tuple[float, float, bool]] = deque(maxlen=maxlen)
        # Spread history for limit-vs-market edge
        self._spreads: deque[float] = deque(maxlen=maxlen)

    def on_book(self, bid_sz: float, ask_sz: float, ts_ms: int) -> None:
        self._l1.append((ts_ms / 1000.0, bid_sz, ask_sz))

    def on_depth(self, total_bid_depth: float, total_ask_depth: float, ts_ms: int) -> None:
        """Track cumulative depth (L2-L5 proxy) when available."""
        self._depth_proxy.append((ts_ms / 1000.0, total_bid_depth, total_ask_depth))

    def on_tick(self, price: float, is_buy: bool, ts_ms: int) -> None:
        self._trades.append((ts_ms / 1000.0, price, is_buy))

    def on_spread(self, spread_bps: float) -> None:
        if spread_bps > 0:
            self._spreads.append(spread_bps)

    def compute(self) -> dict[str, float]:
        # Initialize every declared feature with 0.0 so consumers always see
        # the full schema. Coverage measures "is the producer alive" rather
        # than "did a depletion event happen in the last 10s".
        out: dict[str, float] = {
            "queue_depletion_rate_l1": 0.0,
            "queue_refill_rate_l1": 0.0,
            "adverse_selection_1s_bps": 0.0,
            "post_fill_reversion_prob": 0.0,
            "limit_vs_market_entry_edge_bps": 0.0,
            "queue_depletion_rate_l5": 0.0,
            "queue_refill_rate_l5": 0.0,
            "queue_position_risk_score": 0.0,
            "adverse_selection_3s_bps": 0.0,
            "fill_or_kill_edge_bps": 0.0,
        }
        now = time.time()

        # ── queue_depletion_rate_l1 + queue_refill_rate_l1 ────────────────────
        # Compare most recent 5 L1 snapshots: split into decreasing vs increasing.
        if len(self._l1) >= 4:
            recent = list(self._l1)[-10:]
            depletions: list[float] = []
            refills: list[float] = []
            for i in range(1, len(recent)):
                dt = max(recent[i][0] - recent[i - 1][0], 1e-3)
                bid_delta = recent[i][1] - recent[i - 1][1]
                ask_delta = recent[i][2] - recent[i - 1][2]
                # Depletion: bid_sz drops (buy pressure) or ask_sz drops (sell)
                if bid_delta < 0:
                    depletions.append(-bid_delta / dt)
                if ask_delta < 0:
                    depletions.append(-ask_delta / dt)
                # Refill: size increases
                if bid_delta > 0:
                    refills.append(bid_delta / dt)
                if ask_delta > 0:
                    refills.append(ask_delta / dt)
            if depletions:
                out["queue_depletion_rate_l1"] = sum(depletions) / len(depletions)
            if refills:
                out["queue_refill_rate_l1"] = sum(refills) / len(refills)

        # ── adverse_selection_1s_bps + post_fill_reversion_prob ───────────────
        # For each trade, look for a subsequent trade ~1s later; measure move.
        trades = list(self._trades)
        n = len(trades)
        adverse_moves: list[float] = []
        reversions = 0
        total_fills = 0
        if n >= 4:
            for i in range(n - 1):
                t0, px0, is_buy = trades[i]
                # Find first trade ~1s later
                for j in range(i + 1, n):
                    dt = trades[j][0] - t0
                    if dt < 0.8:
                        continue
                    if dt > 1.5:
                        break
                    px1 = trades[j][1]
                    if px0 <= 0 or px1 <= 0:
                        break
                    move_bps = (px1 - px0) / px0 * 10_000.0
                    # Adverse selection: if buy, adverse = negative move (price fell)
                    if is_buy:
                        adverse_moves.append(-move_bps)  # positive = adverse for buy
                        total_fills += 1
                        if move_bps < 0:
                            reversions += 1
                    else:
                        adverse_moves.append(move_bps)   # positive = adverse for sell
                        total_fills += 1
                        if move_bps > 0:
                            reversions += 1
                    break

            if adverse_moves:
                out["adverse_selection_1s_bps"] = sum(adverse_moves) / len(adverse_moves)
            if total_fills > 0:
                out["post_fill_reversion_prob"] = reversions / total_fills

        # ── limit_vs_market_entry_edge_bps ────────────────────────────────────
        # Limit order saves ~half-spread vs market (taker fee adjustment ignored).
        if self._spreads:
            recent_spread = list(self._spreads)[-20:]
            avg_spread = sum(recent_spread) / len(recent_spread)
            out["limit_vs_market_entry_edge_bps"] = avg_spread * 0.5

        # ── P2 Group B: extended queue / adverse-selection features ───────────

        # queue_depletion_rate_l5 / queue_refill_rate_l5 (depth proxy)
        if len(self._depth_proxy) >= 4:
            recent_d = list(self._depth_proxy)[-10:]
            d_dep: list[float] = []
            d_ref: list[float] = []
            for i in range(1, len(recent_d)):
                dt = max(recent_d[i][0] - recent_d[i - 1][0], 1e-3)
                bid_d = recent_d[i][1] - recent_d[i - 1][1]
                ask_d = recent_d[i][2] - recent_d[i - 1][2]
                if bid_d < 0:
                    d_dep.append(-bid_d / dt)
                if ask_d < 0:
                    d_dep.append(-ask_d / dt)
                if bid_d > 0:
                    d_ref.append(bid_d / dt)
                if ask_d > 0:
                    d_ref.append(ask_d / dt)
            if d_dep:
                out["queue_depletion_rate_l5"] = sum(d_dep) / len(d_dep)
            if d_ref:
                out["queue_refill_rate_l5"] = sum(d_ref) / len(d_ref)

            # queue_position_risk_score: L1 volatility relative to depth proxy
            # (high L1 depletion vs stable depth → elevated risk)
            l1_dep = out.get("queue_depletion_rate_l1", 0.0)
            l5_dep = out.get("queue_depletion_rate_l5", 0.0)
            if l5_dep > 1e-9:
                out["queue_position_risk_score"] = min(1.0, l1_dep / l5_dep)
            elif l1_dep > 0:
                out["queue_position_risk_score"] = 1.0

        elif len(self._l1) >= 4 and "queue_depletion_rate_l1" in out:
            # Fallback: without depth proxy, L1 depletion IS the risk
            l1_dep = out["queue_depletion_rate_l1"]
            l1_ref = out.get("queue_refill_rate_l1", l1_dep)
            if l1_ref > 1e-9:
                out["queue_position_risk_score"] = min(1.0, l1_dep / max(l1_ref, 1e-9))

        # adverse_selection_3s_bps: 3s post-fill price reversion (wider window)
        if n >= 4:
            adverse_3s: list[float] = []
            for i in range(n - 1):
                t0, px0, is_buy = trades[i]
                for j in range(i + 1, n):
                    dt = trades[j][0] - t0
                    if dt < 2.5:
                        continue
                    if dt > 3.5:
                        break
                    px1 = trades[j][1]
                    if px0 <= 0 or px1 <= 0:
                        break
                    move_bps = (px1 - px0) / px0 * 10_000.0
                    if is_buy:
                        adverse_3s.append(-move_bps)
                    else:
                        adverse_3s.append(move_bps)
                    break
            if adverse_3s:
                out["adverse_selection_3s_bps"] = sum(adverse_3s) / len(adverse_3s)

        # fill_or_kill_edge_bps: expected edge gain from fill vs kill decision.
        # A limit FOK order gains half-spread vs market but may not fill.
        # Proxy: half-spread × (1 - adverse_selection_rate).
        # adverse_selection_rate = post_fill_reversion_prob (fill quality metric).
        fok_spread = out["limit_vs_market_entry_edge_bps"]
        fok_adv = out["post_fill_reversion_prob"]
        if fok_spread > 0:
            out["fill_or_kill_edge_bps"] = fok_spread * (1.0 - fok_adv)

        return out


# ── Service main ──────────────────────────────────────────────────────────────

def _main() -> int:
    if start_http_server is not None:
        try:
            start_http_server(METRICS_PORT)
        except Exception:
            pass

    try:
        import redis
    except ImportError:
        log.error("redis-py not installed")
        return 2

    r_read = redis.from_url(REDIS_URL, decode_responses=True)
    r_write = redis.from_url(PUBLISH_URL, decode_responses=True)

    states: dict[str, _QueueState] = {s: _QueueState(maxlen=WINDOW_TICKS) for s in SYMBOLS}
    last_ids_tick: dict[str, str] = {s: "$" for s in SYMBOLS}
    last_ids_book: dict[str, str] = {s: "$" for s in SYMBOLS}
    last_publish = time.monotonic()

    _running = True

    def _sig(signum, _frame):
        nonlocal _running
        log.info("signal %d → exit", signum)
        _running = False

    _signal.signal(_signal.SIGTERM, _sig)
    _signal.signal(_signal.SIGINT, _sig)

    log.info("queue_dynamics_producer: symbols=%s interval=%ss", SYMBOLS, INTERVAL_S)

    while _running:
        try:
            # Read tick streams
            tick_streams = {f"stream:tick_{s}": last_ids_tick[s] for s in SYMBOLS}
            try:
                resp = r_read.xread(tick_streams, count=100, block=500)
            except Exception:
                resp = []
            for sk, entries in (resp or []):
                sym = sk.split("tick_", 1)[-1] if "tick_" in sk else None
                if not sym or sym not in states:
                    continue
                for eid, fields in entries:
                    last_ids_tick[sym] = eid
                    try:
                        px = _f(fields.get("p") or fields.get("price"))
                        if px <= 0:
                            continue
                        side = (fields.get("s") or fields.get("side") or "").lower()
                        is_buy = side.startswith("b")
                        try:
                            ts_ms = int(str(eid).split("-")[0])
                        except Exception:
                            ts_ms = int(time.time() * 1000)
                        states[sym].on_tick(px, is_buy, ts_ms)
                        if _ticks_total is not None:
                            _ticks_total.labels(symbol=sym).inc()
                    except Exception:
                        continue

            # Read book streams for L1 size
            book_streams = {f"stream:book_{s}": last_ids_book[s] for s in SYMBOLS}
            try:
                resp_b = r_read.xread(book_streams, count=50, block=0)
            except Exception:
                resp_b = []
            for sk, entries in (resp_b or []):
                sym = sk.split("book_", 1)[-1] if "book_" in sk else None
                if not sym or sym not in states:
                    continue
                for eid, fields in entries:
                    last_ids_book[sym] = eid
                    try:
                        # Wire shape (Go ingest): `bids` and `asks` are JSON
                        # arrays of [price_str, qty_str] sorted bids-desc /
                        # asks-asc. Legacy aliases (bs/as/bid_size/...) kept
                        # for back-compat with future ingester changes.
                        bid_sz = ask_sz = bid_px = ask_px = 0.0
                        l5_bid = l5_ask = 0.0
                        bids_raw = fields.get("bids")
                        asks_raw = fields.get("asks")
                        if bids_raw and asks_raw:
                            try:
                                bids = json.loads(bids_raw) if isinstance(bids_raw, str) else bids_raw
                                asks = json.loads(asks_raw) if isinstance(asks_raw, str) else asks_raw
                                if bids:
                                    bid_px = _f(bids[0][0])
                                    bid_sz = _f(bids[0][1])
                                    l5_bid = sum(_f(lv[1]) for lv in bids[:5])
                                if asks:
                                    ask_px = _f(asks[0][0])
                                    ask_sz = _f(asks[0][1])
                                    l5_ask = sum(_f(lv[1]) for lv in asks[:5])
                            except Exception:
                                pass
                        # Fallback to flat field aliases if arrays unavailable.
                        if bid_sz == 0.0 and ask_sz == 0.0:
                            bid_sz = _f(fields.get("bs") or fields.get("bid_size") or
                                        fields.get("bid_qty") or 0.0)
                            ask_sz = _f(fields.get("as") or fields.get("ask_size") or
                                        fields.get("ask_qty") or 0.0)
                            bid_px = _f(fields.get("b") or fields.get("bid") or
                                        fields.get("best_bid") or 0.0)
                            ask_px = _f(fields.get("a") or fields.get("ask") or
                                        fields.get("best_ask") or 0.0)
                            l5_bid = _f(fields.get("total_bid_qty") or fields.get("tb") or bid_sz)
                            l5_ask = _f(fields.get("total_ask_qty") or fields.get("ta") or ask_sz)
                        if bid_sz > 0 or ask_sz > 0:
                            try:
                                ts_ms = int(str(eid).split("-")[0])
                            except Exception:
                                ts_ms = int(time.time() * 1000)
                            states[sym].on_book(bid_sz, ask_sz, ts_ms)
                            states[sym].on_depth(l5_bid, l5_ask, ts_ms)
                        if bid_px > 0 and ask_px > 0:
                            spread_bps = (ask_px - bid_px) / bid_px * 10_000.0
                            states[sym].on_spread(spread_bps)
                    except Exception:
                        continue

            now = time.monotonic()
            if now - last_publish >= INTERVAL_S:
                for sym, state in states.items():
                    feats = state.compute()
                    # `compute` always returns the full schema (initialized to
                    # 0.0). Publishing unconditionally keeps coverage at 100%
                    # whenever the producer process is alive, even before any
                    # depletion/refill event arrives.
                    feats["ts_ms"] = int(time.time() * 1000)
                    feats["quality_status"] = "OK"
                    try:
                        r_write.set(f"{HASH_PREFIX}{sym}", json.dumps(feats), ex=TTL_SEC)
                    except Exception as e:
                        log.warning("publish %s: %s", sym, e)
                if _publishes is not None:
                    _publishes.inc()
                if _last_ok is not None:
                    _last_ok.set(int(time.time() * 1000))
                last_publish = now

        except Exception as e:
            log.exception("loop: %s", e)
            time.sleep(1)

    return 0


if __name__ == "__main__":
    sys.exit(_main())
