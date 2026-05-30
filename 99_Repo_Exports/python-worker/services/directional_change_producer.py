"""directional_change_producer.py — P2 Group H directional-change (DC) features.

Subscribes to `stream:tick_{SYMBOL}` and detects DC events using the
Intrinsic Time framework (Glattfelder et al. 2011):

  A DC event occurs when price moves ≥ DC_THRESHOLD_BPS from the last DC price.
  The move after the threshold creates an "overshoot" in the same direction.

Features produced (written to `ctx:dc:{SYMBOL}` JSON):
  dc_event_dir           — direction of most recent DC: 1=up, -1=down, 0=none
  dc_event_age_ms        — ms since last DC event
  dc_overshoot_bps       — overshoot magnitude in bps past the threshold
  dc_reversal_count_15m  — alternating DC direction changes in last 15 min

ENV:
  REDIS_URL           tick read source (default redis-ticks:6379/0)
  DCP_PUBLISH_URL     snapshot write target (default redis-worker-1:6379/0)
  DCP_SYMBOLS         comma-separated symbols
  DCP_THRESHOLD_BPS   DC threshold in bps (default 50 = 0.5%)
  DCP_INTERVAL_S      publish cadence seconds (default 15)
  DCP_TTL_SEC         snapshot TTL (default 120)
  METRICS_PORT        Prometheus port (default 9896)
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
log = logging.getLogger("directional_change_producer")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-ticks:6379/0")
PUBLISH_URL = os.getenv("DCP_PUBLISH_URL",
                        os.getenv("REDIS_PUBLISH_URL", "redis://redis-worker-1:6379/0"))
SYMBOLS = [s.strip().upper() for s in os.getenv(
    "DCP_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,1000PEPEUSDT"
).split(",") if s.strip()]
DC_THRESHOLD_BPS = float(os.getenv("DCP_THRESHOLD_BPS", "50"))
INTERVAL_S = float(os.getenv("DCP_INTERVAL_S", "15"))
TTL_SEC = int(os.getenv("DCP_TTL_SEC", "120"))
HASH_PREFIX = "ctx:dc:"
METRICS_PORT = int(os.getenv("METRICS_PORT", "9896"))
_15M_MS = 15 * 60 * 1000

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _dc_events_total = Counter("dcp_dc_events_total", "DC events detected", ["symbol", "direction"])
    _publishes = Counter("dcp_publishes_total", "Snapshots published")
    _last_ok = Gauge("dcp_last_ok_ms", "Last publish ts ms")
except Exception:
    _dc_events_total = _publishes = _last_ok = None  # type: ignore
    start_http_server = None  # type: ignore


def _f(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


class _DCState:
    """Per-symbol directional change detector (Glattfelder intrinsic time)."""

    def __init__(self, threshold_bps: float = 50.0) -> None:
        self._threshold = threshold_bps / 10_000.0  # convert to fraction
        self._dc_price: float | None = None       # price at last DC event
        self._extreme_price: float | None = None  # extreme in current OS run
        self._direction: int = 0                  # 1=up, -1=down (last DC direction)
        # (ts_ms, direction, overshoot_bps) — DC events
        self._events: deque[tuple[int, int, float]] = deque(maxlen=200)
        # Extended state for new features
        self._trend_start_ms: int = 0             # ts_ms of last direction flip
        self._last_confirmation_bps: float = 0.0  # overshoot of PREVIOUS DC event
        self._last_overshoot_bps: float = 0.0     # overshoot of most recent DC event
        self._event_ts_15m: deque[int] = deque()  # ts_ms of all DC events (for noise ratio)

    def on_tick(self, price: float, ts_ms: int) -> None:
        if price <= 0:
            return
        if self._dc_price is None:
            self._dc_price = price
            self._extreme_price = price
            return

        # Update extreme price in current OS direction
        if self._direction >= 0 and price > (self._extreme_price or price):
            self._extreme_price = price
        elif self._direction < 0 and price < (self._extreme_price or price):
            self._extreme_price = price

        dc_price = self._dc_price
        move = (price - dc_price) / dc_price

        if move >= self._threshold:
            # Upward DC event
            overshoot = (move - self._threshold) * 10_000.0
            self._events.append((ts_ms, 1, overshoot))
            # direction flip tracking
            if self._direction != 1:
                self._trend_start_ms = ts_ms
            self._last_confirmation_bps = self._last_overshoot_bps
            self._last_overshoot_bps = overshoot
            self._event_ts_15m.append(ts_ms)
            self._dc_price = price
            self._extreme_price = price
            self._direction = 1
            if _dc_events_total is not None:
                try:
                    _dc_events_total.labels(symbol="?", direction="up").inc()
                except Exception:
                    pass
        elif move <= -self._threshold:
            # Downward DC event
            overshoot = (-move - self._threshold) * 10_000.0
            self._events.append((ts_ms, -1, overshoot))
            # direction flip tracking
            if self._direction != -1:
                self._trend_start_ms = ts_ms
            self._last_confirmation_bps = self._last_overshoot_bps
            self._last_overshoot_bps = overshoot
            self._event_ts_15m.append(ts_ms)
            self._dc_price = price
            self._extreme_price = price
            self._direction = -1
            if _dc_events_total is not None:
                try:
                    _dc_events_total.labels(symbol="?", direction="down").inc()
                except Exception:
                    pass

    def compute(self, now_ms: int) -> dict[str, Any]:
        if not self._events:
            return {
                "dc_event_dir": 0.0,
                "dc_event_age_ms": float(now_ms),
                "dc_overshoot_bps": 0.0,
                "dc_reversal_count_15m": 0.0,
                "dc_trend_duration_ms": 0.0,
                "dc_last_confirmation_bps": 0.0,
                "dc_noise_ratio": 0.0,
            }
        last_ts, last_dir, last_over = self._events[-1]
        out: dict[str, Any] = {
            "dc_event_dir": float(last_dir),
            "dc_event_age_ms": float(now_ms - last_ts),
            "dc_overshoot_bps": last_over,
        }
        # dc_reversal_count_15m: alternating direction changes in 15-min window
        cutoff = now_ms - _15M_MS
        events_15m = [(t, d, o) for t, d, o in self._events if t >= cutoff]
        reversals = sum(
            1 for i in range(1, len(events_15m))
            if events_15m[i][1] != events_15m[i - 1][1]
        )
        out["dc_reversal_count_15m"] = float(reversals)

        # dc_trend_duration_ms: ms since last direction flip
        out["dc_trend_duration_ms"] = float(max(0, now_ms - self._trend_start_ms))

        # dc_last_confirmation_bps: overshoot at previous DC event
        out["dc_last_confirmation_bps"] = self._last_confirmation_bps

        # dc_noise_ratio: micro-reversals / total DC events in 15m window ∈ [0, 1]
        # Expire stale entries from _event_ts_15m
        while self._event_ts_15m and self._event_ts_15m[0] < cutoff:
            self._event_ts_15m.popleft()
        total_events_15m = float(len(self._event_ts_15m))
        out["dc_noise_ratio"] = min(1.0, reversals / max(1.0, total_events_15m))

        return out


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

    states: dict[str, _DCState] = {s: _DCState(DC_THRESHOLD_BPS) for s in SYMBOLS}
    last_ids: dict[str, str] = {s: "$" for s in SYMBOLS}
    last_publish = time.monotonic()
    _running = True

    def _sig(signum, _frame):
        nonlocal _running
        log.info("signal %d → exit", signum)
        _running = False

    _signal.signal(_signal.SIGTERM, _sig)
    _signal.signal(_signal.SIGINT, _sig)

    log.info(
        "directional_change_producer: symbols=%s threshold=%.0fbps interval=%ss",
        SYMBOLS, DC_THRESHOLD_BPS, INTERVAL_S,
    )

    while _running:
        try:
            streams = {f"stream:tick_{s}": last_ids[s] for s in SYMBOLS}
            try:
                resp = r_read.xread(streams, count=200, block=500)
            except Exception:
                resp = []
            for sk, entries in (resp or []):
                sym = sk.split("tick_", 1)[-1] if "tick_" in sk else None
                if not sym or sym not in states:
                    continue
                for eid, fields in entries:
                    last_ids[sym] = eid
                    try:
                        px = _f(fields.get("p") or fields.get("price"))
                        if px <= 0:
                            continue
                        try:
                            ts_ms = int(str(eid).split("-")[0])
                        except Exception:
                            ts_ms = int(time.time() * 1000)
                        states[sym].on_tick(px, ts_ms)
                    except Exception:
                        continue

            now_m = time.monotonic()
            if now_m - last_publish >= INTERVAL_S:
                now_ms = int(time.time() * 1000)
                for sym, state in states.items():
                    feats = state.compute(now_ms)
                    feats["ts_ms"] = now_ms
                    feats["quality_status"] = "OK"
                    try:
                        r_write.set(f"{HASH_PREFIX}{sym}", json.dumps(feats), ex=TTL_SEC)
                    except Exception as e:
                        log.warning("publish %s: %s", sym, e)
                if _publishes is not None:
                    _publishes.inc()
                if _last_ok is not None:
                    _last_ok.set(now_ms)
                last_publish = now_m

        except Exception as e:
            log.exception("loop: %s", e)
            time.sleep(1)

    return 0


if __name__ == "__main__":
    sys.exit(_main())
