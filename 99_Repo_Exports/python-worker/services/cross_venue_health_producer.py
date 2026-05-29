"""cross_venue_health_producer.py — P1 #19-20 cross-venue quality features.

Reads `runtime:bybit:{symbol}` HASH (Go bybit_features_collector, ~15s cadence)
and `stream:tick_{symbol}` (Binance trades) to compute cross-venue lead-lag and
price consensus persistence. Writes `ctx:cross_venue:{symbol}` every INTERVAL_S.

Features produced:
  cross_venue_lead_lag_ms       — Signed time delta (ms): Binance last_trade_ts
                                  minus Bybit last_price_ts. Positive = Binance leads.
  venue_consensus_persistence_3s — Count of consecutive same-direction price-diff
                                   sign over last 3 seconds. 0 = divergent.

ENV:
  CVP_READ_URL      tick read source (default redis-ticks:6379/0)
  CVP_BYBIT_URL     bybit context read (default redis:6379/0, main redis)
  CVP_PUBLISH_URL   snapshot write target (default redis-worker-1:6379/0)
  CVP_SYMBOLS       comma-separated symbols
  CVP_INTERVAL_S    publish cadence (default 30)
  CVP_TTL_SEC       snapshot TTL (default 120)
  METRICS_PORT      Prometheus port (default 9889)
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
log = logging.getLogger("cross_venue_health_producer")

CVP_READ_URL = os.getenv("CVP_READ_URL", "redis://redis-ticks:6379/0")
CVP_BYBIT_URL = os.getenv("CVP_BYBIT_URL", "redis://redis:6379/0")
PUBLISH_URL = os.getenv("CVP_PUBLISH_URL",
                        os.getenv("REDIS_PUBLISH_URL", "redis://redis-worker-1:6379/0"))
SYMBOLS = [s.strip().upper() for s in os.getenv(
    "CVP_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,1000PEPEUSDT"
).split(",") if s.strip()]
INTERVAL_S = float(os.getenv("CVP_INTERVAL_S", "30"))
TTL_SEC = int(os.getenv("CVP_TTL_SEC", "120"))
HASH_PREFIX = "ctx:cross_venue:"
METRICS_PORT = int(os.getenv("METRICS_PORT", "9889"))

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _publishes = Counter("cvp_publishes_total", "Snapshots published")
    _last_ok = Gauge("cvp_last_ok_ms", "Last publish ts ms")
except Exception:
    _publishes = _last_ok = None  # type: ignore
    start_http_server = None  # type: ignore


def _f(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


class _CrossVenueState:
    """Per-symbol cross-venue price tracking."""

    def __init__(self) -> None:
        # (ts_ms, price) from Binance ticks (newest last)
        self._binance: deque[tuple[int, float]] = deque(maxlen=50)
        # (ts_ms, price_diff_bps_sign) from bybit comparisons (newest last)
        self._diff_signs: deque[tuple[float, int]] = deque(maxlen=60)
        # lead-lag observations: 1=binance leads, -1=bybit leads, 0=tied
        self._lead_obs: deque[int] = deque(maxlen=60)

    def on_binance_tick(self, price: float, ts_ms: int) -> None:
        if price > 0:
            self._binance.append((ts_ms, price))

    def compute(
        self,
        bybit_price: float,
        bybit_ts_ms: int,
        bybit_spread_bps: float = 0.0,
        bybit_trade_count: int = 0,
        bybit_window_ms: int = 30_000,
        bybit_book_age_ms: float = 0.0,
    ) -> dict[str, float]:
        out: dict[str, float] = {}
        now_s = time.time()
        now_ms = int(now_s * 1000)

        if self._binance:
            bin_ts = self._binance[-1][0]
            bin_px = self._binance[-1][1]

            if bybit_ts_ms > 0:
                lag = float(bin_ts - bybit_ts_ms)
                out["cross_venue_lead_lag_ms"] = lag
                # cross_venue_latency_diff_ms: absolute latency gap
                out["cross_venue_latency_diff_ms"] = abs(lag)
                lead = 1 if lag > 10 else (-1 if lag < -10 else 0)
                self._lead_obs.append(lead)

            if bybit_price > 0 and bin_px > 0:
                diff_bps = (bin_px - bybit_price) / bybit_price * 10_000.0
                sign = 1 if diff_bps > 0 else (-1 if diff_bps < 0 else 0)
                self._diff_signs.append((now_s, sign))

                # cross_venue_spread_diff_bps: Binance spread minus Bybit spread
                # Binance spread proxy: recent bid-ask from book stream
                if bybit_spread_bps > 0:
                    out["cross_venue_spread_diff_bps"] = abs(diff_bps) - bybit_spread_bps

        # venue_consensus_persistence_3s: consecutive same-sign diff in last 3s
        t_cut_3s = now_s - 3.0
        recent_3s = [(t, s) for t, s in self._diff_signs if t >= t_cut_3s]
        if not recent_3s:
            out["venue_consensus_persistence_3s"] = 0.0
        else:
            streak = 1
            last_s = recent_3s[-1][1]
            for _, s in reversed(recent_3s[:-1]):
                if s == last_s:
                    streak += 1
                else:
                    break
            out["venue_consensus_persistence_3s"] = float(streak) if last_s != 0 else 0.0

        # venue_consensus_flip_count_10s: sign changes in last 10s
        t_cut_10s = now_s - 10.0
        recent_10s = [s for t, s in self._diff_signs if t >= t_cut_10s]
        flips_10s = sum(
            1 for i in range(1, len(recent_10s))
            if recent_10s[i] != recent_10s[i - 1] and recent_10s[i] != 0
        )
        out["venue_consensus_flip_count_10s"] = float(flips_10s)

        # binance_leads_bybit_score / bybit_leads_binance_score (rolling fractions)
        if self._lead_obs:
            n_obs = len(self._lead_obs)
            bin_leads = sum(1 for x in self._lead_obs if x == 1)
            byb_leads = sum(1 for x in self._lead_obs if x == -1)
            out["binance_leads_bybit_score"] = bin_leads / n_obs
            out["bybit_leads_binance_score"] = byb_leads / n_obs

        # bybit_book_age_ms: age of the bybit book snapshot
        if bybit_book_age_ms > 0:
            out["bybit_book_age_ms"] = bybit_book_age_ms
        elif bybit_ts_ms > 0:
            out["bybit_book_age_ms"] = float(now_ms - bybit_ts_ms)

        # bybit_trade_rate_hz: trades per second in the observation window
        if bybit_trade_count > 0 and bybit_window_ms > 0:
            out["bybit_trade_rate_hz"] = bybit_trade_count / (bybit_window_ms / 1000.0)

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

    r_ticks = redis.from_url(CVP_READ_URL, decode_responses=True)
    r_bybit = redis.from_url(CVP_BYBIT_URL, decode_responses=True)
    r_write = redis.from_url(PUBLISH_URL, decode_responses=True)

    states: dict[str, _CrossVenueState] = {s: _CrossVenueState() for s in SYMBOLS}
    last_ids: dict[str, str] = {s: "$" for s in SYMBOLS}
    last_publish = time.monotonic()
    _running = True

    def _sig(signum, _frame):
        nonlocal _running
        log.info("signal %d → exit", signum)
        _running = False

    _signal.signal(_signal.SIGTERM, _sig)
    _signal.signal(_signal.SIGINT, _sig)

    log.info("cross_venue_health_producer: symbols=%s", SYMBOLS)

    while _running:
        try:
            # Read Binance tick stream
            streams = {f"stream:tick_{s}": last_ids[s] for s in SYMBOLS}
            try:
                resp = r_ticks.xread(streams, count=100, block=500)
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
                        states[sym].on_binance_tick(px, ts_ms)
                    except Exception:
                        continue

            now_m = time.monotonic()
            if now_m - last_publish >= INTERVAL_S:
                for sym, state in states.items():
                    # Read bybit context from main redis HASH
                    bybit_price = 0.0
                    bybit_ts_ms = 0
                    bybit_spread_bps = 0.0
                    bybit_trade_count = 0
                    bybit_book_age_ms = 0.0
                    try:
                        raw = r_bybit.hgetall(f"runtime:bybit:{sym}")
                        if raw:
                            bybit_price = _f(raw.get("last_price") or raw.get("price"))
                            bybit_ts_ms = int(_f(raw.get("ts_ms") or raw.get("last_update_ms")))
                            bybit_spread_bps = _f(raw.get("spread_bps") or raw.get("spread"))
                            bybit_trade_count = int(_f(raw.get("trade_count") or raw.get("trades_1m"), 0))
                            # book_age_ms: time since last bybit book update
                            last_book_ms = _f(raw.get("last_book_ms") or raw.get("book_ts_ms"), 0)
                            if last_book_ms > 0:
                                bybit_book_age_ms = max(0.0, time.time() * 1000 - last_book_ms)
                    except Exception:
                        pass

                    feats = state.compute(
                        bybit_price, bybit_ts_ms,
                        bybit_spread_bps=bybit_spread_bps,
                        bybit_trade_count=bybit_trade_count,
                        bybit_window_ms=int(INTERVAL_S * 1000),
                        bybit_book_age_ms=bybit_book_age_ms,
                    )
                    if not feats:
                        continue
                    feats["ts_ms"] = int(time.time() * 1000)
                    feats["quality_status"] = "OK" if bybit_ts_ms > 0 else "absent"
                    try:
                        r_write.set(f"{HASH_PREFIX}{sym}", json.dumps(feats), ex=TTL_SEC)
                    except Exception as e:
                        log.warning("publish %s: %s", sym, e)

                if _publishes is not None:
                    _publishes.inc()
                if _last_ok is not None:
                    _last_ok.set(int(time.time() * 1000))
                last_publish = now_m

        except Exception as e:
            log.exception("loop: %s", e)
            time.sleep(1)

    return 0


if __name__ == "__main__":
    sys.exit(_main())
