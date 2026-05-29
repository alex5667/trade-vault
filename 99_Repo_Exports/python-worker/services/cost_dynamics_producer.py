"""cost_dynamics_producer.py — P1 #13 cost_widening_5s_bps.

Reads `tca:ema:{symbol}:{kind}:{session}` snapshots (from tca_priors_exporter)
and `ctx:deriv:{symbol}` for spread, then computes the 5-second change in
effective spread cost. Writes `ctx:cost_dynamics:{symbol}` every INTERVAL_S.

Features produced:
  cost_widening_5s_bps — Change in eff_spread_bps_ema over last 5 seconds.
                         Positive = widening (cost increasing), negative = tightening.

ENV:
  CDP_PUBLISH_URL   snapshot write target  (default redis-worker-1:6379/0)
  CDP_SYMBOLS       comma-separated symbols
  CDP_INTERVAL_S    publish cadence (default 30)
  CDP_TTL_SEC       snapshot TTL (default 120)
  CDP_WINDOW_S      look-back window for widening (default 10)
  METRICS_PORT      Prometheus port (default 9887)
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
log = logging.getLogger("cost_dynamics_producer")

PUBLISH_URL = os.getenv("CDP_PUBLISH_URL",
                        os.getenv("REDIS_PUBLISH_URL", "redis://redis-worker-1:6379/0"))
SYMBOLS = [s.strip().upper() for s in os.getenv(
    "CDP_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,1000PEPEUSDT"
).split(",") if s.strip()]
INTERVAL_S = float(os.getenv("CDP_INTERVAL_S", "30"))
TTL_SEC = int(os.getenv("CDP_TTL_SEC", "120"))
WINDOW_S = float(os.getenv("CDP_WINDOW_S", "10"))
HASH_PREFIX = "ctx:cost_dynamics:"
METRICS_PORT = int(os.getenv("METRICS_PORT", "9887"))

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _publishes = Counter("cdp_publishes_total", "Snapshots published")
    _last_ok = Gauge("cdp_last_ok_ms", "Last publish ts ms")
except Exception:
    _publishes = _last_ok = None  # type: ignore
    start_http_server = None  # type: ignore


def _f(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


class _CostState:
    """Rolling cost history for one symbol."""

    def __init__(self, maxlen: int = 60) -> None:
        # (wall_s, spread_bps)
        self._history: deque[tuple[float, float]] = deque(maxlen=maxlen)

    def observe(self, spread_bps: float, wall_s: float) -> None:
        if spread_bps > 0:
            self._history.append((wall_s, spread_bps))

    def compute(self, window_s: float) -> dict[str, float]:
        if len(self._history) < 2:
            return {}
        now_s = self._history[-1][0]
        cutoff = now_s - window_s
        window = [(t, s) for t, s in self._history if t >= cutoff]
        if len(window) < 2:
            return {}
        oldest_spread = window[0][1]
        newest_spread = window[-1][1]
        return {"cost_widening_5s_bps": newest_spread - oldest_spread}


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

    r = redis.from_url(PUBLISH_URL, decode_responses=True)
    states: dict[str, _CostState] = {s: _CostState() for s in SYMBOLS}
    last_publish = time.monotonic()
    _running = True

    def _sig(signum, _frame):
        nonlocal _running
        log.info("signal %d → exit", signum)
        _running = False

    _signal.signal(_signal.SIGTERM, _sig)
    _signal.signal(_signal.SIGINT, _sig)

    log.info("cost_dynamics_producer: symbols=%s window=%.0fs", SYMBOLS, WINDOW_S)

    while _running:
        try:
            # Poll spread from tca:ema or ctx:deriv snapshots
            for sym in SYMBOLS:
                spread_bps = 0.0
                # Try TCA EMA snapshot first (most accurate)
                for kind in ("default", "iceberg", "delta_spike"):
                    for session in ("all", "us", "europe", "asia"):
                        raw = r.hget(f"tca:ema:{sym}:{kind}:{session}", "eff_spread_bps_ema")
                        if raw:
                            v = _f(raw)
                            if v > 0:
                                spread_bps = v
                                break
                    if spread_bps > 0:
                        break
                # Fallback: derive from deriv ctx (spread approximated from funding/basis)
                if spread_bps <= 0:
                    raw_deriv = r.get(f"ctx:deriv:{sym}")
                    if raw_deriv:
                        try:
                            d = json.loads(raw_deriv)
                            spread_bps = _f(d.get("spread_bps") or d.get("basis_bps") or 0)
                        except Exception:
                            pass
                if spread_bps > 0:
                    states[sym].observe(spread_bps, time.time())

            now = time.monotonic()
            if now - last_publish >= INTERVAL_S:
                for sym, state in states.items():
                    feats = state.compute(WINDOW_S)
                    if not feats:
                        continue
                    feats["ts_ms"] = int(time.time() * 1000)
                    feats["quality_status"] = "OK"
                    try:
                        r.set(f"{HASH_PREFIX}{sym}", json.dumps(feats), ex=TTL_SEC)
                    except Exception as e:
                        log.warning("publish %s: %s", sym, e)
                if _publishes is not None:
                    _publishes.inc()
                if _last_ok is not None:
                    _last_ok.set(int(time.time() * 1000))
                last_publish = now

            time.sleep(5)

        except Exception as e:
            log.exception("loop: %s", e)
            time.sleep(2)

    return 0


if __name__ == "__main__":
    sys.exit(_main())
