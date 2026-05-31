"""fear_greed_exporter.py — daily fetch of Crypto Fear&Greed Index.

Unlocks v14_of features:
  • crypto_fear_greed     — normalised to [0, 1]
  • market_breadth_score  — derived from FNG classification ("Fear", "Greed", etc.)

Source: https://api.alternative.me/fng/?limit=1 (free, no auth, 1 req/hour).
Writes: `cache:fear_greed` JSON with `{"value": <0-100>, "classification": ..., "ts_ms": ...}`.

Update cadence: hourly (FNG updates daily but cheap to poll more often).

ENV:
  REDIS_URL          default redis://redis-worker-1:6379/0
  FGE_API_URL        default https://api.alternative.me/fng/?limit=1
  FGE_REDIS_KEY      default cache:fear_greed
  FGE_TTL_SEC        default 7200 (2h — safety against API outage)
  FGE_INTERVAL_S     default 3600 (1h between fetches)
  FGE_HTTP_TIMEOUT_S default 10
  METRICS_PORT       default 9879
"""
from __future__ import annotations

import json
import logging
import os
import signal as _signal
import sys
import time

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("fear_greed_exporter")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
API_URL = os.getenv("FGE_API_URL", "https://api.alternative.me/fng/?limit=1")
REDIS_KEY = os.getenv("FGE_REDIS_KEY", "cache:fear_greed")
TTL_SEC = int(os.getenv("FGE_TTL_SEC", "7200"))
INTERVAL_S = int(os.getenv("FGE_INTERVAL_S", "3600"))
HTTP_TIMEOUT_S = float(os.getenv("FGE_HTTP_TIMEOUT_S", "10"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9879"))

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _fetches_total = Counter("fge_fetches_total", "API fetches", ["result"])
    _current_value = Gauge("fge_value", "Latest Fear&Greed value (0-100)")
    _last_fetch_ts = Gauge("fge_last_fetch_ts_ms", "Last successful fetch ts ms")
    _METRICS_OK = True
except Exception:
    _fetches_total = _current_value = _last_fetch_ts = None  # type: ignore
    start_http_server = None  # type: ignore
    _METRICS_OK = False


# Breadth mapping — FNG classifications to a continuous score in [0, 1]
# Source: alternative.me API constants.
_CLASSIFICATION_BREADTH: dict[str, float] = {
    "Extreme Fear": 0.10,
    "Fear":         0.30,
    "Neutral":      0.50,
    "Greed":        0.70,
    "Extreme Greed": 0.90,
}


def fetch_fng() -> dict | None:
    """Single API call. Returns parsed dict or None on failure."""
    try:
        import urllib.request
        req = urllib.request.Request(API_URL, headers={"User-Agent": "scanner-fge/1.0"})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
            body = resp.read().decode("utf-8", "ignore")
        data = json.loads(body)
        if not isinstance(data, dict):
            return None
        entries = data.get("data") or []
        if not entries or not isinstance(entries[0], dict):
            return None
        e = entries[0]
        value_raw = e.get("value")
        if value_raw is None:
            return None
        try:
            value = int(value_raw)
        except (TypeError, ValueError):
            return None
        return {
            "value": value,                                # 0-100 integer
            "classification": str(e.get("value_classification") or ""),
            "timestamp": str(e.get("timestamp") or ""),
        }
    except Exception as e:
        log.warning("fetch_fng failed: %s", e)
        return None


def build_snapshot(fng: dict) -> dict:
    classification = fng.get("classification", "")
    breadth = _CLASSIFICATION_BREADTH.get(classification, 0.5)
    return {
        "value": int(fng.get("value") or 50),
        "classification": classification,
        "market_breadth_score": breadth,
        "source_timestamp": fng.get("timestamp", ""),
        "ts_ms": int(time.time() * 1000),
    }


def publish(r, snapshot: dict) -> bool:
    try:
        r.set(REDIS_KEY, json.dumps(snapshot), ex=TTL_SEC)
        return True
    except Exception as e:
        log.error("redis SET failed: %s", e)
        return False


_running = True


def _sighandler(signum, _frame):
    global _running
    log.info("signal %d → exit", signum)
    _running = False


def run() -> int:
    if _METRICS_OK and start_http_server is not None:
        try:
            start_http_server(METRICS_PORT)
            log.info("prometheus on :%d", METRICS_PORT)
        except Exception as e:
            log.warning("metrics server failed: %s", e)

    _signal.signal(_signal.SIGTERM, _sighandler)
    _signal.signal(_signal.SIGINT, _sighandler)

    try:
        import redis
    except ImportError:
        log.error("redis-py not installed")
        return 2

    r = redis.from_url(REDIS_URL, decode_responses=True)
    log.info("starting: api=%s key=%s interval=%ds", API_URL, REDIS_KEY, INTERVAL_S)

    # First fetch on startup
    while _running:
        fng = fetch_fng()
        if fng is None:
            if _fetches_total is not None:
                _fetches_total.labels("error").inc()
        else:
            snapshot = build_snapshot(fng)
            if publish(r, snapshot):
                log.info("published: value=%d class=%s breadth=%.2f",
                         snapshot["value"], snapshot["classification"], snapshot["market_breadth_score"])
                if _current_value is not None:
                    _current_value.set(snapshot["value"])
                if _last_fetch_ts is not None:
                    _last_fetch_ts.set(snapshot["ts_ms"])
                if _fetches_total is not None:
                    _fetches_total.labels("ok").inc()
            else:
                if _fetches_total is not None:
                    _fetches_total.labels("publish_error").inc()

        # Interruptible sleep
        slept = 0
        while _running and slept < INTERVAL_S:
            time.sleep(min(5, INTERVAL_S - slept))
            slept += 5

    log.info("stopped")
    return 0


if __name__ == "__main__":
    sys.exit(run())
