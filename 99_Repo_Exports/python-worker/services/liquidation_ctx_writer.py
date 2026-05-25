"""liquidation_ctx_writer.py — per-symbol liquidation context aggregator.

Subscribes to `stream:liq_evt` (liquidation event stream from go-worker) and
maintains per-symbol rolling stats. Writes `ctx:liq:{SYMBOL}` JSON:

  • liquidation_usd_1m   — sum(liquidated_usd) over last 60 seconds
  • liqmap_1h_age_ms     — age of last liqmap snapshot for the symbol

ENV:
  REDIS_URL                  default redis-worker-1 (where liq_evt is published)
  LCW_PUBLISH_URL            snapshot target (default REDIS_URL)
  LCW_SYMBOLS                comma-separated
  LCW_LIQ_STREAM             default stream:liq_evt
  LCW_LIQMAP_KEY_PREFIX      default liqmap:snapshot:
  LCW_WINDOW_SEC             default 60
  LCW_INTERVAL_S             default 10
  LCW_TTL_SEC                default 90
  METRICS_PORT               default 9885
"""
from __future__ import annotations

import json
import logging
import math
import os
import signal as _signal
import sys
import time
from collections import defaultdict, deque
from typing import Any

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("liquidation_ctx_writer")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
PUBLISH_URL = os.getenv("LCW_PUBLISH_URL",
                       os.getenv("REDIS_PUBLISH_URL", REDIS_URL))
SYMBOLS = [s.strip().upper() for s in os.getenv(
    "LCW_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,1000PEPEUSDT"
).split(",") if s.strip()]
LIQ_STREAM = os.getenv("LCW_LIQ_STREAM", "stream:liq_evt")
LIQMAP_KEY_PREFIX = os.getenv("LCW_LIQMAP_KEY_PREFIX", "liqmap:snapshot:")
WINDOW_SEC = int(os.getenv("LCW_WINDOW_SEC", "60"))
INTERVAL_S = int(os.getenv("LCW_INTERVAL_S", "10"))
HASH_PREFIX = os.getenv("LCW_HASH_PREFIX", "ctx:liq:")
TTL_SEC = int(os.getenv("LCW_TTL_SEC", "90"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9885"))

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _liq_events = Counter("lcw_liq_events_total", "Liquidation events", ["symbol"])
    _publishes = Counter("lcw_publishes_total", "Snapshots published")
    _last_ok = Gauge("lcw_last_ok_ms", "Last publish ts ms")
except Exception:
    _liq_events = _publishes = _last_ok = None  # type: ignore
    start_http_server = None  # type: ignore


def _inc(m, *labels):
    if m is None:
        return
    try:
        (m.labels(*labels) if labels else m).inc()
    except Exception:
        pass


def _safe_float(v: Any, d: float = 0.0) -> float:
    if v is None:
        return d
    try:
        f = float(v)
        if math.isfinite(f):
            return f
    except (TypeError, ValueError):
        pass
    return d


def parse_liq_event(fields: dict[str, Any]) -> tuple[str, float, int] | None:
    """Extract (symbol, usd_value, ts_ms) from a stream:liq_evt entry.

    Returns None when the entry is unparseable.
    """
    try:
        symbol = (fields.get("symbol") or fields.get("s") or "").upper()
        if not symbol:
            return None
        # USD value can come as `usd`, `value_usd`, or qty × price
        usd = fields.get("usd") or fields.get("value_usd")
        if usd is not None:
            usd_val = _safe_float(usd)
        else:
            qty = _safe_float(fields.get("q") or fields.get("qty") or 0)
            px = _safe_float(fields.get("p") or fields.get("price") or 0)
            usd_val = qty * px
        if usd_val <= 0:
            return None
        ts_ms = int(_safe_float(fields.get("ts_ms") or fields.get("ts") or 0)) or int(time.time() * 1000)
        return symbol, usd_val, ts_ms
    except Exception:
        return None


_running = True


def _sighandler(signum, _frame):
    global _running
    log.info("signal %d → exit", signum)
    _running = False


def get_liqmap_age_ms(r, symbol: str) -> int | None:
    """Best-effort: probe several possible liqmap snapshot keys and report age."""
    candidates = [
        f"{LIQMAP_KEY_PREFIX}{symbol}",
        f"liqmap:{symbol}:1h",
        f"liqmap:1h:{symbol}",
    ]
    for key in candidates:
        try:
            raw = r.get(key)
            if not raw:
                continue
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", "ignore")
            data = json.loads(raw)
            ts = int(_safe_float(data.get("ts_ms") or data.get("updated_at_ms") or 0))
            if ts > 0:
                age = int(time.time() * 1000) - ts
                if age >= 0:
                    return age
        except Exception:
            continue
    return None


def run() -> int:
    if start_http_server is not None:
        try:
            start_http_server(METRICS_PORT)
        except Exception:
            pass

    _signal.signal(_signal.SIGTERM, _sighandler)
    _signal.signal(_signal.SIGINT, _sighandler)

    try:
        import redis
    except ImportError:
        log.error("redis-py not installed")
        return 2

    r_read = redis.from_url(REDIS_URL, decode_responses=True)
    r_write = redis.from_url(PUBLISH_URL, decode_responses=True)

    # Per-symbol: deque of (ts_ms, usd_val)
    liq_window: dict[str, deque[tuple[int, float]]] = defaultdict(deque)
    last_id = "$"

    log.info("starting: stream=%s symbols=%s window=%ds interval=%ds",
             LIQ_STREAM, SYMBOLS, WINDOW_SEC, INTERVAL_S)
    last_publish = time.monotonic()
    global _running
    _running = True

    while _running:
        try:
            try:
                resp = r_read.xread({LIQ_STREAM: last_id}, count=200, block=2000)
            except Exception as e:
                log.debug("XREAD liq_evt: %s", e)
                resp = []
            now_ms = int(time.time() * 1000)
            cutoff_ms = now_ms - WINDOW_SEC * 1000
            for _stream, entries in (resp or []):
                for entry_id, fields in entries:
                    last_id = entry_id
                    parsed = parse_liq_event(fields if isinstance(fields, dict) else {})
                    if parsed is None:
                        continue
                    sym, usd, ts_ms = parsed
                    if sym not in SYMBOLS:
                        # Track all symbols (auto-discover) but cap memory
                        if len(liq_window) > 200:
                            continue
                    liq_window[sym].append((ts_ms, usd))
                    _inc(_liq_events, sym)
            # Trim windows
            for sym, buf in liq_window.items():
                while buf and buf[0][0] < cutoff_ms:
                    buf.popleft()

            now = time.monotonic()
            if now - last_publish >= INTERVAL_S:
                # Publish snapshots for configured symbols
                for sym in SYMBOLS:
                    buf = liq_window.get(sym) or deque()
                    while buf and buf[0][0] < cutoff_ms:
                        buf.popleft()
                    liq_usd_1m = sum(v for _, v in buf)
                    liqmap_age = get_liqmap_age_ms(r_write, sym)
                    feats: dict[str, float] = {
                        "liquidation_usd_1m": liq_usd_1m,
                        "_n_liq_events_1m": float(len(buf)),
                        "ts_ms": now_ms,
                    }
                    if liqmap_age is not None:
                        feats["liqmap_1h_age_ms"] = float(liqmap_age)
                    try:
                        r_write.set(f"{HASH_PREFIX}{sym}", json.dumps(feats), ex=TTL_SEC)
                        _inc(_publishes)
                    except Exception as e:
                        log.warning("publish %s failed: %s", sym, e)
                if _last_ok is not None:
                    try:
                        _last_ok.set(now_ms)
                    except Exception:
                        pass
                last_publish = now

        except Exception as e:
            log.exception("loop error: %s", e)
            time.sleep(1)

    log.info("stopped")
    return 0


if __name__ == "__main__":
    sys.exit(run())
