"""orderflow_pressure_v2.py — per-symbol trade-flow features.

Subscribes to `stream:tick_{SYMBOL}` and maintains rolling window of trades.
Computes 5 v14_of features and writes `pressure_v2:{SYMBOL}` JSON snapshot:

  • trade_freq_per_hr      — count(trades) / hours-elapsed-in-window
  • trade_size_skew        — Fisher-Pearson skewness of trade-qty distribution
  • ofi                    — order-flow imbalance: sum(signed_qty)
  • ofi_stability_score    — 1 / (1 + relative_std), so high = stable, low = volatile
  • ofi_stable_secs        — consecutive seconds with ofi sign unchanged

ENV:
  REDIS_URL              read source (default redis-ticks for tick streams)
  OPV2_PUBLISH_URL       publish target (default redis-worker-1, where snapshots live)
  OPV2_SYMBOLS           comma-separated symbols
  OPV2_WINDOW_SEC        rolling window in seconds (default 600 = 10min)
  OPV2_INTERVAL_S        publish cadence (default 30)
  OPV2_HASH_PREFIX       default pressure_v2:
  OPV2_TTL_SEC           default 120
  METRICS_PORT           default 9882
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
log = logging.getLogger("orderflow_pressure_v2")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-ticks:6379/0")
PUBLISH_URL = os.getenv("OPV2_PUBLISH_URL",
                       os.getenv("REDIS_PUBLISH_URL", "redis://redis-worker-1:6379/0"))
SYMBOLS = [s.strip().upper() for s in os.getenv(
    "OPV2_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,1000PEPEUSDT"
).split(",") if s.strip()]
WINDOW_SEC = int(os.getenv("OPV2_WINDOW_SEC", "600"))
INTERVAL_S = int(os.getenv("OPV2_INTERVAL_S", "30"))
HASH_PREFIX = os.getenv("OPV2_HASH_PREFIX", "pressure_v2:")
TTL_SEC = int(os.getenv("OPV2_TTL_SEC", "120"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9882"))

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _ticks = Counter("opv2_ticks_total", "Ticks processed", ["symbol"])
    _publishes = Counter("opv2_publishes_total", "Snapshots published")
    _last_ok = Gauge("opv2_last_ok_ms", "Last publish ts ms")
except Exception:
    _ticks = _publishes = _last_ok = None  # type: ignore
    start_http_server = None  # type: ignore


def _inc(m, *labels):
    if m is None:
        return
    try:
        (m.labels(*labels) if labels else m).inc()
    except Exception:
        pass


# ── Pure stats ────────────────────────────────────────────────────────────────


def trade_freq_per_hr(n_trades: int, window_sec: float) -> float:
    if window_sec <= 0:
        return 0.0
    return n_trades * 3600.0 / window_sec


def skewness(xs: list[float]) -> float:
    """Fisher-Pearson skewness (sample). Returns 0.0 if degenerate."""
    n = len(xs)
    if n < 3:
        return 0.0
    mean = sum(xs) / n
    m2 = sum((x - mean) ** 2 for x in xs) / n
    m3 = sum((x - mean) ** 3 for x in xs) / n
    std = math.sqrt(m2) if m2 > 0 else 0.0
    if std <= 0:
        return 0.0
    return m3 / (std ** 3)


def ofi_features(signed_qtys: list[float], ts_ms_list: list[int]) -> dict[str, float]:
    """Compute ofi + stability + stable_secs from signed-qty time series."""
    if not signed_qtys:
        return {}
    n = len(signed_qtys)
    ofi_total = sum(signed_qtys)
    out: dict[str, float] = {"ofi": ofi_total}
    # Stability: 1 / (1 + relative_std). Bucket into N=10 chunks, compute std of running ofi.
    if n >= 10:
        chunk = max(1, n // 10)
        partial_ofis = []
        cum = 0.0
        for i, sv in enumerate(signed_qtys):
            cum += sv
            if (i + 1) % chunk == 0:
                partial_ofis.append(cum)
        if len(partial_ofis) >= 2:
            mu = sum(partial_ofis) / len(partial_ofis)
            var = sum((x - mu) ** 2 for x in partial_ofis) / (len(partial_ofis) - 1)
            std = math.sqrt(var)
            rel_std = std / (abs(mu) + 1e-9)
            out["ofi_stability_score"] = 1.0 / (1.0 + rel_std)
    # Stable secs — walk back from latest, count seconds while ofi sign matches latest
    if n >= 2 and ts_ms_list:
        cum_running = 0.0
        last_ts = ts_ms_list[-1]
        # Scan from end backwards, accumulating running ofi over a moving window
        for i in range(n - 1, -1, -1):
            cum_running += signed_qtys[i]
        latest_sign = 1 if cum_running > 0 else (-1 if cum_running < 0 else 0)
        if latest_sign != 0:
            # find earliest tick where running-ofi-from-here-to-end has same sign
            running_from_end = 0.0
            stable_start_ts = last_ts
            for i in range(n - 1, -1, -1):
                running_from_end += signed_qtys[i]
                cur_sign = 1 if running_from_end > 0 else (-1 if running_from_end < 0 else 0)
                if cur_sign == latest_sign:
                    stable_start_ts = ts_ms_list[i]
                else:
                    break
            out["ofi_stable_secs"] = (last_ts - stable_start_ts) / 1000.0
    return out


# ── Service ───────────────────────────────────────────────────────────────────


_running = True


def _sighandler(signum, _frame):
    global _running
    log.info("signal %d → exit", signum)
    _running = False


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

    # Per-symbol: (ts_ms, signed_qty, qty)
    trades: dict[str, deque[tuple[int, float, float]]] = {
        s: deque() for s in SYMBOLS
    }
    last_ids: dict[str, str] = {s: "$" for s in SYMBOLS}

    log.info("starting: symbols=%s window=%ds interval=%ds", SYMBOLS, WINDOW_SEC, INTERVAL_S)
    last_publish = time.monotonic()
    global _running
    _running = True

    while _running:
        try:
            streams = {f"stream:tick_{s}": last_ids[s] for s in SYMBOLS}
            try:
                resp = r_read.xread(streams, count=200, block=2000)
            except Exception as e:
                log.debug("XREAD: %s", e)
                resp = []
            now_ms = int(time.time() * 1000)
            cutoff_ms = now_ms - WINDOW_SEC * 1000
            for stream_key, entries in (resp or []):
                sym = stream_key.split("tick_", 1)[-1] if "tick_" in stream_key else None
                if sym is None or sym not in trades:
                    continue
                for entry_id, fields in entries:
                    last_ids[sym] = entry_id
                    try:
                        qty = float(fields.get("q") or fields.get("qty") or 0.0)
                        side = (fields.get("s") or fields.get("side") or "").lower()
                        sign = 1.0 if side.startswith("b") else (-1.0 if side.startswith("s") else 0.0)
                        ts_ms = int(entry_id.split("-")[0])
                        trades[sym].append((ts_ms, sign * qty, qty))
                        _inc(_ticks, sym)
                    except Exception:
                        continue
                # Trim old
                while trades[sym] and trades[sym][0][0] < cutoff_ms:
                    trades[sym].popleft()

            now = time.monotonic()
            if now - last_publish >= INTERVAL_S:
                for sym, buf in trades.items():
                    # Trim again to be safe
                    while buf and buf[0][0] < cutoff_ms:
                        buf.popleft()
                    if len(buf) < 5:
                        continue
                    n = len(buf)
                    ts_first = buf[0][0]
                    ts_last = buf[-1][0]
                    window_sec_actual = (ts_last - ts_first) / 1000.0
                    if window_sec_actual <= 0:
                        continue
                    signed_qtys = [t[1] for t in buf]
                    qtys = [t[2] for t in buf]
                    ts_list = [t[0] for t in buf]
                    feats: dict[str, float] = {
                        "trade_freq_per_hr": trade_freq_per_hr(n, window_sec_actual),
                        "trade_size_skew": skewness(qtys),
                    }
                    feats.update(ofi_features(signed_qtys, ts_list))
                    feats["_window_sec"] = window_sec_actual
                    feats["ts_ms"] = now_ms
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
