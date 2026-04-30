from __future__ import annotations

import os
import time
from typing import List

import redis
from prometheus_client import start_http_server

from core.streams import list_microbar_symbols, microbar_legacy_stream, microbar_stream_for_symbol, microbar_symbols_set
from services.observability.metrics_registry import (
    atr_bad_active
    cvd_quarantine_active
    delta_fallback_mode
    microbar_stream_xlen
    microbar_symbols_active
    redis_used_memory_mb
)


def _b2s(x) -> str:
    return x.decode("utf-8") if isinstance(x, (bytes, bytearray)) else str(x)


def _read_set(r: redis.Redis, key: str, max_n: int) -> List[str]:
    xs = list(r.smembers(key) or [])
    out: List[str] = []
    for x in xs[:max_n]:
        out.append(_b2s(x))
    return out


def main() -> None:
    if os.getenv("METRICS_ENABLE", "0") != "1":
        return

    redis_url = os.getenv("REPORTS_REDIS_URL") or os.getenv("REDIS_URL") or "redis://localhost:6379/0"
    max_connections = int(os.getenv("METRICS_REDIS_MAX_CONNECTIONS", "10"))
    r = redis.Redis.from_url(
        redis_url
        decode_responses=False
        max_connections=max_connections
        socket_connect_timeout=5
        socket_timeout=15
        socket_keepalive=True
        health_check_interval=30
    )

    bind = os.getenv("METRICS_BIND", "0.0.0.0")
    port = int(os.getenv("METRICS_PORT", "9109"))
    update_sec = int(os.getenv("METRICS_UPDATE_SEC", "10"))
    max_syms = int(os.getenv("METRICS_SYMBOLS_MAX", "200"))

    start_http_server(port, addr=bind)

    legacy = microbar_legacy_stream()
    symbols_set = microbar_symbols_set()

    while True:
        # Redis memory
        try:
            info = r.info()
            used_mb = float(info.get("used_memory", 0)) / (1024.0 * 1024.0)
            redis_used_memory_mb.set(float(used_mb))
        except Exception:
            pass

        # microbar streams
        try:
            microbar_symbols_active.set(float(r.scard(symbols_set) or 0))
            microbar_stream_xlen.labels(stream=legacy).set(float(r.xlen(legacy) or 0))
        except Exception:
            pass

        # ATR bad / CVD quarantine sets (best-effort)
        try:
            bad_syms = _read_set(r, "cfg:atr_bad:symbols", max_syms)
            for sym in bad_syms:
                atr_bad_active.labels(symbol=sym).set(1.0)
        except Exception:
            pass

        try:
            q_syms = _read_set(r, "cfg:cvd_quarantine:symbols", max_syms)
            for sym in q_syms:
                cvd_quarantine_active.labels(symbol=sym).set(1.0)
                delta_fallback_mode.labels(symbol=sym).set(2.0)
        except Exception:
            pass

        # per-symbol stream lengths (sample)
        try:
            syms = list_microbar_symbols(r, max_n=max_syms)
            for sym in syms:
                k = microbar_stream_for_symbol(sym)
                microbar_stream_xlen.labels(stream=k).set(float(r.xlen(k) or 0))
        except Exception:
            pass

        time.sleep(float(update_sec))


if __name__ == "__main__":
    main()

