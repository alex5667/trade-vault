"""
news_pipeline/news_agent_timer.py — P6 timer service for news agent.

Responsibilities:
  1. Prometheus metrics export (port NEWS_AGENT_TIMER_PORT, default 9840)
  2. Stream XLEN lag polling (per-stream) → news_stream_lag_ms{stream, group}
  3. Consumer group pending (per-group) → news_stream_pending_n{stream, group}
     Reads NEWS_STREAM_GROUPS env to discover which groups to monitor.
  4. Budget cleanup: SCAN + DEL old news:budget:calls:* / news:budget:usd:* keys
  5. Budget gauges refresh: news_budget_calls_used, news_budget_usd_used, limits

ENV vars consumed:
  NEWS_AGENT_TIMER_PORT      default 9840
  NEWS_AGENT_TIMER_INTERVAL  default 30  (seconds between iterations)
  NEWS_REDIS_URL             fallback to REDIS_URL
  NEWS_STREAM_RAW            stream:news_raw  (or news:raw for this project)
  NEWS_STREAM_GROUPS         "stream:news_raw=news_raw_g;stream:news_norm=news_norm_g"
  NEWS_LLM_MAX_CALLS_PER_DAY  2500
  NEWS_LLM_BUDGET_DAILY_USD   10.0
  NEWS_BUDGET_CLEANUP_DAYS    3  (delete stale budget keys older than N days)
"""
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import datetime
import logging
import os
import sys
import time
from typing import Dict, List

log = logging.getLogger("news_agent_timer")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [timer] %(message)s",
)

try:
    import aioredis  # type: ignore
except ImportError:
    # Fallback to redis.asyncio (redis-py >= 4.2)
    try:
        import redis.asyncio as aioredis  # type: ignore
    except ImportError:
        aioredis = None  # type: ignore


def _env(key: str, default: str) -> str:
    v = os.getenv(key)
    return v if v else default


# ── Configuration ──────────────────────────────────────────────────────────────

PORT = int(_env("NEWS_AGENT_TIMER_PORT", "9840"))
INTERVAL_S = int(_env("NEWS_AGENT_TIMER_INTERVAL", "30"))
REDIS_URL = _env("NEWS_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
CLEANUP_DAYS = int(_env("NEWS_BUDGET_CLEANUP_DAYS", "3"))
MAX_CALLS_PER_DAY = int(_env("NEWS_LLM_MAX_CALLS_PER_DAY", "2500"))
BUDGET_USD_LIMIT = float(_env("NEWS_LLM_BUDGET_DAILY_USD", "10.0"))

# Streams to monitor for XLEN lag (format: news:raw as used in this project)
STREAMS: List[str] = [
    _env("NEWS_STREAM_RAW",     "news:raw"),
    _env("NEWS_STREAM_ANALYSIS","news:analysis"),
    _env("NEWS_STREAM_DLQ",     "news:raw:dlq"),
]

# Parse NEWS_STREAM_GROUPS: "stream=group1,group2;stream2=group"
# Example: "news:raw=news-analyzer;news:analysis=news-feature-store"
STREAM_GROUPS_RAW = _env(
    "NEWS_STREAM_GROUPS",
    "news:raw=news-analyzer;news:analysis=news-feature-store",
)


def _parse_stream_groups(spec: str) -> Dict[str, List[str]]:
    """Parse NEWS_STREAM_GROUPS env into {stream: [group1, group2, ...]}.

    Format: "<stream>=<group1>,<group2>;<stream2>=<group>"
    """
    out: Dict[str, List[str]] = {}
    for part in (spec or "").split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        stream, groups_raw = part.split("=", 1)
        stream = stream.strip()
        gl = [g.strip() for g in groups_raw.split(",") if g.strip()]
        if stream and gl:
            out[stream] = gl
    return out


STREAM_GROUPS: Dict[str, List[str]] = _parse_stream_groups(STREAM_GROUPS_RAW)

# ── Prometheus metrics (local, timer-specific) ─────────────────────────────────
try:
    from prometheus_client import Counter, Gauge, start_http_server  # type: ignore

    stream_lag_ms = Gauge(
        "news_stream_lag_ms",
        "Approximate stream lag (ms): last-entry-id minus now_ms",
        ["stream", "group"],
    )
    stream_pending_n = Gauge(
        "news_stream_pending_n",
        "Pending entries per consumer group (XINFO GROUPS pending)",
        ["stream", "group"],
    )
    budget_calls_used = Gauge("news_budget_calls_used", "LLM calls used today", [])
    budget_calls_limit = Gauge("news_budget_calls_limit", "LLM calls/day limit", [])
    budget_usd_used = Gauge("news_budget_usd_used", "LLM USD used today", [])
    budget_usd_limit = Gauge("news_budget_usd_limit", "LLM USD/day limit", [])
    budget_keys_deleted = Counter(
        "news_timer_budget_keys_deleted_total",
        "Stale budget keys deleted by cleanup loop",
    )
    stream_lag_errors = Counter(
        "news_timer_stream_lag_errors_total",
        "Errors reading stream lag",
        ["stream"],
    )
    _PROM_OK = True
except ImportError:
    log.warning("prometheus_client not installed; metrics disabled")
    _PROM_OK = False


# ── Core async loops ──────────────────────────────────────────────────────────

async def _stream_lag_loop(r: "aioredis.Redis") -> None:  # type: ignore
    """Poll XINFO STREAM + XINFO GROUPS for lag/pending metrics."""
    while True:
        now_ms = get_ny_time_millis()
        for stream in STREAMS:
            try:
                info = await r.xinfo_stream(stream)
                # last-entry id: "1234567890-0" or similar
                last_entry = info.get("last-generated-id") or info.get("last-entry")
                if last_entry is None:
                    # try reading last-entry as list of (id, fields)
                    last_entry = info.get("last-entry", None)
                if isinstance(last_entry, (list, tuple)) and len(last_entry) >= 1:
                    last_entry = last_entry[0]
                if last_entry:
                    id_str = last_entry if isinstance(last_entry, str) else last_entry.decode("utf-8", errors="ignore")
                    ms_part = int(str(id_str).split("-")[0])
                    lag = max(0.0, float(now_ms - ms_part))
                    if _PROM_OK:
                        stream_lag_ms.labels(stream=stream, group="").set(lag)

                # Per-consumer-group lag + pending (for backlog detection)
                groups = STREAM_GROUPS.get(stream) or []
                if groups:
                    ginfo_list = await r.xinfo_groups(stream)
                    for gi in ginfo_list:
                        name = gi.get("name") or gi.get(b"name")
                        if isinstance(name, bytes):
                            name = name.decode()
                        if name not in groups:
                            continue
                        last_delivered = (
                            gi.get("last-delivered-id")
                            or gi.get(b"last-delivered-id")
                            or "0-0"
                        )
                        if isinstance(last_delivered, bytes):
                            last_delivered = last_delivered.decode()
                        ms2 = int(str(last_delivered).split("-")[0])
                        lag2 = max(0.0, float(now_ms - ms2))
                        pend = gi.get("pending") or gi.get(b"pending") or 0
                        if _PROM_OK:
                            stream_lag_ms.labels(stream=stream, group=str(name)).set(lag2)
                            stream_pending_n.labels(stream=stream, group=str(name)).set(float(pend))
            except Exception as exc:
                if _PROM_OK:
                    stream_lag_errors.labels(stream=stream).inc()
                log.debug("stream lag error %s: %s", stream, exc)

        await asyncio.sleep(INTERVAL_S)


async def _budget_gauge_loop(r: "aioredis.Redis") -> None:  # type: ignore
    """Read today's call/USD budget usage from Redis and export as gauges."""
    while True:
        today = time.strftime("%Y%m%d", time.gmtime())
        try:
            calls_key = f"news:budget:calls:{today}"
            usd_key = f"news:budget:usd:{today}"

            calls_raw = await r.get(calls_key)
            usd_raw = await r.get(usd_key)

            calls_used = float(calls_raw) if calls_raw else 0.0
            usd_used = float(usd_raw) if usd_raw else 0.0

            if _PROM_OK:
                budget_calls_used.set(calls_used)
                budget_calls_limit.set(float(MAX_CALLS_PER_DAY))
                budget_usd_used.set(usd_used)
                budget_usd_limit.set(BUDGET_USD_LIMIT)
        except Exception as exc:
            log.debug("budget gauge error: %s", exc)

        await asyncio.sleep(INTERVAL_S)


async def _budget_cleanup_loop(r: "aioredis.Redis") -> None:  # type: ignore
    """Delete stale budget keys (older than CLEANUP_DAYS) to avoid Redis bloat."""
    while True:
        await asyncio.sleep(3600)  # run hourly
        try:
            today_epoch = time.time()
            patterns = ["news:budget:calls:*", "news:budget:usd:*"]
            deleted = 0
            for pat in patterns:
                cursor = 0
                while True:
                    cursor, keys = await r.scan(
                        cursor=cursor,
                        match=pat,
                        count=10000,
                    )
                    for key in keys:
                        if isinstance(key, bytes):
                            key = key.decode()
                        # Extract date part: news:budget:*:YYYYMMDD
                        parts = key.split(":")
                        if len(parts) < 4:
                            continue
                        date_str = parts[-1]
                        if len(date_str) != 8 or not date_str.isdigit():
                            continue
                        try:
                            key_date = datetime.datetime.strptime(date_str, "%Y%m%d")
                            key_epoch = key_date.timestamp()
                            if today_epoch - key_epoch > CLEANUP_DAYS * 86400:
                                await r.delete(key)
                                deleted += 1
                                if _PROM_OK:
                                    budget_keys_deleted.inc()
                        except Exception:
                            pass
                    if cursor == 0:
                        break
            if deleted:
                log.info("budget cleanup: deleted %d stale keys", deleted)
        except Exception as exc:
            log.warning("budget cleanup error: %s", exc)


async def main() -> None:
    if aioredis is None:
        log.error("redis.asyncio / aioredis not installed; cannot start timer")
        sys.exit(1)

    if _PROM_OK:
        start_http_server(PORT)
        log.info("Prometheus metrics server started on port %d", PORT)

    log.info(
        "news_agent_timer started: interval=%ds, streams=%s, groups=%s",
        INTERVAL_S, STREAMS, STREAM_GROUPS,
    )

    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        await asyncio.gather(
            _stream_lag_loop(r),
            _budget_gauge_loop(r),
            _budget_cleanup_loop(r),
        )
    finally:
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
