#!/usr/bin/env python3
"""pit_priors_exporter_v1.py — ADR-0007 skeleton.

Periodic exporter that publishes health metrics for PIT priors materialized
by tools/build_pit_priors_v1.py.

Emits per-{symbol, kind, session}:
  pit_priors_age_ms        — time since last as-of date
  pit_priors_sample_count  — samples in latest bucket
  pit_priors_winrate       — for Grafana visibility
  pit_priors_stale         — 1 if age > PIT_PRIOR_STALE_MS

STATUS: SKELETON. Wire as cronjob/long-running service.

ENV
  PIT_PRIORS_EXPORTER_PORT      (default 9146)
  PIT_PRIORS_EXPORTER_INTERVAL  (default 60s)
  PIT_PRIOR_STALE_MS            (default 86400000 = 24h)
  PIT_PRIORS_SYMBOLS            (default "BTCUSDT,ETHUSDT" — comma-separated)
  PIT_PRIORS_KINDS              (default "default,reclaim,sweep,absorption")
"""
from __future__ import annotations

import logging
import os
import signal
import time
from typing import Any

from prometheus_client import REGISTRY, Gauge, start_http_server  # type: ignore

from core.redis_client import get_redis
from utils.time_utils import get_ny_time_millis

logger = logging.getLogger("pit_priors_exporter")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name) or default)
    except Exception:
        return default


def _get_or_create_gauge(name: str, doc: str, labels: list[str]) -> Gauge:
    try:
        return Gauge(name, doc, labels)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore
        raise


SESSIONS = ("asia", "europe", "us")


def _date_from_str(s: str) -> int:
    import datetime as _dt
    try:
        dt = _dt.datetime.strptime(s, "%Y%m%d").replace(tzinfo=_dt.timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


def _decode(v: Any) -> str:
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "ignore")
    return str(v) if v is not None else ""


def _safe_float(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "ignore")
        return float(v)
    except Exception:
        return 0.0


def scrape_once(
    redis_client: Any,
    symbols: list[str],
    kinds: list[str],
    *,
    g_age: Gauge,
    g_samples: Gauge,
    g_winrate: Gauge,
    g_stale: Gauge,
    stale_ms: int,
) -> int:
    """Read latest PIT prior for each (symbol, kind, session) and publish gauges.

    Returns the number of buckets with valid data.
    """
    now_ms = get_ny_time_millis()
    valid = 0
    for symbol in symbols:
        for kind in kinds:
            for session in SESSIONS:
                latest_key = f"pit_priors:latest:{symbol}:{kind}:{session}"
                try:
                    as_of_date = _decode(redis_client.get(latest_key))
                except Exception:
                    as_of_date = ""
                labels = {"symbol": symbol, "kind": kind, "session": session}
                if not as_of_date:
                    g_age.labels(**labels).set(stale_ms * 2)  # treat missing as stale
                    g_samples.labels(**labels).set(0)
                    g_stale.labels(**labels).set(1)
                    continue
                as_of_ts = _date_from_str(as_of_date)
                age = max(0, now_ms - as_of_ts)
                hash_key = f"pit_priors:{symbol}:{kind}:{session}:{as_of_date}"
                try:
                    fields = redis_client.hgetall(hash_key) or {}
                except Exception:
                    fields = {}
                fields = {_decode(k): _decode(v) for k, v in fields.items()}
                samples = _safe_float(fields.get("sample_count"))
                winrate = _safe_float(fields.get("winrate"))
                g_age.labels(**labels).set(age)
                g_samples.labels(**labels).set(samples)
                g_winrate.labels(**labels).set(winrate)
                g_stale.labels(**labels).set(1 if age > stale_ms else 0)
                if samples > 0:
                    valid += 1
    return valid


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    port = _env_int("PIT_PRIORS_EXPORTER_PORT", 9146)
    interval = _env_int("PIT_PRIORS_EXPORTER_INTERVAL", 60)
    stale_ms = _env_int("PIT_PRIOR_STALE_MS", 86_400_000)
    symbols = [s.strip().upper() for s in os.getenv("PIT_PRIORS_SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]
    kinds = [k.strip() for k in os.getenv("PIT_PRIORS_KINDS", "default,reclaim,sweep,absorption").split(",") if k.strip()]

    logger.info(
        "Starting PIT priors exporter: port=%d interval=%ds symbols=%s kinds=%s (SKELETON)",
        port, interval, symbols, kinds,
    )

    g_age = _get_or_create_gauge("pit_priors_age_ms", "Age of latest PIT prior (ms)", ["symbol", "kind", "session"])
    g_samples = _get_or_create_gauge("pit_priors_sample_count", "Sample count in latest PIT prior", ["symbol", "kind", "session"])
    g_winrate = _get_or_create_gauge("pit_priors_winrate", "Win rate in latest PIT prior", ["symbol", "kind", "session"])
    g_stale = _get_or_create_gauge("pit_priors_stale", "1 if PIT prior age exceeds threshold else 0", ["symbol", "kind", "session"])

    redis_client = get_redis()
    start_http_server(port)
    logger.info("HTTP /metrics on :%d", port)

    stop = {"flag": False}

    def _sig(_a: int, _b: Any) -> None:
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    while not stop["flag"]:
        try:
            valid = scrape_once(
                redis_client, symbols, kinds,
                g_age=g_age, g_samples=g_samples, g_winrate=g_winrate, g_stale=g_stale,
                stale_ms=stale_ms,
            )
            logger.debug("Published gauges for %d buckets with data", valid)
        except Exception as e:
            logger.error("scrape_once failed: %s", e)
        for _ in range(interval):
            if stop["flag"]:
                break
            time.sleep(1)

    logger.info("PIT priors exporter stopped")


if __name__ == "__main__":
    main()
