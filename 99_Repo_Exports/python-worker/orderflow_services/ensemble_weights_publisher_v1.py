"""
ensemble_weights_publisher_v1.py — Phase 3.1 service wrapper.

Nightly timer (default 6h) that:
  1. Queries resolved signal_outcome rows for the trailing window.
  2. Calls calibration.ensemble_weights.compute_weights().
  3. HSET RS.ENSEMBLE_WEIGHTS_TPL.format(symbol=...) per symbol.

SHADOW: ENSEMBLE_WEIGHTS_ENABLED=0 → compute but don't HSET.
Consumer-side guard: ENSEMBLE_WEIGHTS_READ_ENABLED=0 (separate flag).

ENV:
  ENSEMBLE_WEIGHTS_ENABLED        = 0
  ENSEMBLE_WEIGHTS_INTERVAL_SEC   = 21600  (6h)
  ENSEMBLE_WEIGHTS_WINDOW_DAYS    = 30
  ENSEMBLE_WEIGHTS_MIN_SAMPLES    = 100
  ENSEMBLE_WEIGHTS_METRIC         = neg_log_loss | sharpe
  ENSEMBLE_WEIGHTS_TEMPERATURE    = 1.0
  ENSEMBLE_WEIGHTS_HALFLIFE_DAYS  = 10
  ENSEMBLE_WEIGHTS_DB_DSN         = (from TRADES_DB_DSN)
  ENSEMBLE_WEIGHTS_REDIS_URL      = (from REDIS_URL)
  ENSEMBLE_WEIGHTS_PORT           = 9916
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

log = logging.getLogger("ensemble_weights_publisher")


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d


def _env_float(k: str, d: float) -> float:
    try:
        return float(_env(k, str(d)))
    except Exception:
        return d


def _env_bool(k: str, d: bool) -> bool:
    raw = _env(k, "")
    if not raw:
        return d
    return raw.strip().lower() in ("1", "true", "yes", "on")


def fetch_window(conn: Any, since_ms: int, until_ms: int, limit: int = 200_000) -> list[dict]:
    sql = """
        SELECT symbol, source, decision_time_ms, calib_prob, realized_r, label
        FROM signal_outcome
        WHERE label IS NOT NULL
          AND decision_time_ms >= %s
          AND decision_time_ms < %s
        LIMIT %s
    """
    rows: list[dict] = []
    with conn.cursor() as cur:
        cur.execute(sql, (since_ms, until_ms, limit))
        for sym, src, dt, cp, rr, lb in cur.fetchall():
            rows.append(
                dict(
                    symbol=sym,
                    source=src,
                    decision_time_ms=int(dt),
                    calib_prob=cp,
                    realized_r=rr,
                    label=lb,
                )
            )
    return rows


def main() -> None:
    from calibration.ensemble_weights import compute_weights, to_redis_payload
    from core.redis_keys import RedisKeyPrefixes as RK

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    enabled       = _env_bool("ENSEMBLE_WEIGHTS_ENABLED", False)
    interval_sec  = _env_int("ENSEMBLE_WEIGHTS_INTERVAL_SEC", 21_600)
    window_days   = _env_int("ENSEMBLE_WEIGHTS_WINDOW_DAYS", 30)
    min_samples   = _env_int("ENSEMBLE_WEIGHTS_MIN_SAMPLES", 100)
    metric        = _env("ENSEMBLE_WEIGHTS_METRIC", "neg_log_loss")
    temperature   = _env_float("ENSEMBLE_WEIGHTS_TEMPERATURE", 1.0)
    halflife      = _env_float("ENSEMBLE_WEIGHTS_HALFLIFE_DAYS", 10.0)
    db_dsn        = _env("ENSEMBLE_WEIGHTS_DB_DSN", _env("TRADES_DB_DSN", ""))
    redis_url     = _env("ENSEMBLE_WEIGHTS_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    port          = _env_int("ENSEMBLE_WEIGHTS_PORT", 9916)

    from prometheus_client import Counter, Gauge, start_http_server
    import redis  # type: ignore

    start_http_server(port)
    g_syms   = Gauge("ensemble_weights_symbols", "Symbols with active weights")
    g_srcs   = Gauge("ensemble_weights_sources", "Sources per symbol", ["symbol"])
    c_cycle  = Counter("ensemble_weights_cycle_total", "Cycles", ["status"])

    rc = redis.from_url(redis_url, decode_responses=True)

    log.info(
        "ensemble_weights_publisher starting | enabled=%s port=%d interval=%ds window=%dd metric=%s",
        enabled, port, interval_sec, window_days, metric,
    )

    while True:
        start = time.time()
        try:
            now_ms   = int(time.time() * 1000)
            since_ms = now_ms - window_days * 86_400_000

            import psycopg2
            with psycopg2.connect(db_dsn) as conn:
                rows = fetch_window(conn, since_ms, now_ms)

            weights = compute_weights(
                rows,
                metric=metric,
                min_samples=min_samples,
                temperature=temperature,
                halflife_days=halflife,
                now_ms=now_ms,
            )
            payload = to_redis_payload(weights)

            # Autopilot fallback: if ENV not set, honor the sticky flag.
            effective_enabled = enabled
            if not enabled:
                try:
                    from orderflow_services.calibration_autopilot_v1 import (
                        read_autopilot_flag,
                    )
                    effective_enabled = read_autopilot_flag(
                        rc, "ensemble_weights_enabled"
                    )
                except Exception:
                    effective_enabled = False

            if effective_enabled and payload:
                pipe = rc.pipeline(transaction=False)
                for sym, mapping in payload.items():
                    key = RK.ENSEMBLE_WEIGHTS_TPL.format(symbol=sym)
                    pipe.delete(key)
                    if mapping:
                        pipe.hset(key, mapping=mapping)
                    pipe.expire(key, 7 * 86400)
                pipe.execute()
                c_cycle.labels(status="published").inc()
                log.info(
                    "ensemble_weights: published %d symbol weights (rows=%d)",
                    len(payload), len(rows),
                )
            else:
                c_cycle.labels(status="shadow").inc()
                log.info(
                    "ensemble_weights: SHADOW (enabled=%s symbols=%d rows=%d)",
                    enabled, len(payload), len(rows),
                )

            g_syms.set(len(payload))
            for sym, mapping in payload.items():
                g_srcs.labels(symbol=sym).set(len(mapping))

        except Exception as e:
            c_cycle.labels(status="error").inc()
            log.warning("ensemble_weights cycle error: %s", e)

        elapsed = time.time() - start
        sleep_for = max(1.0, interval_sec - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
