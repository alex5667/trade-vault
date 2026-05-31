"""
adaptive_ttl_publisher_v1.py — Phase 2.3 service wrapper.

Periodic timer (default hourly) that:
  1. Queries resolved signal_outcome rows over the configured window.
  2. Calls calibration.adaptive_ttl.recommend() to derive per-group barriers.
  3. Publishes snapshot to RS.ADAPTIVE_TTL_STATE for downstream readers.

SHADOW by default — only publishes the snapshot. The snapshot writer
(`signal_outcome_snapshot_writer.py`) does not read it unless
ADAPTIVE_TTL_READ_ENABLED=1 is set (separate consumer-side flag,
deferred to consumer integration phase).

ENV:
  ADAPTIVE_TTL_ENABLED           = 0    master switch
  ADAPTIVE_TTL_INTERVAL_SEC      = 3600
  ADAPTIVE_TTL_WINDOW_HOURS      = 168  rolling 7d
  ADAPTIVE_TTL_MIN_SAMPLES       = 50   per group
  ADAPTIVE_TTL_MIN_SL_R          = 0.5
  ADAPTIVE_TTL_DB_DSN            = (from TRADES_DB_DSN)
  ADAPTIVE_TTL_REDIS_URL         = (from REDIS_URL)
  ADAPTIVE_TTL_PORT              = 9915
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

log = logging.getLogger("adaptive_ttl_publisher")


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d


def _env_bool(k: str, d: bool) -> bool:
    raw = _env(k, "")
    if not raw:
        return d
    return raw.strip().lower() in ("1", "true", "yes", "on")


def fetch_resolved(conn: Any, since_ms: int, until_ms: int, limit: int = 100_000) -> list[dict]:
    sql = """
        SELECT symbol, source, regime, side, label, mfe_r, mae_r, realized_r
        FROM signal_outcome
        WHERE label IS NOT NULL
          AND decision_time_ms >= %s
          AND decision_time_ms < %s
        LIMIT %s
    """
    rows: list[dict] = []
    with conn.cursor() as cur:
        cur.execute(sql, (since_ms, until_ms, limit))
        for sym, src, regime, side, label, mfe, mae, rr in cur.fetchall():
            rows.append(
                dict(
                    symbol=sym,
                    source=src,
                    regime=regime,
                    side=side,
                    label=label,
                    mfe_r=mfe,
                    mae_r=mae,
                    realized_r=rr,
                )
            )
    return rows


def main() -> None:
    from calibration.adaptive_ttl import recommend, to_redis_payload
    from core.redis_keys import RedisKeyPrefixes as RK

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    enabled       = _env_bool("ADAPTIVE_TTL_ENABLED", False)
    interval_sec  = _env_int("ADAPTIVE_TTL_INTERVAL_SEC", 3600)
    window_hours  = _env_int("ADAPTIVE_TTL_WINDOW_HOURS", 168)
    min_samples   = _env_int("ADAPTIVE_TTL_MIN_SAMPLES", 50)
    min_sl_r      = float(_env("ADAPTIVE_TTL_MIN_SL_R", "0.5"))
    db_dsn        = _env("ADAPTIVE_TTL_DB_DSN", _env("TRADES_DB_DSN", ""))
    redis_url     = _env("ADAPTIVE_TTL_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    port          = _env_int("ADAPTIVE_TTL_PORT", 9915)

    from prometheus_client import Counter, Gauge, start_http_server
    import redis  # type: ignore

    start_http_server(port)
    g_recs   = Gauge("adaptive_ttl_recs_total", "Recommendations published")
    g_at_ms  = Gauge("adaptive_ttl_generated_at_ms", "Generation timestamp")
    c_cycle  = Counter("adaptive_ttl_cycle_total", "Cycles", ["status"])

    rc = redis.from_url(redis_url, decode_responses=True)

    log.info(
        "adaptive_ttl_publisher starting | enabled=%s port=%d interval=%ds window=%dh min_samples=%d",
        enabled, port, interval_sec, window_hours, min_samples,
    )

    while True:
        start = time.time()
        try:
            now_ms   = int(time.time() * 1000)
            since_ms = now_ms - window_hours * 3_600_000

            import psycopg2
            with psycopg2.connect(db_dsn) as conn:
                rows = fetch_resolved(conn, since_ms, now_ms)

            recs = recommend(rows, min_samples=min_samples, min_sl_r=min_sl_r)
            payload = to_redis_payload(recs, generated_at_ms=now_ms)

            # Autopilot fallback: if ENV not explicitly set, honor the
            # autopilot flag (set sticky once data threshold reached).
            effective_enabled = enabled
            if not enabled:
                try:
                    from orderflow_services.calibration_autopilot_v1 import (
                        read_autopilot_flag,
                    )
                    effective_enabled = read_autopilot_flag(rc, "adaptive_ttl_enabled")
                except Exception:
                    effective_enabled = False

            if effective_enabled and recs:
                rc.set(RK.ADAPTIVE_TTL_STATE, json.dumps(payload))
                c_cycle.labels(status="published").inc()
                log.info("adaptive_ttl: published %d recs (rows=%d)", len(recs), len(rows))
            else:
                c_cycle.labels(status="shadow").inc()
                log.info(
                    "adaptive_ttl: SHADOW (enabled=%s recs=%d rows=%d)",
                    enabled, len(recs), len(rows),
                )

            g_recs.set(len(recs))
            g_at_ms.set(now_ms)

        except Exception as e:
            c_cycle.labels(status="error").inc()
            log.warning("adaptive_ttl cycle error: %s", e)

        elapsed = time.time() - start
        sleep_for = max(1.0, interval_sec - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
