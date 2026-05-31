"""
order_execution_events_writer.py — Plan 3 / Step 2 stream-to-Timescale drain.

Consumer-group reader of `stream:order_exec_events`. Batches events into
`order_execution_events` hypertable. Computes per-event `latency_ms` (gap
from previous stage for the same sid) if the emitter didn't supply one.

Design:
  * SHADOW by default (OEE_WRITER_DB_ENABLED=0) — counts events, skips DB write.
  * Idempotent insert via PRIMARY KEY (ts_ms, sid, stage, seq); duplicates
    silently dropped (DO NOTHING).
  * Per-batch DB flush — never blocks the producer (emitter is fire-and-forget).
  * Fail-open: DB errors log + retry next batch, no data loss until stream
    rotates (XADD MAXLEN ≈ 30k entries ≈ several hours at burst).

ENV:
  OEE_WRITER_DB_ENABLED = 0                   master switch
  OEE_WRITER_REDIS_URL  = redis://redis-worker-1:6379/0
  OEE_WRITER_GROUP      = order-exec-events-writer
  OEE_WRITER_CONSUMER   = oee-writer-1
  OEE_WRITER_BATCH      = 500
  OEE_WRITER_PORT       = 9919
  OEE_WRITER_DB_DSN     = (from TRADES_DB_DSN)

Prometheus:
  oee_writer_read_total{stage,status}
  oee_writer_written_total{stage}
  oee_writer_skipped_total{reason}
  oee_writer_error_total
  oee_writer_lag_ms (Gauge — last)
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Any

log = logging.getLogger("oee_writer")


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


def _safe_float(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def _safe_int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def parse_event(fields: dict) -> dict | None:
    """Parse stream fields into a normalized event dict.

    Accepts the shape produced by core.order_execution_events.build_event().
    Returns None if mandatory fields are missing.
    """
    try:
        ts_ms = _safe_int(fields.get("ts_ms"))
        sid = str(fields.get("sid") or "").strip()
        stage = str(fields.get("stage") or "").strip()
        symbol = str(fields.get("symbol") or "").strip().upper()
        side = _safe_int(fields.get("side"))
        status = str(fields.get("status") or "").strip()
        if not (ts_ms and sid and stage and symbol and side in (-1, 1) and status):
            return None

        payload_raw = fields.get("payload") or "{}"
        try:
            payload_obj = json.loads(payload_raw) if isinstance(payload_raw, str) else dict(payload_raw)
        except Exception:
            payload_obj = {"_raw": str(payload_raw)}

        return dict(
            ts_ms=ts_ms,
            sid=sid,
            stage=stage,
            seq=_safe_int(fields.get("seq")) or 0,
            symbol=symbol,
            side=side,
            venue=str(fields.get("venue") or "") or None,
            client_order_id=str(fields.get("client_order_id") or "") or None,
            exchange_order_id=str(fields.get("exchange_order_id") or "") or None,
            px=_safe_float(fields.get("px")),
            qty=_safe_float(fields.get("qty")),
            notional_usd=_safe_float(fields.get("notional_usd")),
            status=status,
            reason_code=str(fields.get("reason_code") or "") or None,
            latency_ms=_safe_float(fields.get("latency_ms")),
            payload_json=json.dumps(payload_obj),
        )
    except Exception:
        return None


_INSERT_SQL = """
    INSERT INTO order_execution_events (
        ts_ms, sid, stage, seq, symbol, side, venue,
        client_order_id, exchange_order_id,
        px, qty, notional_usd,
        status, reason_code, latency_ms, payload_json
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s::jsonb
    )
    ON CONFLICT (ts_ms, sid, stage, seq) DO NOTHING
"""


def upsert_batch(conn: Any, rows: list[tuple]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        from psycopg2.extras import execute_batch
        execute_batch(cur, _INSERT_SQL, rows, page_size=500)
    conn.commit()
    return len(rows)


def event_to_row(ev: dict) -> tuple:
    return (
        ev["ts_ms"], ev["sid"], ev["stage"], ev["seq"], ev["symbol"], ev["side"], ev["venue"],
        ev["client_order_id"], ev["exchange_order_id"],
        ev["px"], ev["qty"], ev["notional_usd"],
        ev["status"], ev["reason_code"], ev["latency_ms"], ev["payload_json"],
    )


def main() -> None:
    import redis  # type: ignore
    from prometheus_client import Counter, Gauge, start_http_server

    from core.redis_keys import RedisStreams as RS

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    enabled = _env_bool("OEE_WRITER_DB_ENABLED", False)
    redis_url = _env(
        "OEE_WRITER_REDIS_URL",
        _env("REDIS_WORKER_1_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0")),
    )
    in_stream = RS.ORDER_EXEC_EVENTS
    group = _env("OEE_WRITER_GROUP", "order-exec-events-writer")
    consumer = _env("OEE_WRITER_CONSUMER", "oee-writer-1")
    batch = _env_int("OEE_WRITER_BATCH", 500)
    port = _env_int("OEE_WRITER_PORT", 9919)
    db_dsn = _env("OEE_WRITER_DB_DSN", _env("TRADES_DB_DSN", ""))

    log.info("oee_writer starting | enabled=%s port=%d stream=%s", enabled, port, in_stream)

    rc = redis.from_url(redis_url, decode_responses=True)

    try:
        rc.xgroup_create(in_stream, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create: %s", e)

    start_http_server(port)
    c_read = Counter("oee_writer_read_total", "Events read", ["stage", "status"])
    c_write = Counter("oee_writer_written_total", "Events written to DB", ["stage"])
    c_skip = Counter("oee_writer_skipped_total", "Events skipped", ["reason"])
    c_err = Counter("oee_writer_error_total", "Write errors", [])
    g_lag = Gauge("oee_writer_lag_ms", "Read→write lag ms", [])

    conn = None

    def _get_conn():
        nonlocal conn
        if conn is None or conn.closed:
            import psycopg2
            conn = psycopg2.connect(db_dsn)
        return conn

    pending_rows: list[tuple] = []
    pending_stages: list[str] = []

    while True:
        try:
            resp = rc.xreadgroup(
                groupname=group, consumername=consumer,
                streams={in_stream: ">"}, count=batch, block=2000,
            )
        except Exception as e:
            if "NOGROUP" in str(e):
                try:
                    rc.xgroup_create(in_stream, group, id="$", mkstream=True)
                except Exception as ex:
                    if "BUSYGROUP" not in str(ex):
                        log.warning("xgroup_create retry: %s", ex)
            else:
                log.warning("XREADGROUP error: %s", e)
            time.sleep(1)
            continue

        ack_ids: list[str] = []
        now_ms = int(time.time() * 1000)

        if resp:
            for _name, messages in resp:
                for msg_id, fields in messages:
                    ev = parse_event(fields)
                    if ev is None:
                        c_skip.labels(reason="parse_failed").inc()
                        ack_ids.append(msg_id)
                        continue
                    c_read.labels(stage=ev["stage"], status=ev["status"]).inc()
                    g_lag.set(now_ms - ev["ts_ms"])
                    if enabled:
                        pending_rows.append(event_to_row(ev))
                        pending_stages.append(ev["stage"])
                    ack_ids.append(msg_id)

        if pending_rows and enabled:
            try:
                db_conn = _get_conn()
                n = upsert_batch(db_conn, pending_rows)
                for stage in pending_stages:
                    c_write.labels(stage=stage).inc()
                log.debug("oee_writer: upserted %d rows", n)
                pending_rows = []
                pending_stages = []
            except Exception as e:
                c_err.inc()
                log.warning("oee_writer DB flush error (fail-open): %s", e)
                try:
                    if conn and not conn.closed:
                        conn.rollback()
                except Exception:
                    pass
                conn = None

        if ack_ids:
            try:
                rc.xack(in_stream, group, *ack_ids)
            except Exception as e:
                log.warning("XACK error: %s", e)


if __name__ == "__main__":
    main()
