"""gated_out_outcome_persister.py — Plan 2 Gap 2 sidecar.

Drains the Redis stream stream:signals:gated_out_outcomes (written by
services.gated_out_outcome_tracker) into the Timescale hypertable
signal_gated_out_outcomes so passed/rejected/gated_out outcomes share a
durable, queryable join surface.

Design:
  * SHADOW by default (GATED_OUT_PERSISTER_ENABLED=0 → counts but no write).
  * Idempotent: INSERT … ON CONFLICT (sid, ts_ms) DO NOTHING. Safe under PEL
    replay or duplicate XADDs.
  * Consumer group: separate from the tracker so the tracker keeps its own PEL.
  * Fail-open: any single payload error is counted and skipped.
  * Batch INSERT via execute_batch for throughput.

ENV:
  GATED_OUT_PERSISTER_ENABLED   = 0          master switch (shadow when 0)
  GATED_OUT_PERSISTER_REDIS_URL = redis://redis-worker-1:6379/0
  GATED_OUT_PERSISTER_DB_DSN    = (from TRADES_DB_DSN)
  GATED_OUT_PERSISTER_PORT      = 9922
  GATED_OUT_PERSISTER_GROUP     = gated-out-persister
  GATED_OUT_PERSISTER_CONSUMER  = gated-out-persister-1
  GATED_OUT_PERSISTER_BATCH     = 200
  GATED_OUT_PERSISTER_IN_STREAM = stream:signals:gated_out_outcomes
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Any

log = logging.getLogger("gated_out_persister")


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
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return None


_INSERT_SQL = """
    INSERT INTO signal_gated_out_outcomes (
        sid, ts_ms, ts_close_ms, symbol, direction, kind, schema_version,
        entry_px, tp_bps, sl_bps, horizon_ms, confidence, min_conf,
        outcome, label, close_price, high_px, low_px, ret_bps, r_mult,
        tp_hit, sl_hit,
        y_edge_cost_aware, cost_bps, cost_fees_bps, cost_spread_bps,
        cost_slippage_bps, edge_after_cost_bps,
        sample_policy, selection_policy_version, selection_prob,
        selection_weight, virtual_min_conf, meets_virtual_threshold,
        ingest_time_ms
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s,
        %s, %s, %s, %s,
        %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s
    )
    ON CONFLICT (sid, ts_ms) DO NOTHING
"""


def parse_outcome_payload(raw: str) -> dict | None:
    """Parse the JSON blob the tracker writes to stream:signals:gated_out_outcomes.

    Returns the dict on success, None on malformed payload.
    """
    if not raw:
        return None
    try:
        d = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(d, dict):
        return None
    return d


def payload_to_row(payload: dict, *, now_ms: int) -> tuple | None:
    """Convert a tracker outcome payload to the INSERT row tuple.

    Returns None when required identity fields are missing — callers should
    increment a skip counter and ack the message to avoid stuck PEL.
    """
    sid = str(payload.get("sid") or "").strip()
    if not sid:
        return None
    ts_ms = _safe_int(payload.get("ts_ms"))
    if ts_ms is None or ts_ms <= 0:
        return None
    symbol = str(payload.get("symbol") or "").strip().upper()
    if not symbol:
        return None

    direction_raw = str(payload.get("direction") or "").upper()
    if direction_raw not in ("LONG", "SHORT"):
        return None
    direction = 1 if direction_raw == "LONG" else -1

    outcome = str(payload.get("outcome") or "").upper()
    if outcome not in ("TP_HIT", "SL_HIT", "TIMEOUT"):
        return None

    # Label: TP_HIT=+1, SL_HIT=-1, TIMEOUT=0 — matches signal_outcome.label.
    if outcome == "TP_HIT":
        label = 1
    elif outcome == "SL_HIT":
        label = -1
    else:
        label = 0

    entry_px = _safe_float(payload.get("entry"))
    if entry_px is None or entry_px <= 0:
        return None

    tp_bps = _safe_float(payload.get("tp_bps"))
    sl_bps = _safe_float(payload.get("sl_bps"))
    horizon_ms = _safe_int(payload.get("horizon_ms"))
    if tp_bps is None or sl_bps is None or horizon_ms is None:
        return None

    return (
        sid,
        ts_ms,
        _safe_int(payload.get("ts_close_ms")),
        symbol,
        direction,
        str(payload.get("kind") or "") or None,
        _safe_int(payload.get("v")) or 2,
        entry_px,
        tp_bps,
        sl_bps,
        horizon_ms,
        _safe_float(payload.get("confidence")),
        _safe_float(payload.get("min_conf")),
        outcome,
        label,
        _safe_float(payload.get("close_price")),
        _safe_float(payload.get("high")),
        _safe_float(payload.get("low")),
        _safe_float(payload.get("ret_bps")),
        _safe_float(payload.get("r_mult")),
        int(bool(payload.get("tp_hit"))),
        int(bool(payload.get("sl_hit"))),
        _safe_int(payload.get("y_edge_cost_aware")),
        _safe_float(payload.get("cost_bps")),
        _safe_float(payload.get("cost_fees_bps")),
        _safe_float(payload.get("cost_spread_bps")),
        _safe_float(payload.get("cost_slippage_bps")),
        _safe_float(payload.get("edge_after_cost_bps")),
        str(payload.get("sample_policy") or "") or None,
        str(payload.get("selection_policy_version") or "") or None,
        _safe_float(payload.get("selection_prob")),
        _safe_float(payload.get("selection_weight")),
        _safe_float(payload.get("virtual_min_conf")),
        _safe_int(payload.get("meets_virtual_threshold")),
        now_ms,
    )


def _upsert_batch(conn: Any, rows: list[tuple]) -> int:
    if not rows:
        return 0
    from psycopg2.extras import execute_batch
    with conn.cursor() as cur:
        execute_batch(cur, _INSERT_SQL, rows, page_size=200)
    conn.commit()
    return len(rows)


def main() -> None:
    import redis  # type: ignore
    from prometheus_client import Counter, Gauge, start_http_server

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    env_enabled = _env_bool("GATED_OUT_PERSISTER_ENABLED", False)
    redis_url = _env(
        "GATED_OUT_PERSISTER_REDIS_URL",
        _env("REDIS_URL", "redis://redis-worker-1:6379/0"),
    )
    in_stream = _env(
        "GATED_OUT_PERSISTER_IN_STREAM",
        "stream:signals:gated_out_outcomes",
    )
    group = _env("GATED_OUT_PERSISTER_GROUP", "gated-out-persister")
    consumer = _env("GATED_OUT_PERSISTER_CONSUMER", "gated-out-persister-1")
    batch = _env_int("GATED_OUT_PERSISTER_BATCH", 200)
    port = _env_int("GATED_OUT_PERSISTER_PORT", 9922)
    db_dsn = _env("GATED_OUT_PERSISTER_DB_DSN", _env("TRADES_DB_DSN", ""))

    log.info(
        "gated_out_persister starting | env_enabled=%s port=%d stream=%s",
        env_enabled, port, in_stream,
    )

    rc = redis.from_url(redis_url, decode_responses=True)

    # Effective enable = ENV OR Plan 2 autopilot flag. Refreshed per loop iteration
    # so the autopilot can promote this service live without restart. ENV always
    # wins (operator override); autopilot only switches OFF→ON, never reverses.
    from core.plan2_autopilot_flags import (
        FLAG_PERSISTER_ENABLED as _FLAG,
        read_plan2_flag as _read_flag,
    )

    def _effective_enabled() -> bool:
        return env_enabled or _read_flag(rc, _FLAG)
    try:
        rc.xgroup_create(in_stream, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create: %s", e)

    start_http_server(port)
    c_read = Counter("gated_out_persister_read_total", "Messages read", ["symbol"])
    c_skip = Counter("gated_out_persister_skipped_total", "Messages skipped", ["reason"])
    c_write = Counter("gated_out_persister_written_total", "Rows persisted", ["symbol"])
    c_err = Counter("gated_out_persister_error_total", "Errors", [])
    g_lag = Gauge("gated_out_persister_lag_ms", "Processing lag ms", [])

    conn = None

    def _get_conn():
        nonlocal conn
        if conn is None or conn.closed:
            import psycopg2
            conn = psycopg2.connect(db_dsn)
        return conn

    pending_rows: list[tuple] = []

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

        ack_ids = []
        now_ms = int(time.time() * 1000)
        enabled = _effective_enabled()

        if resp:
            for _stream_name, messages in resp:
                for msg_id, fields in messages:
                    try:
                        raw = fields.get("payload") or fields.get("data")
                        payload = parse_outcome_payload(raw)
                        if payload is None:
                            c_skip.labels(reason="parse_failed").inc()
                            ack_ids.append(msg_id)
                            continue

                        row = payload_to_row(payload, now_ms=now_ms)
                        if row is None:
                            c_skip.labels(reason="missing_fields").inc()
                            ack_ids.append(msg_id)
                            continue

                        symbol = str(payload.get("symbol") or "").upper()
                        c_read.labels(symbol=symbol).inc()
                        g_lag.set(max(0, now_ms - int(payload.get("ts_ms") or now_ms)))

                        if enabled:
                            pending_rows.append(row)
                            c_write.labels(symbol=symbol).inc()
                        # shadow: count read, don't write
                    except Exception as ex:
                        log.debug("payload parse error: %s", ex)
                        c_skip.labels(reason="field_error").inc()
                    ack_ids.append(msg_id)

        if pending_rows and enabled:
            try:
                n = _upsert_batch(_get_conn(), pending_rows)
                log.debug("persister upserted %d rows", n)
                pending_rows = []
            except Exception as e:
                c_err.inc()
                log.warning("DB flush error (fail-open): %s", e)
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
