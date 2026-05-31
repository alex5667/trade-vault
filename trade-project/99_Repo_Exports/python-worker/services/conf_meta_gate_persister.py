"""conf_meta_gate_persister.py — Plan 1 Phase 5.

Drains `stream:decisions:conf_meta_gate` (written by the meta-gate emit
helper) into the Timescale hypertable `confidence_meta_gate_decisions`.
Mirrors the design of `services.gated_out_outcome_persister`:

  * SHADOW by default (`CONF_META_GATE_PERSISTER_ENABLED=0` → counts reads,
    no INSERT). Flip the flag once we want durable audit.
  * Idempotent INSERT ON CONFLICT (ts, sid) DO NOTHING — safe under PEL
    replay or duplicate XADDs.
  * Fail-open: any per-message error increments a skip counter and acks
    the message; the loop never gets stuck.
  * Batch INSERT via execute_batch for throughput.

ENV:
  CONF_META_GATE_PERSISTER_ENABLED   = 0
  CONF_META_GATE_PERSISTER_REDIS_URL = redis://redis-worker-1:6379/0
  CONF_META_GATE_PERSISTER_DB_DSN    = (from TRADES_DB_DSN)
  CONF_META_GATE_PERSISTER_IN_STREAM = stream:decisions:conf_meta_gate
  CONF_META_GATE_PERSISTER_GROUP     = conf-meta-gate-persister
  CONF_META_GATE_PERSISTER_CONSUMER  = conf-meta-gate-persister-1
  CONF_META_GATE_PERSISTER_BATCH     = 200
  CONF_META_GATE_PERSISTER_PORT      = 9927
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Any

log = logging.getLogger("conf_meta_gate_persister")


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except (TypeError, ValueError):
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
    INSERT INTO confidence_meta_gate_decisions (
        ts, sid, symbol, kind, side,
        mode, active,
        legacy_confidence, legacy_min_confidence, legacy_decision,
        meta_decision, active_decision,
        p_win_raw, p_win_calibrated, p_win_floor,
        expected_r, expected_edge_bps, risk_multiplier,
        spread_bps, expected_slippage_bps, fee_bps, dq_score,
        regime, session,
        model_ver, schema_hash, feature_cols_hash,
        canary_bucket, canary_selected,
        reason_codes, features_small,
        latency_ms
    ) VALUES (
        to_timestamp(%s / 1000.0), %s, %s, %s, %s,
        %s, %s,
        %s, %s, %s,
        %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s,
        %s, %s, %s,
        %s, %s,
        %s::jsonb, %s::jsonb,
        %s
    )
    ON CONFLICT (ts, sid) DO NOTHING
"""


def parse_decision_payload(raw: str) -> dict | None:
    """Parse the JSON payload the meta-gate emit helper writes.

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


def payload_to_row(payload: dict) -> tuple | None:
    """Convert a decision payload to the INSERT row tuple.

    Returns None when required identity fields are missing.
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
    mode = str(payload.get("mode") or "").strip()
    if not mode:
        return None
    meta_decision = str(payload.get("meta_decision") or "").strip()
    if not meta_decision:
        return None
    model_ver = str(payload.get("model_ver") or "").strip()
    # model_ver can be empty when the gate fell back; persist anyway.

    reason_codes_json = json.dumps(payload.get("reason_codes") or [])
    features_json = json.dumps(payload.get("features") or {})

    canary_selected = payload.get("canary_selected")
    if canary_selected is None:
        canary_selected_val: bool | None = None
    else:
        canary_selected_val = bool(canary_selected)

    # `active` is derived from active_decision vs legacy_decision: when the
    # caller forwarded `active=True` we keep it; otherwise infer.
    active_flag = payload.get("active")
    if active_flag is None:
        # Infer: active when active_decision was driven by meta-gate.
        active_flag = (
            str(payload.get("active_decision") or "").upper()
            != str(payload.get("legacy_decision") or "").upper()
        )
    active_flag = bool(active_flag)

    return (
        ts_ms,
        sid,
        symbol,
        str(payload.get("kind") or ""),
        str(payload.get("side") or ""),
        mode,
        active_flag,
        _safe_float(payload.get("legacy_confidence")) or 0.0,
        _safe_float(payload.get("legacy_min_confidence")) or 0.0,
        str(payload.get("legacy_decision") or ""),
        meta_decision,
        str(payload.get("active_decision") or ""),
        _safe_float(payload.get("p_win_raw")),
        _safe_float(payload.get("p_win_calibrated")),
        _safe_float(payload.get("p_win_floor")),
        _safe_float(payload.get("expected_r")),
        _safe_float(payload.get("expected_edge_bps")),
        _safe_float(payload.get("risk_multiplier")),
        _safe_float(payload.get("spread_bps")),
        _safe_float(payload.get("expected_slippage_bps")),
        _safe_float(payload.get("fee_bps")),
        _safe_float(payload.get("dq_score")),
        str(payload.get("regime") or ""),
        str(payload.get("session") or ""),
        model_ver,
        str(payload.get("schema_hash") or ""),
        str(payload.get("feature_cols_hash") or ""),
        _safe_int(payload.get("canary_bucket")),
        canary_selected_val,
        reason_codes_json,
        features_json,
        _safe_float(payload.get("latency_ms")),
    )


def upsert_batch(conn: Any, rows: list[tuple]) -> int:
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

    env_enabled = _env_bool("CONF_META_GATE_PERSISTER_ENABLED", False)
    redis_url = _env(
        "CONF_META_GATE_PERSISTER_REDIS_URL",
        _env("REDIS_URL", "redis://redis-worker-1:6379/0"),
    )
    in_stream = _env(
        "CONF_META_GATE_PERSISTER_IN_STREAM",
        "stream:decisions:conf_meta_gate",
    )
    group = _env("CONF_META_GATE_PERSISTER_GROUP", "conf-meta-gate-persister")
    consumer = _env(
        "CONF_META_GATE_PERSISTER_CONSUMER", "conf-meta-gate-persister-1",
    )
    batch = _env_int("CONF_META_GATE_PERSISTER_BATCH", 200)
    port = _env_int("CONF_META_GATE_PERSISTER_PORT", 9927)
    db_dsn = _env(
        "CONF_META_GATE_PERSISTER_DB_DSN", _env("TRADES_DB_DSN", ""),
    )

    log.info(
        "conf_meta_gate_persister starting | enabled=%s port=%d stream=%s",
        env_enabled, port, in_stream,
    )

    rc = redis.from_url(redis_url, decode_responses=True)

    try:
        rc.xgroup_create(in_stream, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create: %s", e)

    start_http_server(port)
    c_read = Counter(
        "conf_meta_gate_persister_read_total", "Messages read", ["mode"],
    )
    c_skip = Counter(
        "conf_meta_gate_persister_skipped_total", "Messages skipped",
        ["reason"],
    )
    c_write = Counter(
        "conf_meta_gate_persister_written_total", "Rows persisted",
        ["mode"],
    )
    c_err = Counter(
        "conf_meta_gate_persister_error_total", "Errors", [],
    )
    g_lag = Gauge(
        "conf_meta_gate_persister_lag_ms", "Processing lag ms", [],
    )

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

        ack_ids: list[str] = []
        now_ms = int(time.time() * 1000)

        if resp:
            for _stream_name, messages in resp:
                for msg_id, fields in messages:
                    try:
                        raw = fields.get("payload") or fields.get("data")
                        payload = parse_decision_payload(raw)
                        if payload is None:
                            c_skip.labels(reason="parse_failed").inc()
                            ack_ids.append(msg_id)
                            continue

                        row = payload_to_row(payload)
                        if row is None:
                            c_skip.labels(reason="missing_fields").inc()
                            ack_ids.append(msg_id)
                            continue

                        mode_label = str(payload.get("mode") or "UNKNOWN")
                        c_read.labels(mode=mode_label).inc()
                        g_lag.set(
                            max(0, now_ms - int(payload.get("ts_ms") or now_ms)),
                        )

                        if env_enabled:
                            pending_rows.append(row)
                            c_write.labels(mode=mode_label).inc()
                        # SHADOW: counted but not persisted.
                    except Exception as ex:
                        log.debug("decision parse error: %s", ex)
                        c_skip.labels(reason="field_error").inc()
                    ack_ids.append(msg_id)

        if pending_rows and env_enabled:
            try:
                n = upsert_batch(_get_conn(), pending_rows)
                log.debug("conf_meta_gate_persister upserted %d rows", n)
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
