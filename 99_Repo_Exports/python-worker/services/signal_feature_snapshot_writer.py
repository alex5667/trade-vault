"""
signal_feature_snapshot_writer.py — Plan 3 / Step 1 canonical immutable FeatureSnapshot writer.

DISTINCT from signal_outcome_snapshot_writer:
  * signal_outcome holds barrier config + label (mutable; resolver UPDATEs).
  * signal_feature_snapshots is APPEND-ONLY immutable point-in-time freeze
    of the full feature dict, schema_hash, and submit-side execution context.
    Trainers / Optuna / drift monitors read this table.

Why a second consumer instead of dual-writing from the outcome writer:
  * Two responsibilities → two failure domains. If the feature-snapshot DB
    has a slow chunk, it must not back-pressure the outcome writer (which
    feeds the resolver / labeling).
  * Independent consumer group lets ops drain / replay one without the other.
  * Schema-hash logic and dq_flags belong to feature governance, not outcome
    resolution; keeping them isolated keeps the outcome writer minimal.

ENV (all optional with sensible defaults):
  FEATURE_SNAPSHOT_DB_WRITE_ENABLED = 0      master switch (0 = shadow/no-write)
  SFS_REDIS_URL                     = redis://redis-worker-1:6379/0
  SFS_IN_STREAM                     = signals:of:inputs
  SFS_GROUP                         = signal-feature-snapshot-writer
  SFS_CONSUMER                      = sfs-writer-1
  SFS_BATCH                         = 200
  SFS_PORT                          = 9918
  SFS_DB_DSN                        = (from TRADES_DB_DSN)
  SFS_SCHEMA_NAME                   = of_v1
  SFS_SCHEMA_VERSION                = v1

Prometheus (port SFS_PORT):
  signal_feature_snapshot_read_total{symbol}
  signal_feature_snapshot_write_total{symbol,status}
  signal_feature_snapshot_skipped_total{reason}
  signal_feature_snapshot_lag_ms (Gauge — last)
  signal_feature_snapshot_schema_hash_seen_total{schema_hash}
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Any

log = logging.getLogger("sfs_writer")


# ─── ENV helpers ─────────────────────────────────────────────────────────────


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


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


# ─── Signal parsing (shared shape with so_snapshot_writer) ───────────────────


def parse_signal(fields: dict[str, str]) -> dict | None:
    raw = fields.get("payload") or fields.get("data") or fields.get("signal")
    if raw:
        try:
            sig = json.loads(raw)
            if isinstance(sig, dict):
                return sig
        except Exception:
            pass
    if fields.get("symbol") or fields.get("signal_id"):
        return dict(fields)
    return None


def extract_indicators(signal: dict) -> dict:
    inds = signal.get("indicators") or {}
    if isinstance(inds, str):
        try:
            inds = json.loads(inds)
        except Exception:
            inds = {}
    return inds if isinstance(inds, dict) else {}


# ─── Snapshot row builder ────────────────────────────────────────────────────


def build_snapshot_row(
    sig: dict,
    *,
    now_ms: int,
    schema_name: str,
    schema_version: str,
    slip_prior_bps: float = 1.5,
) -> tuple | None:
    """Return DB row tuple or None on bad-input skip.

    All math is pure (no Redis / DB / clock side-effects) — easy to unit-test.
    """
    from core.feature_snapshot_hash import (
        compute_feature_cols_hash,
        compute_schema_hash,
        extract_feature_cols,
    )

    sid = str(sig.get("signal_id") or sig.get("sid") or "").strip()
    if not sid:
        return None

    symbol = str(sig.get("symbol") or "").strip().upper()
    if not symbol:
        return None

    direction = str(sig.get("direction") or sig.get("side") or "LONG").upper()
    side_int = 1 if direction != "SHORT" else -1

    inds = extract_indicators(sig)

    process_ms = _safe_int(sig.get("ts_ms") or sig.get("timestamp_ms") or now_ms, now_ms)
    event_ms = _safe_int(sig.get("event_time_ms") or inds.get("event_time_ms"), 0) or None
    ingest_ms = _safe_int(sig.get("ingest_time_ms") or inds.get("ingest_time_ms"), 0) or None

    mid_px = _safe_float(
        sig.get("price") or sig.get("entry") or sig.get("entry_price")
        or inds.get("entry_price") or inds.get("price"),
        0.0,
    )
    spread_bps = _safe_float(
        inds.get("spread_bps") or inds.get("bid_ask_spread_bps"),
        0.0,
    )

    # Entry expected = mid ± half-spread ± slip prior (consistent with so_snapshot_writer)
    if mid_px > 0:
        side_sign = 1.0 if side_int > 0 else -1.0
        offset_bps = (spread_bps / 2.0) + slip_prior_bps
        entry_px_expected = mid_px * (1.0 + side_sign * offset_bps / 10_000.0)
    else:
        entry_px_expected = 0.0

    source = str(sig.get("source") or sig.get("strategy") or "crypto-of")
    trace_id = str(sig.get("trace_id") or sid)
    kind = str(sig.get("kind") or sig.get("signal_kind") or "") or None

    schema_hash = compute_schema_hash(inds)
    feature_cols = extract_feature_cols(inds)
    feature_cols_hash = compute_feature_cols_hash(feature_cols)

    # DQ flags: list of strings ("no_price", "no_spread", etc.) — useful for ad-hoc audits.
    dq_flags: list[str] = []
    if mid_px <= 0:
        dq_flags.append("no_mid_px")
    if spread_bps == 0.0:
        dq_flags.append("no_spread")
    if not inds:
        dq_flags.append("empty_indicators")

    meta = {
        "feature_cols_n": len(feature_cols),
        "raw_score": _safe_float(sig.get("raw_score") or sig.get("score") or inds.get("of_score"), 0.0),
        "calib_prob": _safe_float(sig.get("calib_prob") or sig.get("ml_p_edge") or inds.get("p_edge"), 0.0),
    }

    features_json = json.dumps(inds, default=str)
    dq_flags_json = json.dumps(dq_flags)
    meta_json = json.dumps(meta, default=str)

    return (
        process_ms,
        sid,
        symbol,
        kind,
        side_int,
        source,
        trace_id,
        schema_name,
        schema_version,
        schema_hash,
        feature_cols_hash,
        event_ms,
        ingest_ms,
        process_ms,
        entry_px_expected if entry_px_expected > 0 else None,
        mid_px if mid_px > 0 else None,
        spread_bps if spread_bps > 0 else None,
        slip_prior_bps,
        features_json,
        dq_flags_json,
        meta_json,
    )


# ─── DB ───────────────────────────────────────────────────────────────────────

_INSERT_SQL = """
    INSERT INTO signal_feature_snapshots (
        decision_time_ms, sid, symbol, kind, side, source, trace_id,
        schema_name, schema_version, schema_hash, feature_cols_hash,
        event_time_ms, ingest_time_ms, process_time_ms,
        entry_px_expected, mid_px_submit, spread_bps_submit, expected_slippage_bps,
        features, dq_flags, meta
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s,
        %s::jsonb, %s::jsonb, %s::jsonb
    )
    ON CONFLICT (decision_time_ms, sid) DO NOTHING
"""


def upsert_batch(conn: Any, rows: list[tuple]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        from psycopg2.extras import execute_batch
        execute_batch(cur, _INSERT_SQL, rows, page_size=200)
    conn.commit()
    return len(rows)


# ─── Main service ────────────────────────────────────────────────────────────


def main() -> None:
    import redis  # type: ignore
    from prometheus_client import Counter, Gauge, start_http_server

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    enabled = _env_bool("FEATURE_SNAPSHOT_DB_WRITE_ENABLED", False)
    redis_url = _env(
        "SFS_REDIS_URL",
        _env("REDIS_WORKER_1_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0")),
    )
    in_stream = _env("SFS_IN_STREAM", "signals:of:inputs")
    group = _env("SFS_GROUP", "signal-feature-snapshot-writer")
    consumer = _env("SFS_CONSUMER", "sfs-writer-1")
    batch = _env_int("SFS_BATCH", 200)
    port = _env_int("SFS_PORT", 9918)
    db_dsn = _env("SFS_DB_DSN", _env("TRADES_DB_DSN", ""))
    schema_name = _env("SFS_SCHEMA_NAME", "of_v1")
    schema_version = _env("SFS_SCHEMA_VERSION", "v1")

    log.info(
        "sfs_writer starting | enabled=%s port=%d stream=%s schema=%s/%s",
        enabled, port, in_stream, schema_name, schema_version,
    )

    rc = redis.from_url(redis_url, decode_responses=True)

    try:
        rc.xgroup_create(in_stream, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create: %s", e)

    start_http_server(port)
    c_read = Counter("signal_feature_snapshot_read_total", "Stream messages read", ["symbol"])
    c_write = Counter(
        "signal_feature_snapshot_write_total", "Snapshot rows written",
        ["symbol", "status"],
    )
    c_skip = Counter(
        "signal_feature_snapshot_skipped_total", "Snapshot skips",
        ["reason"],
    )
    c_err = Counter("signal_feature_snapshot_error_total", "Write errors", [])
    g_lag = Gauge("signal_feature_snapshot_lag_ms", "Decision→write lag (ms)", [])
    c_hash = Counter(
        "signal_feature_snapshot_schema_hash_seen_total",
        "Per-schema-hash counter for drift detection",
        ["schema_hash"],
    )

    conn = None

    def _get_conn():
        nonlocal conn
        if conn is None or conn.closed:
            import psycopg2
            conn = psycopg2.connect(db_dsn)
        return conn

    pending: list[tuple] = []
    pending_symbols: list[str] = []

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
                        sig = parse_signal(fields)
                        if not sig:
                            c_skip.labels(reason="parse_failed").inc()
                            ack_ids.append(msg_id)
                            continue

                        row = build_snapshot_row(
                            sig,
                            now_ms=now_ms,
                            schema_name=schema_name,
                            schema_version=schema_version,
                        )
                        if row is None:
                            c_skip.labels(reason="build_failed").inc()
                            ack_ids.append(msg_id)
                            continue

                        # row[2]=symbol, row[9]=schema_hash, row[0]=decision_time_ms
                        symbol = row[2]
                        schema_hash = row[9]
                        decision_ms = row[0]

                        c_read.labels(symbol=symbol).inc()
                        c_hash.labels(schema_hash=schema_hash).inc()
                        g_lag.set(now_ms - decision_ms)

                        if enabled:
                            pending.append(row)
                            pending_symbols.append(symbol)
                            c_write.labels(symbol=symbol, status="queued").inc()
                        else:
                            c_write.labels(symbol=symbol, status="shadow").inc()

                    except Exception as ex:
                        log.debug("snapshot build error: %s", ex)
                        c_skip.labels(reason="exception").inc()
                    ack_ids.append(msg_id)

        if pending and enabled:
            try:
                db_conn = _get_conn()
                n = upsert_batch(db_conn, pending)
                for sym in pending_symbols:
                    c_write.labels(symbol=sym, status="ok").inc()
                log.debug("sfs_writer: upserted %d rows", n)
                pending = []
                pending_symbols = []
            except Exception as e:
                c_err.inc()
                log.warning("sfs_writer DB flush error (fail-open): %s", e)
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
