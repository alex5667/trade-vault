"""
exec_quality_writer.py — Phase 4: Execution quality write-back to signal_outcome.

Reads trades:closed stream and writes actual fill_px, exec_slippage_bps, fees_bps
back to signal_outcome by sid. Closes the measurement loop:

  signal_outcome.entry_px   = estimated fill (mid ± spread + slip_prior)  [Phase 0]
  signal_outcome.fill_px    = actual fill from execution system            [Phase 4, this service]
  exec_slippage_bps         = |fill_px − entry_px| / entry_px × 10 000

Idempotent: UPDATE WHERE fill_px IS NULL (records already written are skipped).
SHADOW by default: EXEC_QUALITY_WRITER_ENABLED=0 → counts but no DB writes.

ENV:
  EXEC_QUALITY_WRITER_ENABLED  = 0              master switch
  EXEC_QUALITY_WRITER_REDIS_URL = redis://redis-worker-1:6379/0
  EXEC_QUALITY_WRITER_DB_DSN   = (SO_RESOLVER_DB_DSN or TRADES_DB_DSN)
  EXEC_QUALITY_WRITER_PORT     = 9914
  EXEC_QUALITY_WRITER_IN_STREAM = trades:closed
  EXEC_QUALITY_WRITER_GROUP    = exec-quality-writer
  EXEC_QUALITY_WRITER_CONSUMER = exec-quality-writer-1
  EXEC_QUALITY_WRITER_BATCH    = 200
  EXEC_QUALITY_WRITER_FLUSH_SEC = 30

Prometheus metrics (port EXEC_QUALITY_WRITER_PORT):
  exec_quality_writes_total{status}     written / skipped_no_sid / skipped_no_fill_px / error
  exec_quality_slippage_bps            Histogram of realized slippage bps (p50/p95/p99)
  exec_quality_open_gauge              Gauge: records seen but not yet in signal_outcome
"""
from __future__ import annotations

import logging
import math
import os
import time
from typing import Any

log = logging.getLogger("exec_quality_writer")


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


# ─── Fill event parsing ───────────────────────────────────────────────────────

def parse_fill_from_closed(fields: dict) -> dict | None:
    """
    Extract fill quality fields from a trades:closed stream record.

    Returns dict with {sid, fill_px, fee_bps, ts_fill_ms} or None if unusable.
    """
    sid = fields.get("sid") or fields.get("signal_id") or fields.get("id")
    if not sid:
        return None
    sid = str(sid)

    # Actual fill price at entry (entry_price is the actual fill from execution)
    fill_px = _safe_float(
        fields.get("entry_price") or
        fields.get("fill_price") or
        fields.get("fill_px") or
        fields.get("px"), 0.0,
    )
    if fill_px <= 0:
        return None

    fee_bps = _safe_float(
        fields.get("commission_bps") or
        fields.get("fee_bps") or
        fields.get("fees_bps") or
        fields.get("fee"), 0.0,
    )

    ts_fill_ms = int(
        _safe_float(
            fields.get("ts_fill_ms") or
            fields.get("fill_ts_ms") or
            fields.get("ts_ms") or
            fields.get("ts"),
            time.time() * 1000,
        )
    )

    return dict(sid=sid, fill_px=fill_px, fee_bps=fee_bps, ts_fill_ms=ts_fill_ms)


# ─── DB write ────────────────────────────────────────────────────────────────

_UPDATE_SQL = """
    UPDATE signal_outcome
    SET fill_px            = %s,
        exec_slippage_bps  = CASE WHEN entry_px > 0
            THEN ABS(%s - entry_px) / entry_px * 10000.0
            ELSE 0.0 END,
        fees_bps           = %s
    WHERE sid = %s
      AND fill_px IS NULL
"""


def apply_fill_updates(conn: Any, updates: list[tuple]) -> int:
    """
    Batch-update signal_outcome with fill data. Returns count of updates attempted.
    Idempotent: WHERE fill_px IS NULL means already-written records are skipped.
    """
    if not updates:
        return 0
    with conn.cursor() as cur:
        from psycopg2.extras import execute_batch  # type: ignore
        execute_batch(cur, _UPDATE_SQL, updates, page_size=50)
    conn.commit()
    return len(updates)


# ─── Main service ─────────────────────────────────────────────────────────────

def main() -> None:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    enabled    = _env_bool("EXEC_QUALITY_WRITER_ENABLED", False)
    db_dsn     = _env("EXEC_QUALITY_WRITER_DB_DSN",
                      _env("SO_RESOLVER_DB_DSN", _env("TRADES_DB_DSN", "")))
    redis_url  = _env("EXEC_QUALITY_WRITER_REDIS_URL",
                      _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    port       = _env_int("EXEC_QUALITY_WRITER_PORT", 9914)
    in_stream  = _env("EXEC_QUALITY_WRITER_IN_STREAM", "trades:closed")
    group      = _env("EXEC_QUALITY_WRITER_GROUP", "exec-quality-writer")
    consumer   = _env("EXEC_QUALITY_WRITER_CONSUMER", "exec-quality-writer-1")
    batch      = _env_int("EXEC_QUALITY_WRITER_BATCH", 200)
    flush_sec  = _env_int("EXEC_QUALITY_WRITER_FLUSH_SEC", 30)

    log.info(
        "exec_quality_writer starting | enabled=%s port=%d stream=%s",
        enabled, port, in_stream,
    )

    import redis  # type: ignore
    rc = redis.from_url(redis_url, decode_responses=True)

    try:
        rc.xgroup_create(in_stream, group, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create: %s", e)

    start_http_server(port)
    c_writes = Counter("exec_quality_writes_total", "Write outcomes", ["status"])
    h_slip   = Histogram(
        "exec_quality_slippage_bps",
        "Realized slippage bps (|fill_px - entry_px| / entry_px * 10000)",
        buckets=[0.5, 1.0, 2.0, 3.0, 5.0, 7.0, 10.0, 15.0, 20.0, 50.0],
    )

    conn = None

    def _get_conn():
        nonlocal conn
        if conn is None or conn.closed:
            import psycopg2  # type: ignore
            conn = psycopg2.connect(db_dsn)
        return conn

    updates: list[tuple] = []
    last_flush_ms = int(time.time() * 1000)

    while True:
        try:
            resp = rc.xreadgroup(
                groupname=group, consumername=consumer,
                streams={in_stream: ">"}, count=batch, block=2000,
            )
        except Exception as e:
            if "NOGROUP" in str(e):
                try:
                    rc.xgroup_create(in_stream, group, id="0", mkstream=True)
                except Exception as ex:
                    if "BUSYGROUP" not in str(ex):
                        log.warning("xgroup_create retry: %s", ex)
            else:
                log.warning("XREADGROUP error: %s", e)
            time.sleep(1)
            continue

        ack_ids = []
        if resp:
            for _stream, messages in resp:
                for msg_id, fields in messages:
                    fill = parse_fill_from_closed(fields)
                    if fill is None:
                        c_writes.labels(status="skipped_no_fill_px" if fields.get("sid") else "skipped_no_sid").inc()
                        ack_ids.append(msg_id)
                        continue

                    updates.append((
                        fill["fill_px"],
                        fill["fill_px"],   # second arg: for slippage formula ABS(%s - entry_px)
                        fill["fee_bps"],
                        fill["sid"],
                    ))

                    # Plan 3 / Step 2: TCA CLOSE stage (and FILL — trades:closed
                    # is post-close, so the entry fill is no longer a separate
                    # signal in this pipeline; we record FILL alongside CLOSE
                    # with the same px so per-stage queries don't see gaps).
                    # Master switch ORDER_EXEC_EVENTS_ENABLED=0 → no-op.
                    try:
                        from core.order_execution_events import Stage as _OEEStage
                        from core.order_execution_events import emit as _oee_emit
                        _sym = str(fields.get("symbol") or "").strip().upper()
                        _dir = str(fields.get("direction") or fields.get("side") or "").upper()
                        _side = 1 if _dir not in ("SHORT", "SELL", "-1") else -1
                        if _sym:
                            _close_px = _safe_float(
                                fields.get("close_price") or fields.get("exit_price")
                                or fields.get("close_px"), 0.0,
                            )
                            _pnl = _safe_float(fields.get("pnl") or fields.get("realized_pnl"), 0.0)
                            _r_mult = _safe_float(fields.get("r_multiple") or fields.get("r_mult"), 0.0)
                            _venue = str(fields.get("venue") or fields.get("exchange") or "")
                            # FILL = entry side (px=entry_price already captured in `fill_px`)
                            _oee_emit(
                                rc, sid=fill["sid"], stage=_OEEStage.FILL,
                                symbol=_sym, side=_side, status="ok",
                                ts_ms=fill["ts_fill_ms"],
                                venue=_venue or None,
                                px=fill["fill_px"],
                                payload={"fee_bps": fill["fee_bps"]},
                            )
                            # CLOSE = exit side
                            _close_ts = int(
                                _safe_float(
                                    fields.get("ts_close_ms") or fields.get("close_ts_ms")
                                    or fields.get("ts_ms"),
                                    fill["ts_fill_ms"],
                                )
                            )
                            _oee_emit(
                                rc, sid=fill["sid"], stage=_OEEStage.CLOSE,
                                symbol=_sym, side=_side, status="ok",
                                ts_ms=_close_ts,
                                venue=_venue or None,
                                px=_close_px if _close_px > 0 else None,
                                payload={
                                    "pnl": _pnl, "r_multiple": _r_mult,
                                    "close_reason": str(fields.get("close_reason") or ""),
                                },
                            )
                    except Exception:
                        pass  # fail-open

                    ack_ids.append(msg_id)

            if ack_ids:
                try:
                    rc.xack(in_stream, group, *ack_ids)
                except Exception as e:
                    log.warning("XACK error: %s", e)

        now_ms = int(time.time() * 1000)
        if updates and (now_ms - last_flush_ms) >= flush_sec * 1000:
            last_flush_ms = now_ms

            if not db_dsn:
                log.debug("EXEC_QUALITY_WRITER_DB_DSN not set — skipping flush")
                updates.clear()
                continue

            if enabled:
                try:
                    db_conn = _get_conn()
                    n = apply_fill_updates(db_conn, updates)
                    c_writes.labels(status="written").inc(n)
                    log.info("exec_quality_writer: wrote %d fill updates", n)
                except Exception as e:
                    c_writes.labels(status="error").inc(len(updates))
                    log.warning("exec_quality_writer: DB error: %s", e)
                    try:
                        if conn and not conn.closed:
                            conn.rollback()
                    except Exception:
                        pass
                    conn = None
            else:
                log.debug(
                    "exec_quality_writer SHADOW: would write %d fill updates (EXEC_QUALITY_WRITER_ENABLED=0)",
                    len(updates),
                )
                c_writes.labels(status="skipped_no_sid").inc(0)  # keep counter alive

            updates.clear()


if __name__ == "__main__":
    main()
