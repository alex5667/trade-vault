"""liq_evt_archiver_v1.py — Persist raw liquidation events to Timescale.

Consume stream:liq_evt via dedicated consumer group `liq_archive_group`
and batch-insert into Timescale hypertable `liquidation_events_raw` (see
migration 20260516_01_liquidation_events_raw.sql).

Design
------
* Separate consumer group from liqmap-snapshot-timer (no PEL collision).
* Idempotent INSERT via ON CONFLICT (PK = ts_event, venue, symbol, event_id).
* XACK only after successful Postgres commit (at-least-once delivery).
* Batched commits: LIQ_ARCHIVER_BATCH_SIZE rows or LIQ_ARCHIVER_FLUSH_MS,
  whichever first.
* Source-agnostic: accepts both legacy ("src", "ts_ms", "raw_side") and
  canonical ("venue", "ts_event_ms", "order_side") field names.

ENV (all optional)
------------------
REDIS_URL                redis://redis-worker-1:6379/0
PG_DSN                   postgresql://trading:.../scanner_analytics
LIQ_EVT_STREAM           stream:liq_evt
LIQ_ARCHIVER_GROUP       liq_archive_group
LIQ_ARCHIVER_CONSUMER    liq_archive_1
LIQ_ARCHIVER_BATCH_SIZE  200
LIQ_ARCHIVER_FLUSH_MS    2000
LIQ_ARCHIVER_BLOCK_MS    2000
LIQ_ARCHIVER_METRICS_PORT 9134
"""
from __future__ import annotations

import json
import logging
import os
import signal
import time
from typing import Any

logger = logging.getLogger("liq_evt_archiver")


_REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
_PG_DSN: str = os.getenv(
    "PG_DSN",
    "postgresql://trading:R0xU1KuRUygJqfxeOBTzQh423Qw0DOG8@scanner-postgres:5432/scanner_analytics",
)
_STREAM: str = os.getenv("LIQ_EVT_STREAM", "stream:liq_evt")
_GROUP: str = os.getenv("LIQ_ARCHIVER_GROUP", "liq_archive_group")
_CONSUMER: str = os.getenv("LIQ_ARCHIVER_CONSUMER", os.getenv("HOSTNAME", "liq_archive_1"))
_BATCH_SIZE: int = int(os.getenv("LIQ_ARCHIVER_BATCH_SIZE", "200"))
_FLUSH_MS: int = int(os.getenv("LIQ_ARCHIVER_FLUSH_MS", "2000"))
_BLOCK_MS: int = int(os.getenv("LIQ_ARCHIVER_BLOCK_MS", "2000"))
_METRICS_PORT: int = int(os.getenv("LIQ_ARCHIVER_METRICS_PORT", "9134"))


_INSERT_SQL = """
INSERT INTO liquidation_events_raw (
    ts_event_ms, ts_event, ts_ingest_ms,
    venue, symbol, liq_side, order_side,
    price, qty, notional_usd,
    event_id, trace_id, quality_flags, schema_version,
    redis_msg_id, payload_json
) VALUES %s
ON CONFLICT DO NOTHING
"""


def _f(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _i(v: Any, default: int = 0) -> int:
    try:
        if v is None or v == "":
            return default
        return int(float(v))
    except (TypeError, ValueError):
        return default


def _s(v: Any) -> str:
    if v is None:
        return ""
    return v if isinstance(v, str) else str(v)


def _norm_venue(v: str) -> str:
    """Normalize 'binance_usdm' → 'binance_usdtm' (canonical)."""
    v = v.strip().lower()
    if v == "binance_usdm":
        return "binance_usdtm"
    return v


def _normalize_row(msg_id: str, fields: dict[str, str]) -> tuple | None:
    """Build a single INSERT tuple from a stream entry.

    Returns None if the entry is missing critical fields (caller should ACK
    without re-insert, to avoid blocking the consumer group).
    """
    venue = _norm_venue(_s(fields.get("venue") or fields.get("src")))
    symbol = _s(fields.get("symbol")).upper()
    ts_event_ms = _i(fields.get("ts_event_ms") or fields.get("ts_ms"))
    ts_ingest_ms = _i(fields.get("ts_ingest_ms") or fields.get("recv_ts_ms") or ts_event_ms)

    if not venue or not symbol or ts_event_ms <= 0:
        return None

    liq_side = _s(fields.get("liq_side")).lower()
    if liq_side not in ("long", "short"):
        return None

    price = _f(fields.get("price"))
    qty = _f(fields.get("qty"))
    notional = _f(fields.get("notional_usd"))
    if price <= 0 or qty <= 0 or notional <= 0:
        return None

    event_id = _s(fields.get("event_id")) or f"{venue}:{symbol}:{ts_event_ms}"
    order_side = _s(fields.get("order_side") or fields.get("raw_side"))
    trace_id = _s(fields.get("trace_id"))
    quality_flags = _s(fields.get("quality_flags"))
    schema_version = _i(fields.get("schema_version"), default=1)

    # Strip Redis-stream-only fields when persisting `payload_json`.
    payload = {k: fields.get(k) for k in fields}
    payload_json = json.dumps(payload, ensure_ascii=False)

    # Convert ts_event_ms → TIMESTAMPTZ via Postgres function in SQL is overkill;
    # we use Python datetime to keep INSERT signature stable.
    from datetime import datetime, timezone
    ts_event_dt = datetime.fromtimestamp(ts_event_ms / 1000.0, tz=timezone.utc)

    return (
        ts_event_ms, ts_event_dt, ts_ingest_ms,
        venue, symbol, liq_side, order_side or None,
        price, qty, notional,
        event_id, trace_id or None, quality_flags or None, schema_version,
        msg_id, payload_json,
    )


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    try:
        import psycopg2
        import psycopg2.extras
        import redis as _redis
    except ImportError as e:
        logger.error("missing dependency: %s", e)
        return 2

    try:
        from prometheus_client import Counter, Gauge, start_http_server
        m_read = Counter("liq_archiver_read_total", "Stream entries read")
        m_inserted = Counter("liq_archiver_inserted_total", "Rows inserted (no-op on conflict OK)")
        m_skipped = Counter("liq_archiver_skipped_total", "Entries skipped", ["reason"])
        m_db_errors = Counter("liq_archiver_db_errors_total", "DB errors")
        m_lag_ms = Gauge("liq_archiver_lag_ms", "Now - last ts_event_ms")
        m_last_event = Gauge("liq_archiver_last_event_ts_ms", "Last archived event ts (ms)")
        m_pel = Gauge("liq_archiver_pel_depth", "PEL depth for our consumer")
        start_http_server(_METRICS_PORT)
        logger.info("Prometheus metrics on :%d", _METRICS_PORT)
    except Exception as e:
        logger.warning("metrics disabled: %s", e)
        m_read = m_inserted = m_db_errors = None  # type: ignore[assignment]
        m_skipped = m_lag_ms = m_last_event = m_pel = None  # type: ignore[assignment]

    r = _redis.from_url(_REDIS_URL, decode_responses=True)
    logger.info("Redis connected: %s", _REDIS_URL)

    # Ensure consumer group exists (idempotent).
    try:
        r.xgroup_create(_STREAM, _GROUP, id="0", mkstream=True)
        logger.info("Created consumer group %s on %s", _GROUP, _STREAM)
    except _redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            logger.error("xgroup_create failed: %s", e)
            return 3
        logger.info("Consumer group %s already exists on %s", _GROUP, _STREAM)

    # Connect to Postgres.
    pg = psycopg2.connect(_PG_DSN)
    pg.autocommit = False
    logger.info("Postgres connected: %s", _PG_DSN.split("@")[-1])

    stop = {"flag": False}

    def _sig(_a: int, _b: Any) -> None:
        logger.info("signal received → graceful shutdown")
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    pending_rows: list[tuple] = []
    pending_ack_ids: list[str] = []
    last_flush_ms = int(time.time() * 1000)

    def _flush() -> None:
        nonlocal pending_rows, pending_ack_ids, last_flush_ms
        if not pending_rows:
            return
        try:
            with pg.cursor() as cur:
                psycopg2.extras.execute_values(cur, _INSERT_SQL, pending_rows, page_size=_BATCH_SIZE)
            pg.commit()
            if m_inserted:
                m_inserted.inc(len(pending_rows))
        except Exception as e:
            pg.rollback()
            logger.exception("DB insert failed (%d rows): %s", len(pending_rows), e)
            if m_db_errors:
                m_db_errors.inc()
            # Do NOT ack — let messages stay in PEL for retry/autoclaim.
            return
        # Ack only after successful commit.
        if pending_ack_ids:
            try:
                r.xack(_STREAM, _GROUP, *pending_ack_ids)
            except Exception as e:
                logger.warning("xack failed: %s", e)
        pending_rows = []
        pending_ack_ids = []
        last_flush_ms = int(time.time() * 1000)

    logger.info(
        "Starting archiver: stream=%s group=%s consumer=%s batch=%d flush=%dms",
        _STREAM, _GROUP, _CONSUMER, _BATCH_SIZE, _FLUSH_MS,
    )

    while not stop["flag"]:
        try:
            # XREADGROUP with block.
            resp = r.xreadgroup(
                _GROUP, _CONSUMER,
                {_STREAM: ">"},
                count=_BATCH_SIZE,
                block=_BLOCK_MS,
            )
        except Exception as e:
            logger.warning("xreadgroup error: %s", e)
            time.sleep(1.0)
            continue

        if resp:
            for _stream, entries in resp:
                for msg_id, fields in entries:
                    if m_read:
                        m_read.inc()
                    row = _normalize_row(msg_id, fields)
                    if row is None:
                        # bad entry — ack so it doesn't block, count as skipped.
                        if m_skipped:
                            m_skipped.labels(reason="normalize_failed").inc()
                        try:
                            r.xack(_STREAM, _GROUP, msg_id)
                        except Exception:
                            pass
                        continue
                    pending_rows.append(row)
                    pending_ack_ids.append(msg_id)
                    if m_last_event:
                        m_last_event.set(float(row[0]))  # ts_event_ms
                    if m_lag_ms:
                        m_lag_ms.set(max(0.0, time.time() * 1000 - float(row[0])))

        # Flush on batch full or time elapsed.
        now_ms = int(time.time() * 1000)
        if pending_rows and (
            len(pending_rows) >= _BATCH_SIZE or (now_ms - last_flush_ms) >= _FLUSH_MS
        ):
            _flush()

        # Update PEL depth metric occasionally (every 10s).
        if m_pel and now_ms % 10000 < 200:
            try:
                pend = r.xpending(_STREAM, _GROUP)
                if pend and isinstance(pend, dict):
                    m_pel.set(float(pend.get("pending", 0)))
            except Exception:
                pass

    # Drain on shutdown.
    _flush()
    try:
        pg.close()
    except Exception:
        pass
    logger.info("liq_evt_archiver stopped")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
