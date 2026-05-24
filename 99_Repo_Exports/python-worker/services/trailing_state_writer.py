"""Postgres/Timescale writer for `events:trailing:state` audit stream.

Consumes audit events produced by ``services/trailing_state_worker.py:_emit_audit()``
and persists them into the ``trailing_state_transitions`` hypertable
(see ``migrations/065_trailing_state_transitions.sql``).

Pattern mirrors ``runners/decision_snapshot_writer.py``:
  XREADGROUP -> batch INSERT (executemany) -> XACK on success -> DLQ on bad payloads.

The target table is append-only; no ON CONFLICT clause is used.
"""

import asyncio
import json
import logging
import os
import socket
import time
from dataclasses import dataclass
from typing import Any

try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover
    aioredis = None

from prometheus_client import Counter, Gauge, Histogram, start_http_server

logger = logging.getLogger("trailing_state_writer")


# ────────────────────────────────────────────────────────────────────────────
# Metrics
# ────────────────────────────────────────────────────────────────────────────
@dataclass
class Metrics:
    processed_total: Counter
    written_total: Counter
    dlq_total: Counter
    db_fail_total: Counter
    pg_lag_ms: Histogram
    pending_count: Gauge
    last_ok: Gauge
    last_batch_rows: Gauge


def build_metrics() -> Metrics:
    return Metrics(
        processed_total=Counter(
            "trailing_state_writer_processed_total",
            "Total trailing-state audit events processed",
        ),
        written_total=Counter(
            "trailing_state_writer_written_total",
            "Total trailing-state rows written to DB",
        ),
        dlq_total=Counter(
            "trailing_state_writer_dlq_total",
            "Trailing-state events sent to DLQ",
            ["reason"],
        ),
        db_fail_total=Counter(
            "trailing_state_writer_db_fail_total",
            "Trailing-state DB write failures",
        ),
        pg_lag_ms=Histogram(
            "trailing_state_writer_pg_lag_ms",
            "Lag between event ts_ms and DB write",
            buckets=[10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000],
        ),
        pending_count=Gauge(
            "trailing_state_writer_pending_count",
            "Pending messages in consumer group",
        ),
        last_ok=Gauge(
            "trailing_state_writer_last_ok",
            "1 if last batch succeeded, 0 otherwise",
        ),
        last_batch_rows=Gauge(
            "trailing_state_writer_last_batch_rows",
            "Rows written in the last batch",
        ),
    )


# ────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────
def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(_env(name, str(default))))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env(name, str(default)))
    except Exception:
        return default


def pick_dsn() -> str:
    """Pick the first non-empty DSN from a prioritized env list."""
    return (
        (os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN"))
        or os.getenv("TIMESCALE_DSN")
        or os.getenv("DATABASE_URL")
        or ""
    )


def _decode(v: Any) -> Any:
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode("utf-8", "replace")
        except Exception:
            return str(v)
    return v


def _parse_stream_fields(fields: dict[Any, Any]) -> dict[str, Any]:
    """Decode bytes keys/values from a Redis stream entry into a str->str dict.

    The trailing-state audit stream is FLAT (no nested ``payload`` JSON), so we
    just decode every k/v.
    """
    out: dict[str, Any] = {}
    for k, v in fields.items():
        out[str(_decode(k))] = _decode(v)
    return out


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        s = str(v).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _to_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        s = str(v).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


# Required fields for a valid trailing-state audit row.
_REQUIRED_FIELDS = (
    "sid",
    "symbol",
    "side",
    "to_state",
    "event_type",
    "reason_code",
    "ts_ms",
)


def _normalize_row(fields: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    """Convert a raw stream payload into a row dict for INSERT.

    Returns ``(row, "")`` on success or ``(None, "missing_<field>")`` on
    validation error. Optional numeric fields convert to ``None`` if they fail
    to parse (the original raw string is still retained inside ``payload``).
    """
    # Mandatory presence + non-empty check
    for f in _REQUIRED_FIELDS:
        v = fields.get(f)
        if v is None or (isinstance(v, str) and not v.strip()):
            return None, f"missing_{f}"

    ts_ms = _to_int(fields.get("ts_ms"))
    if ts_ms is None or ts_ms <= 0:
        return None, "missing_ts_ms"

    # Build a deterministic JSON payload retaining every original key/value
    # (strings) for full audit fidelity.
    try:
        payload_json = json.dumps(
            {str(k): (None if v is None else str(v)) for k, v in fields.items()},
            ensure_ascii=False,
            sort_keys=True,
        )
    except Exception:
        payload_json = json.dumps({}, ensure_ascii=False)

    row = {
        "ts_ms": ts_ms,
        "sid": str(fields.get("sid") or "").strip(),
        "position_id": str(fields.get("position_id") or "").strip() or None,
        "symbol": str(fields.get("symbol") or "").strip(),
        "side": str(fields.get("side") or "").strip(),
        "from_state": (str(fields.get("from_state")).strip()
                       if fields.get("from_state") is not None else None) or None,
        "to_state": str(fields.get("to_state") or "").strip(),
        "event_type": str(fields.get("event_type") or "").strip(),
        "price": _to_float(fields.get("price")),
        "old_sl": _to_float(fields.get("old_sl")),
        "new_sl": _to_float(fields.get("new_sl")),
        "high_watermark": _to_float(fields.get("high_watermark")),
        "low_watermark": _to_float(fields.get("low_watermark")),
        "atr_value": _to_float(fields.get("atr_value")),
        "atr_mult": _to_float(fields.get("atr_mult")),
        "reason_code": str(fields.get("reason_code") or "").strip(),
        "profile": (str(fields.get("profile")).strip()
                    if fields.get("profile") is not None else None) or None,
        "payload": payload_json,
    }
    return row, ""


# ────────────────────────────────────────────────────────────────────────────
# Postgres writer
# ────────────────────────────────────────────────────────────────────────────
_INSERT_SQL = (
    "INSERT INTO trailing_state_transitions ("
    "ts, sid, position_id, symbol, side, from_state, to_state, event_type, "
    "price, old_sl, new_sl, high_watermark, low_watermark, "
    "atr_value, atr_mult, reason_code, profile, payload"
    ") VALUES ("
    "to_timestamp(%(ts_ms)s / 1000.0), %(sid)s, %(position_id)s, %(symbol)s, %(side)s, "
    "%(from_state)s, %(to_state)s, %(event_type)s, "
    "%(price)s, %(old_sl)s, %(new_sl)s, %(high_watermark)s, %(low_watermark)s, "
    "%(atr_value)s, %(atr_mult)s, %(reason_code)s, %(profile)s, %(payload)s"
    ")"
)


class PgWriter:
    """Thin synchronous wrapper around psycopg/psycopg2 ``executemany``."""

    def __init__(self, dsn: str):
        self.dsn = dsn

    def _connect(self):
        try:
            import psycopg  # type: ignore
            return psycopg.connect(self.dsn)
        except Exception:
            import psycopg2  # type: ignore
            return psycopg2.connect(self.dsn)

    def insert_rows(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.executemany(_INSERT_SQL, rows)
            conn.commit()
            return len(rows)
        finally:
            try:
                conn.close()
            except Exception:
                pass


# ────────────────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────────────────
@dataclass
class Cfg:
    redis_url: str
    stream: str
    group: str
    consumer: str
    block_ms: int
    count: int
    dlq_stream: str
    dlq_maxlen: int
    batch_size: int
    metrics_port: int
    fail_sleep_sec: float

    @staticmethod
    def from_env() -> "Cfg":
        host = socket.gethostname()
        return Cfg(
            redis_url=_env("REDIS_URL", "redis://redis-worker-1:6379/0"),
            stream=_env("TSW_STREAM", "events:trailing:state"),
            group=_env("TSW_CG", "trailing_state_persistence_cg"),
            consumer=_env("TSW_CONSUMER", f"{host}:{os.getpid()}"),
            block_ms=_env_int("TSW_BLOCK_MS", 5000),
            count=_env_int("TSW_COUNT", 500),
            dlq_stream=_env("TSW_DLQ_STREAM", "events:trailing:state:dlq"),
            dlq_maxlen=_env_int("TSW_DLQ_MAXLEN", 200000),
            batch_size=_env_int("TSW_BATCH_SIZE", 500),
            metrics_port=_env_int("TSW_METRICS_PORT", 9924),
            fail_sleep_sec=_env_float("TSW_FAIL_SLEEP_SEC", 1.0),
        )


# ────────────────────────────────────────────────────────────────────────────
# Stream group bootstrap
# ────────────────────────────────────────────────────────────────────────────
async def _ensure_group(r: Any, *, stream: str, group: str) -> None:
    while True:
        try:
            await r.xgroup_create(stream, group, id="$", mkstream=True)
            return
        except Exception as e:
            msg = str(e).upper()
            if "BUSYGROUP" in msg:
                return
            if "LOADING" in msg:
                await asyncio.sleep(1.0)
                continue
            raise


# ────────────────────────────────────────────────────────────────────────────
# Main loop
# ────────────────────────────────────────────────────────────────────────────
async def main() -> None:
    if aioredis is None:
        raise RuntimeError("redis-py is required")
    dsn = pick_dsn()
    if not dsn:
        raise RuntimeError(
            "ANALYTICS_DB_DSN / TRADES_DB_DSN / TIMESCALE_DSN must be set"
        )

    cfg = Cfg.from_env()
    metrics = build_metrics()
    start_http_server(cfg.metrics_port)

    r = aioredis.from_url(cfg.redis_url, decode_responses=False)
    await _ensure_group(r, stream=cfg.stream, group=cfg.group)
    pg = PgWriter(dsn)

    logger.info(
        "trailing_state_writer started: stream=%s group=%s consumer=%s",
        cfg.stream,
        cfg.group,
        cfg.consumer,
    )

    while True:
        try:
            res = await r.xreadgroup(
                groupname=cfg.group,
                consumername=cfg.consumer,
                streams={cfg.stream: ">"},
                count=cfg.count,
                block=cfg.block_ms,
            )
            if not res:
                try:
                    pend = await r.xpending(cfg.stream, cfg.group)
                    pending = int(pend["pending"] if isinstance(pend, dict) else pend[0])
                    metrics.pending_count.set(pending)
                except Exception:
                    pass
                continue

            ack_ids: list[str] = []
            rows: list[dict[str, Any]] = []

            for _stream, entries in res:
                for msg_id, fields in entries:
                    payload = _parse_stream_fields(fields)
                    metrics.processed_total.inc()
                    row, reason = _normalize_row(payload)
                    if row is None:
                        metrics.dlq_total.labels(reason=reason).inc()
                        try:
                            await r.xadd(
                                cfg.dlq_stream,
                                {
                                    "reason": reason,
                                    "src_msg_id": str(_decode(msg_id) or ""),
                                    "payload": json.dumps(
                                        {str(k): (None if v is None else str(v))
                                         for k, v in payload.items()},
                                        ensure_ascii=False,
                                    ),
                                },
                                maxlen=cfg.dlq_maxlen,
                                approximate=True,
                            )
                        except Exception:
                            logger.exception("trailing_state_writer DLQ XADD failed")
                        ack_ids.append(msg_id)
                        continue

                    now_ms = int(time.time() * 1000)
                    lag_ms = max(0, now_ms - int(row["ts_ms"]))
                    metrics.pg_lag_ms.observe(lag_ms)
                    rows.append(row)
                    ack_ids.append(msg_id)

            if rows:
                try:
                    written = pg.insert_rows(rows[: cfg.batch_size])
                    metrics.written_total.inc(written)
                    metrics.last_ok.set(1)
                    metrics.last_batch_rows.set(written)
                except Exception:
                    metrics.db_fail_total.inc()
                    metrics.last_ok.set(0)
                    logger.exception("trailing_state_writer DB failure")
                    await asyncio.sleep(cfg.fail_sleep_sec)
                    # do not ACK — leave in PEL for retry
                    continue

            if ack_ids:
                await r.xack(cfg.stream, cfg.group, *ack_ids)

            try:
                pend = await r.xpending(cfg.stream, cfg.group)
                pending = int(pend["pending"] if isinstance(pend, dict) else pend[0])
            except Exception:
                pending = 0
            metrics.pending_count.set(pending)

        except Exception:
            logger.exception("trailing_state_writer loop failure")
            metrics.last_ok.set(0)
            await asyncio.sleep(cfg.fail_sleep_sec)


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    raise SystemExit(asyncio.run(main()))
