from __future__ import annotations

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

"""bbo_ts_writer — Redis Stream → Timescale bbo_ts table (Phase B1).

This is the warm-path component for BBO time-series.

Hot-path publishes compact snapshots to `events:bbo_ts` (see services/orderflow/bbo_store.py).
This writer persists them to Timescale/PG to support TCA joins.

Operational requirements
------------------------
* at-least-once: consumer group + XACK after successful DB write
* idempotent: DB primary key (sym,venue,ts_ms,ts) with ON CONFLICT DO NOTHING
* fail-open: invalid payloads go to DLQ and are ACK'ed
* bounded: batch writes

ENV
---
REDIS_URL=redis://redis-worker-1:6379/0
TRADES_DB_DSN=postgresql://trading:...@postgres:5432/scanner_analytics

BBO_TS_STREAM=events:bbo_ts
BBO_TS_CG=bbo_ts_writer
BBO_TS_CONSUMER=<hostname:pid>
BBO_TS_BLOCK_MS=5000
BBO_TS_COUNT=256

BBO_TS_DLQ_STREAM=events:bbo_ts:dlq
BBO_TS_DLQ_MAXLEN=200000

BBO_TS_WRITER_BATCH_SIZE=500
BBO_TS_WRITER_METRICS_PORT=9826

Schema management
-----------------
Apply services/posttrade/sql/tca_timescale_v1.sql via migrations/psql.
"""

import asyncio
import json
import logging
import os
import socket
from dataclasses import dataclass
from typing import Any

try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover
    aioredis = None

from services.posttrade.bbo_ts_writer_metrics import build_metrics, start_metrics_server
from services.posttrade.redis_stream_dlq import publish_dlq

logger = logging.getLogger("bbo_ts_writer")


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def _env_int(name: str, default: str) -> int:
    try:
        return int(float(_env(name, default)))
    except Exception:
        return int(float(default))


def pick_dsn() -> str:
    return (
        (os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN"))
        or os.getenv("TIMESCALE_DSN")
        or os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("ANALYTICS_DSN")
        or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN"))
        or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL"))
        or ""
    )


def _now_ms() -> int:
    return get_ny_time_millis()


def _loads_json(v: Any) -> dict | None:
    if v is None:
        return None
    if isinstance(v, dict):
        return v
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8", "replace")
    if not isinstance(v, str):
        v = str(v)
    s = v.strip()
    if not s:
        return None
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _parse_stream_fields(fields: dict[Any, Any]) -> dict[str, Any]:
    # Standard contract: JSON in field `payload`.
    if b"payload" in fields:
        obj = _loads_json(fields.get(b"payload"))
        return obj or {}
    if "payload" in fields:
        obj = _loads_json(fields.get("payload"))
        return obj or {}
    # Fallback: treat fields as raw
    out: dict[str, Any] = {}
    for k, v in fields.items():
        try:
            kk = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
            out[kk] = v.decode() if isinstance(v, (bytes, bytearray)) else v
        except Exception:
            continue
    return out


def _validate_payload(p: dict[str, Any]) -> tuple[bool, str]:
    for k in ("ts_ms", "symbol", "venue", "bid", "ask", "mid"):
        if k not in p or p.get(k) in (None, ""):
            return False, f"missing:{k}"
    try:
        if int(p.get("ts_ms")) <= 0:  # type: ignore
            return False, "bad:ts_ms"
    except Exception:
        return False, "bad:ts_ms"
    return True, ""


class PgWriter:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._conn = None

    def _connect(self):
        try:
            import psycopg  # type: ignore
            return psycopg.connect(self.dsn)
        except Exception:
            import psycopg2  # type: ignore
            return psycopg2.connect(self.dsn)

    def _get_conn(self):
        if self._conn is None or self._conn.closed:
            self._conn = self._connect()
        return self._conn

    def _execute_insert(self, rows: list[dict[str, Any]]) -> int:
        conn = self._get_conn()
        cur = conn.cursor()
        sql = (
            "INSERT INTO bbo_ts (ts, ts_ms, sym, venue, bid, ask, mid, producer, schema_version, stream_id) "
            "VALUES (to_timestamp(%(ts_ms)s/1000.0), %(ts_ms)s, %(sym)s, %(venue)s, %(bid)s, %(ask)s, %(mid)s, %(producer)s, %(schema_version)s, %(stream_id)s) "
            "ON CONFLICT (sym, venue, ts_ms, ts) DO NOTHING"
        )
        params = []
        for r in rows:
            params.append(
                {
                    "ts_ms": int(r["ts_ms"]),
                    "sym": str(r["sym"]),
                    "venue": str(r["venue"]),
                    "bid": float(r["bid"]),
                    "ask": float(r["ask"]),
                    "mid": float(r["mid"]),
                    "producer": (r.get("producer") or ""),
                    "schema_version": int(r.get("schema_version") or 1),
                    "stream_id": (r.get("stream_id") or ""),
                }
            )
        cur.executemany(sql, params)
        conn.commit()
        return len(rows)

    def insert_rows(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        try:
            return self._execute_insert(rows)
        except Exception:
            self._conn = None
            return self._execute_insert(rows)


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

    @staticmethod
    def from_env() -> Cfg:
        host = socket.gethostname()
        return Cfg(
            redis_url=_env("REDIS_URL", "redis://redis-worker-1:6379/0"),
            stream=_env("BBO_TS_STREAM", RS.EVENTS_BBO_TS),
            group=_env("BBO_TS_CG", "bbo_ts_writer"),
            consumer=_env("BBO_TS_CONSUMER", f"{host}:{os.getpid()}"),
            block_ms=_env_int("BBO_TS_BLOCK_MS", "5000"),
            count=_env_int("BBO_TS_COUNT", "256"),
            dlq_stream=_env("BBO_TS_DLQ_STREAM", "events:bbo_ts:dlq"),
            dlq_maxlen=_env_int("BBO_TS_DLQ_MAXLEN", "200000"),
            batch_size=_env_int("BBO_TS_WRITER_BATCH_SIZE", "500"),
            metrics_port=_env_int("BBO_TS_WRITER_METRICS_PORT", "9826"),
        )


async def _ensure_group(r: Any, *, stream: str, group: str) -> None:
    while True:
        try:
            await r.xgroup_create(stream, group, id="$", mkstream=True)
            return
        except Exception as e:
            if "BUSYGROUP" in str(e).upper():
                return
            if "LOADING" in str(e).upper():
                await asyncio.sleep(1.0)
                continue
            raise


async def main() -> None:
    if aioredis is None:
        raise RuntimeError("redis-py is required")
    dsn = pick_dsn()
    if not dsn:
        raise RuntimeError("TRADES_DB_DSN (or TIMESCALE_DSN) must be set")

    cfg = Cfg.from_env()
    metrics = build_metrics()
    start_metrics_server(cfg.metrics_port)

    r = aioredis.from_url(cfg.redis_url, decode_responses=False)
    await _ensure_group(r, stream=cfg.stream, group=cfg.group)

    pg = PgWriter(dsn)

    logger.info("bbo_ts_writer started: stream=%s group=%s", cfg.stream, cfg.group)

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
                # pending gauge (best-effort)
                try:
                    if metrics.get("pending_count") is not None:
                        pend = await r.xpending(cfg.stream, cfg.group)
                        metrics["pending_count"].set(int(pend["pending"] if isinstance(pend, dict) else pend[0]))
                except Exception:
                    pass
                continue

            rows: list[dict[str, Any]] = []
            ack_ids: list[str] = []
            for _stream, msgs in res:
                for mid, fields in msgs:
                    mid_s = mid.decode() if isinstance(mid, (bytes, bytearray)) else str(mid)
                    if metrics.get("seen_total") is not None:
                        metrics["seen_total"].inc()
                    payload = _parse_stream_fields(fields)
                    ok, reason = _validate_payload(payload)
                    if not ok:
                        if metrics.get("dlq_total") is not None:
                            metrics["dlq_total"].inc()
                        await publish_dlq(
                            r,
                            dlq_stream=cfg.dlq_stream,
                            reason=reason,
                            error="invalid_bbo_payload",
                            src_stream=cfg.stream,
                            src_entry_id=mid_s,
                            payload=payload,
                            maxlen=cfg.dlq_maxlen,
                        )
                        # ACK poison pill to avoid blocking the group.
                        await r.xack(cfg.stream, cfg.group, mid)
                        continue

                    ts_ms = int(payload["ts_ms"])
                    now_ms = _now_ms()
                    if metrics.get("redis_lag_ms") is not None:
                        metrics["redis_lag_ms"].observe(max(0.0, float(now_ms - ts_ms)))

                    rows.append(
                        {
                            "ts_ms": ts_ms,
                            "sym": (payload.get("symbol") or "").upper(),
                            "venue": (payload.get("venue") or "binance").lower(),
                            "bid": float(payload.get("bid")),  # type: ignore
                            "ask": float(payload.get("ask")),  # type: ignore
                            "mid": float(payload.get("mid")),  # type: ignore
                            "producer": (payload.get("producer") or ""),
                            "schema_version": int(payload.get("schema_version") or 1),
                            "stream_id": mid_s,
                        }
                    )
                    ack_ids.append(mid_s)

            # DB write batch
            if rows:
                try:
                    pg.insert_rows(rows)
                    if metrics.get("written_total") is not None:
                        metrics["written_total"].inc(len(rows))
                    # ACK after DB commit
                    await r.xack(cfg.stream, cfg.group, *ack_ids)
                except Exception:
                    if metrics.get("db_fail_total") is not None:
                        metrics["db_fail_total"].inc()
                    logger.exception("DB write failed; will retry (no ACK)")

        except Exception as e:
            err_str = str(e).upper()
            if "LOADING" in err_str:
                logger.warning("Redis is loading dataset in memory, waiting...")
            elif "NOPERM" in err_str:
                logger.error(f"Redis permission error: {e}")
            elif "CONNECTION" in err_str or "TIMEOUT" in err_str:
                logger.warning(f"Redis connection/timeout error: {e}")
            elif "NOGROUP" in err_str:
                logger.warning("Consumer group missing (NOGROUP), recreating...")
                await _ensure_group(r, stream=cfg.stream, group=cfg.group)
            else:
                logger.exception("bbo_ts_writer loop error")
            await asyncio.sleep(1.0)


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    asyncio.run(main())
