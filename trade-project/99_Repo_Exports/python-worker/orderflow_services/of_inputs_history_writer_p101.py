"""
of_inputs_history_writer_p101.py — persistent archive of signals:of:inputs → Postgres.

Consumes signals:of:inputs via XREADGROUP and upserts into of_inputs_history.
Fixes: Redis stream (maxlen=5000 ≈ 15 days) loses old signals → training dataset
was limited to ~2.8 days. With this writer, all signals are kept in PG indefinitely
(subject to retention policy on of_inputs_history).

ENV:
  REDIS_URL                 redis-worker-1 connection string (required)
  DATABASE_URL              Postgres DSN (required)
  OF_INPUTS_STREAM          default: signals:of:inputs
  OF_INPUTS_HISTORY_GROUP   consumer group name (default: of_inputs_history_writer)
  OF_INPUTS_HISTORY_CONSUMER consumer name (default: hostname)
  OF_INPUTS_HISTORY_BATCH   XREADGROUP count per iteration (default: 50)
  OF_INPUTS_HISTORY_BLOCK_MS XREADGROUP block ms (default: 1000)
  METRICS_PORT              Prometheus port (default: 9157)
"""

import asyncio
import json
import logging
import os
import socket
from typing import Any

import asyncpg
import redis.asyncio as aioredis
from prometheus_client import Counter, Gauge, start_http_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("of_inputs_history_writer")

STREAM = os.getenv("OF_INPUTS_STREAM", "signals:of:inputs")
GROUP = os.getenv("OF_INPUTS_HISTORY_GROUP", "of_inputs_history_writer")
CONSUMER = os.getenv("OF_INPUTS_HISTORY_CONSUMER", socket.gethostname())
BATCH = int(os.getenv("OF_INPUTS_HISTORY_BATCH", "50") or 50)
BLOCK_MS = int(os.getenv("OF_INPUTS_HISTORY_BLOCK_MS", "1000") or 1000)
METRICS_PORT = int(os.getenv("METRICS_PORT", "9160") or 9160)

REDIS_URL = os.environ["REDIS_URL"]
DATABASE_URL = os.environ["DATABASE_URL"]

# ── Prometheus ────────────────────────────────────────────────────────────────
inserted_total = Counter("of_inputs_history_inserted_total", "Rows inserted into of_inputs_history")
skipped_total  = Counter("of_inputs_history_skipped_total",  "Rows skipped (duplicate sid)")
error_total    = Counter("of_inputs_history_error_total",    "Insert errors")
lag_gauge      = Gauge("of_inputs_history_stream_lag_entries", "Pending entries in consumer group")
last_ts_gauge  = Gauge("of_inputs_history_last_ts_ms", "ts_ms of last processed signal")

# ── SQL ───────────────────────────────────────────────────────────────────────
UPSERT_SQL = """
INSERT INTO of_inputs_history
    (ts_ms, sid, symbol, direction, feature_schema_version, regime, confidence, is_virtual, payload)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
ON CONFLICT (sid) DO NOTHING
"""


def _parse_payload(fields: dict[str, Any]) -> dict | None:
    raw = fields.get("payload") or fields.get("data")
    if not raw:
        return None
    try:
        return json.loads(raw) if isinstance(raw, (str, bytes)) else raw
    except Exception:
        return None


async def _ensure_group(r: aioredis.Redis) -> None:
    try:
        await r.xgroup_create(STREAM, GROUP, id="0", mkstream=False)
        logger.info("Created consumer group %s on %s", GROUP, STREAM)
    except Exception as exc:
        if "BUSYGROUP" in str(exc):
            logger.info("Consumer group %s already exists", GROUP)
        else:
            logger.warning("xgroup_create: %s", exc)


async def _process_batch(
    r: aioredis.Redis,
    pg: asyncpg.Connection,
    messages: list,
) -> None:
    rows: list[tuple] = []
    msg_ids: list[str] = []

    for msg_id, fields in messages:
        payload = _parse_payload(fields)
        if payload is None:
            msg_ids.append(msg_id)
            continue

        sid = str(payload.get("sid") or payload.get("signal_id") or "")
        if not sid:
            msg_ids.append(msg_id)
            continue

        symbol    = str(payload.get("symbol") or "")
        direction = str(payload.get("direction") or payload.get("side") or "")
        ts_ms     = int(payload.get("ts_ms") or payload.get("ts") or 0)
        fsv       = int(payload.get("feature_schema_version") or 14)
        inds      = payload.get("indicators") or {}
        regime    = str(inds.get("regime") or "unknown")
        confidence= float(payload.get("confidence") or payload.get("confidence01") or 0.0)
        is_virtual= bool(payload.get("is_virtual") or payload.get("virtual") or False)

        rows.append((ts_ms, sid, symbol, direction, fsv, regime, confidence, is_virtual,
                     json.dumps(payload)))
        msg_ids.append(msg_id)

    if rows:
        try:
            await pg.executemany(UPSERT_SQL, rows)
            # executemany returns "INSERT 0 N" or similar; count via rows length
            inserted_total.inc(len(rows))
            if ts_ms := rows[-1][0]:
                last_ts_gauge.set(ts_ms)
        except Exception as exc:
            error_total.inc(len(rows))
            logger.error("Batch insert error (%d rows): %s", len(rows), exc)

    if msg_ids:
        try:
            await r.xack(STREAM, GROUP, *msg_ids)
        except Exception as exc:
            logger.warning("xack error: %s", exc)


async def _update_lag(r: aioredis.Redis) -> None:
    try:
        groups = await r.xinfo_groups(STREAM)
        for g in groups:
            if g.get("name") == GROUP:
                lag_gauge.set(int(g.get("pending") or 0))
                break
    except Exception:
        pass


async def run() -> None:
    start_http_server(METRICS_PORT)
    logger.info("of_inputs_history_writer started: stream=%s group=%s port=%d", STREAM, GROUP, METRICS_PORT)

    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    pg = await asyncpg.connect(DATABASE_URL)

    await _ensure_group(r)

    lag_tick = 0
    while True:
        try:
            results = await r.xreadgroup(
                GROUP, CONSUMER,
                {STREAM: ">"},
                count=BATCH,
                block=BLOCK_MS,
            )
            if results:
                for _stream, messages in results:
                    await _process_batch(r, pg, messages)

            lag_tick += 1
            if lag_tick % 30 == 0:
                await _update_lag(r)

        except aioredis.ConnectionError as exc:
            logger.error("Redis connection error: %s — retrying in 5s", exc)
            await asyncio.sleep(5)
        except asyncpg.PostgresError as exc:
            logger.error("Postgres error: %s — reconnecting", exc)
            try:
                pg = await asyncpg.connect(DATABASE_URL)
            except Exception as e2:
                logger.error("Reconnect failed: %s", e2)
                await asyncio.sleep(5)
        except Exception as exc:
            logger.error("Unexpected error: %s", exc)
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(run())
