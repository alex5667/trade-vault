"""DecisionSnapshotWriter (A3 — legacy location; A4 writer is in python-worker/services/posttrade/).

This service consumes Redis Stream `events:decision_snapshot` (A2) and writes rows into
Postgres/Timescale `decision_snapshot` table.

Properties:
- at-least-once delivery (Redis Streams consumer group)
- idempotent DB upsert on UNIQUE(sid, ts_decision_ms)
- batched writes (reduce DB overhead)
- fail-open wrt the trading path: writer can be down without blocking signal publishing.

ENV:
  # Redis
  REDIS_URL=redis://redis-worker-1:6379/0
  DECISION_SNAPSHOT_STREAM=events:decision_snapshot
  DECISION_SNAPSHOT_CG=decision_snapshot_writer
  DECISION_SNAPSHOT_CONSUMER=<auto>
  DECISION_SNAPSHOT_XREAD_BLOCK_MS=2000
  DECISION_SNAPSHOT_BATCH_SIZE=200

  # DB
  TRADES_DB_DSN=postgresql://trading:${TRADING_PASSWORD}@postgres:5432/scanner_analytics
  TIMESCALE_DSN=<same as above, back-compat>
  DECISION_SNAPSHOT_DB_TYPE=postgres|sqlite   (sqlite only for tests/dev)
  DECISION_SNAPSHOT_DB_ENSURE_SCHEMA=0|1      (prod should apply SQL migrations)
  DECISION_SNAPSHOT_DB_UPSERT_CHUNK=500

  # Operational
  DECISION_SNAPSHOT_LOG_EVERY_N=200
  DECISION_SNAPSHOT_FAIL_SLEEP_SEC=1.0
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from services.posttrade.decision_snapshot_db import (
    PostgresDecisionSnapshotDB,
    SQLiteDecisionSnapshotDB,
    _to_int,
    _to_float,
    _to_text_array,
)

logger = logging.getLogger("decision_snapshot_writer")


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return str(v) if v is not None and str(v) != "" else default


def _env_int(name: str, default: int) -> int:
    return _to_int(os.getenv(name), default=default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name) or default)
    except Exception:
        return float(default)


def _consumer_id() -> str:
    v = os.getenv("DECISION_SNAPSHOT_CONSUMER")
    if v:
        return v
    return f"{socket.gethostname()}-{os.getpid()}"


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class DecisionSnapshotWriterConfig:
    redis_url: str = _env("REDIS_URL", "redis://redis-worker-1:6379/0")
    stream: str = _env("DECISION_SNAPSHOT_STREAM", "events:decision_snapshot")
    group: str = _env("DECISION_SNAPSHOT_CG", "decision_snapshot_writer")
    consumer: str = _consumer_id()
    block_ms: int = _env_int("DECISION_SNAPSHOT_XREAD_BLOCK_MS", 2000)
    batch_size: int = _env_int("DECISION_SNAPSHOT_BATCH_SIZE", 200)

    db_type: str = _env("DECISION_SNAPSHOT_DB_TYPE", "postgres")
    # Canonical DB DSN naming in scanner_infra:
    # - TRADES_DB_DSN (python workers, archivers)
    # - PG_DSN (go services)
    # - ANALYTICS_DB_DSN / ANALYTICS_DSN (backend)
    # - DATABASE_URL (backend)
    # Writer accepts all, with priority on TRADES_DB_DSN.
    timescale_dsn: str = _env(
        "TRADES_DB_DSN",
        _env("TIMESCALE_DSN", _env("ANALYTICS_DB_DSN", _env("ANALYTICS_DSN", _env("PG_DSN", _env("DATABASE_URL", ""))))),
    )
    ensure_schema: bool = bool(_env_int("DECISION_SNAPSHOT_DB_ENSURE_SCHEMA", 0))
    upsert_chunk: int = _env_int("DECISION_SNAPSHOT_DB_UPSERT_CHUNK", 500)

    log_every_n: int = _env_int("DECISION_SNAPSHOT_LOG_EVERY_N", 200)
    fail_sleep_sec: float = _env_float("DECISION_SNAPSHOT_FAIL_SLEEP_SEC", 1.0)


def _parse_payload(raw: Any) -> Optional[Dict[str, Any]]:
    """Parse Redis Stream entry field `payload` into dict."""
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8", errors="replace")
        except Exception:
            raw = str(raw)
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return None
    if isinstance(raw, dict):
        return raw
    return None


def _normalize_row(evt: Dict[str, Any]) -> Dict[str, Any]:
    """Map event dict to DB row dict (best-effort)."""
    # event schema is produced by services/orderflow/decision_snapshot.py (A2)
    ts_decision_ms = _to_int(evt.get("decision_ts_ms") or evt.get("ts_emit_ms") or evt.get("ts_event_ms"), 0)
    sid = str(evt.get("sid") or evt.get("signal_id") or "").strip()
    symbol = str(evt.get("symbol") or evt.get("sym") or "").strip()

    row: Dict[str, Any] = {
        "ts_decision_ms": int(ts_decision_ms),
        "sid": sid,
        "symbol": symbol or "UNKNOWN",
        "venue": str(evt.get("venue") or "binance"),
        "session": str(evt.get("session") or ""),
        "tf": str(evt.get("tf") or ""),
        "kind": str(evt.get("kind") or ""),
        "side": str(evt.get("side") or evt.get("direction") or ""),
        "direction": str(evt.get("direction") or ""),
        "decision_bid": _to_float(evt.get("decision_bid")),
        "decision_ask": _to_float(evt.get("decision_ask")),
        "decision_mid": _to_float(evt.get("decision_mid")),
        "decision_spread_bps": _to_float(evt.get("decision_spread_bps")),
        "decision_depth_bid_5": _to_float(evt.get("decision_depth_bid_5")),
        "decision_depth_ask_5": _to_float(evt.get("decision_depth_ask_5")),
        "decision_depth_bid_20": _to_float(evt.get("decision_depth_bid_20")),
        "decision_depth_ask_20": _to_float(evt.get("decision_depth_ask_20")),
        "decision_book_slope_bid": _to_float(evt.get("decision_book_slope_bid")),
        "decision_book_slope_ask": _to_float(evt.get("decision_book_slope_ask")),
        "decision_dws_bps": _to_float(evt.get("decision_dws_bps")),
        "decision_ofi_norm": _to_float(evt.get("decision_ofi_norm")),
        "decision_expected_slippage_bps": _to_float(evt.get("decision_expected_slippage_bps")),
        "decision_exec_risk_norm": _to_float(evt.get("decision_exec_risk_norm")),
        "decision_price": _to_float(evt.get("decision_price") or evt.get("decision_mid")),
        "tca_ready": bool(evt.get("tca_ready") or False),
        "book_sanity_flags": _to_text_array(evt.get("book_sanity_flags")),
        "schema_version": _to_int(evt.get("schema_version") or evt.get("schema_ver") or 1, 1),
        "producer": str(evt.get("producer") or evt.get("service") or os.getenv("SERVICE_NAME", "python-worker")),
        "ts_insert_ms": _now_ms(),
    }

    # Preserve remaining fields for audits/debugging. Keep bounded to avoid huge rows.
    extra = dict(evt)
    # Remove known columns to avoid duplication.
    for k in list(row.keys()):
        extra.pop(k, None)
    # Also remove common aliases.
    extra.pop("ts_emit_ms", None)
    extra.pop("ts_event_ms", None)
    extra.pop("signal_id", None)
    row["extra"] = extra if extra else None

    # Defensive: require minimal keys.
    if not row["sid"] or row["ts_decision_ms"] <= 0:
        # Writer can still store UNKNOWN, but missing sid breaks joins. Skip to avoid junk rows.
        raise ValueError(f"bad decision_snapshot row: sid={row['sid']!r} ts={row['ts_decision_ms']}")
    return row


class DecisionSnapshotStreamWorker:
    def __init__(self, *, cfg: DecisionSnapshotWriterConfig, redis: Any, db: Any):
        self.cfg = cfg
        self.redis = redis
        self.db = db
        self._seen = 0
        self._written = 0

    async def ensure_group(self) -> None:
        """Create consumer group if missing (MKSTREAM)."""
        try:
            await self.redis.xgroup_create(name=self.cfg.stream, groupname=self.cfg.group, id="0-0", mkstream=True)
        except RedisError as e:
            # BUSYGROUP means group exists - that's OK.
            if "BUSYGROUP" in str(e):
                return
            raise

    async def run_once(self) -> int:
        """Read one batch, write to DB, ACK on success."""
        resp = await self.redis.xreadgroup(
            groupname=self.cfg.group,
            consumername=self.cfg.consumer,
            streams={self.cfg.stream: ">"},
            count=self.cfg.batch_size,
            block=self.cfg.block_ms,
        )
        if not resp:
            return 0

        # redis-py format: [(stream, [(id, {field: value, ...}), ...])]
        total = 0
        to_ack: List[str] = []
        rows: List[Dict[str, Any]] = []

        for _, entries in resp:
            for entry_id, fields in entries:
                total += 1
                payload = _parse_payload(fields.get(b"payload") or fields.get("payload"))
                if payload is None:
                    # malformed - ack to avoid stuck pending entries
                    to_ack.append(entry_id)
                    continue
                try:
                    row = _normalize_row(payload)
                    rows.append(row)
                    to_ack.append(entry_id)
                except Exception as e:
                    # Bad row: ack to avoid poison-pill loops; keep a trace in logs.
                    logger.warning("bad decision_snapshot row (acked): id=%s err=%s", entry_id, e)
                    to_ack.append(entry_id)

        if rows:
            # Upsert in chunks for better DB behavior under spikes.
            n = 0
            for i in range(0, len(rows), self.cfg.upsert_chunk):
                chunk = rows[i:i+self.cfg.upsert_chunk]
                n += await asyncio.to_thread(self.db.upsert_decision_snapshots, chunk)
            self._written += n

        if to_ack:
            # ACK after DB commit (best-effort).
            try:
                await self.redis.xack(self.cfg.stream, self.cfg.group, *to_ack)
            except Exception as e:
                logger.warning("xack failed: %s", e)

        self._seen += total
        if self.cfg.log_every_n > 0 and (self._seen % self.cfg.log_every_n) < total:
            logger.info("decision_snapshot_writer: seen=%d written=%d", self._seen, self._written)

        return total

    async def run_forever(self) -> None:
        await self.ensure_group()
        while True:
            try:
                n = await self.run_once()
                if n == 0:
                    await asyncio.sleep(0.01)
            except Exception as e:
                logger.exception("decision_snapshot_writer loop error: %s", e)
                await asyncio.sleep(self.cfg.fail_sleep_sec)


async def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    cfg = DecisionSnapshotWriterConfig()

    if cfg.db_type.lower() == "sqlite":
        import sqlite3
        conn = sqlite3.connect(_env("DECISION_SNAPSHOT_SQLITE_PATH", ":memory:"))
        db = SQLiteDecisionSnapshotDB(conn=conn)
    else:
        if not cfg.timescale_dsn:
            raise RuntimeError("TRADES_DB_DSN (or TIMESCALE_DSN/DATABASE_URL) is required for DECISION_SNAPSHOT_DB_TYPE=postgres")
        db = PostgresDecisionSnapshotDB(dsn=cfg.timescale_dsn)

    if cfg.ensure_schema:
        # For dev/staging only; prod should apply SQL migrations explicitly.
        await asyncio.to_thread(db.ensure_schema)

    r = aioredis.from_url(cfg.redis_url, decode_responses=False)
    worker = DecisionSnapshotStreamWorker(cfg=cfg, redis=r, db=db)
    await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
