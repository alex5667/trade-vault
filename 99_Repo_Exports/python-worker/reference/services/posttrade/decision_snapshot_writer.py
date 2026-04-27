"""DecisionSnapshotWriter (A3/A4)

Consumes Redis Stream `events:decision_snapshot` (A2) and writes rows into
TimescaleDB/Postgres table `decision_snapshot`.

Operational properties:
- at-least-once delivery via Redis Streams consumer group
- idempotent DB writes via UNIQUE(sid, ts_decision_ms)
- batched writes (reduce DB overhead)
- fail-open relative to the trading path: writer can be down without blocking EMIT

A4 additions:
- DLQ stream for malformed payloads / invalid rows
- Pending entries reclaim (XAUTOCLAIM/XCLAIM) to avoid stuck PEL
- Prometheus metrics for SRE (written_total/db_fail_total/redis_lag histogram)

A5 additions:
- pending_count Gauge (PEL size, polled via XPENDING/XINFO GROUPS)
- claim_fail_total Counter (XAUTOCLAIM/XCLAIM errors)
- dlq_by_reason_total Counter with label 'reason' (top DLQ reasons for Grafana)

ENV (main):
  # Redis
  REDIS_URL=redis://redis-worker-1:6379/0
  DECISION_SNAPSHOT_STREAM=events:decision_snapshot
  DECISION_SNAPSHOT_CG=decision_snapshot_writer
  DECISION_SNAPSHOT_CONSUMER=<auto>
  DECISION_SNAPSHOT_XREAD_BLOCK_MS=2000
  DECISION_SNAPSHOT_BATCH_SIZE=200

  # DB
  TIMESCALE_DSN=postgresql://user:pass@host:5432/dbname
  DECISION_SNAPSHOT_DB_TYPE=postgres|sqlite   (sqlite only for tests/dev)
  DECISION_SNAPSHOT_DB_ENSURE_SCHEMA=0|1      (prod should apply SQL migrations)
  DECISION_SNAPSHOT_DB_UPSERT_CHUNK=500

  # DLQ (bad payloads only; DB failures are retried)
  DECISION_SNAPSHOT_DLQ_ENABLE=1
  DECISION_SNAPSHOT_DLQ_STREAM=stream:decision_snapshot:dlq
  DECISION_SNAPSHOT_DLQ_MAXLEN=200000
  DECISION_SNAPSHOT_DLQ_PAYLOAD_MAX_BYTES=8192

  # Pending reclaim (PEL)
  DECISION_SNAPSHOT_PEL_ENABLE=1
  DECISION_SNAPSHOT_PEL_RECOVERY_EVERY_SEC=30
  DECISION_SNAPSHOT_PEL_MIN_IDLE_MS=15000
  DECISION_SNAPSHOT_PEL_CLAIM_COUNT=100
  DECISION_SNAPSHOT_PEL_START_ID=0-0
  DECISION_SNAPSHOT_PEL_CLAIM_MAX_ITERS=2

  # PEL observability (A5)
  DECISION_SNAPSHOT_PENDING_POLL_EVERY_SEC=15   # how often to refresh pending_count gauge

  # Metrics
  DECISION_SNAPSHOT_WRITER_METRICS_ENABLE=1
  DECISION_SNAPSHOT_WRITER_METRICS_PORT=9825

  # Operational
  DECISION_SNAPSHOT_LOG_EVERY_N=200
  DECISION_SNAPSHOT_FAIL_SLEEP_SEC=1.0
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
from utils.task_manager import safe_create_task

import json
import logging
import os
import socket
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    import redis.asyncio as aioredis  # type: ignore
    from redis.exceptions import RedisError  # type: ignore
except Exception:  # pragma: no cover
    # Unit/integration tests run with FakeRedis stubs and do not require redis-py.
    aioredis = None  # type: ignore
    class RedisError(Exception):
        pass

from services.posttrade.decision_snapshot_db import (
    PostgresDecisionSnapshotDB,
    SQLiteDecisionSnapshotDB,
    _to_int,
    _to_float,
    _to_text_array,
)
from services.posttrade.decision_snapshot_writer_metrics import build_metrics, start_metrics_server

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


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return bool(default)
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _consumer_id() -> str:
    v = os.getenv("DECISION_SNAPSHOT_CONSUMER")
    if v:
        return v
    return f"{socket.gethostname()}-{os.getpid()}"


def _now_ms() -> int:
    return get_ny_time_millis()


@dataclass
class DecisionSnapshotWriterConfig:
    # Redis stream
    redis_url: str = _env("REDIS_URL", "redis://redis-worker-1:6379/0")
    stream: str = _env("DECISION_SNAPSHOT_STREAM", "events:decision_snapshot")
    group: str = _env("DECISION_SNAPSHOT_CG", "decision_snapshot_writer")
    consumer: str = _consumer_id()
    block_ms: int = _env_int("DECISION_SNAPSHOT_XREAD_BLOCK_MS", 2000)
    batch_size: int = _env_int("DECISION_SNAPSHOT_BATCH_SIZE", 200)

    # DB
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

    # DLQ
    dlq_enable: bool = _env_bool("DECISION_SNAPSHOT_DLQ_ENABLE", True)
    dlq_stream: str = _env("DECISION_SNAPSHOT_DLQ_STREAM", "stream:decision_snapshot:dlq")
    dlq_maxlen: int = _env_int("DECISION_SNAPSHOT_DLQ_MAXLEN", 200000)
    dlq_payload_max_bytes: int = _env_int("DECISION_SNAPSHOT_DLQ_PAYLOAD_MAX_BYTES", 8192)

    # PEL reclaim
    pel_enable: bool = _env_bool("DECISION_SNAPSHOT_PEL_ENABLE", True)
    pel_every_sec: int = _env_int("DECISION_SNAPSHOT_PEL_RECOVERY_EVERY_SEC", 30)
    pel_min_idle_ms: int = _env_int("DECISION_SNAPSHOT_PEL_MIN_IDLE_MS", 15000)
    pel_claim_count: int = _env_int("DECISION_SNAPSHOT_PEL_CLAIM_COUNT", 100)
    pel_start_id: str = _env("DECISION_SNAPSHOT_PEL_START_ID", "0-0")
    pel_max_iters: int = _env_int("DECISION_SNAPSHOT_PEL_CLAIM_MAX_ITERS", 2)

    # PEL observability (A5): how often to probe XPENDING and update pending_count gauge.
    pending_poll_every_sec: int = _env_int("DECISION_SNAPSHOT_PENDING_POLL_EVERY_SEC", 15)

    # Operational
    log_every_n: int = _env_int("DECISION_SNAPSHOT_LOG_EVERY_N", 200)
    fail_sleep_sec: float = _env_float("DECISION_SNAPSHOT_FAIL_SLEEP_SEC", 1.0)


def _decode_bytes(v: Any) -> Any:
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode("utf-8", errors="replace")
        except Exception:
            return str(v)
    return v


def _parse_payload(raw: Any) -> Optional[Dict[str, Any]]:
    """Parse Redis Stream entry field `payload` into dict."""
    if raw is None:
        return None
    raw = _decode_bytes(raw)
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

    # Preserve remaining fields for audits/debugging.
    extra = dict(evt)
    for k in list(row.keys()):
        extra.pop(k, None)
    extra.pop("ts_emit_ms", None)
    extra.pop("ts_event_ms", None)
    extra.pop("signal_id", None)
    row["extra"] = extra if extra else None

    # Defensive: require minimal keys.
    if not row["sid"] or row["ts_decision_ms"] <= 0:
        raise ValueError(f"bad decision_snapshot row: sid={row['sid']!r} ts={row['ts_decision_ms']}")
    return row


class DecisionSnapshotStreamWorker:
    def __init__(self, *, cfg: DecisionSnapshotWriterConfig, redis: Any, db: Any):
        self.cfg = cfg
        self.redis = redis
        self.db = db
        self._seen = 0
        self._written = 0
        self._metrics = build_metrics()
        # SQLite connections are thread-bound by default; avoid to_thread in tests/dev.
        self._db_threadsafe = not isinstance(db, SQLiteDecisionSnapshotDB)
        self._pel_task: Optional[asyncio.Task] = None
        self._pending_task: Optional[asyncio.Task] = None  # A5: pending gauge poll loop

    async def ensure_group(self) -> None:
        """Create consumer group if missing (MKSTREAM)."""
        try:
            await self.redis.xgroup_create(name=self.cfg.stream, groupname=self.cfg.group, id="0-0", mkstream=True)
        except RedisError as e:
            if "BUSYGROUP" in str(e):
                return
            raise

    def _truncate_bytes(self, s: str) -> str:
        try:
            b = s.encode("utf-8", errors="replace")
        except Exception:
            return s[: self.cfg.dlq_payload_max_bytes]
        if len(b) <= self.cfg.dlq_payload_max_bytes:
            return s
        return b[: self.cfg.dlq_payload_max_bytes].decode("utf-8", errors="replace")

    async def _dlq(self, *, entry_id: str, fields: Any, reason: str, err: str) -> None:
        """Send a bad entry to the DLQ stream and increment the DLQ counter."""
        if not self.cfg.dlq_enable:
            return
        try:
            raw = fields.get(b"payload") if isinstance(fields, dict) else None
            raw = raw if raw is not None else (fields.get("payload") if isinstance(fields, dict) else None)
            raw_s = _decode_bytes(raw)
            if not isinstance(raw_s, str):
                raw_s = json.dumps(raw_s, ensure_ascii=False, separators=(",", ":")) if raw_s is not None else ""
            raw_s = self._truncate_bytes(raw_s)

            await self.redis.xadd(
                self.cfg.dlq_stream,
                fields={
                    "ts_ms": str(_now_ms()),
                    "stream": self.cfg.stream,
                    "group": self.cfg.group,
                    "consumer": self.cfg.consumer,
                    "entry_id": str(entry_id),
                    "reason": str(reason),
                    "error": self._truncate_bytes(str(err)),
                    "payload": raw_s,
                },
                maxlen=self.cfg.dlq_maxlen,
            )
            self._metrics.dlq_total.inc()
            # A5: Low-cardinality breakdown (top reasons in Grafana).
            try:
                self._metrics.dlq_by_reason_total.labels(reason=str(reason)).inc()
            except Exception:
                # No-op metrics backend or label init race; never break DLQ path.
                pass
        except Exception as e:
            logger.warning("dlq publish failed: %s", e)

    async def _ack(self, *ids: str) -> None:
        if not ids:
            return
        try:
            await self.redis.xack(self.cfg.stream, self.cfg.group, *ids)
        except Exception as e:
            logger.warning("xack failed: %s", e)

    def _observe_lag(self, payload: Dict[str, Any]) -> None:
        """Observe end-to-end lag from decision_ts_ms to now for Prometheus histogram."""
        try:
            ts = _to_int(payload.get("decision_ts_ms") or payload.get("ts_emit_ms") or payload.get("ts_event_ms"), 0)
            if ts <= 0:
                return
            lag = _now_ms() - int(ts)
            if lag < 0:
                lag = 0
            self._metrics.redis_lag_ms.observe(float(lag))
        except Exception:
            return

    async def _db_upsert(self, rows: Sequence[Dict[str, Any]]) -> int:
        """Upsert rows to DB.

        Postgres adapter is safe to execute in a thread.
        SQLite adapter (tests/dev) is usually thread-bound; run in-loop.
        """
        if not rows:
            return 0
        if self._db_threadsafe:
            return await asyncio.to_thread(self.db.upsert_decision_snapshots, rows)
        return int(self.db.upsert_decision_snapshots(rows))

    async def _process_entries(self, entries: Sequence[Tuple[str, Any]], *, allow_db: bool) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Parse/normalize entries into rows.

        Returns: (rows_to_write, ack_ids_for_immediate_ack)

        - Bad payloads/rows are DLQ'ed and returned in ack_ids.
        - Good rows are returned in rows_to_write (ACK after DB commit).
        """
        rows: List[Dict[str, Any]] = []
        ack_ids: List[str] = []

        for entry_id, fields in entries:
            self._metrics.processed_total.inc()

            payload = _parse_payload(fields.get(b"payload") or fields.get("payload"))
            if payload is None:
                await self._dlq(entry_id=str(entry_id), fields=fields, reason="bad_payload_json", err="payload is not valid JSON")
                ack_ids.append(str(entry_id))
                continue

            self._observe_lag(payload)

            try:
                row = _normalize_row(payload)
                rows.append(row)
            except Exception as e:
                await self._dlq(entry_id=str(entry_id), fields=fields, reason="bad_payload_row", err=str(e))
                ack_ids.append(str(entry_id))

        return rows, ack_ids

    async def run_once(self) -> int:
        """Read one batch from ">", write to DB, ACK on success.

        DB failures are retried (messages remain pending).
        Bad payloads are sent to DLQ and ACKed to avoid poison-pill loops.
        """
        resp = await self.redis.xreadgroup(
            groupname=self.cfg.group,
            consumername=self.cfg.consumer,
            streams={self.cfg.stream: ">"},
            count=self.cfg.batch_size,
            block=self.cfg.block_ms,
        )
        if not resp:
            return 0

        total = 0
        all_entries: List[Tuple[str, Any]] = []
        for _, entries in resp:
            for entry_id, fields in entries:
                total += 1
                all_entries.append((str(entry_id), fields))

        if not all_entries:
            return 0

        rows, ack_bad = await self._process_entries(all_entries, allow_db=True)
        if ack_bad:
            await self._ack(*ack_bad)

        if not rows:
            self._seen += total
            return total

        # Write rows (idempotent upsert). ACK only after commit.
        written = 0
        try:
            for i in range(0, len(rows), self.cfg.upsert_chunk):
                chunk = rows[i : i + self.cfg.upsert_chunk]
                written += await self._db_upsert(chunk)
        except Exception as e:
            self._metrics.db_fail_total.inc()
            logger.exception("db upsert failed (will retry pending entries): %s", e)
            # Do not ACK good entries — they will be retried.
            return 0

        self._metrics.written_total.inc(float(written))
        self._written += written

        # ACK all entry ids corresponding to successful db write.
        # We ACK everything except those already acked as bad.
        good_ids = [eid for (eid, _) in all_entries if eid not in set(ack_bad)]
        await self._ack(*good_ids)

        self._seen += total
        if self.cfg.log_every_n > 0 and (self._seen % self.cfg.log_every_n) < total:
            logger.info("decision_snapshot_writer: seen=%d written=%d", self._seen, self._written)

        return total

    async def recover_pending_once(self) -> int:
        """Claim and process pending entries (PEL recovery).

        Uses XAUTOCLAIM when available (Redis 6.2+). Falls back to XPENDING+XCLAIM.

        Returns number of entries claimed.
        """
        if not self.cfg.pel_enable:
            return 0

        claimed_total = 0
        cursor = self.cfg.pel_start_id
        for _ in range(max(1, self.cfg.pel_max_iters)):
            # Prefer XAUTOCLAIM (fast path, Redis 6.2+)
            try:
                xautoclaim = getattr(self.redis, "xautoclaim", None)
                if callable(xautoclaim):
                    res = await xautoclaim(
                        name=self.cfg.stream,
                        groupname=self.cfg.group,
                        consumername=self.cfg.consumer,
                        min_idle_time=self.cfg.pel_min_idle_ms,
                        start_id=cursor,
                        count=self.cfg.pel_claim_count,
                    )
                    if not res:
                        break
                    # redis-py: (next_id, [(id, fields), ...], [deleted_ids]?)
                    next_id = res[0]
                    msgs = res[1] if len(res) > 1 else []
                    cursor = str(next_id)
                    if not msgs:
                        break

                    self._metrics.reclaim_total.inc(float(len(msgs)))
                    claimed_total += len(msgs)

                    rows, ack_bad = await self._process_entries([(str(i), f) for (i, f) in msgs], allow_db=True)
                    if ack_bad:
                        await self._ack(*ack_bad)

                    if rows:
                        written = 0
                        try:
                            for i in range(0, len(rows), self.cfg.upsert_chunk):
                                chunk = rows[i : i + self.cfg.upsert_chunk]
                                written += await self._db_upsert(chunk)
                        except Exception as e:
                            self._metrics.db_fail_total.inc()
                            logger.exception("db upsert failed during reclaim (will keep pending): %s", e)
                            # Do not ACK good rows.
                            return claimed_total

                        self._metrics.written_total.inc(float(written))

                        good_ids = [str(i) for (i, _) in msgs if str(i) not in set(ack_bad)]
                        await self._ack(*good_ids)

                    # Continue to process more pending entries.
                    continue

            except Exception as e:
                # A5: record claim failures for SRE alerting
                try:
                    self._metrics.claim_fail_total.inc()
                except Exception:
                    pass
                logger.debug("xautoclaim not usable: %s", e)

            # Fallback: XPENDING RANGE + XCLAIM (Redis < 6.2)
            try:
                xpending_range = getattr(self.redis, "xpending_range", None)
                xclaim = getattr(self.redis, "xclaim", None)
                if not (callable(xpending_range) and callable(xclaim)):
                    break

                pending = await xpending_range(
                    self.cfg.stream,
                    self.cfg.group,
                    min=cursor,
                    max="+",
                    count=self.cfg.pel_claim_count,
                    idle=self.cfg.pel_min_idle_ms,
                )
                if not pending:
                    break

                ids = [p["message_id"] if isinstance(p, dict) and "message_id" in p else p[0] for p in pending]
                claimed = await xclaim(
                    self.cfg.stream,
                    self.cfg.group,
                    self.cfg.consumer,
                    min_idle_time=self.cfg.pel_min_idle_ms,
                    message_ids=ids,
                )
                if not claimed:
                    break

                self._metrics.reclaim_total.inc(float(len(claimed)))
                claimed_total += len(claimed)

                rows, ack_bad = await self._process_entries([(str(i), f) for (i, f) in claimed], allow_db=True)
                if ack_bad:
                    await self._ack(*ack_bad)

                if rows:
                    written = 0
                    try:
                        for i in range(0, len(rows), self.cfg.upsert_chunk):
                            chunk = rows[i : i + self.cfg.upsert_chunk]
                            written += await self._db_upsert(chunk)
                    except Exception as e:
                        self._metrics.db_fail_total.inc()
                        logger.exception("db upsert failed during reclaim (will keep pending): %s", e)
                        return claimed_total

                    self._metrics.written_total.inc(float(written))

                    good_ids = [str(i) for (i, _) in claimed if str(i) not in set(ack_bad)]
                    await self._ack(*good_ids)

                # Advance cursor for next iteration
                cursor = str(ids[-1]) if ids else cursor

            except Exception as e:
                # A5: record fallback claim failures for SRE alerting
                try:
                    self._metrics.claim_fail_total.inc()
                except Exception:
                    pass
                logger.warning("pel reclaim fallback failed: %s", e)
                break

        return claimed_total

    async def _fetch_pending_count(self) -> Optional[int]:
        """Return current pending count for the consumer group (best-effort).

        We try multiple Redis commands because redis-py APIs differ by version:
        - XPENDING <stream> <group> (summary)
        - XINFO GROUPS <stream>

        This value is used for:
        - SRE alerting (PEL growing)
        - operational debugging (writer down / DB stalls)
        """
        # 1) XPENDING summary (preferred)
        try:
            xpending = getattr(self.redis, "xpending", None)
            if callable(xpending):
                res = await xpending(self.cfg.stream, self.cfg.group)
                # redis-py may return dict or tuple
                if isinstance(res, dict) and "pending" in res:
                    return int(res.get("pending") or 0)
                if isinstance(res, (list, tuple)) and len(res) >= 1:
                    return int(res[0] or 0)
        except Exception:
            pass

        # 2) XINFO GROUPS fallback
        try:
            xinfo_groups = getattr(self.redis, "xinfo_groups", None)
            if callable(xinfo_groups):
                groups = await xinfo_groups(self.cfg.stream)
                for g in groups or []:
                    # redis-py may decode bytes or keep as bytes
                    name = g.get("name") if isinstance(g, dict) else None
                    name = _decode_bytes(name)
                    if str(name) == str(self.cfg.group):
                        return int(g.get("pending") or 0)
        except Exception:
            pass

        return None

    async def _pending_poll_once(self) -> Optional[int]:
        """Poll XPENDING and update pending_count gauge. Returns count or None."""
        n = await self._fetch_pending_count()
        if n is None:
            return None
        try:
            self._metrics.pending_count.set(float(n))
        except Exception:
            pass
        return int(n)

    async def _pending_poll_loop(self) -> None:
        """Background loop: keep pending gauge reasonably fresh for alerting.

        Fail-open: any errors are logged at DEBUG level and never break the writer.
        """
        while True:
            try:
                await self._pending_poll_once()
            except Exception as e:
                logger.debug("pending poll error: %s", e)
            await asyncio.sleep(max(5, int(self.cfg.pending_poll_every_sec)))

    async def _pel_recover_loop(self) -> None:
        """Background coroutine: periodically reclaim stuck pending entries."""
        while True:
            try:
                n = await self.recover_pending_once()
                if n > 0:
                    logger.info("pel reclaimed=%d", n)
            except Exception as e:
                logger.warning("pel recover loop error: %s", e)
            await asyncio.sleep(max(1, int(self.cfg.pel_every_sec)))

    async def run_forever(self) -> None:
        await self.ensure_group()

        # Start PEL recovery loop (does not block the main stream consumption).
        if self.cfg.pel_enable:
            self._pel_task = safe_create_task(self._pel_recover_loop())

        # A5: Pending gauge poll loop (helps detect PEL growth even when lag histograms look OK).
        self._pending_task = safe_create_task(self._pending_poll_loop())

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

    # Metrics first: so healthcheck sees /metrics early even before Redis/DB are ready.
    start_metrics_server()

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
        await asyncio.to_thread(db.ensure_schema)

    if aioredis is None:
        raise RuntimeError("redis (redis-py) is required to run decision_snapshot_writer")
    r = aioredis.from_url(cfg.redis_url, encoding=None, decode_responses=False)
    worker = DecisionSnapshotStreamWorker(cfg=cfg, redis=r, db=db)
    await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
