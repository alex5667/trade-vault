"""
Async in-memory batch writer for PostgreSQL.

Buffers rows in a thread-safe queue and flushes them to the DB via
executemany on a timer and/or when the batch_size threshold is exceeded.

Benefits over per-row INSERTs:
  - One round-trip per batch instead of N.
  - Uses a ThreadedConnectionPool → no new TCP connection per write.
  - Zero-latency enqueue path for hot signal/event paths.

Usage
-----
    from services.db_batch_writer import AsyncBatchWriter

    writer = AsyncBatchWriter(
        table="execution_order_events"
        columns=["sid", "symbol", "event_type", "event_ts_ms", "payload_jsonb"]
        dsn=os.getenv("EXECUTION_JOURNAL_DSN", "")
        batch_size=int(os.getenv("JOURNAL_BATCH_SIZE", "200"))
        flush_interval_s=float(os.getenv("JOURNAL_FLUSH_INTERVAL_S", "2.0"))
        # For tables with ON CONFLICT … DO NOTHING / DO UPDATE:
        on_conflict_sql="ON CONFLICT DO NOTHING"
    )
    writer.start()

    # Later — non-blocking:
    writer.enqueue({"sid": sid, "symbol": sym, ...})

    # On shutdown (also registered with atexit automatically):
    writer.shutdown()

ENV
---
  DB_BATCH_WRITER_LOG_LEVEL   DEBUG | INFO (default INFO)
"""
from __future__ import annotations

import atexit
import logging
import os
import queue
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence

try:
    from prometheus_client import Counter, Histogram, REGISTRY

    def _metric(factory, name, *args, **kwargs):
        try:
            return factory(name, *args, **kwargs)
        except ValueError:
            return (REGISTRY._names_to_collectors or {}).get(name)

    _FLUSH_FAIL = _metric(Counter
        "db_batch_writer_flush_fail_total"
        "Flush failures per table"
        ["table"]
    )
    _FLUSH_LATENCY = _metric(Histogram
        "db_batch_writer_flush_latency_seconds"
        "Time to execute one batch flush"
        ["table"]
        buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
    )
    _ENQUEUE_TOTAL = _metric(Counter
        "db_batch_writer_enqueue_total"
        "Rows enqueued per table"
        ["table"]
    )
    _ROWS_DROPPED = _metric(Counter
        "db_batch_writer_rows_dropped_total"
        "Rows permanently dropped after all retries exhausted"
        ["table"]
    )
    _DLQ_WRITE = _metric(Counter
        "db_batch_writer_dlq_write_total"
        "Rows written to durable DLQ after exhausted retries"
        ["table"]
    )
    _DLQ_WRITE_FAIL = _metric(Counter
        "db_batch_writer_dlq_write_fail_total"
        "DLQ write failures (rows may be permanently lost)"
        ["table"]
    )
except Exception:  # pragma: no cover — prometheus not installed
    _FLUSH_FAIL = _FLUSH_LATENCY = _ENQUEUE_TOTAL = None
    _ROWS_DROPPED = _DLQ_WRITE = _DLQ_WRITE_FAIL = None


_LOG = logging.getLogger("db_batch_writer")


class AsyncBatchWriter:
    """Thread-safe async batch writer.

    Parameters
    ----------
    table           Target table name (used verbatim in INSERT).
    columns         Ordered list of column names.
    dsn             PostgreSQL connection string.
    batch_size      Flush if queue reaches this size (default 200).
    flush_interval_s Flush every N seconds even if batch_size not reached (default 2.0).
    on_conflict_sql Appended after VALUES clause, e.g. "ON CONFLICT DO NOTHING".
    max_retries     On transient error retry up to N times with back-off.
    pool_minconn    Min connections in ThreadedConnectionPool.
    pool_maxconn    Max connections in ThreadedConnectionPool.
    extra_adapter   Optional callable(row_dict) -> row_dict applied before enqueue
                    (useful for JSON serialisation).
    """

    def __init__(
        self
        table: str
        columns: Sequence[str]
        dsn: str
        batch_size: int = 200
        flush_interval_s: float = 2.0
        on_conflict_sql: str = "ON CONFLICT DO NOTHING"
        max_retries: int = 3
        pool_minconn: int = 1
        pool_maxconn: int = 5
        extra_adapter: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None
    ) -> None:
        self.table = table
        self.columns: List[str] = list(columns)
        self.dsn = dsn
        self.batch_size = max(1, batch_size)
        self.flush_interval_s = max(0.1, flush_interval_s)
        self.on_conflict_sql = on_conflict_sql
        self.max_retries = max_retries
        self.pool_minconn = pool_minconn
        self.pool_maxconn = pool_maxconn
        self.extra_adapter = extra_adapter

        # Internal state
        self._queue: queue.Queue[Optional[Dict[str, Any]]] = queue.Queue()
        self._pool = None  # lazy init on first use
        self._pool_lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._shutdown_event = threading.Event()
        self._started = False

        # Precompute SQL (columns fixed at construction time)
        placeholders = ", ".join(f"%({c})s" for c in self.columns)
        col_list = ", ".join(self.columns)
        self._sql = (
            f"INSERT INTO {self.table} ({col_list}) "
            f"VALUES ({placeholders}) "
            f"{self.on_conflict_sql}"
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> "AsyncBatchWriter":
        """Start the background flush thread. Idempotent."""
        if self._started:
            return self
        self._thread = threading.Thread(
            target=self._run
            name=f"db-batch-{self.table}"
            daemon=True
        )
        self._thread.start()
        self._started = True
        atexit.register(self.shutdown)
        _LOG.info("[AsyncBatchWriter] started for table=%s batch_size=%d interval=%.1fs"
                  self.table, self.batch_size, self.flush_interval_s)
        return self

    def enqueue(self, row: Dict[str, Any]) -> None:
        """Non-blocking. Add a row dict to the queue.

        Triggers an immediate flush if queue size >= batch_size.
        """
        if not self._started:
            # Safety: if called before start(), do a direct synchronous insert.
            self._flush_direct([row])
            return
        if self.extra_adapter is not None:
            row = self.extra_adapter(row)
        self._queue.put_nowait(row)
        if _ENQUEUE_TOTAL is not None:
            _ENQUEUE_TOTAL.labels(table=self.table).inc()

    def flush_now(self) -> int:
        """Drain queue and flush synchronously. Returns number of rows flushed."""
        batch: List[Dict[str, Any]] = []
        while True:
            try:
                item = self._queue.get_nowait()
                if item is None:
                    break
                batch.append(item)
            except queue.Empty:
                break
        if batch:
            self._flush_direct(batch)
        return len(batch)

    def shutdown(self) -> None:
        """Flush pending rows and stop the background thread."""
        if not self._started:
            return
        _LOG.info("[AsyncBatchWriter] shutdown for table=%s", self.table)
        self._shutdown_event.set()
        self._queue.put_nowait(None)  # sentinel
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        self.flush_now()  # drain any remaining
        self._close_pool()
        self._started = False

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get_pool(self):
        """Lazy-init ThreadedConnectionPool."""
        if self._pool is not None:
            return self._pool
        with self._pool_lock:
            if self._pool is not None:
                return self._pool
            if not self.dsn:
                return None
            try:
                import psycopg2
                from psycopg2 import pool as pgpool
                self._pool = pgpool.ThreadedConnectionPool(
                    self.pool_minconn
                    self.pool_maxconn
                    dsn=self.dsn
                )
                _LOG.info("[AsyncBatchWriter] pool created for table=%s", self.table)
            except Exception as exc:
                _LOG.warning("[AsyncBatchWriter] pool init failed: %s", exc)
                self._pool = None
        return self._pool

    def _close_pool(self) -> None:
        with self._pool_lock:
            if self._pool is not None:
                try:
                    self._pool.closeall()
                except Exception:
                    pass
                self._pool = None

    def _run(self) -> None:
        """Background loop: collect rows and flush on interval or size."""
        last_flush = time.monotonic()
        batch: List[Dict[str, Any]] = []

        while not self._shutdown_event.is_set():
            deadline = last_flush + self.flush_interval_s
            timeout = max(0.0, deadline - time.monotonic())

            try:
                item = self._queue.get(timeout=timeout)
                if item is None:  # sentinel
                    break
                batch.append(item)
            except queue.Empty:
                pass

            # Drain any additional items already in queue (up to batch_size)
            while len(batch) < self.batch_size:
                try:
                    item = self._queue.get_nowait()
                    if item is None:
                        break
                    batch.append(item)
                except queue.Empty:
                    break

            should_flush = (
                len(batch) >= self.batch_size
                or time.monotonic() >= deadline
            )
            if should_flush and batch:
                self._flush_direct(batch)
                batch = []
                last_flush = time.monotonic()

        # Final drain
        if batch:
            self._flush_direct(batch)
        self.flush_now()

    def _flush_direct(self, batch: List[Dict[str, Any]]) -> None:
        """Flush a batch to DB with retry logic."""
        if not batch:
            return
        t0 = time.monotonic()
        last_exc: Optional[Exception] = None

        for attempt in range(1, self.max_retries + 1):
            pool = self._get_pool()
            conn = None
            try:
                if pool is None:
                    # Fallback: single direct connection
                    import psycopg2
                    conn = psycopg2.connect(self.dsn)
                    own_conn = True
                else:
                    conn = pool.getconn()
                    own_conn = False

                with conn.cursor() as cur:
                    cur.executemany(self._sql, batch)
                conn.commit()

                elapsed = time.monotonic() - t0
                _LOG.debug("[AsyncBatchWriter] flushed %d rows to %s in %.3fs"
                           len(batch), self.table, elapsed)
                if _FLUSH_LATENCY is not None:
                    _FLUSH_LATENCY.labels(table=self.table).observe(elapsed)

                if pool and conn and not own_conn:
                    pool.putconn(conn)
                elif own_conn and conn:
                    conn.close()
                return  # success

            except Exception as exc:
                last_exc = exc
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    try:
                        if pool and not own_conn:  # type: ignore[possibly-undefined]
                            pool.putconn(conn, close=True)
                        else:
                            conn.close()
                    except Exception:
                        pass

                wait = min(0.5 * (2 ** (attempt - 1)), 8.0)
                _LOG.warning("[AsyncBatchWriter] flush attempt %d/%d failed: %s — retry in %.1fs"
                             attempt, self.max_retries, exc, wait)
                time.sleep(wait)

        # All retries exhausted — write to durable DLQ before dropping
        _LOG.error("[AsyncBatchWriter] all %d retries failed for table=%s, dropping %d rows: %s"
                   self.max_retries, self.table, len(batch), last_exc)
        if _FLUSH_FAIL is not None:
            _FLUSH_FAIL.labels(table=self.table).inc()
        if _ROWS_DROPPED is not None:
            _ROWS_DROPPED.labels(table=self.table).inc(len(batch))
        self._write_dlq(batch, last_exc)

    def _write_dlq(self, batch: List[Dict[str, Any]], error: Optional[Exception]) -> None:
        """Persist dropped batch to a durable DLQ so rows can be replayed manually."""
        import json as _json
        dlq_dir = os.getenv("DB_BATCH_DLQ_DIR", "/var/lib/scanner/db_batch_dlq")
        dlq_stream = os.getenv("DB_BATCH_DLQ_STREAM", "db:batch:dlq")
        payload = {
            "table": self.table
            "columns": self.columns
            "rows": batch
            "error": str(error)[:1000] if error else ""
            "ts_ms": int(time.time() * 1000)
        }
        written = False
        # Attempt 1: Redis Stream DLQ (preferred — survives process restart)
        try:
            import redis as _redis
            dsn = self.dsn  # reuse same host is intentional only for logging; real DLQ is independent
            dlq_redis_url = os.getenv("DB_BATCH_DLQ_REDIS_URL", "")
            if dlq_redis_url:
                r = _redis.from_url(dlq_redis_url, socket_connect_timeout=2, socket_timeout=2)
                r.xadd(
                    f"{dlq_stream}:{self.table}"
                    {"data": _json.dumps(payload, default=str)}
                    maxlen=50000
                    approximate=True
                )
                written = True
                if _DLQ_WRITE is not None:
                    _DLQ_WRITE.labels(table=self.table).inc(len(batch))
        except Exception as re:
            _LOG.debug("[AsyncBatchWriter] Redis DLQ write failed: %s", re)
        # Attempt 2: local NDJSON WAL file
        if not written:
            try:
                os.makedirs(dlq_dir, exist_ok=True)
                dlq_path = os.path.join(dlq_dir, f"{self.table}.ndjson")
                with open(dlq_path, "a", encoding="utf-8") as fh:
                    fh.write(_json.dumps(payload, default=str) + "\n")
                written = True
                if _DLQ_WRITE is not None:
                    _DLQ_WRITE.labels(table=self.table).inc(len(batch))
            except Exception as fe:
                _LOG.error("[AsyncBatchWriter] DLQ file write also failed: %s", fe)
                if _DLQ_WRITE_FAIL is not None:
                    _DLQ_WRITE_FAIL.labels(table=self.table).inc(len(batch))


# ---------------------------------------------------------------------------
# Module-level convenience registry
# ---------------------------------------------------------------------------

_writers: Dict[str, AsyncBatchWriter] = {}
_writers_lock = threading.Lock()


def get_or_create_writer(
    table: str
    columns: Sequence[str]
    dsn: str
    **kwargs: Any
) -> AsyncBatchWriter:
    """Get or create a shared AsyncBatchWriter for a given table.

    Thread-safe. Calling multiple times with the same table name returns
    the same instance (ignoring extra kwargs after first creation).
    """
    with _writers_lock:
        if table not in _writers:
            writer = AsyncBatchWriter(table, columns, dsn, **kwargs)
            writer.start()
            _writers[table] = writer
        return _writers[table]


def shutdown_all() -> None:
    """Flush and close all registered writers (called on process exit)."""
    with _writers_lock:
        for w in _writers.values():
            try:
                w.shutdown()
            except Exception:
                pass
        _writers.clear()


atexit.register(shutdown_all)
