from __future__ import annotations

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
        table="execution_order_events",
        columns=["sid", "symbol", "event_type", "event_ts_ms", "payload_jsonb"],
        dsn=os.getenv("EXECUTION_JOURNAL_DSN", ""),
        batch_size=int(os.getenv("JOURNAL_BATCH_SIZE", "200")),
        flush_interval_s=float(os.getenv("JOURNAL_FLUSH_INTERVAL_S", "2.0")),
        # For tables with ON CONFLICT … DO NOTHING / DO UPDATE:
        on_conflict_sql="ON CONFLICT DO NOTHING",
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

import atexit
import logging
import queue
import threading
import time
from collections.abc import Callable, Sequence
from typing import Any

try:
    from prometheus_client import REGISTRY, Counter, Histogram

    def _metric(factory, name, *args, **kwargs):
        try:
            return factory(name, *args, **kwargs)
        except ValueError:
            return (REGISTRY._names_to_collectors or {}).get(name)

    _FLUSH_FAIL = _metric(Counter,
        "db_batch_writer_flush_fail_total",
        "Flush failures per table",
        ["table"],
    )
    _FLUSH_LATENCY = _metric(Histogram,
        "db_batch_writer_flush_latency_seconds",
        "Time to execute one batch flush",
        ["table"],
        buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
    )
    _ENQUEUE_TOTAL = _metric(Counter,
        "db_batch_writer_enqueue_total",
        "Rows enqueued per table",
        ["table"],
    )
except Exception:  # pragma: no cover — prometheus not installed
    _FLUSH_FAIL = _FLUSH_LATENCY = _ENQUEUE_TOTAL = None


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
        self,
        table: str,
        columns: Sequence[str],
        dsn: str,
        batch_size: int = 200,
        flush_interval_s: float = 2.0,
        on_conflict_sql: str = "ON CONFLICT DO NOTHING",
        max_retries: int = 3,
        pool_minconn: int = 1,
        pool_maxconn: int = 5,
        extra_adapter: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    ) -> None:
        self.table = table
        self.columns: list[str] = list(columns)
        self.dsn = dsn
        self.batch_size = max(1, batch_size)
        self.flush_interval_s = max(0.1, flush_interval_s)
        self.on_conflict_sql = on_conflict_sql
        self.max_retries = max_retries
        self.pool_minconn = pool_minconn
        self.pool_maxconn = pool_maxconn
        self.extra_adapter = extra_adapter

        # Internal state
        self._queue: queue.Queue[dict[str, Any] | None] = queue.Queue()
        self._pool = None  # lazy init on first use
        self._pool_lock = threading.Lock()
        self._thread: threading.Thread | None = None
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

    def start(self) -> AsyncBatchWriter:
        """Start the background flush thread. Idempotent."""
        if self._started:
            return self
        self._thread = threading.Thread(
            target=self._run,
            name=f"db-batch-{self.table}",
            daemon=True,
        )
        self._thread.start()
        self._started = True
        atexit.register(self.shutdown)
        _LOG.info("[AsyncBatchWriter] started for table=%s batch_size=%d interval=%.1fs",
                  self.table, self.batch_size, self.flush_interval_s)
        return self

    def enqueue(self, row: dict[str, Any]) -> None:
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
        batch: list[dict[str, Any]] = []
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
                from psycopg2 import pool as pgpool
                self._pool = pgpool.ThreadedConnectionPool(
                    self.pool_minconn,
                    self.pool_maxconn,
                    dsn=self.dsn,
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
        batch: list[dict[str, Any]] = []

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

    def _flush_direct(self, batch: list[dict[str, Any]]) -> None:
        """Flush a batch to DB with retry logic."""
        if not batch:
            return
        t0 = time.monotonic()
        last_exc: Exception | None = None

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
                _LOG.debug("[AsyncBatchWriter] flushed %d rows to %s in %.3fs",
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
                _LOG.warning("[AsyncBatchWriter] flush attempt %d/%d failed: %s — retry in %.1fs",
                             attempt, self.max_retries, exc, wait)
                time.sleep(wait)

        # All retries exhausted
        _LOG.error("[AsyncBatchWriter] all %d retries failed for table=%s, dropping %d rows: %s",
                   self.max_retries, self.table, len(batch), last_exc)
        if _FLUSH_FAIL is not None:
            _FLUSH_FAIL.labels(table=self.table).inc()


# ---------------------------------------------------------------------------
# Module-level convenience registry
# ---------------------------------------------------------------------------

_writers: dict[str, AsyncBatchWriter] = {}
_writers_lock = threading.Lock()


def get_or_create_writer(
    table: str,
    columns: Sequence[str],
    dsn: str,
    **kwargs: Any,
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
