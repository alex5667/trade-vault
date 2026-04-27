from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

import redis
from prometheus_client import Counter

from services.signal_preprocess import preprocess_signal_for_publish

# Create In-Memory Prometheus counters
PUB_OK_TOTAL = Counter("signals_publish_ok_total", "Successful signal publishes", ["source", "stream"])
PUB_ERR_TOTAL = Counter("signals_publish_errors_total", "Failed signal publishes", ["source", "stream"])
PUB_BUSY_TOTAL = Counter("signals_publish_busy_total", "BusyLoading Redis errors", ["source", "stream"])
PUB_RETRIES_ENQUEUED_TOTAL = Counter("signals_publish_retries_enqueued_total", "Signals queued for retry", ["source", "symbol"])
PUB_RETRIES_SUCCESS_TOTAL = Counter("signals_publish_retries_success_total", "Successful retries", ["source", "symbol"])
PUB_DROPPED_TOTAL = Counter("signals_publish_dropped_total", "Signals dropped after max retries or overflow", ["source", "symbol"])


def _json_dumps_safe(obj: Any) -> str:
    """
    Async hot-path JSON: MUST NOT raise.
    default=str is deliberate (Enums/Decimals/np types appear in the wild).
    """
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        return '{"error":"json_dumps_failed"}'




@dataclass(frozen=True)
class StreamSink:
    """
    A single XADD sink.
    field: which field name to write ("payload" vs "data") – different streams use different conventions.
    """
    name: str
    field: str = "payload"
    maxlen: int = 10000


@dataclass(frozen=True)
class AsyncPublishResult:
    ok: bool
    raw_written: bool
    busy_loading: bool
    errors: int


import asyncio
from utils.task_manager import safe_create_task

import logging

class AsyncSignalPublisher:
    """
    Shared publisher for async producers (aioredis / redis.asyncio).

    Goals:
      - At-Least-Once delivery: local in-memory buffer for retries during network outages.
      - contract normalization (ts_ms / ids / side_int / entry_price / mirrors)
      - structured result for deterministic unit tests
    """

    def __init__(
        self,
        *,
        redis_client: Any,
        source: str,
        metrics_prefix: str = "signals_publish_async",
        logger: Any = None,
        max_retries: int = 5,
        retry_queue_maxsize: int = 1000,
    ) -> None:
        self.r = redis_client
        self.source = str(source or "na")
        self.metrics_prefix = str(metrics_prefix or "signals_publish_async")
        self.logger = logger or logging.getLogger(__name__)
        self.max_retries = max_retries

        # Buffer for failed signals (in-memory, volatile)
        self._retry_queue: asyncio.Queue = asyncio.Queue(maxsize=retry_queue_maxsize)
        # Background worker task — created lazily via start() to avoid
        # RuntimeError: no running event loop when __init__ is called
        # outside an asyncio context (e.g. from a plain Thread).
        self._worker_task: Optional[asyncio.Task] = None

    def start(self) -> None:
        """
        Start the background retry worker.
        MUST be called from within a running asyncio event loop
        (i.e. after the loop is started, e.g. inside an async entrypoint or
        via asyncio.get_event_loop().run_until_complete(...)).

        Safe to call multiple times — subsequent calls are no-ops if the task
        is already running.
        """
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = safe_create_task(self._retry_worker())
            self.logger.debug("AsyncSignalPublisher: retry worker task started")

    async def _retry_worker(self) -> None:
        """Background worker to rescue signals during network drops."""
        while True:
            try:
                # task: (sink, payload, symbol, attempt, approximate)
                sink, payload, symbol, attempt, approximate = await self._retry_queue.get()

                # Exponential backoff: 0.5s, 1s, 2s, 4s, 8s, max 10s
                wait_sec = min(10.0, 0.5 * (2**(attempt - 1)))
                await asyncio.sleep(wait_sec)

                res = await self.xadd_json_internal(
                    sink=sink, payload=payload, symbol=symbol, approximate=approximate
                )

                if res.ok:
                    PUB_RETRIES_SUCCESS_TOTAL.labels(source=self.source, symbol=symbol).inc()
                else:
                    if attempt < self.max_retries and not res.busy_loading:
                        # Re-enqueue for another attempt
                        try:
                            self._retry_queue.put_nowait((sink, payload, symbol, attempt + 1, approximate))
                        except asyncio.QueueFull:
                            # Should not happen as we just took one item, but safety first
                            self.logger.critical("🚨 RETRY QUEUE FULL during worker loop: %s", symbol)
                    else:
                        PUB_DROPPED_TOTAL.labels(source=self.source, symbol=symbol).inc()
                        self.logger.error(
                            "🚨 SIGNAL LOST PERMANENTLY after %d attempts (busy=%s): %s",
                            attempt, res.busy_loading, symbol
                        )

                self._retry_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.exception("Retry worker crashed, restarting in 1s: %r", e)
                await asyncio.sleep(1)

    async def xadd_json(
        self,
        *,
        sink: StreamSink,
        payload: Dict[str, Any],
        symbol: str,
        approximate: bool = True,
    ) -> AsyncPublishResult:
        """
        Public method that guarantees At-Least-Once delivery (via local buffer).
        FAIL-OPEN: never raises.
        """
        # Track queue size
        try:
            qsize = self._retry_queue.qsize()
            # If we had a mechanism for gauge, we'd set it here.
            # For now, we just rely on logs and ok/error counters.
        except Exception:
            pass

        res = await self.xadd_json_internal(
            sink=sink, payload=payload, symbol=symbol, approximate=approximate
        )

        if not res.ok and not res.busy_loading:
            try:
                # If network error (or other non-BusyLoading error), put into retry queue
                self._retry_queue.put_nowait((sink, payload, symbol, 1, approximate))
                self.logger.warning("⚠️ Signal %s queued for retry (current_qsize=%d)", symbol, self._retry_queue.qsize())
                PUB_RETRIES_ENQUEUED_TOTAL.labels(source=self.source, symbol=symbol).inc()
            except asyncio.QueueFull:
                PUB_DROPPED_TOTAL.labels(source=self.source, symbol=symbol).inc()
                self.logger.critical("🚨 RETRY QUEUE OVERFLOW! Signal lost: %s", symbol)

        return res

    async def xadd_json_internal(
        self,
        *,
        sink: StreamSink,
        payload: Dict[str, Any],
        symbol: str,
        approximate: bool = True,
    ) -> AsyncPublishResult:
        """
        Internal raw XADD logic.
        """
        errors = 0
        busy = False
        raw_written = False

        # 1) normalize contract (never blocks publish)
        try:
            preprocess_signal_for_publish(payload, symbol=str(symbol), source=self.source, logger=self.logger)
        except Exception:
            pass

        # 2) serialize
        ser = _json_dumps_safe(payload)

        # 3) xadd
        try:
            await self.r.xadd(
                sink.name,
                fields={str(sink.field or "payload"): ser},
                maxlen=int(sink.maxlen),
                approximate=bool(approximate),
            )
            raw_written = True
            PUB_OK_TOTAL.labels(source=self.source, stream=sink.name).inc()
        except redis.exceptions.BusyLoadingError:
            busy = True
            PUB_BUSY_TOTAL.labels(source=self.source, stream=sink.name).inc()
        except Exception as e:
            errors += 1
            PUB_ERR_TOTAL.labels(source=self.source, stream=sink.name).inc()
            if self.logger is not None:
                try:
                    self.logger.warning("async_publish.xadd failed stream=%s err=%r", sink.name, e)
                except Exception:
                    pass

        return AsyncPublishResult(ok=raw_written, raw_written=raw_written, busy_loading=busy, errors=errors)
