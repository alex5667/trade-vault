from __future__ import annotations

"""
MetricsBatcher — Bounded async sink for non-critical Redis write operations.

Replaces fire-and-forget safe_create_task(self.redis.set/sadd/incr/expire...)
calls in the hot path (process_tick, _on_microbar_closed).

Design:
  - asyncio.Queue(maxsize) bounds memory growth under Redis latency spikes.
  - Background worker drains in batches of up to BATCH_SIZE ops via pipeline
    (1 RTT per batch instead of N RTTs).
  - put_nowait() + silent drop on QueueFull = load shedding (memory safe).
  - metrics_queue_dropped_total Counter tracks dropped ops for alerting.

Usage:
    batcher = MetricsBatcher(redis=redis_client, maxsize=10_000)
    safe_create_task(batcher.run())          # start once in event loop

    # In hot path (non-blocking):
    batcher.put("set", "key", "value", ttl=600)
    batcher.put("sadd", "myset", "member")
    batcher.put("incr", "counter")
    batcher.put("expire", "counter", 3600)
    batcher.put("xadd", "stream", {"field": "val"}, maxlen=20000)
"""

import asyncio
import logging
from typing import Any
import contextlib

logger = logging.getLogger("metrics_batcher")

_BATCH_SIZE = 100          # max ops per pipeline flush
_POLL_TIMEOUT_S = 0.05    # max wait for first item in queue (50ms)


class MetricsBatcher:
    """
    Bounded async sink for non-critical Redis write operations.

    Supported ops (op, *args):
        ("set",    key, value, ex=<int>)
        ("sadd",   key, *members)
        ("incr",   key)
        ("incrby", key, amount)
        ("expire", key, seconds)
        ("xadd",   stream, fields_dict, maxlen=<int>)

    All ops are best-effort: any pipeline error is logged but does NOT
    propagate to the caller.
    """

    def __init__(
        self,
        redis: Any,
        *,
        maxsize: int = 10_000,
        batch_size: int = _BATCH_SIZE,
        poll_timeout_s: float = _POLL_TIMEOUT_S,
        worker_label: str = "orderflow",
    ) -> None:
        self._redis = redis
        self._queue: asyncio.Queue[tuple] = asyncio.Queue(maxsize=maxsize)
        self._batch_size = batch_size
        self._poll_timeout_s = poll_timeout_s
        self._worker_label = worker_label
        self._dropped = 0
        self._running = False

        # Lazy import to avoid circular deps at module level
        self._dropped_counter = None

    def _get_counter(self):
        if self._dropped_counter is None:
            try:
                from services.orderflow.metrics import metrics_queue_dropped_total
                self._dropped_counter = metrics_queue_dropped_total
            except Exception:
                pass
        return self._dropped_counter

    def put(self, op: str, *args: Any, **kwargs: Any) -> bool:
        """
        Non-blocking enqueue. Returns True if enqueued, False if dropped (QueueFull).
        Never raises.
        """
        try:
            self._queue.put_nowait((op, args, kwargs))
            return True
        except asyncio.QueueFull:
            self._dropped += 1
            try:
                ctr = self._get_counter()
                if ctr is not None:
                    ctr.labels(worker=self._worker_label).inc()
            except Exception:
                pass
            return False

    async def run(self) -> None:
        """Background worker. Run as a single asyncio.Task for the service lifetime."""
        self._running = True
        logger.info("MetricsBatcher[%s] started (maxsize=%d, batch=%d)",
                    self._worker_label, self._queue.maxsize, self._batch_size)
        while self._running:
            try:
                await self._flush_batch()
            except Exception as exc:
                logger.error("MetricsBatcher[%s] flush error: %r", self._worker_label, exc)
                await asyncio.sleep(0.1)

    async def stop(self) -> None:
        """Graceful stop: drain remaining items then exit."""
        self._running = False
        # Drain whatever is left
        with contextlib.suppress(Exception):
            await self._flush_batch(drain=True)

    async def _flush_batch(self, *, drain: bool = False) -> None:
        """Collect up to batch_size items and execute them via pipeline."""
        batch = []

        # Wait for first item
        try:
            timeout = None if drain else self._poll_timeout_s
            first = await asyncio.wait_for(self._queue.get(), timeout=timeout)
            batch.append(first)
        except TimeoutError:
            return
        except asyncio.QueueEmpty:
            return

        # Collect remaining without blocking
        while len(batch) < self._batch_size:
            try:
                batch.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        if not batch:
            return

        # Execute via pipeline (1 RTT) with retry for transient errors
        for attempt in range(1, 4):
            try:
                # We need to recreate the pipeline on each attempt in case it was left in a bad state
                async with self._redis.pipeline(transaction=False) as pipe:
                    for op, args, kwargs in batch:
                        await self._apply_op(pipe, op, args, kwargs)
                    await pipe.execute()
                # Success
                return
            except Exception as exc:
                import redis.exceptions
                if isinstance(exc, (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError)) and attempt < 3:
                    logger.debug(
                        "MetricsBatcher[%s] transient error on flush (attempt %d/3): %r, retrying...",
                        self._worker_label, attempt, exc
                    )
                    await asyncio.sleep(0.5 * attempt)
                    continue
                else:
                    logger.warning(
                        "MetricsBatcher[%s] pipeline flush failed (%d ops) after %d attempts: %r",
                        self._worker_label, len(batch), attempt, exc,
                    )
                    return

    @staticmethod
    async def _apply_op(pipe: Any, op: str, args: tuple, kwargs: dict) -> None:
        """Apply a single op to the pipeline object (no await needed for pipeline cmds)."""
        try:
            if op == "set":
                key, value = args[0], args[1]
                ex = kwargs.get("ex") or (args[2] if len(args) > 2 else None)
                if ex:
                    pipe.set(key, value, ex=int(ex))
                else:
                    pipe.set(key, value)

            elif op == "sadd":
                key = args[0]
                members = args[1:]
                pipe.sadd(key, *members)

            elif op == "incr":
                pipe.incr(args[0])

            elif op == "incrby":
                pipe.incrby(args[0], int(args[1]))

            elif op == "expire":
                key = args[0]
                seconds = int(args[1] if len(args) > 1 else kwargs.get("seconds", 0))
                if seconds > 0:
                    pipe.expire(key, seconds)

            elif op == "hincrby":
                key, field = args[0], args[1]
                amount = int(args[2] if len(args) > 2 else kwargs.get("amount", 1))
                pipe.hincrby(key, field, amount)

            elif op == "xadd":
                stream = args[0]
                fields: dict[str, Any] = args[1] if len(args) > 1 else {}
                maxlen = kwargs.get("maxlen")
                approximate = kwargs.get("approximate", True)
                if maxlen:
                    pipe.xadd(stream, fields, maxlen=int(maxlen), approximate=approximate)
                else:
                    pipe.xadd(stream, fields, maxlen=50000)

            else:
                logger.debug("MetricsBatcher: unknown op %r — skipped", op)

        except Exception as exc:
            logger.debug("MetricsBatcher: op=%r failed: %r", op, exc)

    def _flush_prom_batch(self, prom_batch: dict[tuple[str, tuple], float]) -> None:
        """Flushes aggregated prometheus metrics."""
        for (op, labels), val in prom_batch.items():
            try:
                # Op is "prom_inc" or "prom_set"
                # Need to find the metric object. For now we assume they are global.
                # In real implementation we would need a registry of metrics.
                pass
            except Exception:
                pass

class PrometheusBatcher:
    """
    Experimental local aggregator for high-frequency Prometheus counters.
    Reduces label lookup and GIL overhead.
    """
    def __init__(self, interval: float = 1.0):
        self._counts: dict[Any, float] = {}
        self._interval = interval
        self._lock = asyncio.Lock()
        self._task = None

    def inc(self, metric: Any, labels: dict[str, str], amount: float = 1.0):
        key = (metric, tuple(sorted(labels.items())))
        self._counts[key] = self._counts.get(key, 0) + amount

    async def run(self):
        while True:
            await asyncio.sleep(self._interval)
            async with self._lock:
                to_flush = self._counts
                self._counts = {}
            for (metric, labels_tuple), val in to_flush.items():
                with contextlib.suppress(Exception):
                    metric.labels(**dict(labels_tuple)).inc(val)
