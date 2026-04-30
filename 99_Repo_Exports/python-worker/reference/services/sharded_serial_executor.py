from __future__ import annotations

import time
import queue
import threading
import zlib
from dataclasses import dataclass
from typing import Callable, Any, Dict
from concurrent.futures import Future


def _crc32(s: str) -> int:
    return zlib.crc32(s.encode("utf-8")) & 0xFFFFFFFF


@dataclass
class ExecutorStats:
    submitted: int = 0
    executed: int = 0
    failed: int = 0
    queue_full: int = 0


@dataclass
class _Task:
    key: str
    name: str
    fn: Callable[[], Any]
    fut: Future
    enqueued_ts: float


class ShardedSerialExecutor:
    """
    Fixed number of single-thread shards. Within a shard tasks are executed serially.
    Routing: shard = crc32(key) % shards.

    Important for lossless stream ACK:
      - caller can wait on Future.result()
      - ACK only after success
    """

    def __init__(
        self
        *
        shards: int = 8
        queue_max: int = 20000
        submit_timeout_s: float = 2.0
        name: str = "SymbolExec"
        logger=None
    ):
        self.shards = max(1, int(shards))
        self.queue_max = max(1, int(queue_max))
        self.submit_timeout_s = max(0.0, float(submit_timeout_s))
        self.name = name
        self.logger = logger

        self._qs = [queue.Queue(maxsize=self.queue_max) for _ in range(self.shards)]
        self._threads: list[threading.Thread] = []
        self._stop = threading.Event()
        self.stats = ExecutorStats()
        self._stats_lock = threading.Lock()

        for i in range(self.shards):
            t = threading.Thread(
                target=self._run_shard
                args=(i,)
                name=f"{self.name}-shard-{i}"
                daemon=True
            )
            t.start()
            self._threads.append(t)

    def shutdown(self, *, join_timeout_s: float = 2.0) -> None:
        self._stop.set()
        # Best-effort: wake up shards
        for q in self._qs:
            try:
                q.put_nowait(None)  # type: ignore[arg-type]
            except Exception:
                pass
        for t in self._threads:
            try:
                t.join(timeout=join_timeout_s)
            except Exception:
                pass

    def _pick_shard(self, key: str) -> int:
        return _crc32(key) % self.shards

    def submit(self, key: str, fn: Callable[[], Any], *, name: str = "") -> Future:
        """
        Submit task to a shard determined by key. Returns Future.
        If queue is full and cannot enqueue within submit_timeout_s -> Future set to exception.
        """
        k = str(key or "unknown")
        fut: Future = Future()
        task = _Task(
            key=k
            name=name or "task"
            fn=fn
            fut=fut
            enqueued_ts=time.time()
        )

        shard = self._pick_shard(k)
        q = self._qs[shard]

        with self._stats_lock:
            self.stats.submitted += 1

        try:
            q.put(task, timeout=self.submit_timeout_s)
        except queue.Full:
            with self._stats_lock:
                self.stats.queue_full += 1
            fut.set_exception(RuntimeError(f"executor queue full (shard={shard}, key={k}, name={task.name})"))
        return fut

    def _run_shard(self, shard_id: int) -> None:
        q = self._qs[shard_id]
        while not self._stop.is_set():
            try:
                item = q.get(timeout=0.5)
            except queue.Empty:
                continue
            if item is None:
                continue
            task: _Task = item
            if task.fut.cancelled():
                q.task_done()
                continue
            try:
                res = task.fn()
                task.fut.set_result(res)
                with self._stats_lock:
                    self.stats.executed += 1
            except Exception as e:
                task.fut.set_exception(e)
                with self._stats_lock:
                    self.stats.failed += 1
                if self.logger:
                    try:
                        # Safely convert exception to string to avoid name resolution errors
                        error_msg = str(e) if e else "unknown error"
                        self.logger.warning("⚠️ executor task failed shard=%s key=%s name=%s: %s", shard_id, task.key, task.name, error_msg)
                    except Exception:
                        # Fallback: log without exception details
                        try:
                            self.logger.warning("⚠️ executor task failed shard=%s key=%s name=%s (error formatting failed)", shard_id, task.key, task.name)
                        except Exception:
                            pass
            finally:
                q.task_done()

    def shard_queue_len(self, shard_id: int) -> int:
        shard_id = int(shard_id)
        if shard_id < 0 or shard_id >= self.shards:
            return 0
        return int(self._qs[shard_id].qsize())

    def snapshot_stats(self) -> Dict[str, int]:
        with self._stats_lock:
            return {
                "submitted": int(self.stats.submitted)
                "executed": int(self.stats.executed)
                "failed": int(self.stats.failed)
                "queue_full": int(self.stats.queue_full)
            }
