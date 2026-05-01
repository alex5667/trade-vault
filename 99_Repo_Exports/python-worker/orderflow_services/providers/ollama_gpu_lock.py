from __future__ import annotations
"""Redis-based distributed GPU lock for Ollama.

Only one consumer (AIOps Agent, Local Fallback Plane, etc.)
may hold the GPU at a time. Uses Redis SET NX EX to implement
a distributed mutex with automatic expiry.

Usage (sync):
    from orderflow_services.providers.ollama_gpu_lock import OllamaGpuLock
    lock = OllamaGpuLock(redis_url="redis://redis-worker-1:6379/0")
    with lock.acquire_sync(owner="aiops_agent", timeout_sec=60):
        # call Ollama here
        ...

Usage (async):
    lock = OllamaGpuLock(redis_url="...")
    async with lock.acquire(owner="local_fallback", timeout_sec=30):
        # call Ollama here
        ...
"""

import os
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from typing import Any, AsyncIterator, Iterator


LOCK_KEY = os.getenv("OLLAMA_GPU_LOCK_KEY", "lock:ollama:gpu")
DEFAULT_LOCK_TTL = int(os.getenv("OLLAMA_GPU_LOCK_TTL_SEC", "600"))
DEFAULT_ACQUIRE_TIMEOUT = int(os.getenv("OLLAMA_GPU_LOCK_ACQUIRE_TIMEOUT_SEC", "120"))
POLL_INTERVAL = float(os.getenv("OLLAMA_GPU_LOCK_POLL_SEC", "2.0"))


class OllamaGpuLockTimeout(Exception):
    """Raised when the GPU lock cannot be acquired within the timeout."""


class OllamaGpuLock:
    """Distributed mutex for Ollama GPU access via Redis.

    Parameters
    ----------
    redis_url : str
        Redis connection URL.
    lock_ttl_sec : int
        Maximum time the lock is held (auto-expires). Defaults to 600s.
    """

    def __init__(
        self,
        redis_url: str = "",
        lock_ttl_sec: int = DEFAULT_LOCK_TTL,
    ) -> None:
        self._redis_url = redis_url or os.getenv(
            "REDIS_URL", "redis://localhost:6379/0"
        )
        self._lock_ttl = lock_ttl_sec

    # ── Async path (for Local Fallback Plane) ───────────────────────────

    @asynccontextmanager
    async def acquire(
        self,
        owner: str = "unknown",
        timeout_sec: int = DEFAULT_ACQUIRE_TIMEOUT,
    ) -> AsyncIterator[str]:
        """Async context manager that acquires the GPU lock.

        Raises OllamaGpuLockTimeout if the lock is not acquired within
        *timeout_sec* seconds.
        """
        try:
            import redis.asyncio as aioredis
        except ImportError as exc:
            raise RuntimeError("redis.asyncio required for async lock") from exc

        token = f"{owner}:{uuid.uuid4().hex[:8]}"
        client = aioredis.from_url(self._redis_url)
        deadline = time.monotonic() + timeout_sec

        try:
            while True:
                acquired = await client.set(
                    LOCK_KEY, token, nx=True, ex=self._lock_ttl,
                )
                if acquired:
                    break
                if time.monotonic() >= deadline:
                    current_holder = await client.get(LOCK_KEY)
                    holder_str = (
                        current_holder.decode()
                        if isinstance(current_holder, bytes)
                        else str(current_holder)
                    )
                    raise OllamaGpuLockTimeout(
                        f"GPU lock not acquired within {timeout_sec}s. "
                        f"Current holder: {holder_str}"
                    )
                await _async_sleep(POLL_INTERVAL)
            yield token
        finally:
            # Release only if we still own it (compare-and-delete)
            current = await client.get(LOCK_KEY)
            if current and (
                current.decode() if isinstance(current, bytes) else current
            ) == token:
                await client.delete(LOCK_KEY)
            await client.aclose()

    # ── Sync path (for AIOps Agent) ─────────────────────────────────────

    @contextmanager
    def acquire_sync(
        self,
        owner: str = "unknown",
        timeout_sec: int = DEFAULT_ACQUIRE_TIMEOUT,
    ) -> Iterator[str]:
        """Sync context manager that acquires the GPU lock.

        Raises OllamaGpuLockTimeout if the lock is not acquired within
        *timeout_sec* seconds.
        """
        try:
            import redis as sync_redis
        except ImportError as exc:
            raise RuntimeError("redis package required for sync lock") from exc

        token = f"{owner}:{uuid.uuid4().hex[:8]}"
        client = sync_redis.from_url(self._redis_url)
        deadline = time.monotonic() + timeout_sec

        try:
            while True:
                acquired = client.set(
                    LOCK_KEY, token, nx=True, ex=self._lock_ttl,
                )
                if acquired:
                    break
                if time.monotonic() >= deadline:
                    current_holder = client.get(LOCK_KEY)
                    holder_str = (
                        current_holder.decode()
                        if isinstance(current_holder, bytes)
                        else str(current_holder)
                    )
                    raise OllamaGpuLockTimeout(
                        f"GPU lock not acquired within {timeout_sec}s. "
                        f"Current holder: {holder_str}"
                    )
                time.sleep(POLL_INTERVAL)
            yield token
        finally:
            current = client.get(LOCK_KEY)
            if current and (
                current.decode() if isinstance(current, bytes) else current
            ) == token:
                client.delete(LOCK_KEY)
            client.close()


async def _async_sleep(seconds: float) -> None:
    import asyncio
    await asyncio.sleep(seconds)
