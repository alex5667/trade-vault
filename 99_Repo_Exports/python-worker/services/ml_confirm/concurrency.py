from __future__ import annotations

"""
Concurrency utilities for OFConfirm engine build.

Provides:
- is_of_sync_build(): kill-switch to run build synchronously (OF_SYNC_BUILD=1)
- run_bounded_of_build(): bounded async executor with semaphore + timeout
- _get_ml_executor(): shared ThreadPoolExecutor for ML inference

Migrated from ml_confirm_gate_old.py to the new ml_confirm_gate package.
"""

import asyncio
import concurrent.futures
import logging
import os
from collections.abc import Callable
from typing import Any
import contextlib

logger = logging.getLogger("ml_confirm_gate.concurrency")

# ──────────────────────────────────────────────────────────────────────────────
# Process-level shared ThreadPoolExecutor for ML inference.
# LightGBM's predict() releases the GIL → true parallelism across threads.
# Max workers default=2: handles burst of 2 simultaneous signal evals;
# configure via ML_CONFIRM_THREADS without rebuild.
# ──────────────────────────────────────────────────────────────────────────────
_ML_INFER_EXECUTOR: concurrent.futures.ThreadPoolExecutor | None = None
_OF_BUILD_SEMAPHORE: asyncio.Semaphore | None = None


def _get_of_build_slots() -> int:
    raw = os.getenv("OF_BUILD_MAX_INFLIGHT", os.getenv("ML_CONFIRM_THREADS", "2"))
    try:
        return max(1, int(raw or "2"))
    except Exception:
        return 2


def _get_of_build_semaphore() -> asyncio.Semaphore:
    global _OF_BUILD_SEMAPHORE
    if _OF_BUILD_SEMAPHORE is None:
        _OF_BUILD_SEMAPHORE = asyncio.Semaphore(_get_of_build_slots())
    return _OF_BUILD_SEMAPHORE


def _get_ml_executor() -> concurrent.futures.ThreadPoolExecutor:
    global _ML_INFER_EXECUTOR
    if _ML_INFER_EXECUTOR is None:
        n = int(os.getenv("ML_CONFIRM_THREADS", "2") or "2")
        _ML_INFER_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
            max_workers=max(1, n),
            thread_name_prefix="ml-infer",
        )
    return _ML_INFER_EXECUTOR


async def run_bounded_of_build(
    fn: Callable[[], Any],
    *,
    timeout_s: float,
    acquire_timeout_s: float | None = None,
) -> tuple[Any, str | None]:
    """Run OF build in the shared executor without allowing unbounded backlog.

    Returns (result, error_reason). If error_reason is not None, result is None.
    """
    from services.orderflow.metrics import (
        of_confirm_build_inflight,
        of_confirm_build_rejected_total,
        of_confirm_build_timeout_total,
    )

    symbol = "unknown"
    tf = "1s"
    try:
        symbol = str(getattr(fn, "_of_build_symbol", "unknown"))
        tf = str(getattr(fn, "_of_build_tf", "1s"))
    except Exception:
        pass

    semaphore = _get_of_build_semaphore()
    acquire_timeout = acquire_timeout_s
    if acquire_timeout is None:
        acquire_timeout = float(os.getenv("OF_BUILD_ACQUIRE_TIMEOUT_S", "0.01") or 0.01)
    acquire_timeout = max(0.001, float(acquire_timeout))

    try:
        await asyncio.wait_for(semaphore.acquire(), timeout=acquire_timeout)
    except TimeoutError:
        with contextlib.suppress(Exception):
            of_confirm_build_rejected_total.labels(symbol=symbol, tf=tf, reason="executor_busy").inc()
        return None, "executor_busy"

    with contextlib.suppress(Exception):
        of_confirm_build_inflight.set(float(_get_of_build_slots() - semaphore._value))

    loop = asyncio.get_running_loop()
    future = loop.run_in_executor(_get_ml_executor(), fn)
    released = False

    def _release_slot(_f: Any = None) -> None:
        nonlocal released
        if released:
            return
        released = True
        with contextlib.suppress(Exception):
            semaphore.release()
        with contextlib.suppress(Exception):
            of_confirm_build_inflight.set(float(_get_of_build_slots() - semaphore._value))

    future.add_done_callback(_release_slot)

    try:
        result = await asyncio.wait_for(asyncio.shield(future), timeout=timeout_s)
        _release_slot()
        return result, None
    except TimeoutError:
        with contextlib.suppress(Exception):
            of_confirm_build_timeout_total.labels(symbol=symbol, tf=tf).inc()
        return None, "timeout"


def is_of_sync_build() -> bool:
    """Kill-switch: if OF_SYNC_BUILD=1, of_engine.build() runs synchronously
    in the event loop (blocks it) instead of using the thread pool.
    Use only for emergency rollback or debugging.
    """
    return os.getenv("OF_SYNC_BUILD", "0").strip() == "1"


def _shutdown_ml_executor() -> None:
    """Gracefully shutdown the ML inference executor on process exit.
    Prevents thread pool leak on hot-reload or container restart.
    """
    global _ML_INFER_EXECUTOR
    if _ML_INFER_EXECUTOR is not None:
        with contextlib.suppress(Exception):
            _ML_INFER_EXECUTOR.shutdown(wait=False)
        _ML_INFER_EXECUTOR = None


# Register graceful shutdown to prevent thread leak on process exit
import atexit as _atexit

_atexit.register(_shutdown_ml_executor)
