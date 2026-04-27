from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

import pytest


@pytest.fixture(autouse=True)
def _reset_executor_state(monkeypatch):
    from services import ml_confirm_gate as mg

    monkeypatch.setenv("OF_BUILD_MAX_INFLIGHT", "1")
    monkeypatch.setenv("ML_CONFIRM_THREADS", "1")
    mg._shutdown_ml_executor()
    mg._OF_BUILD_SEMAPHORE = None
    yield
    mg._shutdown_ml_executor()
    mg._OF_BUILD_SEMAPHORE = None


@pytest.mark.asyncio
async def test_run_bounded_of_build_rejects_when_slot_busy():
    from services import ml_confirm_gate as mg

    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="test-of-build")
    monkeypatch_executor = pytest.MonkeyPatch()
    monkeypatch_executor.setattr(mg, "_ML_INFER_EXECUTOR", pool)

    try:
        def slow_build():
            time.sleep(0.15)
            return ("ok", "done")

        slow_build._of_build_symbol = "BTCUSDT"
        slow_build._of_build_tf = "1s"

        first = asyncio.create_task(mg.run_bounded_of_build(slow_build, timeout_s=0.5, acquire_timeout_s=0.01))
        await asyncio.sleep(0.02)
        second = await mg.run_bounded_of_build(slow_build, timeout_s=0.5, acquire_timeout_s=0.01)
        first_result = await first

        assert first_result == (("ok", "done"), None)
        assert second == (None, "executor_busy")
    finally:
        monkeypatch_executor.undo()
        pool.shutdown(wait=True)


@pytest.mark.asyncio
async def test_run_bounded_of_build_timeout_keeps_slot_until_thread_finishes():
    from services import ml_confirm_gate as mg

    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="test-of-build")
    monkeypatch_executor = pytest.MonkeyPatch()
    monkeypatch_executor.setattr(mg, "_ML_INFER_EXECUTOR", pool)

    try:
        def very_slow_build():
            time.sleep(0.2)
            return ("ok", "late")

        very_slow_build._of_build_symbol = "ETHUSDT"
        very_slow_build._of_build_tf = "1s"

        timed_out = await mg.run_bounded_of_build(very_slow_build, timeout_s=0.05, acquire_timeout_s=0.01)
        rejected_while_old_thread_still_runs = await mg.run_bounded_of_build(
            very_slow_build, timeout_s=0.05, acquire_timeout_s=0.01
        )
        await asyncio.sleep(0.22)
        succeeds_after_thread_finishes = await mg.run_bounded_of_build(
            very_slow_build, timeout_s=0.5, acquire_timeout_s=0.01
        )

        assert timed_out == (None, "timeout")
        assert rejected_while_old_thread_still_runs == (None, "executor_busy")
        assert succeeds_after_thread_finishes == (("ok", "late"), None)
    finally:
        monkeypatch_executor.undo()
        pool.shutdown(wait=True)
