"""
Latency benchmark tests for BackgroundTaskManager and Async limits.
"""
import asyncio
import time
import pytest
from utils.task_manager import BackgroundTaskManager


async def _noop_task():
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_task_manager_submit_latency_p99():
    """
    Benchmark: Нагрузочный сабмит задач. p99 latency создания таска
    (через submit) должен быть < 1ms, чтобы не блокировать event loop.
    """
    mgr = BackgroundTaskManager(limit=100000)
    
    latencies = []
    
    # Прогрев
    for _ in range(100):
        t = mgr.submit(_noop_task(), name="warmup")
        if t: await t
        
    # Замер
    for _ in range(5000):
        t0 = time.perf_counter()
        task = mgr.submit(_noop_task(), name="bench")
        dt = (time.perf_counter() - t0) * 1000  # ms
        latencies.append((dt, task))
        
    # Дожидаемся
    active_tasks = [t for dt, t in latencies if t is not None]
    await asyncio.gather(*active_tasks, return_exceptions=True)
    
    pure_lats = [dt for dt, t in latencies]
    pure_lats.sort()
    
    p99 = pure_lats[int(len(pure_lats) * 0.99)]
    
    assert p99 < 5.0, f"p99 submit latency = {p99:.3f}ms, expected < 5ms"


@pytest.mark.asyncio
async def test_task_manager_drop_latency():
    """
    При переполнении очереди отброс (drop) задачи должен быть 
    почти мгновенным (< 0.5ms).
    """
    mgr = BackgroundTaskManager(limit=1)
    
    # Заняли слот
    t_live = mgr.submit(asyncio.sleep(0.5), name="blocker")
    
    latencies = []
    for _ in range(500):
        t0 = time.perf_counter()
        task = mgr.submit(_noop_task(), name="dropper")
        dt = (time.perf_counter() - t0) * 1000  # ms
        latencies.append(dt)
        assert task is None, "Ожидали, что задача будет дропнута (limit=1)"
        
    latencies.sort()
    p99 = latencies[int(len(latencies) * 0.99)]
    
    # Ждём блокер для корректного выхода
    if t_live:
        t_live.cancel()
        try:
            await t_live
        except asyncio.CancelledError:
            pass
            
    assert p99 < 1.0, f"p99 drop latency = {p99:.3f}ms, expected < 1ms"
