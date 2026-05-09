"""
Regression pack — BackgroundTaskManager (2026-04-18 wave).

Проверяет:
1. done_callback: Redis/asyncio TimeoutError логируется как WARNING, не ERROR.
2. done_callback: любая другая ошибка → ERROR.
3. Успешный таск → нет логов ошибок.
4. Превышение лимита → drop (без краша).
5. "Task exception was never retrieved" НЕ появляется после done_callback.
"""
import asyncio
import logging
import warnings

import pytest


async def _coro_redis_timeout():
    import redis.exceptions
    raise redis.exceptions.TimeoutError("simulated redis timeout")


async def _coro_connection_error():
    import redis.exceptions
    raise redis.exceptions.ConnectionError("simulated connection error")


async def _coro_asyncio_timeout():
    raise TimeoutError("simulated asyncio timeout")


async def _coro_generic_error():
    raise ValueError("critical unexpected failure")


async def _coro_ok():
    return 42


# ---------------------------------------------------------------------------
# 1. Redis TimeoutError → WARNING
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_done_callback_redis_timeout_warns(caplog):
    """Redis TimeoutError в bg task → WARNING уровень, не ERROR."""
    from utils.task_manager import BackgroundTaskManager

    mgr = BackgroundTaskManager(limit=100)
    with caplog.at_level(logging.DEBUG, logger="task_manager"):
        task = mgr.submit(_coro_redis_timeout(), name="test-redis-timeout")
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0.05)

    warn_records = [r for r in caplog.records
                    if r.levelno == logging.WARNING and ("timeout" in r.message.lower() or "redis" in r.message.lower())]
    error_records = [r for r in caplog.records
                     if r.levelno >= logging.ERROR and ("timeout" in r.message.lower() or "redis" in r.message.lower())]

    assert warn_records, "Ожидали хотя бы один WARNING для Redis timeout"
    assert not error_records, f"Не должно быть ERROR для Redis timeout, но получили: {error_records}"


# ---------------------------------------------------------------------------
# 2. Redis ConnectionError → WARNING
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_done_callback_connection_error_warns(caplog):
    """Redis ConnectionError → WARNING."""
    from utils.task_manager import BackgroundTaskManager

    mgr = BackgroundTaskManager(limit=100)
    with caplog.at_level(logging.DEBUG, logger="task_manager"):
        task = mgr.submit(_coro_connection_error(), name="test-conn-err")
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0.05)

    # Должен быть WARNING, не ERROR
    for rec in caplog.records:
        if "connection" in rec.message.lower():
            assert rec.levelno <= logging.WARNING, \
                f"Ожидали ≤ WARNING для ConnectionError, получили {rec.levelname}"


# ---------------------------------------------------------------------------
# 3. asyncio.TimeoutError → WARNING
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_done_callback_asyncio_timeout_warns(caplog):
    """asyncio.TimeoutError в bg task → WARNING."""
    from utils.task_manager import BackgroundTaskManager

    mgr = BackgroundTaskManager(limit=100)
    with caplog.at_level(logging.DEBUG, logger="task_manager"):
        task = mgr.submit(_coro_asyncio_timeout(), name="test-asyncio-timeout")
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0.05)

    # Не должно быть ERROR
    error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert not error_records, f"asyncio.TimeoutError не должен давать ERROR: {error_records}"


# ---------------------------------------------------------------------------
# 4. Generic ValueError → ERROR
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_done_callback_generic_error_logs_error(caplog):
    """ValueError (не Redis) → ERROR уровень."""
    from utils.task_manager import BackgroundTaskManager

    mgr = BackgroundTaskManager(limit=100)
    with caplog.at_level(logging.DEBUG, logger="task_manager"):
        task = mgr.submit(_coro_generic_error(), name="test-generic-err")
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0.05)

    error_records = [r for r in caplog.records
                     if r.levelno >= logging.ERROR and "critical unexpected failure" in r.message]
    assert error_records, "Ожидали ERROR лог для ValueError"


# ---------------------------------------------------------------------------
# 5. Успешный таск — нет ERROR-логов
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_done_callback_ok_task_silent(caplog):
    """Успешная coroutine → нет ERROR/WARNING логов."""
    from utils.task_manager import BackgroundTaskManager

    mgr = BackgroundTaskManager(limit=100)
    with caplog.at_level(logging.WARNING, logger="task_manager"):
        task = mgr.submit(_coro_ok(), name="test-ok-task")
        await task
        await asyncio.sleep(0.05)

    bad = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert not bad, f"Успешная задача не должна давать ошибок: {bad}"


# ---------------------------------------------------------------------------
# 6. Лимит → drop, не краш
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_task_limit_drops_excess():
    """Задачи сверх лимита дропаются (возвращают None), не крашатся."""
    from utils.task_manager import BackgroundTaskManager

    mgr = BackgroundTaskManager(limit=2)

    results = [mgr.submit(_coro_ok(), name=f"task-{i}") for i in range(10)]
    live_tasks = [t for t in results if t is not None]

    await asyncio.gather(*live_tasks, return_exceptions=True)

    dropped = [t for t in results if t is None]
    assert len(dropped) >= 1, "Часть задач должна быть дропнута при превышении лимита"
    assert len(live_tasks) <= 2


# ---------------------------------------------------------------------------
# 7. "Task exception was never retrieved" НЕ появляется
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_no_task_exception_never_retrieved_warning():
    """
    Golden regression: done_callback должен забирать исключение,
    чтобы Python НЕ выдавал RuntimeWarning 'Task exception was never retrieved'.
    """
    from utils.task_manager import BackgroundTaskManager

    mgr = BackgroundTaskManager(limit=100)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        task = mgr.submit(_coro_generic_error(), name="test-never-retrieved")
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0.1)

    never_retrieved = [
        x for x in w
        if issubclass(x.category, RuntimeWarning)
        and "never retrieved" in str(x.message).lower()
    ]
    assert not never_retrieved, \
        f"Получили 'Task exception was never retrieved' предупреждения: {never_retrieved}"


# ---------------------------------------------------------------------------
# 8. safe_create_task — drop-in wrapper
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_safe_create_task_returns_task():
    """safe_create_task возвращает asyncio.Task (не None) при наличии места."""
    from utils.task_manager import safe_create_task

    task = safe_create_task(_coro_ok(), name="safe-test")
    assert task is not None
    assert isinstance(task, asyncio.Task)
    result = await task
    assert result == 42
