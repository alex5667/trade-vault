"""
Regression pack — _maintain_ml_gate_loop & load_dynamic_symbols concurrency.

Проверяет:
1. _maintain_ml_gate_loop не крашится, если ml_gate=None или нет метода refresh_async.
2. При наличии gate.refresh_async → он вызывается с ml_gate_client.
3. Ошибки внутри loop (TimeoutError) перехватываются и не ломают цикл.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.asyncio
async def test_maintain_ml_gate_loop_no_gate_no_crash():
    """
    Если of_engine не имеет ml_gate (например gate выключен) → 
    цикл работает без ошибок и не падает.
    """
    from services.crypto_orderflow_service import CryptoOrderflowService

    svc = MagicMock(spec=CryptoOrderflowService)
    svc._shutdown = False
    svc.of_engine = MagicMock()
    svc.of_engine.ml_gate = None  # gate выключен или не инициализирован
    svc.ml_gate_client = AsyncMock()

    # Ограничиваем цикл двумя проходами
    call_count = 0
    original_sleep = asyncio.sleep

    async def mock_sleep(t):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            svc._shutdown = True
        await original_sleep(0)

    with patch("asyncio.sleep", mock_sleep):
        await CryptoOrderflowService._maintain_ml_gate_loop(svc)

    # Не должно быть вызовов к ml_gate_client
    svc.ml_gate_client.get.assert_not_called()


@pytest.mark.asyncio
async def test_maintain_ml_gate_loop_refresh_called():
    """Если gate.refresh_async доступен → он вызывается с ml_gate_client."""
    from services.crypto_orderflow_service import CryptoOrderflowService

    refresh_called = []

    mock_gate = AsyncMock()
    mock_gate.refresh_async = AsyncMock(side_effect=lambda client: refresh_called.append(client))
    mock_gate._cfg = {"kind": "util_mh_v1"}

    svc = MagicMock(spec=CryptoOrderflowService)
    svc._shutdown = False
    svc.of_engine = MagicMock()
    svc.of_engine.ml_gate = mock_gate
    svc.ml_gate_client = AsyncMock()

    call_count = 0
    original_sleep = asyncio.sleep

    async def mock_sleep(t):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            svc._shutdown = True
        await original_sleep(0)

    with patch("asyncio.sleep", mock_sleep):
        await CryptoOrderflowService._maintain_ml_gate_loop(svc)

    assert len(refresh_called) >= 1, "refresh_async должен быть вызван"
    # Должен быть вызван с ml_gate_client
    assert refresh_called[0] == svc.ml_gate_client


@pytest.mark.asyncio
async def test_maintain_ml_gate_loop_exception_recovery():
    """
    Если refresh_async кидает исключение, _maintain_ml_gate_loop
    ловит его, логирует и продолжает работу (через asyncio.sleep).
    """
    from services.crypto_orderflow_service import CryptoOrderflowService

    mock_gate = AsyncMock()
    mock_gate.refresh_async = AsyncMock(side_effect=ValueError("simulated network error"))
    mock_gate._cfg = {"kind": "test"}

    svc = MagicMock(spec=CryptoOrderflowService)
    svc._shutdown = False
    svc.of_engine = MagicMock()
    svc.of_engine.ml_gate = mock_gate
    svc.ml_gate_client = AsyncMock()

    call_count = 0
    original_sleep = asyncio.sleep

    async def mock_sleep(t):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            svc._shutdown = True
        await original_sleep(0)

    with patch("asyncio.sleep", mock_sleep):
        try:
            await CryptoOrderflowService._maintain_ml_gate_loop(svc)
        except ValueError:
            pytest.fail("Exception leaked from _maintain_ml_gate_loop")

    assert mock_gate.refresh_async.call_count >= 1
