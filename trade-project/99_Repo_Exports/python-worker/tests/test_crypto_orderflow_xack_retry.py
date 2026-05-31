from __future__ import annotations

"""Retry behaviour for CryptoOrderflowService._xack_pipeline.

Regression guard for the "XACK FAILURE ... Connection lost" storms on
alt/meme shards: transient Redis errors must retry with exponential
backoff before the batch is DLQ'd.
"""


import types
from unittest.mock import AsyncMock, patch

import pytest
import redis.exceptions as rexc

from services.crypto_orderflow_service import CryptoOrderflowService


@pytest.fixture
def service() -> CryptoOrderflowService:
    """Build a bare service instance without touching Redis/config loaders."""
    svc = CryptoOrderflowService.__new__(CryptoOrderflowService)
    svc._ack_batch = 100
    svc._shutdown = False
    # ticks is the Redis async client used by _xack_pipeline.
    svc.ticks = types.SimpleNamespace(
        xack=AsyncMock(return_value=1),
        connection_pool=None,
    )
    # main is only used on the DLQ write path; tests that exercise it patch
    # self.main.xadd directly.
    svc.main = types.SimpleNamespace(xadd=AsyncMock(return_value="1-0"))
    return svc


class TestXackRetryTransient:
    @pytest.mark.asyncio
    async def test_success_first_try_no_retry(self, service, monkeypatch):
        monkeypatch.setenv("CRYPTO_OF_XACK_RETRIES", "3")
        monkeypatch.setenv("CRYPTO_OF_XACK_BACKOFF_MS", "0")
        await service._xack_pipeline(
            stream="stream:book_TEST", group="g", ids=["1-0", "2-0"], symbol="TEST", op="book",
        )
        assert service.ticks.xack.await_count == 1

    @pytest.mark.asyncio
    async def test_transient_retries_then_succeeds(self, service, monkeypatch):
        monkeypatch.setenv("CRYPTO_OF_XACK_RETRIES", "3")
        monkeypatch.setenv("CRYPTO_OF_XACK_BACKOFF_MS", "0")
        # First two calls fail transiently, third succeeds.
        service.ticks.xack = AsyncMock(side_effect=[
            rexc.ConnectionError("Error UNKNOWN while writing to socket. Connection lost."),
            rexc.TimeoutError("Timeout reading from socket"),
            1,
        ])
        await service._xack_pipeline(
            stream="stream:book_TEST", group="g", ids=["1-0"], symbol="TEST", op="book",
        )
        assert service.ticks.xack.await_count == 3
        # DLQ must NOT be written when retry eventually succeeds.
        service.main.xadd.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_non_transient_no_retry(self, service, monkeypatch):
        monkeypatch.setenv("CRYPTO_OF_XACK_RETRIES", "3")
        monkeypatch.setenv("CRYPTO_OF_XACK_BACKOFF_MS", "0")
        # Plain ValueError is not transient → exit loop immediately, DLQ fires.
        service.ticks.xack = AsyncMock(side_effect=ValueError("bad protocol"))
        with patch("services.crypto_orderflow_service.safe_create_task") as spawn:
            await service._xack_pipeline(
                stream="stream:book_TEST", group="g", ids=["1-0"], symbol="TEST", op="book",
            )
            assert service.ticks.xack.await_count == 1
            spawn.assert_called_once()

    @pytest.mark.asyncio
    async def test_transient_exhausts_retries_then_dlq(self, service, monkeypatch):
        monkeypatch.setenv("CRYPTO_OF_XACK_RETRIES", "2")
        monkeypatch.setenv("CRYPTO_OF_XACK_BACKOFF_MS", "0")
        service.ticks.xack = AsyncMock(side_effect=rexc.ConnectionError("Connection lost"))
        with patch("services.crypto_orderflow_service.safe_create_task") as spawn:
            await service._xack_pipeline(
                stream="stream:book_TEST", group="g", ids=["1-0"], symbol="TEST", op="book",
            )
            # Initial + 2 retries = 3 attempts
            assert service.ticks.xack.await_count == 3
            spawn.assert_called_once()

    @pytest.mark.asyncio
    async def test_chunking_preserved(self, service, monkeypatch):
        """Batch is chunked; each chunk ACKed once on success."""
        monkeypatch.setenv("CRYPTO_OF_XACK_RETRIES", "0")
        monkeypatch.setenv("CRYPTO_OF_XACK_BACKOFF_MS", "0")
        service._ack_batch = 2
        await service._xack_pipeline(
            stream="s", group="g",
            ids=["1-0", "2-0", "3-0", "4-0", "5-0"],
            symbol="TEST", op="book",
        )
        # 3 chunks: [1,2], [3,4], [5]
        assert service.ticks.xack.await_count == 3

    @pytest.mark.asyncio
    async def test_empty_ids_noop(self, service):
        await service._xack_pipeline(stream="s", group="g", ids=[], symbol="T", op="o")
        service.ticks.xack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_retries_env_override_to_zero(self, service, monkeypatch):
        """CRYPTO_OF_XACK_RETRIES=0 → single attempt, no retry."""
        monkeypatch.setenv("CRYPTO_OF_XACK_RETRIES", "0")
        monkeypatch.setenv("CRYPTO_OF_XACK_BACKOFF_MS", "0")
        service.ticks.xack = AsyncMock(side_effect=rexc.ConnectionError("Connection lost"))
        with patch("services.crypto_orderflow_service.safe_create_task"):
            await service._xack_pipeline(
                stream="s", group="g", ids=["1-0"], symbol="T", op="o",
            )
        assert service.ticks.xack.await_count == 1

    @pytest.mark.asyncio
    async def test_stream_response_error_retries(self, service, monkeypatch):
        """ResponseError containing 'XACK' should now be retried."""
        monkeypatch.setenv("CRYPTO_OF_XACK_RETRIES", "2")
        monkeypatch.setenv("CRYPTO_OF_XACK_BACKOFF_MS", "0")
        # Simulate a transient XACK failure (e.g. during rebalance)
        service.ticks.xack = AsyncMock(side_effect=[
            rexc.ResponseError("XACK failed: some transient reason"),
            1
        ])
        await service._xack_pipeline(
            stream="s", group="g", ids=["1-0"], symbol="T", op="o",
        )
        assert service.ticks.xack.await_count == 2

    @pytest.mark.asyncio
    async def test_stream_nogroup_does_not_retry(self, service, monkeypatch):
        """ResponseError containing 'NOGROUP' should NOT be retried (config error)."""
        monkeypatch.setenv("CRYPTO_OF_XACK_RETRIES", "3")
        monkeypatch.setenv("CRYPTO_OF_XACK_BACKOFF_MS", "0")
        service.ticks.xack = AsyncMock(side_effect=rexc.ResponseError("NOGROUP No such key 's' or consumer group 'g'"))
        with patch("services.crypto_orderflow_service.safe_create_task"):
            await service._xack_pipeline(
                stream="s", group="g", ids=["1-0"], symbol="T", op="o",
            )
        # Should fail immediately after 1 attempt
        assert service.ticks.xack.await_count == 1
