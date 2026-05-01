from __future__ import annotations
"""
P0-2 regression: terminal deny (invariant / runtime-policy) must NOT be retried.

Coverage:
  - AsyncPublishResult.retryable=False for invariant_denied
  - AsyncPublishResult.retryable=False for runtime_policy_denied
  - xadd_json() does NOT enqueue to stream:publisher:retry on terminal deny
  - transient Redis failures (retryable=True) ARE enqueued
"""

import asyncio
import unittest
from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock, patch

from services.async_signal_publisher import AsyncPublishResult, StreamSink


def _make_publisher(redis_client=None):
    from services.async_signal_publisher import AsyncSignalPublisher
    r = redis_client or AsyncMock()
    pub = AsyncSignalPublisher(redis_client=r, source="test", max_retries=3)
    return pub


class TestTerminalDenyNotRetried(unittest.IsolatedAsyncioTestCase):
    async def test_invariant_denied_not_retried(self):
        mock_redis = AsyncMock()
        pub = _make_publisher(mock_redis)

        invariant_result = AsyncPublishResult(
            ok=False, raw_written=False, busy_loading=False, errors=1,
            retryable=False, status="invariant_denied",
        )
        sink = StreamSink(name="orders:queue", field="payload", maxlen=1000)

        with patch.object(pub, "xadd_json_internal", return_value=invariant_result):
            result = await pub.xadd_json(sink=sink, payload={"symbol": "BTCUSDT"}, symbol="BTCUSDT")

        assert result.ok is False
        assert result.retryable is False
        assert result.status == "invariant_denied"
        # stream:publisher:retry must NOT be written
        for call in mock_redis.xadd.call_args_list:
            assert "publisher:retry" not in str(call), f"Unexpected retry xadd: {call}"

    async def test_runtime_policy_denied_not_retried(self):
        mock_redis = AsyncMock()
        pub = _make_publisher(mock_redis)

        policy_result = AsyncPublishResult(
            ok=False, raw_written=False, busy_loading=False, errors=1,
            retryable=False, status="runtime_policy_denied",
        )
        sink = StreamSink(name="orders:queue", field="payload", maxlen=1000)

        with patch.object(pub, "xadd_json_internal", return_value=policy_result):
            result = await pub.xadd_json(sink=sink, payload={"symbol": "ETHUSDT"}, symbol="ETHUSDT")

        assert result.retryable is False
        assert result.status == "runtime_policy_denied"
        for call in mock_redis.xadd.call_args_list:
            assert "publisher:retry" not in str(call)

    async def test_redis_error_is_retried(self):
        """A transient Redis failure (retryable=True by default) should enqueue for retry."""
        mock_redis = AsyncMock()
        pub = _make_publisher(mock_redis)

        redis_error_result = AsyncPublishResult(
            ok=False, raw_written=False, busy_loading=False, errors=1,
            retryable=True, status="",
        )
        sink = StreamSink(name="signals:cryptoorderflow:BTCUSDT", field="payload", maxlen=1000)

        with patch.object(pub, "xadd_json_internal", return_value=redis_error_result):
            await pub.xadd_json(sink=sink, payload={"symbol": "BTCUSDT"}, symbol="BTCUSDT")

        # At least one xadd call should target the retry stream
        retry_calls = [c for c in mock_redis.xadd.call_args_list if "publisher:retry" in str(c)]
        assert retry_calls, "Expected retry enqueue for transient Redis error"


class TestAsyncPublishResultDefaults(unittest.TestCase):
    def test_default_retryable_is_true(self):
        r = AsyncPublishResult(ok=True, raw_written=True, busy_loading=False, errors=0)
        assert r.retryable is True
        assert r.status == ""

    def test_terminal_deny_fields(self):
        r = AsyncPublishResult(
            ok=False, raw_written=False, busy_loading=False, errors=1,
            retryable=False, status="invariant_denied",
        )
        assert r.retryable is False
        assert r.status == "invariant_denied"


if __name__ == "__main__":
    unittest.main()
