"""Integration tests for latency contract wiring (stamp, observe, Redis state write)."""
from __future__ import annotations

import asyncio
import sys, os
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from services.observability.latency_contract import (
    stamp_feature_ready,
    observe_feature_ready_async,
    stamp_emit_and_observe_async,
    LatencyStateWriter,
)
from services.observability.latency_semconv import (
    FIELD_TS_EMIT_MS,
    FIELD_TS_FEATURE_MS,
    FIELD_TS_REDIS_READ_MS,
    FIELD_TS_EVENT_MS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_signal(event_ms=1000, redis_read_ms=1010, feature_ms=None, emit_ms=None):
    s = {
        FIELD_TS_EVENT_MS: event_ms,
        FIELD_TS_REDIS_READ_MS: redis_read_ms,
    }
    if feature_ms is not None:
        s[FIELD_TS_FEATURE_MS] = feature_ms
    if emit_ms is not None:
        s[FIELD_TS_EMIT_MS] = emit_ms
    return s


# ---------------------------------------------------------------------------
# stamp_feature_ready
# ---------------------------------------------------------------------------

class TestStampFeatureReady:
    def test_stamps_ts_feature_ms(self):
        sig = _make_signal()
        result = stamp_feature_ready(sig, now_ms=2000)
        assert result[FIELD_TS_FEATURE_MS] == 2000

    def test_picks_up_redis_read_from_tick(self):
        sig = {}
        tick = {"ingest_ts_ms": 1500, "event_ts_ms": 1000}
        stamp_feature_ready(sig, tick=tick, now_ms=2000)
        assert sig.get(FIELD_TS_REDIS_READ_MS) == 1500
        assert sig.get(FIELD_TS_EVENT_MS) == 1000

    def test_does_not_overwrite_existing_ts_redis_read_ms(self):
        sig = {FIELD_TS_REDIS_READ_MS: 999}
        tick = {"ingest_ts_ms": 1500}
        stamp_feature_ready(sig, tick=tick, now_ms=2000)
        assert sig[FIELD_TS_REDIS_READ_MS] == 999

    def test_no_tick_still_stamps_feature_ms(self):
        sig = {}
        stamp_feature_ready(sig, now_ms=3000)
        assert sig[FIELD_TS_FEATURE_MS] == 3000


# ---------------------------------------------------------------------------
# observe_feature_ready_async
# ---------------------------------------------------------------------------

class TestObserveFeatureReadyAsync:
    @pytest.mark.asyncio
    async def test_valid_delta_triggers_redis_write(self):
        sig = _make_signal(redis_read_ms=1000, feature_ms=1060)
        mock_redis = AsyncMock()
        mock_redis.hset = AsyncMock()
        mock_redis.expire = AsyncMock()

        writer = LatencyStateWriter(service="python_worker", min_update_ms=0)
        result = await observe_feature_ready_async(sig, redis_client=mock_redis, service="python_worker", symbol="BTCUSDT", writer=writer)
        assert mock_redis.hset.called

    @pytest.mark.asyncio
    async def test_missing_timestamps_no_redis_write(self):
        sig = {}  # no timestamps → delta = 0 → invalid, no write
        mock_redis = AsyncMock()
        writer = LatencyStateWriter(service="python_worker", min_update_ms=0)
        await observe_feature_ready_async(sig, redis_client=mock_redis, service="python_worker", symbol="BTCUSDT", writer=writer)
        assert not mock_redis.hset.called

    @pytest.mark.asyncio
    async def test_none_redis_no_crash(self):
        sig = _make_signal(redis_read_ms=1000, feature_ms=1050)
        # Must not raise even with None redis
        await observe_feature_ready_async(sig, redis_client=None, service="python_worker", symbol="BTCUSDT")


# ---------------------------------------------------------------------------
# stamp_emit_and_observe_async
# ---------------------------------------------------------------------------

class TestStampEmitAndObserveAsync:
    @pytest.mark.asyncio
    async def test_stamps_ts_emit_ms(self):
        sig = _make_signal(feature_ms=2000)
        await stamp_emit_and_observe_async(sig, redis_client=None, symbol="BTCUSDT", now_ms=2100)
        assert sig[FIELD_TS_EMIT_MS] == 2100

    @pytest.mark.asyncio
    async def test_non_monotonic_emit_no_crash(self):
        # feature_ms == emit_ms → delta == 0 → counter incremented but no crash
        sig = _make_signal(event_ms=1000, feature_ms=1100)
        await stamp_emit_and_observe_async(sig, redis_client=None, symbol="BTCUSDT", now_ms=1050)  # now < feature → non-mono

    @pytest.mark.asyncio
    async def test_writes_both_feature_to_emit_and_end_to_end(self):
        sig = _make_signal(event_ms=1000, redis_read_ms=1010, feature_ms=1050)
        mock_redis = AsyncMock()
        mock_redis.hset = AsyncMock()
        mock_redis.expire = AsyncMock()
        writer = LatencyStateWriter(service="python_worker", min_update_ms=0)
        await stamp_emit_and_observe_async(sig, redis_client=mock_redis, service="python_worker", symbol="BTCUSDT", now_ms=1120, writer=writer)
        # hset should be called twice (feature_to_emit + end_to_end_event)
        assert mock_redis.hset.call_count == 2


# ---------------------------------------------------------------------------
# LatencyStateWriter.write_async
# ---------------------------------------------------------------------------

class TestLatencyStateWriter:
    @pytest.mark.asyncio
    async def test_rate_limit_prevents_double_write(self):
        mock_redis = AsyncMock()
        mock_redis.hset = AsyncMock()
        mock_redis.expire = AsyncMock()
        writer = LatencyStateWriter(service="python_worker", min_update_ms=60_000)  # 60s rate limit
        payload = _make_signal(event_ms=1000, feature_ms=1050, emit_ms=1100)

        await writer.write_async(mock_redis, stage="feature_to_emit", symbol="BTCUSDT", duration_ms=50, payload=payload)
        await writer.write_async(mock_redis, stage="feature_to_emit", symbol="BTCUSDT", duration_ms=60, payload=payload)
        # Second call should be rate-limited
        assert mock_redis.hset.call_count == 1

    @pytest.mark.asyncio
    async def test_none_redis_no_error(self):
        writer = LatencyStateWriter(service="python_worker", min_update_ms=0)
        payload = _make_signal(event_ms=1000, feature_ms=1050)
        # Must not raise
        await writer.write_async(None, stage="feature_to_emit", symbol="BTCUSDT", duration_ms=50, payload=payload)

    @pytest.mark.asyncio
    async def test_symbol_not_in_allowlist_drops_key(self):
        mock_redis = AsyncMock()
        writer = LatencyStateWriter(service="python_worker", min_update_ms=0, allowlist={"BTCUSDT"}, symbol_mode="drop")
        payload = _make_signal(event_ms=1000, feature_ms=1050)
        await writer.write_async(mock_redis, stage="feature_to_emit", symbol="XYZUSDT", duration_ms=50, payload=payload)
        assert not mock_redis.hset.called
