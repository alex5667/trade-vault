"""Integration tests for _emit_outcome atomic Lua emit.

Requires a real Redis instance — Lua/XADD atomicity cannot be verified
with fakeredis when lupa is absent.

Run with:
    TEST_REDIS_URL=redis://localhost:6379 \
    python -m pytest tests/integration/test_gated_out_atomic_emit.py -v -m integration
"""
from __future__ import annotations

import json
import os

import pytest
import redis.asyncio as aioredis

from services.gated_out_outcome_tracker import tracker


@pytest.fixture
def redis_url() -> str:
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("TEST_REDIS_URL not set")
    return url


@pytest.fixture
async def r(redis_url: str):
    client = aioredis.from_url(redis_url, decode_responses=True)
    yield client
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.integration
class TestAtomicEmitIntegration:
    async def _cleanup(self, r, sid: str) -> None:
        dedup_key = tracker.OUTCOME_DEDUP_KEY_TPL.format(sid=sid)
        await r.delete(dedup_key)

    async def test_first_emit_writes_to_stream(self, r):
        sid = "integ-atomic-first"
        await self._cleanup(r, sid)
        payload = {"v": 2, "sid": sid, "symbol": "BTCUSDT", "outcome": "TP_HIT", "y": 1}

        ok = await tracker._emit_outcome(r, payload)

        assert ok is True
        rows = await r.xrevrange(tracker.OUTPUT_STREAM, "+", "-", count=50)
        sids_in_stream = [json.loads(row[1]["payload"])["sid"] for row in rows]
        assert sid in sids_in_stream

        marker = await r.get(tracker.OUTCOME_DEDUP_KEY_TPL.format(sid=sid))
        assert marker is not None

    async def test_second_emit_is_idempotent(self, r):
        sid = "integ-atomic-dedup"
        await self._cleanup(r, sid)
        payload = {"v": 2, "sid": sid, "symbol": "BTCUSDT", "outcome": "SL_HIT", "y": 0}

        ok1 = await tracker._emit_outcome(r, payload)
        ok2 = await tracker._emit_outcome(r, payload)

        assert ok1 is True
        assert ok2 is False

        rows = await r.xrange(tracker.OUTPUT_STREAM, "-", "+")
        matching = [
            row for row in rows
            if json.loads(row[1]["payload"]).get("sid") == sid
        ]
        assert len(matching) == 1

    async def test_marker_stores_stream_id(self, r):
        sid = "integ-atomic-marker"
        await self._cleanup(r, sid)
        payload = {"v": 2, "sid": sid, "symbol": "ETHUSDT", "outcome": "TIMEOUT"}

        await tracker._emit_outcome(r, payload)

        marker = await r.get(tracker.OUTCOME_DEDUP_KEY_TPL.format(sid=sid))
        assert marker is not None
        # Stream ID format: "{ms}-{seq}"
        assert "-" in marker

    async def test_missing_sid_does_not_write(self, r):
        before = await r.xlen(tracker.OUTPUT_STREAM)
        ok = await tracker._emit_outcome(r, {"v": 2, "sid": "", "symbol": "BTCUSDT"})
        after = await r.xlen(tracker.OUTPUT_STREAM)

        assert ok is False
        assert after == before
