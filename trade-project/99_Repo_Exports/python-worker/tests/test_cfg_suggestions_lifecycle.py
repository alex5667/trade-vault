#!/usr/bin/env python3
"""
python-worker/tests/test_cfg_suggestions_lifecycle.py

Tests for cfg_suggestions_lifecycle logic using fakeredis.
"""
import json
import time

import fakeredis.aioredis
import pytest

from tools.cfg_suggestions_lifecycle import check_suggestions_health


@pytest.fixture
def r():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)

@pytest.mark.asyncio
async def test_pending_too_long(r):
    prefix = "cfg:suggestions:entry_policy"
    kind = "meta_freeze"
    scope = "BTCUSDT"
    sid = "sid_123"

    await r.set(f"{prefix}:latest:{kind}:{scope}", sid)

    old_ts = int((time.time() - 7200) * 1000)  # 2h ago
    meta = {
        "sid": sid,
        "kind": kind,
        "scope": scope,
        "created_at_ms": old_ts
    }
    await r.set(f"{prefix}:meta:{sid}", json.dumps(meta))

    summary, alerts = await check_suggestions_health(
        r, prefix, kind, [scope],
        max_created_age_ms=3600000  # 1h
    )

    assert summary["n_pending"] == 1
    assert any("pending_too_long" in a for a in alerts)
    assert any(sid in a for a in alerts)

@pytest.mark.asyncio
async def test_approved_not_applied(r):
    prefix = "cfg:suggestions:entry_policy"
    kind = "meta_freeze"
    scope = "ALL"
    sid = "sid_456"

    await r.set(f"{prefix}:latest:{kind}:{scope}", sid)

    created_ts = int((time.time() - 900) * 1000)  # 15m ago
    meta = {
        "sid": sid,
        "kind": kind,
        "scope": scope,
        "created_at_ms": created_ts
    }
    await r.set(f"{prefix}:meta:{sid}", json.dumps(meta))
    await r.hset(f"{prefix}:approvals:{sid}", "user1", "ok")

    summary, alerts = await check_suggestions_health(
        r, prefix, kind, [scope],
        max_approved_age_ms=600000  # 10m
    )

    assert summary["n_approved"] == 1
    assert any("approved_not_applied" in a for a in alerts)

@pytest.mark.asyncio
async def test_applied_no_alert(r):
    prefix = "cfg:suggestions:entry_policy"
    kind = "meta_freeze"
    scope = "ALL"
    sid = "sid_789"

    await r.set(f"{prefix}:latest:{kind}:{scope}", sid)

    created_ts = int((time.time() - 3600) * 1000)
    meta = {
        "sid": sid,
        "kind": kind,
        "scope": scope,
        "created_at_ms": created_ts
    }
    await r.set(f"{prefix}:meta:{sid}", json.dumps(meta))
    await r.set(f"{prefix}:applied:{sid}", "1")

    summary, alerts = await check_suggestions_health(r, prefix, kind, [scope])

    assert summary["n_applied"] == 1
    assert len(alerts) == 0

if __name__ == "__main__":
    pytest.main([__file__])
