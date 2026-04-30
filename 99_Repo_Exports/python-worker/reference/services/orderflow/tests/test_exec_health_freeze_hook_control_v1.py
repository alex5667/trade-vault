from __future__ import annotations

import asyncio
import json

from services.orderflow.exec_health_freeze_hook import aread_exec_health_auto_freeze


class FakeRedis:
    """Fake async Redis that simulates hgetall for control/state keys and get for raw key."""

    def __init__(self, *, control=None, state=None, raw=None):
        self.control = control or {}
        self.state = state or {}
        self.raw = raw
        self.hgetall_calls = 0
        self.get_calls = 0

    async def hgetall(self, key: str):
        self.hgetall_calls += 1
        if key.endswith(':freeze_control:v1'):
            return dict(self.control)
        return dict(self.state)

    async def get(self, key: str):
        self.get_calls += 1
        return self.raw


def test_control_latch_blocks_even_if_raw_key_missing() -> None:
    """P7: control hash latch must block even when the raw TTL key is absent (deleted)."""
    control = {
        'effective_freeze_active': '1'
        'control_source': 'autoguard'
        'manual_ack_required': '1'
        'last_trigger_ts_ms': '10000'
        'updated_ts_ms': '10000'
        'freeze_reason': 'cross_scope_mode_mismatch'
        'schema_version': '1'
    }
    r = FakeRedis(control=control, raw=None)  # raw key = None (deleted)

    async def _run():
        return await aread_exec_health_auto_freeze(
            redis=r, scope='pipeline_ctl', now_ms=20_000, cache_ttl_ms=1, force=True
        )

    st = asyncio.run(_run())
    assert st.active is True
    assert st.freeze_reason == 'cross_scope_mode_mismatch'
    # Must NOT fall through to raw key lookup
    assert r.get_calls == 0


def test_hook_reads_control_hash_first() -> None:
    """P7: hook consults control hash before autoguard state and raw key.

    Verifies that hgetall is called (for control/state priority reads) and that
    the control hash result takes precedence over the raw freeze key.
    When control hash latch is set, raw key must NOT be consulted.
    """
    control = {
        'effective_freeze_active': '1'
        'control_source': 'autoguard'
        'manual_ack_required': '1'
        'last_trigger_ts_ms': '5000'
        'updated_ts_ms': '5000'
        'freeze_reason': 'rollout_drift'
    }
    raw = json.dumps({'freeze_active': 0, 'freeze_reason': '', 'freeze_until_ts_ms': 0, 'ts_ms': 1000})
    r = FakeRedis(control=control, raw=raw)

    async def _run():
        return await aread_exec_health_auto_freeze(
            redis=r, scope='pipeline_priority', now_ms=20_000, cache_ttl_ms=1, force=True
        )

    st = asyncio.run(_run())
    # Control hash (freeze=1) must win over raw key (freeze=0)
    assert st.active is True
    # Raw key must NOT be consulted
    assert r.get_calls == 0


def test_empty_control_falls_back_to_raw_key() -> None:
    """If control hash is empty, fall back to raw key (backward-compat with P5/P6)."""
    raw = json.dumps({'freeze_active': 1, 'freeze_reason': 'slo_check', 'freeze_until_ts_ms': 99_000, 'ts_ms': 10_000})
    r = FakeRedis(control={}, state={}, raw=raw)

    async def _run():
        return await aread_exec_health_auto_freeze(
            redis=r, scope='pipeline_compat', now_ms=20_000, cache_ttl_ms=1, force=True
        )

    st = asyncio.run(_run())
    assert st.active is True
    assert r.get_calls == 1


def test_fail_open_no_redis() -> None:
    """No Redis client must result in inactive state (fail-open)."""
    async def _run():
        return await aread_exec_health_auto_freeze(
            redis=None, scope='test', now_ms=1000, cache_ttl_ms=1, force=True
        )

    st = asyncio.run(_run())
    assert st.active is False
