from __future__ import annotations

import asyncio
import json

from services.orderflow.exec_health_freeze_hook import (
    aread_exec_health_auto_freeze,
    build_exec_health_auto_freeze_decision,
    parse_exec_health_auto_freeze,
)


class FakeRedis:
    """Minimal async Redis stub for freeze hook unit tests."""

    def __init__(self, value: str | None = None, raises: bool = False):
        self.value = value
        self.raises = raises
        self.get_calls = 0

    async def get(self, key: str):
        self.get_calls += 1
        if self.raises:
            raise RuntimeError("boom")
        return self.value


def test_parse_exec_health_auto_freeze_active_window() -> None:
    """Freeze is active when flag=1 and freeze_until_ts_ms is in the future."""
    raw = json.dumps({
        "freeze_active": 1,
        "freeze_reason": "cross_scope_mode_mismatch",
        "freeze_until_ts_ms": 20_000,
        "ts_ms": 10_000,
        "schema_version": 1,
    })
    st = parse_exec_health_auto_freeze(raw, now_ms=15_000)
    assert st.active is True
    assert st.freeze_reason == "cross_scope_mode_mismatch"
    assert st.freeze_until_ts_ms == 20_000


def test_parse_exec_health_auto_freeze_expired() -> None:
    """Freeze is inactive when freeze_until_ts_ms has passed."""
    raw = json.dumps({"freeze_active": 1, "freeze_until_ts_ms": 9_999, "ts_ms": 5_000})
    st = parse_exec_health_auto_freeze(raw, now_ms=10_000)
    assert st.active is False


def test_parse_exec_health_auto_freeze_missing_raw() -> None:
    """Missing payload returns inactive state (fail-open)."""
    st = parse_exec_health_auto_freeze(None)
    assert st.active is False
    assert st.freeze_reason == ""


def test_parse_exec_health_auto_freeze_malformed_json() -> None:
    """Malformed JSON returns inactive state (fail-open)."""
    st = parse_exec_health_auto_freeze("{not valid json}", now_ms=1000)
    assert st.active is False


def test_aread_exec_health_auto_freeze_uses_cache() -> None:
    """TTL cache: second call does not hit Redis within the cache window."""
    raw = json.dumps({"freeze_active": 1, "freeze_reason": "drift", "freeze_until_ts_ms": 20_000, "ts_ms": 10_000})
    r = FakeRedis(value=raw)

    async def _run():
        a = await aread_exec_health_auto_freeze(redis=r, scope="pipeline_test", now_ms=15_000, cache_ttl_ms=5_000)
        b = await aread_exec_health_auto_freeze(redis=r, scope="pipeline_test", now_ms=16_000, cache_ttl_ms=5_000)
        return a, b

    a, b = asyncio.run(_run())
    assert a.active is True and b.active is True
    # Only one Redis GET due to TTL cache
    assert r.get_calls == 1


def test_aread_exec_health_auto_freeze_force_bypasses_cache() -> None:
    """force=True bypasses cache and always reads from Redis."""
    raw = json.dumps({"freeze_active": 1, "freeze_reason": "drift", "freeze_until_ts_ms": 20_000, "ts_ms": 10_000})
    r = FakeRedis(value=raw)

    async def _run():
        a = await aread_exec_health_auto_freeze(redis=r, scope="entry_test", now_ms=15_000, cache_ttl_ms=5_000)
        b = await aread_exec_health_auto_freeze(redis=r, scope="entry_test", now_ms=16_000, cache_ttl_ms=5_000, force=True)
        return a, b

    a, b = asyncio.run(_run())
    assert a.active is True and b.active is True
    assert r.get_calls == 2  # force bypassed cache


def test_aread_exec_health_auto_freeze_redis_error_fails_open() -> None:
    """Redis errors return inactive state (fail-open)."""
    r = FakeRedis(raises=True)

    async def _run():
        # Use a unique scope to avoid TTL cache pollution from other tests
        return await aread_exec_health_auto_freeze(redis=r, scope="redis_error_test_unique", now_ms=15_000, cache_ttl_ms=100)

    st = asyncio.run(_run())
    assert st.active is False


def test_aread_exec_health_auto_freeze_no_redis_fails_open() -> None:
    """None redis returns inactive state (fail-open)."""

    async def _run():
        # Use a unique scope to avoid TTL cache pollution from other tests
        return await aread_exec_health_auto_freeze(redis=None, scope="no_redis_test_unique", now_ms=15_000)

    st = asyncio.run(_run())
    assert st.active is False


def test_build_exec_health_auto_freeze_decision() -> None:
    """Decision carries correct gate/reason_code/notes for publish path."""
    raw = json.dumps({"freeze_active": 1, "freeze_reason": "drift", "freeze_until_ts_ms": 20_000, "ts_ms": 10_000})
    st = parse_exec_health_auto_freeze(raw, now_ms=15_000)
    dec = build_exec_health_auto_freeze_decision(scope="pipeline", state=st)
    assert dec.block is True
    assert dec.gate == "ExecHealthAutoFreezeGate"
    assert dec.reason_code == "VETO_EXEC_HEALTH_AUTO_FREEZE"
    assert "freeze_until_ts_ms=20000" in dec.notes
    assert "scope=pipeline" in dec.notes
