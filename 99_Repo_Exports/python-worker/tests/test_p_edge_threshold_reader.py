from __future__ import annotations

import json
import os
import random
from collections.abc import Iterator
from typing import Any  # noqa: F401  — used by helper annotations

import pytest

from core.p_edge_threshold_calibrator import PEdgeThresholdCalibrator
from core.p_edge_threshold_reader import (
    PEdgeThresholdReader,
    get_reader,
    reset_reader_for_tests,
)
from core.redis_keys import RK


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


class _FakeRedis:
    """Minimal Redis substitute: GET/SET on a dict."""

    def __init__(self) -> None:
        self.kv: dict[str, bytes] = {}
        self.get_failures: int = 0
        self.get_calls: int = 0

    def get(self, key: str) -> bytes | None:
        self.get_calls += 1
        if self.get_failures > 0:
            self.get_failures -= 1
            raise RuntimeError("simulated redis error")
        return self.kv.get(key)

    def set(self, key: str, value: Any) -> None:
        if isinstance(value, (bytes, bytearray)):
            self.kv[key] = bytes(value)
        else:
            self.kv[key] = str(value).encode("utf-8")


def _build_snapshot(*, enforce: bool = True) -> dict[str, Any]:
    """Construct a calibrator from synthetic data and serialise."""
    cal = PEdgeThresholdCalibrator(
        enforce=enforce,
        target_ev_r=0.10,
        min_total_trades=50,
        min_kept_trades=30,
        recompute_gap_ms=0,
        hold_ms=0,
        abs_thresh=0.0,
        max_jump_abs=1.0,
        conformal_min_losses=10_000,
    )
    rng = random.Random(42)
    base_ms = 1_000_000
    for i in range(600):
        p = rng.uniform(0.40, 0.80)
        # Strong above-cut wins (target_ev_r = 0.10 satisfied at low τ).
        win = "WIN" if (p >= 0.55 and rng.random() < 0.80) or (p < 0.55 and rng.random() < 0.30) else "LOSS"
        cal.observe(
            symbol="BTCUSDT",
            regime="trend",
            kind="breakout",
            p_edge=p,
            r_multiple=1.5 if win == "WIN" else -1.0,
            result=win,
            ts_ms=base_ms + i * 1000,
        )
    return cal.snapshot()


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_redis() -> _FakeRedis:
    return _FakeRedis()


@pytest.fixture(autouse=True)
def _reset_singleton() -> Iterator[None]:
    """Reset module-level singleton before and after each test."""
    reset_reader_for_tests()
    yield
    reset_reader_for_tests()
    os.environ.pop("AUTOCAL_P_EDGE_READ_ENABLED", None)


# ---------------------------------------------------------------------------
# default / cold-path behaviour
# ---------------------------------------------------------------------------


def test_returns_caller_default_when_snapshot_missing(fake_redis: _FakeRedis) -> None:
    """No data in Redis → reader falls back to caller default (gate static_floor)."""
    r = PEdgeThresholdReader(fake_redis, refresh_ms=1, stale_ms=10)
    assert r.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout", default=0.55) == 0.55
    # Snapshot stays unhealthy.
    assert r.is_healthy() is False


def test_uses_calibrated_value_when_fresh(fake_redis: _FakeRedis) -> None:
    fake_redis.set(RK.AUTOCAL_P_EDGE_STATE, json.dumps(_build_snapshot(enforce=True)))
    r = PEdgeThresholdReader(fake_redis, refresh_ms=1, stale_ms=60_000)
    val = r.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout", default=0.55)
    # Synthetic data is high-EV everywhere → calibrator picks τ at or near floor (0.40).
    assert val < 0.55
    assert val >= 0.40
    assert r.is_healthy() is True


def test_returns_default_when_enforce_false(fake_redis: _FakeRedis) -> None:
    fake_redis.set(RK.AUTOCAL_P_EDGE_STATE, json.dumps(_build_snapshot(enforce=False)))
    r = PEdgeThresholdReader(fake_redis, refresh_ms=1, stale_ms=60_000)
    # Even with snapshot present, enforce=False short-circuits to default.
    assert r.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout", default=0.55) == 0.55


def test_unknown_symbol_uses_wildcard_parent(fake_redis: _FakeRedis) -> None:
    """Unknown symbol should fall back through the calibrator's hierarchy and
    eventually hit the ("*", "*", "*") wildcard bin — venue-wide anchor for
    illiquid alts."""
    fake_redis.set(RK.AUTOCAL_P_EDGE_STATE, json.dumps(_build_snapshot(enforce=True)))
    r = PEdgeThresholdReader(fake_redis, refresh_ms=1, stale_ms=60_000)
    val = r.p_min_for(symbol="UNKNOWN", regime="range", kind="absorption", default=0.58)
    # Wildcard parent is populated by every observe() call, so we get a
    # calibrated value below caller default.
    assert val < 0.58
    assert val >= 0.40


def test_empty_snapshot_returns_caller_default(fake_redis: _FakeRedis) -> None:
    """When the snapshot contains no bins, every query returns caller default."""
    empty = {"enforce": True, "target_ev_r": 0.10, "default_p_min": 0.55, "bins": []}
    fake_redis.set(RK.AUTOCAL_P_EDGE_STATE, json.dumps(empty))
    r = PEdgeThresholdReader(fake_redis, refresh_ms=1, stale_ms=60_000)
    assert r.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout", default=0.55) == 0.55


# ---------------------------------------------------------------------------
# TTL / refresh
# ---------------------------------------------------------------------------


def test_ttl_cache_avoids_per_call_redis_get(fake_redis: _FakeRedis) -> None:
    fake_redis.set(RK.AUTOCAL_P_EDGE_STATE, json.dumps(_build_snapshot(enforce=True)))
    r = PEdgeThresholdReader(fake_redis, refresh_ms=60_000, stale_ms=120_000)
    # First call triggers GET.
    r.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout", default=0.55)
    assert fake_redis.get_calls == 1
    # Next 100 calls within TTL must NOT touch Redis.
    for _ in range(100):
        r.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout", default=0.55)
    assert fake_redis.get_calls == 1


def test_force_refresh_loads_new_snapshot(fake_redis: _FakeRedis) -> None:
    fake_redis.set(RK.AUTOCAL_P_EDGE_STATE, json.dumps(_build_snapshot(enforce=True)))
    r = PEdgeThresholdReader(fake_redis, refresh_ms=60_000, stale_ms=120_000)
    assert r.force_refresh() is True
    v1 = r.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout", default=0.55)

    # Replace snapshot with disabled enforce → next force_refresh switches behavior.
    fake_redis.set(RK.AUTOCAL_P_EDGE_STATE, json.dumps(_build_snapshot(enforce=False)))
    assert r.force_refresh() is True
    v2 = r.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout", default=0.55)
    assert v1 != v2
    assert v2 == 0.55


def test_stale_snapshot_falls_back_to_default(fake_redis: _FakeRedis) -> None:
    """Once stale_ms is exceeded with no successful reload, reader returns default.

    Simulated by loading once successfully, then making subsequent refreshes
    fail (snapshot key removed AND Redis errors), and shifting the
    last_load_ok_ms back beyond stale_ms.
    """
    fake_redis.set(RK.AUTOCAL_P_EDGE_STATE, json.dumps(_build_snapshot(enforce=True)))
    r = PEdgeThresholdReader(fake_redis, refresh_ms=10, stale_ms=20)
    assert r.force_refresh() is True
    assert r.is_healthy() is True

    # Drop the key and cause subsequent refreshes to fail (so the old
    # calibrator object stays in place but never gets re-validated).
    del fake_redis.kv[RK.AUTOCAL_P_EDGE_STATE]
    fake_redis.get_failures = 9999

    # Shift load timestamp far into the past — past stale_ms.
    r._last_load_ok_ms -= 10_000  # type: ignore[attr-defined]
    assert r.is_healthy() is False
    # p_min_for triggers a refresh attempt (fails), then sees staleness
    # and returns the caller default.
    assert r.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout", default=0.55) == 0.55


# ---------------------------------------------------------------------------
# error paths
# ---------------------------------------------------------------------------


def test_redis_get_failure_falls_back_to_default(fake_redis: _FakeRedis) -> None:
    fake_redis.get_failures = 5
    r = PEdgeThresholdReader(fake_redis, refresh_ms=1, stale_ms=10_000)
    # Failure during first refresh — calibrator stays None, reader returns default.
    assert r.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout", default=0.55) == 0.55
    assert r.is_healthy() is False


def test_malformed_snapshot_does_not_crash(fake_redis: _FakeRedis) -> None:
    fake_redis.set(RK.AUTOCAL_P_EDGE_STATE, "not-json")
    r = PEdgeThresholdReader(fake_redis, refresh_ms=1, stale_ms=10_000)
    assert r.p_min_for(symbol="BTCUSDT", regime="trend", kind="breakout", default=0.55) == 0.55


# ---------------------------------------------------------------------------
# module-level singleton + enable flag
# ---------------------------------------------------------------------------


def test_get_reader_disabled_by_default() -> None:
    assert get_reader() is None


def test_get_reader_returns_singleton_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTOCAL_P_EDGE_READ_ENABLED", "1")
    monkeypatch.setattr(
        "core.redis_client.get_redis", lambda: _FakeRedis(),
    )
    r1 = get_reader()
    r2 = get_reader()
    assert r1 is not None
    assert r1 is r2


def test_get_reader_returns_none_when_redis_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTOCAL_P_EDGE_READ_ENABLED", "1")

    def _boom() -> Any:
        raise RuntimeError("no redis")

    monkeypatch.setattr("core.redis_client.get_redis", _boom)
    assert get_reader() is None
