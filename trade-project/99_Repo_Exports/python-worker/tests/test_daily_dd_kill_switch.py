"""Tests for Plan 3.5 Daily Equity-Drawdown Kill-Switch.

Covers:
  - check_once: write state HASH with correct fields
  - threshold breach by R triggers kill_armed=1 sticky
  - threshold breach by pct triggers kill_armed=1
  - sticky semantics: even if PnL recovers, armed stays for the day
  - UTC midnight rollover clears armed
  - fail-open on Postgres errors (no kill on data missing)
  - reader: shadow mode does NOT report armed
  - reader: enforce + kill_armed=1 reports armed
  - reader: stale snapshot fails open
  - reader: fail-open on empty Redis
"""
from __future__ import annotations

import importlib
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock

import fakeredis
import pytest


# ─────────────────────────── helpers ────────────────────────────


def _reload_service(monkeypatch, **env):
    """Reload daily_dd_kill_switch_v1 with given env to pick up KILL_DAILY_* etc."""
    for k, v in env.items():
        monkeypatch.setenv(k, str(v))
    import services.daily_dd_kill_switch_v1 as mod
    importlib.reload(mod)
    return mod


def _fake_pg(r_sum: float, pct_sum: float, trades_count: int, *, raise_on_query=None):
    """Build a fake psycopg2 connection that returns (r_sum, pct_sum, trades_count)."""

    class FakeCursor:
        def __init__(self):
            self._row = None

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql, params=None):
            if raise_on_query is not None:
                raise raise_on_query
            self._row = {
                "r_sum": r_sum,
                "pct_sum": pct_sum,
                "trades_count": trades_count,
            }

        def fetchone(self):
            return self._row

        def close(self):
            pass

    class FakeConn:
        def __init__(self):
            self.closed = False
            self.committed = 0
            self.rollbacked = 0

        def cursor(self, *args, **kwargs):
            return FakeCursor()

        def commit(self):
            self.committed += 1

        def rollback(self):
            self.rollbacked += 1

        def close(self):
            self.closed = True

    return FakeConn()


# ─────────────────────────── service tests ────────────────────────────


def test_check_once_writes_state_no_breach(monkeypatch):
    mod = _reload_service(
        monkeypatch,
        KILL_DAILY_R_LIMIT="15",
        KILL_DAILY_PCT_LIMIT="20",
        DAILY_DD_KILLSWITCH_MODE="shadow",
    )
    r = fakeredis.FakeRedis(decode_responses=True)
    conn = _fake_pg(r_sum=-2.5, pct_sum=-3.0, trades_count=10)

    mod.check_once(r=r, conn=conn)

    state = r.hgetall(mod.RK.DAILY_DD_STATE)
    assert state["kill_armed"] == "0"
    assert state["mode"] == "shadow"
    assert float(state["r_sum"]) == pytest.approx(-2.5)
    assert float(state["pct_sum"]) == pytest.approx(-3.0)
    assert int(state["trades_count"]) == 10
    assert float(state["threshold_r"]) == 15.0
    assert float(state["threshold_pct"]) == 20.0


def test_check_once_breach_by_r_arms_kill(monkeypatch):
    mod = _reload_service(
        monkeypatch,
        KILL_DAILY_R_LIMIT="15",
        KILL_DAILY_PCT_LIMIT="20",
        DAILY_DD_KILLSWITCH_MODE="enforce",
    )
    r = fakeredis.FakeRedis(decode_responses=True)
    conn = _fake_pg(r_sum=-15.5, pct_sum=-3.0, trades_count=400)

    mod.check_once(r=r, conn=conn)

    state = r.hgetall(mod.RK.DAILY_DD_STATE)
    assert state["kill_armed"] == "1"
    assert state["mode"] == "enforce"
    assert "r_sum=-15.50" in state["reason"]
    assert int(state["breached_at_ms"]) > 0
    assert state["breached_day_utc"]  # non-empty


def test_check_once_breach_by_pct_arms_kill(monkeypatch):
    mod = _reload_service(
        monkeypatch,
        KILL_DAILY_R_LIMIT="15",
        KILL_DAILY_PCT_LIMIT="20",
        DAILY_DD_KILLSWITCH_MODE="enforce",
    )
    r = fakeredis.FakeRedis(decode_responses=True)
    conn = _fake_pg(r_sum=-3.0, pct_sum=-22.5, trades_count=500)

    mod.check_once(r=r, conn=conn)

    state = r.hgetall(mod.RK.DAILY_DD_STATE)
    assert state["kill_armed"] == "1"
    assert "pct_sum=-22.50" in state["reason"]


def test_check_once_sticky_after_recovery(monkeypatch):
    """If PnL recovers within the same UTC day, kill stays armed."""
    mod = _reload_service(
        monkeypatch,
        KILL_DAILY_R_LIMIT="10",
        KILL_DAILY_PCT_LIMIT="15",
        DAILY_DD_KILLSWITCH_MODE="enforce",
    )
    r = fakeredis.FakeRedis(decode_responses=True)

    # Step 1: breach
    conn = _fake_pg(r_sum=-12.0, pct_sum=-3.0, trades_count=100)
    mod.check_once(r=r, conn=conn)
    state1 = r.hgetall(mod.RK.DAILY_DD_STATE)
    assert state1["kill_armed"] == "1"
    breached_at = state1["breached_at_ms"]

    # Step 2: PnL recovers (well above threshold)
    conn2 = _fake_pg(r_sum=-1.0, pct_sum=-1.0, trades_count=120)
    mod.check_once(r=r, conn=conn2)
    state2 = r.hgetall(mod.RK.DAILY_DD_STATE)
    # Still armed because sticky for the UTC day.
    assert state2["kill_armed"] == "1"
    assert state2["breached_at_ms"] == breached_at  # not overwritten


def test_check_once_utc_rollover_resets(monkeypatch):
    """Different breached_day_utc → reset on next iteration."""
    mod = _reload_service(
        monkeypatch,
        KILL_DAILY_R_LIMIT="10",
        KILL_DAILY_PCT_LIMIT="15",
        DAILY_DD_KILLSWITCH_MODE="enforce",
    )
    r = fakeredis.FakeRedis(decode_responses=True)

    # Seed state from "yesterday" with armed=1
    r.hset(
        mod.RK.DAILY_DD_STATE,
        mapping={
            "kill_armed": "1",
            "mode": "enforce",
            "breached_day_utc": "1999-01-01",  # very old day
            "breached_at_ms": "1000",
            "reason": "stale_yesterday",
            "updated_at_ms": "1000",
        },
    )

    # Today's PnL is fine.
    conn = _fake_pg(r_sum=-2.0, pct_sum=-1.0, trades_count=5)
    mod.check_once(r=r, conn=conn)

    state = r.hgetall(mod.RK.DAILY_DD_STATE)
    assert state["kill_armed"] == "0"
    assert state["breached_day_utc"] == ""
    assert state["reason"] == ""


def test_check_once_failopen_on_pg_error(monkeypatch):
    """When trades_closed missing/SQL fails, kill_armed stays unset (fail-open)."""
    import psycopg2

    mod = _reload_service(
        monkeypatch,
        KILL_DAILY_R_LIMIT="15",
        KILL_DAILY_PCT_LIMIT="20",
        DAILY_DD_KILLSWITCH_MODE="enforce",
    )
    r = fakeredis.FakeRedis(decode_responses=True)
    conn = _fake_pg(
        r_sum=-999.0,
        pct_sum=-999.0,
        trades_count=0,
        raise_on_query=psycopg2.errors.UndefinedTable("trades_closed"),
    )

    mod.check_once(r=r, conn=conn)

    state = r.hgetall(mod.RK.DAILY_DD_STATE)
    # No state written because we returned early.
    assert state == {}
    assert conn.rollbacked == 1


def test_check_breach_helper_r_limit(monkeypatch):
    mod = _reload_service(
        monkeypatch, KILL_DAILY_R_LIMIT="10", KILL_DAILY_PCT_LIMIT="20",
    )
    # Equal to limit (boundary) → breach
    assert mod._check_breach(-10.0, -1.0)[0] is True
    assert mod._check_breach(-9.99, -1.0)[0] is False
    # Positive R never breaches
    assert mod._check_breach(5.0, 5.0)[0] is False


def test_check_breach_helper_pct_limit(monkeypatch):
    mod = _reload_service(
        monkeypatch, KILL_DAILY_R_LIMIT="10", KILL_DAILY_PCT_LIMIT="20",
    )
    assert mod._check_breach(-1.0, -20.0)[0] is True
    assert mod._check_breach(-1.0, -19.99)[0] is False


def test_utc_day_str_format(monkeypatch):
    mod = _reload_service(monkeypatch)
    # 2026-05-19 00:00:00 UTC
    ts = int(datetime(2026, 5, 19, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    assert mod._utc_day_str(ts) == "2026-05-19"
    # Late UTC same day
    ts2 = int(datetime(2026, 5, 19, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)
    assert mod._utc_day_str(ts2) == "2026-05-19"


# ─────────────────────────── reader tests ────────────────────────────


def test_reader_shadow_mode_never_armed(monkeypatch):
    monkeypatch.setenv("DAILY_DD_READER_ENABLED", "1")
    import services.daily_dd_reader as rd
    importlib.reload(rd)
    rd.reset_reader_for_tests()

    r = fakeredis.FakeRedis(decode_responses=True)
    r.hset(
        rd.RK.DAILY_DD_STATE,
        mapping={
            "kill_armed": "1",
            "mode": "shadow",  # ← shadow!
            "updated_at_ms": str(int(time.time() * 1000)),
            "reason": "would_have_armed",
        },
    )
    reader = rd.DailyDdReader(r, refresh_ms=10)
    armed, reason = reader.is_armed()
    assert armed is False
    assert reason == ""


def test_reader_enforce_armed(monkeypatch):
    import services.daily_dd_reader as rd
    importlib.reload(rd)
    rd.reset_reader_for_tests()

    r = fakeredis.FakeRedis(decode_responses=True)
    r.hset(
        rd.RK.DAILY_DD_STATE,
        mapping={
            "kill_armed": "1",
            "mode": "enforce",
            "updated_at_ms": str(int(time.time() * 1000)),
            "reason": "r_sum=-15.50<=-15.00R",
        },
    )
    reader = rd.DailyDdReader(r, refresh_ms=10)
    armed, reason = reader.is_armed()
    assert armed is True
    assert "r_sum" in reason


def test_reader_failopen_empty(monkeypatch):
    import services.daily_dd_reader as rd
    importlib.reload(rd)
    rd.reset_reader_for_tests()

    r = fakeredis.FakeRedis(decode_responses=True)
    reader = rd.DailyDdReader(r)
    armed, reason = reader.is_armed()
    assert armed is False
    assert reason == ""


def test_reader_stale_snapshot_failopen(monkeypatch):
    import services.daily_dd_reader as rd
    importlib.reload(rd)
    rd.reset_reader_for_tests()

    r = fakeredis.FakeRedis(decode_responses=True)
    # updated_at_ms = 10 minutes ago, stale_ms default 5 min
    stale_ts = int(time.time() * 1000) - 10 * 60 * 1000
    r.hset(
        rd.RK.DAILY_DD_STATE,
        mapping={
            "kill_armed": "1",
            "mode": "enforce",
            "updated_at_ms": str(stale_ts),
            "reason": "stale_test",
        },
    )
    reader = rd.DailyDdReader(r, refresh_ms=10, stale_ms=5 * 60 * 1000)
    armed, reason = reader.is_armed()
    assert armed is False  # stale → fail-open


def test_reader_disabled_via_env(monkeypatch):
    monkeypatch.setenv("DAILY_DD_READER_ENABLED", "0")
    import services.daily_dd_reader as rd
    importlib.reload(rd)
    rd.reset_reader_for_tests()

    ctx = MagicMock(spec=[])  # no .redis attribute
    armed, reason = rd.is_armed(ctx)
    assert armed is False
    assert reason == ""


def test_reader_ttl_cache(monkeypatch):
    """Subsequent calls within refresh_ms window don't hit Redis."""
    import services.daily_dd_reader as rd
    importlib.reload(rd)
    rd.reset_reader_for_tests()

    r = fakeredis.FakeRedis(decode_responses=True)
    r.hset(
        rd.RK.DAILY_DD_STATE,
        mapping={
            "kill_armed": "0",
            "mode": "enforce",
            "updated_at_ms": str(int(time.time() * 1000)),
        },
    )
    # Wrap to count calls.
    real_hgetall = r.hgetall
    calls = {"n": 0}

    def counting_hgetall(key):
        calls["n"] += 1
        return real_hgetall(key)

    r.hgetall = counting_hgetall  # type: ignore[method-assign]

    reader = rd.DailyDdReader(r, refresh_ms=10_000)  # long cache
    for _ in range(5):
        reader.is_armed()
    # Only one refresh expected.
    assert calls["n"] == 1
