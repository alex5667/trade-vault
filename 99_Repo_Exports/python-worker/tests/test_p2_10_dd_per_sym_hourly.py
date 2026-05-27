"""P2.10 — Per-symbol daily cap + hourly rolling DD cap.

Covers:
  1.  Per-symbol: breach arms sym key (r_sum <= -KILL_SYM_R_LIMIT)
  2.  Per-symbol: no breach leaves sym key unarmed
  3.  Per-symbol: sticky within same UTC day
  4.  Per-symbol: UTC rollover resets sym key
  5.  Per-symbol: KILL_SYM_ENABLED=0 → sym query not run, no sym keys written
  6.  Per-symbol: KILL_SYM_LIST filters symbols (only listed symbols checked)
  7.  Per-symbol: reader is_armed_for_symbol returns True when enforce+armed
  8.  Per-symbol: reader is_armed_for_symbol fails-open on missing key
  9.  Per-symbol: reader is_armed_for_symbol shadow mode → False
 10.  Hourly: breach arms hourly key (r_sum_1h <= -KILL_HOURLY_R_LIMIT)
 11.  Hourly: no breach → hourly key unarmed
 12.  Hourly: NOT sticky — two cycles: first breach, second recovery → unarmed
 13.  Hourly: KILL_HOURLY_ENABLED=0 → hourly key not written
 14.  Hourly: reader is_armed_hourly returns True when enforce+armed
 15.  Hourly: reader is_armed_hourly shadow mode → False
 16.  Global + sym + hourly coexist in same check_once call
 17.  _query_hourly_pnl SQL uses 1-hour interval (smoke via fake cursor)
 18.  _query_symbol_pnl_today returns per-symbol sums
"""
from __future__ import annotations

import importlib
import time
from unittest.mock import MagicMock

import fakeredis
import pytest


# ──────────────────────────── helpers ────────────────────────────────────────

def _reload_svc(monkeypatch, **env):
    """Reload daily_dd_kill_switch_v1 with patched env vars."""
    defaults = {
        "KILL_DAILY_R_LIMIT": "15",
        "KILL_DAILY_PCT_LIMIT": "20",
        "DAILY_DD_KILLSWITCH_MODE": "enforce",
        "KILL_SYM_ENABLED": "0",
        "KILL_SYM_R_LIMIT": "5",
        "KILL_SYM_LIST": "",
        "KILL_HOURLY_ENABLED": "0",
        "KILL_HOURLY_R_LIMIT": "5",
        "DAILY_DD_EXCLUDE_VIRTUAL": "0",
    }
    defaults.update(env)
    for k, v in defaults.items():
        monkeypatch.setenv(k, str(v))
    import services.daily_dd_kill_switch_v1 as mod
    importlib.reload(mod)
    return mod


def _reload_reader(monkeypatch):
    import services.daily_dd_reader as rd
    importlib.reload(rd)
    rd.reset_reader_for_tests()
    return rd


class _FakeCursor:
    """Cursor that dispatches results by SQL content (GROUP BY symbol → sym, 1 hour → hourly)."""

    def __init__(self, global_row: dict, sym_rows: list, hourly_row: dict):
        self._global = global_row
        self._sym = sym_rows
        self._hourly = hourly_row
        self._row = None
        self._rows: list = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        sql_up = sql.upper()
        if "GROUP BY SYMBOL" in sql_up:
            self._row = self._sym[0] if self._sym else None
            self._rows = self._sym
        elif "1 HOUR" in sql_up or "INTERVAL" in sql_up:
            self._row = self._hourly
            self._rows = [self._hourly]
        else:
            self._row = self._global
            self._rows = [self._global]

    def fetchone(self):
        return self._row

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class _FakeConn:
    def __init__(self, global_row: dict, sym_rows: list, hourly_row: dict):
        self._global = global_row
        self._sym = sym_rows
        self._hourly = hourly_row
        self.closed = False
        self.committed = 0

    def cursor(self, *a, **kw):
        return _FakeCursor(self._global, self._sym, self._hourly)

    def commit(self):
        self.committed += 1

    def rollback(self):
        pass

    def close(self):
        self.closed = True


def _make_conn(
    *,
    r_sum: float = 0.0,
    pct_sum: float = 0.0,
    trades_count: int = 0,
    sym_rows: list | None = None,
    hourly_r_sum: float = 0.0,
):
    """Build a fake PG conn with preset cursor results dispatched by SQL content."""
    global_row = {"r_sum": r_sum, "pct_sum": pct_sum, "trades_count": trades_count}
    sym_data = sym_rows if sym_rows is not None else []
    hourly_row = {"r_sum": hourly_r_sum}
    return _FakeConn(global_row, sym_data, hourly_row)


RK_SYM_PREFIX = "risk:daily_dd:sym:"
RK_HOURLY = "risk:daily_dd:hourly"


# ──────────────────────── 1. Per-symbol breach arms key ──────────────────────

def test_per_sym_breach_arms_sym_key(monkeypatch):
    mod = _reload_svc(monkeypatch, KILL_SYM_ENABLED="1", KILL_SYM_R_LIMIT="5")
    r = fakeredis.FakeRedis(decode_responses=True)
    conn = _make_conn(sym_rows=[{"symbol": "BTCUSDT", "r_sum": -6.0}])
    mod.check_once(r=r, conn=conn)
    state = r.hgetall(RK_SYM_PREFIX + "BTCUSDT")
    assert state["kill_armed"] == "1"
    assert "BTCUSDT" in state.get("reason", "") or "sym_r_sum" in state.get("reason", "")


# ──────────────────────── 2. Per-symbol no breach → unarmed ──────────────────

def test_per_sym_no_breach_unarmed(monkeypatch):
    mod = _reload_svc(monkeypatch, KILL_SYM_ENABLED="1", KILL_SYM_R_LIMIT="5")
    r = fakeredis.FakeRedis(decode_responses=True)
    conn = _make_conn(sym_rows=[{"symbol": "ETHUSDT", "r_sum": -2.0}])
    mod.check_once(r=r, conn=conn)
    state = r.hgetall(RK_SYM_PREFIX + "ETHUSDT")
    assert state["kill_armed"] == "0"


# ──────────────────────── 3. Per-symbol sticky ──────────────────────────────

def test_per_sym_sticky_after_recovery(monkeypatch):
    mod = _reload_svc(monkeypatch, KILL_SYM_ENABLED="1", KILL_SYM_R_LIMIT="5")
    r = fakeredis.FakeRedis(decode_responses=True)

    # Step 1: breach
    conn1 = _make_conn(sym_rows=[{"symbol": "SOLUSDT", "r_sum": -7.0}])
    mod.check_once(r=r, conn=conn1)
    state1 = r.hgetall(RK_SYM_PREFIX + "SOLUSDT")
    assert state1["kill_armed"] == "1"
    breached_at = state1["breached_at_ms"]

    # Step 2: recovery — r_sum improves
    conn2 = _make_conn(sym_rows=[{"symbol": "SOLUSDT", "r_sum": -1.0}])
    mod.check_once(r=r, conn=conn2)
    state2 = r.hgetall(RK_SYM_PREFIX + "SOLUSDT")
    # Must remain armed (sticky for the UTC day)
    assert state2["kill_armed"] == "1"
    assert state2["breached_at_ms"] == breached_at


# ──────────────────────── 4. Per-symbol UTC rollover resets ──────────────────

def test_per_sym_utc_rollover_resets(monkeypatch):
    mod = _reload_svc(monkeypatch, KILL_SYM_ENABLED="1", KILL_SYM_R_LIMIT="5")
    r = fakeredis.FakeRedis(decode_responses=True)

    # Seed old armed state
    r.hset(RK_SYM_PREFIX + "BTCUSDT", mapping={
        "kill_armed": "1",
        "mode": "enforce",
        "breached_day_utc": "1999-01-01",  # yesterday
        "breached_at_ms": "1000",
        "reason": "stale",
        "updated_at_ms": "1000",
    })

    # Today's r_sum fine
    conn = _make_conn(sym_rows=[{"symbol": "BTCUSDT", "r_sum": -1.0}])
    mod.check_once(r=r, conn=conn)
    state = r.hgetall(RK_SYM_PREFIX + "BTCUSDT")
    assert state["kill_armed"] == "0"
    assert state.get("breached_day_utc", "") == ""


# ──────────────────────── 5. KILL_SYM_ENABLED=0 → no sym keys ──────────────

def test_kill_sym_disabled_no_sym_keys(monkeypatch):
    mod = _reload_svc(monkeypatch, KILL_SYM_ENABLED="0", KILL_SYM_R_LIMIT="5")
    r = fakeredis.FakeRedis(decode_responses=True)
    conn = _make_conn(sym_rows=[{"symbol": "BTCUSDT", "r_sum": -99.0}])
    mod.check_once(r=r, conn=conn)
    # No sym key should exist
    assert r.hgetall(RK_SYM_PREFIX + "BTCUSDT") == {}


# ──────────────────────── 6. KILL_SYM_LIST filters symbols ───────────────────

def test_kill_sym_list_filters(monkeypatch):
    mod = _reload_svc(
        monkeypatch,
        KILL_SYM_ENABLED="1", KILL_SYM_R_LIMIT="5", KILL_SYM_LIST="BTCUSDT",
    )
    r = fakeredis.FakeRedis(decode_responses=True)
    # Only BTCUSDT in sym_rows (as if PG filtered by the list)
    conn = _make_conn(sym_rows=[
        {"symbol": "BTCUSDT", "r_sum": -6.0},
    ])
    mod.check_once(r=r, conn=conn)
    # BTCUSDT armed
    assert r.hgetall(RK_SYM_PREFIX + "BTCUSDT")["kill_armed"] == "1"
    # ETHUSDT not queried → no key
    assert r.hgetall(RK_SYM_PREFIX + "ETHUSDT") == {}


# ──────────────────────── 7. Reader is_armed_for_symbol armed ────────────────

def test_reader_sym_armed_when_enforce(monkeypatch):
    monkeypatch.setenv("DAILY_DD_READER_ENABLED", "1")
    rd = _reload_reader(monkeypatch)

    r = fakeredis.FakeRedis(decode_responses=True)
    r.hset(RK_SYM_PREFIX + "BTCUSDT", mapping={
        "kill_armed": "1",
        "mode": "enforce",
        "updated_at_ms": str(int(time.time() * 1000)),
        "reason": "sym_r_sum=-6.00<=-5.00R",
    })
    reader = rd.DailyDdReader(r, refresh_ms=10)
    armed, reason = reader.is_armed_for_symbol("BTCUSDT")
    assert armed is True
    assert "sym_r_sum" in reason


# ──────────────────────── 8. Reader fails-open on missing sym key ─────────────

def test_reader_sym_failopen_missing_key(monkeypatch):
    rd = _reload_reader(monkeypatch)
    r = fakeredis.FakeRedis(decode_responses=True)
    reader = rd.DailyDdReader(r, refresh_ms=10)
    armed, reason = reader.is_armed_for_symbol("UNKNOWN")
    assert armed is False
    assert reason == ""


# ──────────────────────── 9. Reader sym shadow mode → False ──────────────────

def test_reader_sym_shadow_not_armed(monkeypatch):
    rd = _reload_reader(monkeypatch)
    r = fakeredis.FakeRedis(decode_responses=True)
    r.hset(RK_SYM_PREFIX + "ETHUSDT", mapping={
        "kill_armed": "1",
        "mode": "shadow",  # ← shadow
        "updated_at_ms": str(int(time.time() * 1000)),
        "reason": "sym_r_sum=-6.00<=-5.00R",
    })
    reader = rd.DailyDdReader(r, refresh_ms=10)
    armed, _ = reader.is_armed_for_symbol("ETHUSDT")
    assert armed is False


# ──────────────────────── 10. Hourly breach arms key ─────────────────────────

def test_hourly_breach_arms_key(monkeypatch):
    mod = _reload_svc(monkeypatch, KILL_HOURLY_ENABLED="1", KILL_HOURLY_R_LIMIT="5")
    r = fakeredis.FakeRedis(decode_responses=True)
    conn = _make_conn(hourly_r_sum=-6.0)
    mod.check_once(r=r, conn=conn)
    state = r.hgetall(RK_HOURLY)
    assert state["kill_armed"] == "1"
    assert "hourly_r_sum=-6.00" in state.get("reason", "")


# ──────────────────────── 11. Hourly no breach → unarmed ─────────────────────

def test_hourly_no_breach_unarmed(monkeypatch):
    mod = _reload_svc(monkeypatch, KILL_HOURLY_ENABLED="1", KILL_HOURLY_R_LIMIT="5")
    r = fakeredis.FakeRedis(decode_responses=True)
    conn = _make_conn(hourly_r_sum=-2.0)
    mod.check_once(r=r, conn=conn)
    state = r.hgetall(RK_HOURLY)
    assert state["kill_armed"] == "0"


# ──────────────────────── 12. Hourly NOT sticky — recovers ───────────────────

def test_hourly_not_sticky(monkeypatch):
    mod = _reload_svc(monkeypatch, KILL_HOURLY_ENABLED="1", KILL_HOURLY_R_LIMIT="5")
    r = fakeredis.FakeRedis(decode_responses=True)

    # First cycle: breach
    conn1 = _make_conn(hourly_r_sum=-6.0)
    mod.check_once(r=r, conn=conn1)
    assert r.hgetall(RK_HOURLY)["kill_armed"] == "1"

    # Second cycle: recovery (r_sum_1h improved)
    conn2 = _make_conn(hourly_r_sum=-1.0)
    mod.check_once(r=r, conn=conn2)
    # Hourly is NOT sticky → should become unarmed
    assert r.hgetall(RK_HOURLY)["kill_armed"] == "0"


# ──────────────────────── 13. KILL_HOURLY_ENABLED=0 → no key ─────────────────

def test_kill_hourly_disabled_no_key(monkeypatch):
    mod = _reload_svc(monkeypatch, KILL_HOURLY_ENABLED="0", KILL_HOURLY_R_LIMIT="5")
    r = fakeredis.FakeRedis(decode_responses=True)
    conn = _make_conn(hourly_r_sum=-99.0)
    mod.check_once(r=r, conn=conn)
    assert r.hgetall(RK_HOURLY) == {}


# ──────────────────────── 14. Reader is_armed_hourly enforce+armed ────────────

def test_reader_hourly_armed_enforce(monkeypatch):
    rd = _reload_reader(monkeypatch)
    r = fakeredis.FakeRedis(decode_responses=True)
    r.hset(RK_HOURLY, mapping={
        "kill_armed": "1",
        "mode": "enforce",
        "updated_at_ms": str(int(time.time() * 1000)),
        "reason": "hourly_r_sum=-6.00<=-5.00R",
    })
    reader = rd.DailyDdReader(r, refresh_ms=10)
    armed, reason = reader.is_armed_hourly()
    assert armed is True
    assert "hourly_r_sum" in reason


# ──────────────────────── 15. Reader hourly shadow → False ───────────────────

def test_reader_hourly_shadow_not_armed(monkeypatch):
    rd = _reload_reader(monkeypatch)
    r = fakeredis.FakeRedis(decode_responses=True)
    r.hset(RK_HOURLY, mapping={
        "kill_armed": "1",
        "mode": "shadow",
        "updated_at_ms": str(int(time.time() * 1000)),
        "reason": "hourly_r_sum=-6.00<=-5.00R",
    })
    reader = rd.DailyDdReader(r, refresh_ms=10)
    armed, _ = reader.is_armed_hourly()
    assert armed is False


# ──────────────────────── 16. Global + sym + hourly coexist ──────────────────

def test_all_three_caps_coexist(monkeypatch):
    mod = _reload_svc(
        monkeypatch,
        KILL_DAILY_R_LIMIT="15",
        KILL_SYM_ENABLED="1", KILL_SYM_R_LIMIT="5",
        KILL_HOURLY_ENABLED="1", KILL_HOURLY_R_LIMIT="5",
    )
    r = fakeredis.FakeRedis(decode_responses=True)
    conn = _make_conn(
        r_sum=-16.0, pct_sum=-1.0, trades_count=200,
        sym_rows=[
            {"symbol": "BTCUSDT", "r_sum": -6.0},
            {"symbol": "ETHUSDT", "r_sum": -1.0},
        ],
        hourly_r_sum=-6.0,
    )
    mod.check_once(r=r, conn=conn)

    # Global armed
    assert r.hgetall(mod.RK.DAILY_DD_STATE)["kill_armed"] == "1"
    # BTC sym armed, ETH not
    assert r.hgetall(RK_SYM_PREFIX + "BTCUSDT")["kill_armed"] == "1"
    assert r.hgetall(RK_SYM_PREFIX + "ETHUSDT")["kill_armed"] == "0"
    # Hourly armed
    assert r.hgetall(RK_HOURLY)["kill_armed"] == "1"


# ──────────────────────── 17. Hourly query smoke ─────────────────────────────

def test_query_hourly_sql_uses_interval(monkeypatch):
    mod = _reload_svc(monkeypatch)
    sqls = []

    class _CaptureCursor:
        def execute(self, sql, params=None):
            sqls.append(sql)
            self._row = {"r_sum": -3.0}

        def fetchone(self):
            return {"r_sum": -3.0}

        def fetchall(self):
            return []

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    cur = _CaptureCursor()
    mod._query_hourly_pnl(cur)
    assert sqls, "no SQL captured"
    assert "1 hour" in sqls[0].lower() or "interval" in sqls[0].lower()


# ──────────────────────── 18. Per-symbol query returns sums ──────────────────

def test_query_symbol_pnl_returns_sums(monkeypatch):
    mod = _reload_svc(monkeypatch, KILL_SYM_LIST="")  # no filter

    class _SymCursor:
        def execute(self, sql, params=None):
            self._rows = [
                {"symbol": "BTCUSDT", "r_sum": -3.5},
                {"symbol": "ETHUSDT", "r_sum": 1.2},
            ]

        def fetchall(self):
            return self._rows

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    cur = _SymCursor()
    result = mod._query_symbol_pnl_today(cur)
    assert result == pytest.approx({"BTCUSDT": -3.5, "ETHUSDT": 1.2})
