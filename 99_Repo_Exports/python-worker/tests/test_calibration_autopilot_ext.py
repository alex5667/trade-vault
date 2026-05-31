"""Tests for the extended autopilot flags (ADAPTIVE_TTL + ENSEMBLE) and helpers."""
from __future__ import annotations

from unittest.mock import MagicMock

from orderflow_services.calibration_autopilot_v1 import (
    ALL_FLAGS,
    FLAG_ADAPTIVE_TTL,
    FLAG_ENSEMBLE,
    _activate_flag,
    _count_distinct_sources,
    read_autopilot_flag,
)


def _make_rc(hsetnx_val=1):
    rc = MagicMock()
    rc.hsetnx.return_value = hsetnx_val
    rc.hset.return_value = 1
    rc.hget.return_value = None
    rc.hgetall.return_value = {}
    return rc


def _make_cursor(val):
    cur = MagicMock()
    cur.fetchone.return_value = (val,)
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    return cur


def _make_conn(val):
    conn = MagicMock()
    conn.cursor.return_value = _make_cursor(val)
    return conn


def _make_counter():
    c = MagicMock()
    c.labels.return_value = c
    return c


def _make_gauge():
    g = MagicMock()
    g.labels.return_value = g
    return g


# ─── ALL_FLAGS includes new ones ─────────────────────────────────────────────


def test_all_flags_contains_new_flags():
    assert FLAG_ADAPTIVE_TTL in ALL_FLAGS
    assert FLAG_ENSEMBLE in ALL_FLAGS
    assert len(ALL_FLAGS) == 6


# ─── distinct sources counter ───────────────────────────────────────────────


def test_count_distinct_sources_returns_zero():
    conn = _make_conn(0)
    assert _count_distinct_sources(conn) == 0


def test_count_distinct_sources_returns_n():
    conn = _make_conn(3)
    assert _count_distinct_sources(conn) == 3


# ─── adaptive_ttl flag activation ───────────────────────────────────────────


def test_activate_adaptive_ttl_flag_first_time_returns_true():
    rc = _make_rc(hsetnx_val=1)
    c = _make_counter()
    g = _make_gauge()
    activated = _activate_flag(rc, FLAG_ADAPTIVE_TTL, 1_780_000_000_000, c, g)
    assert activated is True
    rc.hsetnx.assert_called_once()
    assert FLAG_ADAPTIVE_TTL in str(rc.hsetnx.call_args)


def test_activate_adaptive_ttl_flag_second_time_returns_false():
    rc = _make_rc(hsetnx_val=0)  # already set
    c = _make_counter()
    g = _make_gauge()
    assert _activate_flag(rc, FLAG_ADAPTIVE_TTL, 1, c, g) is False


def test_activate_ensemble_flag_first_time_returns_true():
    rc = _make_rc(hsetnx_val=1)
    c = _make_counter()
    g = _make_gauge()
    assert _activate_flag(rc, FLAG_ENSEMBLE, 1, c, g) is True


# ─── reader returns correct boolean for new flag names ─────────────────────


def test_read_adaptive_ttl_flag_active():
    rc = MagicMock()
    rc.hget.return_value = "1"
    assert read_autopilot_flag(rc, FLAG_ADAPTIVE_TTL) is True


def test_read_adaptive_ttl_flag_missing():
    rc = MagicMock()
    rc.hget.return_value = None
    assert read_autopilot_flag(rc, FLAG_ADAPTIVE_TTL) is False


def test_read_ensemble_flag_active():
    rc = MagicMock()
    rc.hget.return_value = "1"
    assert read_autopilot_flag(rc, FLAG_ENSEMBLE) is True
