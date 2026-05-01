from __future__ import annotations
"""Tests for P78 Nightly OF-Gate Rollups Refresh timer.

Covers:
  - _parse_hhmm: valid/invalid/edge inputs  (both canonical & tick_flow_full)
  - _in_safe_window_utc: normal window, midnight wrap, degenerate (start==end)
  - run_of_gate_rollups_refresh_nightly: disabled (noop), outside window (noop),
    inside window (calls run_tool), timeout/days env vars, bad env graceful fallback.
"""

import importlib
from datetime import datetime
from unittest.mock import patch

import pytest


# We parametrize the same tests over both module paths so the canonical
# and tick_flow_full copies are both exercised.
MODULES = [
    "services.of_timers_worker",
    "services.of_timers_worker",
]


@pytest.mark.parametrize("mod_name", MODULES)
class TestParseHHMM:
    """Unit tests for _parse_hhmm helper."""

    def _fn(self, mod_name):
        m = importlib.import_module(mod_name)
        return m._parse_hhmm

    def test_valid_input(self, mod_name):
        fn = self._fn(mod_name)
        assert fn("03:46", 0, 0) == (3, 46)
        assert fn("00:00", 0, 0) == (0, 0)
        assert fn("23:59", 0, 0) == (23, 59)

    def test_empty_returns_default(self, mod_name):
        fn = self._fn(mod_name)
        assert fn("", 2, 30) == (2, 30)
        assert fn(None, 5, 15) == (5, 15)
        assert fn("   ", 1, 0) == (1, 0)

    def test_invalid_format_returns_default(self, mod_name):
        fn = self._fn(mod_name)
        assert fn("not-a-time", 2, 30) == (2, 30)
        assert fn("25:00", 2, 30) == (2, 30)   # hour out of range
        assert fn("12:60", 2, 30) == (2, 30)   # minute out of range
        assert fn("-1:00", 2, 30) == (2, 30)   # negative hour

    def test_whitespace_stripped(self, mod_name):
        fn = self._fn(mod_name)
        assert fn("  04:15  ", 0, 0) == (4, 15)


@pytest.mark.parametrize("mod_name", MODULES)
class TestInSafeWindowUTC:
    """Unit tests for _in_safe_window_utc."""

    def _fn(self, mod_name):
        m = importlib.import_module(mod_name)
        return m._in_safe_window_utc

    def _dt(self, h, m):
        return datetime(2024, 1, 15, h, m, 0)

    def test_inside_normal_window(self, mod_name):
        fn = self._fn(mod_name)
        # Window 02:30 – 05:30
        assert fn(self._dt(2, 30), 2, 30, 5, 30) is True   # start == inclusive
        assert fn(self._dt(4, 0),  2, 30, 5, 30) is True
        assert fn(self._dt(5, 29), 2, 30, 5, 30) is True   # last minute inside

    def test_outside_normal_window(self, mod_name):
        fn = self._fn(mod_name)
        # Window 02:30 – 05:30
        assert fn(self._dt(5, 30), 2, 30, 5, 30) is False  # end == exclusive
        assert fn(self._dt(1, 59), 2, 30, 5, 30) is False
        assert fn(self._dt(22, 0), 2, 30, 5, 30) is False

    def test_midnight_wrap(self, mod_name):
        """Window that crosses midnight: e.g. 22:00 – 02:00."""
        fn = self._fn(mod_name)
        assert fn(self._dt(22, 0),  22, 0, 2, 0) is True
        assert fn(self._dt(23, 59), 22, 0, 2, 0) is True
        assert fn(self._dt(0, 0),   22, 0, 2, 0) is True
        assert fn(self._dt(1, 59),  22, 0, 2, 0) is True
        assert fn(self._dt(2, 0),   22, 0, 2, 0) is False  # exclusive end

    def test_degenerate_start_equals_end(self, mod_name):
        """start == end → always open (treat as unrestricted)."""
        fn = self._fn(mod_name)
        assert fn(self._dt(0, 0),   4, 0, 4, 0) is True
        assert fn(self._dt(12, 30), 4, 0, 4, 0) is True


@pytest.mark.parametrize("mod_name", MODULES)
class TestRunOfGateRollupsRefreshNightly:
    """Integration-style tests for run_of_gate_rollups_refresh_nightly."""

    def _mod(self, mod_name):
        return importlib.import_module(mod_name)

    def test_disabled_env_returns_true_noop(self, mod_name, monkeypatch):
        """When ENABLE=0 the function should return True without calling run_tool."""
        m = self._mod(mod_name)
        calls = []

        def fake_run_tool(module, args=None, timeout=None):
            calls.append((module, args, timeout))
            return True

        monkeypatch.setattr(m, "run_tool", fake_run_tool)
        monkeypatch.setenv("ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY", "0")

        result = m.run_of_gate_rollups_refresh_nightly()
        assert result is True
        assert calls == []

    def test_inside_window_calls_run_tool_with_correct_args(self, mod_name, monkeypatch):
        """Inside the safe window, should call run_tool with migration module and --days arg."""
        m = self._mod(mod_name)
        calls = []

        def fake_run_tool(module, args=None, timeout=None):
            calls.append((module, args, timeout))
            return True

        monkeypatch.setattr(m, "run_tool", fake_run_tool)
        monkeypatch.setenv("ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY", "1")
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_DAYS", "7")
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_TIMEOUT_S", "900")
        # Force safe window to be "always open" (start == end)
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_SAFE_START_UTC", "00:00")
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_SAFE_END_UTC", "00:00")

        result = m.run_of_gate_rollups_refresh_nightly()
        assert result is True
        assert len(calls) == 1
        module_called, args_called, timeout_called = calls[0]
        assert module_called == "orderflow_services.of_gate_history_migration_v1"
        assert args_called == ["refresh", "--days", "7"]
        assert timeout_called == 900

    def test_outside_window_returns_true_noop(self, mod_name, monkeypatch):
        """Outside safe window: returns True (skip) without calling run_tool."""
        m = self._mod(mod_name)
        calls = []

        def fake_run_tool(module, args=None, timeout=None):
            calls.append(module)
            return True

        monkeypatch.setattr(m, "run_tool", fake_run_tool)
        monkeypatch.setenv("ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY", "1")
        # Window 03:00 – 04:00. We patch utcnow() to be 01:00 (outside).
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_SAFE_START_UTC", "03:00")
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_SAFE_END_UTC", "04:00")

        with patch(f"{mod_name}.datetime") as mock_dt:
            mock_dt.utcnow.return_value = datetime(2024, 1, 15, 1, 0, 0)
            result = m.run_of_gate_rollups_refresh_nightly()

        assert result is True
        assert calls == []

    def test_default_days_and_timeout(self, mod_name, monkeypatch):
        """Without explicit DAYS/TIMEOUT env vars defaults should be 30 / 1800."""
        m = self._mod(mod_name)
        calls = []

        def fake_run_tool(module, args=None, timeout=None):
            calls.append((module, args, timeout))
            return True

        monkeypatch.setattr(m, "run_tool", fake_run_tool)
        monkeypatch.setenv("ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY", "1")
        # Remove override vars to test defaults
        monkeypatch.delenv("OF_GATE_ROLLUPS_REFRESH_DAYS", raising=False)
        monkeypatch.delenv("OF_GATE_ROLLUPS_REFRESH_TIMEOUT_S", raising=False)
        # Always-open window
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_SAFE_START_UTC", "00:00")
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_SAFE_END_UTC", "00:00")

        result = m.run_of_gate_rollups_refresh_nightly()
        assert result is True
        assert calls[0][1] == ["refresh", "--days", "30"]
        assert calls[0][2] == 1800

    def test_invalid_timeout_env_uses_default(self, mod_name, monkeypatch):
        """Garbage value for timeout falls back to 1800 without crashing."""
        m = self._mod(mod_name)
        calls = []

        def fake_run_tool(module, args=None, timeout=None):
            calls.append(timeout)
            return True

        monkeypatch.setattr(m, "run_tool", fake_run_tool)
        monkeypatch.setenv("ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY", "1")
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_TIMEOUT_S", "not-a-number")
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_SAFE_START_UTC", "00:00")
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_SAFE_END_UTC", "00:00")

        result = m.run_of_gate_rollups_refresh_nightly()
        assert result is True
        assert calls[0] == 1800
