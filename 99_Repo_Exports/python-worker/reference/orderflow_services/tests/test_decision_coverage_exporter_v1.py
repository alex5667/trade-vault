from __future__ import annotations
"""
Tests for P66 Decision Coverage exporter (decision_coverage_exporter_v1).

Covers:
  - _set_metrics: correct gauge values from state dict
  - _set_metrics: age computation and zero handling
  - _read_state: graceful error handling
  - Compile check (no import errors when prometheus_client not installed in test env)
"""
from utils.time_utils import get_ny_time_millis

import time
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Import guard — prometheus_client may not be available in bare CI
# ---------------------------------------------------------------------------

try:
    import prometheus_client  # noqa: F401
    _HAS_PROM = True
except ImportError:
    _HAS_PROM = False

import pytest


@pytest.mark.skipif(not _HAS_PROM, reason="prometheus_client not available")
class TestSetMetrics:
    """Test _set_metrics translates Redis state dict → correct Prometheus gauge calls."""

    def _import_module(self):
        from orderflow_services import decision_coverage_exporter_v1 as mod
        return mod

    def test_basic_state(self):
        mod = self._import_module()
        d = {
            "last_ts_ms": str(get_ny_time_millis() - 5000),  # 5s ago
            "rolling_ok": "100",
            "rolling_warn": "20",
            "rolling_block": "5",
            "rolling_unknown": "2",
            "rolling_total": "127",
        }
        # Patch the module-level gauges
        with patch.object(mod.LAST_TS, "set") as mock_last_ts, \
             patch.object(mod.LAST_AGE, "set") as mock_age, \
             patch.object(mod.N24, "set") as mock_n24, \
             patch.object(mod.N24_REG, "labels", return_value=MagicMock()) as mock_n24_reg, \
             patch.object(mod.SHARE24, "labels", return_value=MagicMock()) as mock_share24:
            mod._set_metrics(d)
            mock_last_ts.assert_called_once()
            mock_age.assert_called_once()
            mock_n24.assert_called_once_with(127.0)

    def test_zero_total_no_division_error(self):
        """total=0 should not raise ZeroDivisionError; all shares should be 0."""
        mod = self._import_module()
        d = {
            "last_ts_ms": "0",
            "rolling_ok": "0",
            "rolling_warn": "0",
            "rolling_block": "0",
            "rolling_unknown": "0",
            "rolling_total": "0",
        }
        # Should not raise
        with patch.object(mod.LAST_TS, "set"), \
             patch.object(mod.LAST_AGE, "set"), \
             patch.object(mod.N24, "set"), \
             patch.object(mod.N24_REG, "labels", return_value=MagicMock()), \
             patch.object(mod.SHARE24, "labels", return_value=MagicMock()):
            mod._set_metrics(d)

    def test_empty_state_no_error(self):
        """Completely empty state dict (Redis key not yet populated) should not raise."""
        mod = self._import_module()
        with patch.object(mod.LAST_TS, "set"), \
             patch.object(mod.LAST_AGE, "set"), \
             patch.object(mod.N24, "set"), \
             patch.object(mod.N24_REG, "labels", return_value=MagicMock()), \
             patch.object(mod.SHARE24, "labels", return_value=MagicMock()):
            mod._set_metrics({})

    def test_age_positive_for_old_ts(self):
        """Age should be > 0 for a timestamp that is in the past."""
        mod = self._import_module()
        old_ts_ms = int((time.time() - 60) * 1000)  # 60 seconds ago
        d = {
            "last_ts_ms": str(old_ts_ms),
            "rolling_ok": "1", "rolling_warn": "0", "rolling_block": "0",
            "rolling_unknown": "0", "rolling_total": "1",
        }
        age_set = []
        with patch.object(mod.LAST_TS, "set"), \
             patch.object(mod.LAST_AGE, "set", side_effect=lambda v: age_set.append(v)), \
             patch.object(mod.N24, "set"), \
             patch.object(mod.N24_REG, "labels", return_value=MagicMock()), \
             patch.object(mod.SHARE24, "labels", return_value=MagicMock()):
            mod._set_metrics(d)
        assert age_set and age_set[0] > 50  # at least 50 seconds

    def test_read_state_redis_error_returns_empty(self):
        """_read_state should swallow exceptions and return {}."""
        mod = self._import_module()
        r = MagicMock()
        r.hgetall.side_effect = Exception("Redis connection refused")
        result = mod._read_state(r, "some:key")
        assert result == {}


class TestCompile:
    """Ensure the module is importable without a running Redis."""

    def test_worker_compiles(self):
        """decision_coverage_kpi_worker_v1 should compile without errors."""
        import py_compile
        from pathlib import Path
        p = Path(__file__).resolve().parents[1] / "decision_coverage_kpi_worker_v1.py"
        py_compile.compile(str(p), doraise=True)

    def test_exporter_compiles(self):
        """decision_coverage_exporter_v1 should compile without errors."""
        import py_compile
        from pathlib import Path
        p = Path(__file__).resolve().parents[1] / "decision_coverage_exporter_v1.py"
        py_compile.compile(str(p), doraise=True)
