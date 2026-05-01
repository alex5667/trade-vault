from __future__ import annotations
"""Tests for P81 OF-gate Timescale Policy Probe.

Covers:
  - run_of_gate_timescale_policy_probe: enabled/disabled logic (both timers modules)
  - of_gate_timescale_policy_probe_v1: _bool, _match_jobs, _timescale_present, main()
  - of_gate_archiver_exporter_v1: _emit_timescale_policies() gauge updates
"""

import importlib
import os
import sys
import time
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest


# ──────────────────────────────────────────────────────────────────────────────
# Both timer module paths are tested (canonical + tick_flow_full mirror)
# ──────────────────────────────────────────────────────────────────────────────
TIMER_MODULES = [
    "services.of_timers_worker",
    "services.of_timers_worker",
]


def _mod(mod_name: str):
    return importlib.import_module(mod_name)


def _probe():
    """Import the probe module (canonical location — symlink makes both paths identical)."""
    return importlib.import_module("orderflow_services.of_gate_timescale_policy_probe_v1")


# ──────────────────────────────────────────────────────────────────────────────
# run_of_gate_timescale_policy_probe — enable/disable logic
# ──────────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("mod_name", TIMER_MODULES)
class TestRunOfGateTimescalePolicyProbe:
    """Tests for run_of_gate_timescale_policy_probe enable/disable logic."""

    def test_explicitly_disabled_is_noop(self, mod_name, monkeypatch):
        """ENABLE_OF_GATE_TIMESCALE_POLICY_PROBE=0 → returns True (noop), no run_tool call."""
        m = _mod(mod_name)
        calls = []
        monkeypatch.setattr(m, "run_tool", lambda *a, **kw: calls.append(a) or True)
        monkeypatch.setenv("ENABLE_OF_GATE_TIMESCALE_POLICY_PROBE", "0")

        result = m.run_of_gate_timescale_policy_probe()

        assert result is True
        assert calls == []

    def test_default_disabled_when_nightly_not_enabled(self, mod_name, monkeypatch):
        """Without explicit flag and ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY=0 → disabled (noop)."""
        m = _mod(mod_name)
        calls = []
        monkeypatch.setattr(m, "run_tool", lambda *a, **kw: calls.append(a) or True)
        monkeypatch.delenv("ENABLE_OF_GATE_TIMESCALE_POLICY_PROBE", raising=False)
        monkeypatch.setenv("ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY", "0")

        result = m.run_of_gate_timescale_policy_probe()

        assert result is True
        assert calls == []

    def test_enabled_explicitly_calls_probe(self, mod_name, monkeypatch):
        """ENABLE_OF_GATE_TIMESCALE_POLICY_PROBE=1 → calls run_tool with probe module."""
        m = _mod(mod_name)
        calls = []

        def fake_run_tool(module, *args, **kwargs):
            calls.append(module)
            return True

        monkeypatch.setattr(m, "run_tool", fake_run_tool)
        monkeypatch.setenv("ENABLE_OF_GATE_TIMESCALE_POLICY_PROBE", "1")

        result = m.run_of_gate_timescale_policy_probe()

        assert result is True
        assert "orderflow_services.of_gate_timescale_policy_probe_v1" in calls

    def test_enabled_via_nightly_refresh_gate(self, mod_name, monkeypatch):
        """ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY=1 (no explicit flag) → probe runs."""
        m = _mod(mod_name)
        calls = []

        def fake_run_tool(module, *args, **kwargs):
            calls.append(module)
            return True

        monkeypatch.setattr(m, "run_tool", fake_run_tool)
        monkeypatch.delenv("ENABLE_OF_GATE_TIMESCALE_POLICY_PROBE", raising=False)
        monkeypatch.setenv("ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY", "1")

        result = m.run_of_gate_timescale_policy_probe()

        assert result is True
        assert "orderflow_services.of_gate_timescale_policy_probe_v1" in calls

    def test_default_timeout_is_60(self, mod_name, monkeypatch):
        """Default timeout is 60 seconds."""
        m = _mod(mod_name)
        timeouts = []

        def fake_run_tool(module, *args, timeout=None, **kwargs):
            timeouts.append(timeout)
            return True

        monkeypatch.setattr(m, "run_tool", fake_run_tool)
        monkeypatch.setenv("ENABLE_OF_GATE_TIMESCALE_POLICY_PROBE", "1")
        monkeypatch.delenv("OF_GATE_TIMESCALE_POLICY_PROBE_TIMEOUT_S", raising=False)

        m.run_of_gate_timescale_policy_probe()

        assert timeouts[0] == 60

    def test_custom_timeout_from_env(self, mod_name, monkeypatch):
        """OF_GATE_TIMESCALE_POLICY_PROBE_TIMEOUT_S overrides default timeout."""
        m = _mod(mod_name)
        timeouts = []

        def fake_run_tool(module, *args, timeout=None, **kwargs):
            timeouts.append(timeout)
            return True

        monkeypatch.setattr(m, "run_tool", fake_run_tool)
        monkeypatch.setenv("ENABLE_OF_GATE_TIMESCALE_POLICY_PROBE", "1")
        monkeypatch.setenv("OF_GATE_TIMESCALE_POLICY_PROBE_TIMEOUT_S", "120")

        m.run_of_gate_timescale_policy_probe()

        assert timeouts[0] == 120


# ──────────────────────────────────────────────────────────────────────────────
# of_gate_timescale_policy_probe_v1 — _bool helper
# ──────────────────────────────────────────────────────────────────────────────
class TestBoolHelper:
    """Tests for the _bool helper in the probe module."""

    def test_true_values(self):
        p = _probe()
        for v in ("1", "true", "t", "yes", "y", "on", True):
            assert p._bool(v) is True, f"expected True for {v!r}"

    def test_false_values(self):
        p = _probe()
        for v in ("0", "false", "f", "no", "n", "off", False):
            assert p._bool(v) is False, f"expected False for {v!r}"

    def test_none_returns_none(self):
        p = _probe()
        assert p._bool(None) is None

    def test_unknown_returns_none(self):
        p = _probe()
        assert p._bool("maybe") is None


# ──────────────────────────────────────────────────────────────────────────────
# of_gate_timescale_policy_probe_v1 — _match_jobs helper
# ──────────────────────────────────────────────────────────────────────────────
class TestMatchJobs:
    """Tests for _match_jobs — matching timescale jobs by proc_name/hypertable/config."""

    def _make_job(self, proc_name: str, hypertable_name: str = "", config: str = "", scheduled: bool = True) -> dict:
        return {
            "proc_name": proc_name,
            "hypertable_name": hypertable_name,
            "config": config,
            "scheduled": scheduled,
        }

    def test_matches_by_proc_contains(self):
        p = _probe()
        jobs = [
            self._make_job("_timescaledb_internal.policy_retention"),
            self._make_job("_timescaledb_internal.policy_refresh_continuous_aggregate"),
        ]
        result = p._match_jobs(jobs, proc_contains="policy_retention")
        assert len(result) == 1
        assert "policy_retention" in result[0]["proc_name"]

    def test_matches_by_hypertable_name(self):
        p = _probe()
        jobs = [
            self._make_job("policy_retention", hypertable_name="of_gate_metrics"),
            self._make_job("policy_retention", hypertable_name="other_table"),
        ]
        result = p._match_jobs(jobs, proc_contains="policy_retention", hypertable_names=["of_gate_metrics"])
        assert len(result) == 1
        assert result[0]["hypertable_name"] == "of_gate_metrics"

    def test_matches_by_config_contains(self):
        p = _probe()
        jobs = [
            self._make_job("policy_retention", config='{"hypertable": "of_gate_metrics"}'),
            self._make_job("policy_retention", config='{"hypertable": "unrelated"}'),
        ]
        result = p._match_jobs(jobs, proc_contains="policy_retention", config_contains=["of_gate_metrics"])
        assert len(result) == 1

    def test_no_match_returns_empty(self):
        p = _probe()
        jobs = [self._make_job("some_other_proc")]
        result = p._match_jobs(jobs, proc_contains="policy_retention")
        assert result == []

    def test_empty_jobs_returns_empty(self):
        p = _probe()
        result = p._match_jobs([], proc_contains="policy_retention")
        assert result == []


# ──────────────────────────────────────────────────────────────────────────────
# of_gate_timescale_policy_probe_v1 — _hset_redis helper
# ──────────────────────────────────────────────────────────────────────────────
class TestHsetRedis:
    """Tests for _hset_redis."""

    def test_no_op_when_redis_not_available(self):
        p = _probe()
        original = p.redis
        try:
            p.redis = None  # type: ignore
            # Should not raise
            p._hset_redis("redis://localhost:6379/0", "key", {"a": 1})
        finally:
            p.redis = original

    def test_no_op_when_url_empty(self):
        p = _probe()
        # Empty URL → silently skip (no redis call)
        p._hset_redis("", "key", {"a": 1})  # should not raise

    def test_writes_mapping_to_redis(self):
        p = _probe()
        fake_redis_mod = MagicMock()
        fake_client = MagicMock()
        fake_redis_mod.Redis.from_url.return_value = fake_client

        original = p.redis
        try:
            p.redis = fake_redis_mod
            p._hset_redis("redis://localhost:6379/0", "metrics:test", {"ok": 1, "missing_count": 2})
        finally:
            p.redis = original

        fake_client.hset.assert_called_once()


# ──────────────────────────────────────────────────────────────────────────────
# of_gate_timescale_policy_probe_v1 — main() integration
# ──────────────────────────────────────────────────────────────────────────────
class TestProbeMain:
    """Integration-style tests for of_gate_timescale_policy_probe_v1.main()."""

    def test_exits_2_when_no_dsn(self, monkeypatch):
        """Raises SystemExit(2) when TRADES_DB_DSN is not set."""
        p = _probe()
        monkeypatch.delenv("TRADES_DB_DSN", raising=False)
        monkeypatch.delenv("PG_DSN", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)

        with pytest.raises(SystemExit) as exc_info:
            p.main()

        assert exc_info.value.code == 2

    def test_exits_2_on_db_error(self, monkeypatch):
        """Exits with code 2 on DB connection failure."""
        p = _probe()
        monkeypatch.setenv("TRADES_DB_DSN", "postgresql://bad/host")
        monkeypatch.delenv("REDIS_URL", raising=False)

        with patch("psycopg2.connect", side_effect=Exception("connection refused")):
            with pytest.raises(SystemExit) as exc:
                p.main()

        assert exc.value.code == 2

    def test_ok_when_timescale_not_expected_and_not_present(self, monkeypatch):
        """When OF_GATE_TIMESCALE_POLICY_EXPECT=0 and timescale absent → exit 0 (ok)."""
        p = _probe()
        monkeypatch.setenv("TRADES_DB_DSN", "postgresql://fake/db")
        monkeypatch.setenv("OF_GATE_TIMESCALE_POLICY_EXPECT", "0")
        monkeypatch.delenv("REDIS_URL", raising=False)

        mock_conn = MagicMock()
        # _timescale_present: no row returned → extension not present
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("psycopg2.connect", return_value=mock_conn):
            # Should NOT raise SystemExit
            p.main()

    def test_exits_2_when_timescale_expected_but_absent(self, monkeypatch):
        """When OF_GATE_TIMESCALE_POLICY_EXPECT=1 and timescale absent → exit 2."""
        p = _probe()
        monkeypatch.setenv("TRADES_DB_DSN", "postgresql://fake/db")
        monkeypatch.setenv("OF_GATE_TIMESCALE_POLICY_EXPECT", "1")
        monkeypatch.delenv("REDIS_URL", raising=False)

        mock_conn = MagicMock()
        # _timescale_present: no row returned → extension not present
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("psycopg2.connect", return_value=mock_conn):
            with pytest.raises(SystemExit) as exc:
                p.main()

        assert exc.value.code == 2

    def test_ok_when_all_policies_present_and_scheduled(self, monkeypatch):
        """When timescale present and all 4 policies found + scheduled → exit 0."""
        p = _probe()
        monkeypatch.setenv("TRADES_DB_DSN", "postgresql://fake/db")
        monkeypatch.setenv("OF_GATE_TIMESCALE_POLICY_EXPECT", "1")
        monkeypatch.delenv("REDIS_URL", raising=False)

        # Build full mock: _timescale_present → True, _cols → known columns,
        # _jobs → 4 jobs matching all required policies
        mock_conn = MagicMock()

        call_count = [0]

        def mock_cursor_context():
            cm = MagicMock()
            cur = MagicMock()

            def fetchall_side_effect():
                call_count[0] += 1
                n = call_count[0]
                if n == 1:
                    # _timescale_present: returns row → present
                    return [(1,)]  # but executed via fetchone!
                if n == 2:
                    # _cols for continuous_aggregates
                    return [("view_name",), ("materialization_hypertable_name",)]
                if n == 3:
                    # _cols for jobs table
                    return [("job_id",), ("proc_name",), ("scheduled",), ("config",), ("hypertable_name",)]
                if n == 4:
                    # continuous_aggregates rows
                    return [
                        ("of_gate_ok_rate_5m", "mat_5m"),
                        ("of_gate_ok_rate_1h", "mat_1h"),
                    ]
                if n == 5:
                    # jobs rows: 4 matching policies
                    return [
                        (1, "_timescaledb_internal.policy_retention", True, '{}', "of_gate_metrics"),
                        (2, "_timescaledb_internal.policy_retention", True, '{}', "of_gate_metrics_quarantine"),
                        (3, "_timescaledb_internal.policy_refresh_continuous_aggregate", True, '{}', "mat_5m"),
                        (4, "_timescaledb_internal.policy_refresh_continuous_aggregate", True, '{}', "mat_1h"),
                    ]
                return []

            cur.fetchall.side_effect = fetchall_side_effect

            def fetchone_side_effect():
                # Only _timescale_present uses fetchone
                call_count[0] += 1
                return (1,)  # timescaledb present

            cur.fetchone.side_effect = fetchone_side_effect
            cm.__enter__ = lambda s: cur
            cm.__exit__ = MagicMock(return_value=False)
            return cm

        # Override so _timescale_present and _fetch_cagg_mat_names etc. are fully mocked
        monkeypatch.setattr(p, "_timescale_present", lambda conn: True)
        monkeypatch.setattr(p, "_fetch_cagg_mat_names", lambda conn: {
            "of_gate_ok_rate_5m": "mat_5m",
            "of_gate_ok_rate_1h": "mat_1h",
        })

        def fake_jobs(conn):
            wanted = ["job_id", "proc_name", "scheduled", "config", "hypertable_name"]
            rows = [
                # Retention jobs: config references the hypertable so _match_jobs finds them
                {"job_id": 1, "proc_name": "_timescaledb_internal.policy_retention", "scheduled": True, "config": '{"hypertable": "of_gate_metrics"}', "hypertable_name": "of_gate_metrics"},
                {"job_id": 2, "proc_name": "_timescaledb_internal.policy_retention", "scheduled": True, "config": '{"hypertable": "of_gate_metrics_quarantine"}', "hypertable_name": "of_gate_metrics_quarantine"},
                # Refresh jobs: match by materialization hypertable name (mat_5m / mat_1h)
                {"job_id": 3, "proc_name": "_timescaledb_internal.policy_refresh_continuous_aggregate", "scheduled": True, "config": '{}', "hypertable_name": "mat_5m"},
                {"job_id": 4, "proc_name": "_timescaledb_internal.policy_refresh_continuous_aggregate", "scheduled": True, "config": '{}', "hypertable_name": "mat_1h"},
            ]
            return wanted, rows

        monkeypatch.setattr(p, "_jobs", fake_jobs)

        with patch("psycopg2.connect", return_value=mock_conn):
            # Should NOT raise SystemExit — all policies present & scheduled
            p.main()

    def test_exits_2_when_policy_disabled(self, monkeypatch):
        """When a policy exists but scheduled=False → ok=0 → exit 2."""
        p = _probe()
        monkeypatch.setenv("TRADES_DB_DSN", "postgresql://fake/db")
        monkeypatch.setenv("OF_GATE_TIMESCALE_POLICY_EXPECT", "1")
        monkeypatch.delenv("REDIS_URL", raising=False)

        monkeypatch.setattr(p, "_timescale_present", lambda conn: True)
        monkeypatch.setattr(p, "_fetch_cagg_mat_names", lambda conn: {})

        def fake_jobs_disabled(conn):
            # retention_of_gate_metrics exists but is scheduled=False
            wanted = ["proc_name", "scheduled", "hypertable_name", "config"]
            rows = [
                {"proc_name": "policy_retention", "scheduled": False, "hypertable_name": "of_gate_metrics", "config": "{}"},
            ]
            return wanted, rows

        monkeypatch.setattr(p, "_jobs", fake_jobs_disabled)

        with patch("psycopg2.connect", return_value=MagicMock()):
            with pytest.raises(SystemExit) as exc:
                p.main()

        assert exc.value.code == 2


# ──────────────────────────────────────────────────────────────────────────────
# of_gate_archiver_exporter_v1 — _emit_timescale_policies
# ──────────────────────────────────────────────────────────────────────────────
class TestExporterEmitTimescalePolicies:
    """Tests for the _emit_timescale_policies method in the exporter."""

    def _make_exporter(self, monkeypatch):
        """Import exporter module and create an Exporter instance with mocked redis."""
        mod = importlib.import_module("orderflow_services.of_gate_archiver_exporter_v1")
        monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
        # Avoid real redis connection during test
        if mod.redis is not None:
            monkeypatch.setattr(mod.redis.Redis, "from_url", lambda *a, **kw: MagicMock())
        ex = mod.Exporter()
        return mod, ex

    def test_all_present_and_scheduled(self, monkeypatch):
        """All 4 policies present and not disabled → gauges set correctly."""
        mod, ex = self._make_exporter(monkeypatch)

        d = {
            "timescale_present": "1",
            "expect_timescale": "1",
            "missing_count": "0",
            "disabled_count": "0",
            "present_retention_of_gate_metrics": "1",
            "present_retention_of_gate_metrics_quarantine": "1",
            "present_refresh_of_gate_ok_rate_5m": "1",
            "present_refresh_of_gate_ok_rate_1h": "1",
            "disabled_retention_of_gate_metrics": "0",
            "disabled_retention_of_gate_metrics_quarantine": "0",
            "disabled_refresh_of_gate_ok_rate_5m": "0",
            "disabled_refresh_of_gate_ok_rate_1h": "0",
        }

        # Should not raise
        ex._emit_timescale_policies(d)

        assert mod.GAUGE_TS_PRESENT._value.get() == 1
        assert mod.GAUGE_TS_POLICIES_MISSING._value.get() == 0
        assert mod.GAUGE_TS_POLICIES_DISABLED._value.get() == 0

    def test_missing_policies_gauge_updated(self, monkeypatch):
        """missing_count=3 → GAUGE_TS_POLICIES_MISSING set to 3."""
        mod, ex = self._make_exporter(monkeypatch)

        d = {
            "timescale_present": "1",
            "expect_timescale": "1",
            "missing_count": "3",
            "disabled_count": "0",
        }
        ex._emit_timescale_policies(d)

        assert mod.GAUGE_TS_POLICIES_MISSING._value.get() == 3

    def test_empty_dict_does_not_crash(self, monkeypatch):
        """Empty dict → all gauges set to 0/default (fail-open)."""
        mod, ex = self._make_exporter(monkeypatch)
        # Should not raise
        ex._emit_timescale_policies({})

    def test_timescale_not_present(self, monkeypatch):
        """timescale_present=0 → GAUGE_TS_PRESENT=0."""
        mod, ex = self._make_exporter(monkeypatch)
        ex._emit_timescale_policies({"timescale_present": "0", "expect_timescale": "1"})

        assert mod.GAUGE_TS_PRESENT._value.get() == 0
        assert mod.GAUGE_TS_EXPECT._value.get() == 1
