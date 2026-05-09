from __future__ import annotations

"""Tests for signal_quality_gating/tools/of_gate_sre_monitor.py

Tests cover: pctl, compute_stats, build_alerts.
No Redis required — all pure functions are tested in isolation.

Uses importlib.util to load the file directly to avoid package naming conflicts
with python-worker/tools/.
"""


import importlib.util
import json
import os
import sys
import types

import pytest

# ---- Mock heavy dependencies BEFORE loading of_gate_sre_monitor ----
_sqg = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _ensure_mock(name: str, **attrs):
    if name not in sys.modules:
        m = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(m, k, v)
        sys.modules[name] = m
    return sys.modules[name]


def _mock_retry(operation, **kwargs):
    return operation()


# Mock redis (avoid connection attempts)
_ensure_mock("redis")

# Mock common namespace so common/__init__.py is never executed
_ensure_mock("common")
_ensure_mock("common.redis_errors", retry_redis_operation=_mock_retry)
_ensure_mock("common.backoff")
_ensure_mock("common.time_norm")

# Now safe to load of_gate_sre_monitor directly
_ogm_file = os.path.join(_sqg, "tools", "of_gate_sre_monitor.py")
_spec = importlib.util.spec_from_file_location("sqg_of_gate_sre_monitor", _ogm_file)
assert _spec is not None
_ogm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ogm)  # type: ignore[union-attr]

pctl = _ogm.pctl
compute_stats = _ogm.compute_stats
build_alerts = _ogm.build_alerts
_dist_l1 = _ogm._dist_l1
_scenario_key = _ogm._scenario_key
_parse_missing_legs = _ogm._parse_missing_legs


# ---------------------------------------------------------------------------
# pctl
# ---------------------------------------------------------------------------

class TestPctl:
    def test_empty_returns_zero(self) -> None:
        assert pctl([], 0.5) == 0.0

    def test_single_element(self) -> None:
        assert pctl([42.0], 0.5) == 42.0

    def test_median(self) -> None:
        xs = [1.0, 2.0, 3.0, 4.0, 5.0]
        assert pctl(xs, 0.5) == pytest.approx(3.0)

    def test_p0_returns_min(self) -> None:
        xs = [5.0, 1.0, 3.0, 2.0, 4.0]
        assert pctl(xs, 0.0) == pytest.approx(1.0)

    def test_p100_returns_max(self) -> None:
        xs = [5.0, 1.0, 3.0, 2.0, 4.0]
        assert pctl(xs, 1.0) == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# _dist_l1
# ---------------------------------------------------------------------------

class TestDistL1:
    def test_identical_dists(self) -> None:
        p = {"a": 0.5, "b": 0.5}
        assert _dist_l1(p, p) == pytest.approx(0.0)

    def test_completely_different(self) -> None:
        p = {"a": 1.0}
        q = {"b": 1.0}
        # |1-0| + |0-1| = 2.0
        assert _dist_l1(p, q) == pytest.approx(2.0)

    def test_partial_overlap(self) -> None:
        p = {"a": 0.6, "b": 0.4}
        q = {"a": 0.5, "b": 0.5}
        assert _dist_l1(p, q) == pytest.approx(0.2)


# ---------------------------------------------------------------------------
# _scenario_key
# ---------------------------------------------------------------------------

class TestScenarioKey:
    def test_prefers_scenario_v4(self) -> None:
        r = {"scenario_v4": "reversal_sweep", "scenario": "reversal"}
        assert _scenario_key(r) == "reversal_sweep"

    def test_falls_back_to_scenario(self) -> None:
        r = {"scenario": "continuation"}
        assert _scenario_key(r) == "continuation"

    def test_empty_returns_na(self) -> None:
        assert _scenario_key({}) == "na"


# ---------------------------------------------------------------------------
# _parse_missing_legs
# ---------------------------------------------------------------------------

class TestParseMissingLegs:
    def test_empty_field(self) -> None:
        assert _parse_missing_legs({}) == []
        assert _parse_missing_legs({"missing_legs": ""}) == []

    def test_valid_json_list(self) -> None:
        r = {"missing_legs": '["leg_a", "leg_b"]'}
        result = _parse_missing_legs(r)
        assert result == ["leg_a", "leg_b"]

    def test_malformed_returns_empty(self) -> None:
        r = {"missing_legs": "not_valid_json"}
        assert _parse_missing_legs(r) == []

    def test_truncates_to_12(self) -> None:
        legs = [f"leg_{i}" for i in range(20)]
        r = {"missing_legs": json.dumps(legs)}
        result = _parse_missing_legs(r)
        assert len(result) == 12


# ---------------------------------------------------------------------------
# compute_stats
# ---------------------------------------------------------------------------

class TestComputeStats:
    def _row(self, ok: int = 0, ok_soft: int = 0, lat: float = 1000.0,
             ml_lat: float = 500.0, exec_norm: float = 0.3,
             scenario: str = "reversal") -> dict:
        return {
            "ok": str(ok),
            "ok_soft": str(ok_soft),
            "latency_us": str(lat),
            "ml_latency_us": str(ml_lat),
            "exec_risk_norm": str(exec_norm),
            "scenario_v4": scenario,
            "_ts_ms": 1_000_000,
        }

    def test_empty_rows(self) -> None:
        stats = compute_stats([], None, dh_bad_th=0.7)
        assert stats["n"] == 0
        assert stats.get("ok_rate", 0.0) == pytest.approx(0.0)
        assert stats.get("soft_rate", 0.0) == pytest.approx(0.0)

    def test_all_ok(self) -> None:
        rows = [self._row(ok=1) for _ in range(10)]
        stats = compute_stats(rows, None, dh_bad_th=0.7)
        assert stats["n"] == 10
        assert stats["ok_rate"] == pytest.approx(1.0)
        assert stats.get("soft_rate", 0.0) == pytest.approx(0.0)

    def test_half_ok(self) -> None:
        rows = [self._row(ok=1) for _ in range(5)] + [self._row(ok=0) for _ in range(5)]
        stats = compute_stats(rows, None, dh_bad_th=0.7)
        assert stats["ok_rate"] == pytest.approx(0.5)

    def test_soft_rate(self) -> None:
        rows = [self._row(ok=0, ok_soft=1) for _ in range(4)] + [self._row(ok=0) for _ in range(6)]
        stats = compute_stats(rows, None, dh_bad_th=0.7)
        assert stats["soft_rate"] == pytest.approx(0.4)

    def test_latency_percentiles(self) -> None:
        rows = [self._row(lat=float(i * 100)) for i in range(1, 11)]
        stats = compute_stats(rows, None, dh_bad_th=0.7)
        assert stats["lat_p50_us"] > 0.0
        assert stats["lat_p99_us"] >= stats["lat_p50_us"]

    def test_scenario_distribution(self) -> None:
        rows = [self._row(scenario="reversal")] * 7 + [self._row(scenario="continuation")] * 3
        stats = compute_stats(rows, None, dh_bad_th=0.7)
        scen = stats["scenario_dist"]
        assert scen.get("reversal", 0) == pytest.approx(0.7)
        assert scen.get("continuation", 0) == pytest.approx(0.3)


# ---------------------------------------------------------------------------
# build_alerts
# ---------------------------------------------------------------------------

class TestBuildAlerts:
    def _cfg(self) -> dict:
        return {
            "min_n": 10,
            "ok_min": 0.10,
            "soft_max": 0.70,
            "lat_p99_us_max": 25000.0,
            "ml_lat_p99_us_max": 25000.0,
            "exec_p90_max": 0.90,
            "scenario_l1_max": 0.35,
            "scenario_max_share_max": 0.75,
            "src_bad_max": 0.02,
            "book_bad_max": 0.02,
            "dh_bad_max": 0.10,
        }

    def test_low_n_returns_early(self) -> None:
        stats = {"n": 5}
        alerts = build_alerts(stats, cfg=self._cfg())
        assert len(alerts) == 1
        assert alerts[0]["code"] == "low_n"

    def test_ok_rate_low_alert(self) -> None:
        stats = {"n": 1000, "ok_rate": 0.05, "soft_rate": 0.2,
                 "lat_p99_us": 1000.0, "ml_lat_p99_us": 1000.0,
                 "exec_p90": 0.3, "scenario_l1": 0.1,
                 "scenario_max_share": 0.5, "source_inconsistency_rate": 0.0,
                 "book_bad_rate": 0.0, "data_health_bad_rate": 0.0}
        alerts = build_alerts(stats, cfg=self._cfg())
        codes = [a["code"] for a in alerts]
        assert "ok_rate_low" in codes

    def test_soft_rate_high_alert(self) -> None:
        stats = {"n": 1000, "ok_rate": 0.15, "soft_rate": 0.8,
                 "lat_p99_us": 1000.0, "ml_lat_p99_us": 1000.0,
                 "exec_p90": 0.3, "scenario_l1": 0.1,
                 "scenario_max_share": 0.5, "source_inconsistency_rate": 0.0,
                 "book_bad_rate": 0.0, "data_health_bad_rate": 0.0}
        alerts = build_alerts(stats, cfg=self._cfg())
        codes = [a["code"] for a in alerts]
        assert "soft_rate_high" in codes

    def test_no_alerts_when_healthy(self) -> None:
        stats = {"n": 1000, "ok_rate": 0.25, "soft_rate": 0.3,
                 "lat_p99_us": 1000.0, "ml_lat_p99_us": 1000.0,
                 "exec_p90": 0.5, "scenario_l1": 0.1,
                 "scenario_max_share": 0.5, "source_inconsistency_rate": 0.0,
                 "book_bad_rate": 0.0, "data_health_bad_rate": 0.0}
        alerts = build_alerts(stats, cfg=self._cfg())
        assert alerts == []
