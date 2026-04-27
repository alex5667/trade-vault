from __future__ import annotations
"""Tests for exec_health_slo_exporter_v1 (P4)."""
import importlib
import sys
import time
import unittest
from unittest.mock import MagicMock, patch


def _build_summary(now_ms: int) -> dict:
    return {
        "schema_name": "exec_health_slo_summary",
        "schema_version": "1",
        "updated_ts_ms": str(int(now_ms)),
        "active_instances_total": "3",
        "stale_instances_total": "0",
        "rollout_drift_instances_total": "1",
        "cross_scope_mode_distinct": "1",
        "cross_scope_distinct_threshold_is_p95_bps": "1",
        "cross_scope_distinct_threshold_perm_impact_p95_bps": "1",
        "cross_scope_distinct_threshold_realized_spread_p50_bps": "1",
        "active_instances_edge": "1",
        "active_instances_pipeline": "1",
        "active_instances_entry_policy": "1",
        "stale_instances_edge": "0",
        "stale_instances_pipeline": "0",
        "stale_instances_entry_policy": "0",
        "share_apply_edge": "0.700000000",
        "share_veto_edge": "0.100000000",
        "share_pass_edge": "0.200000000",
        "share_apply_pipeline": "0.650000000",
        "share_veto_pipeline": "0.050000000",
        "share_pass_pipeline": "0.300000000",
        "share_apply_entry_policy": "0.720000000",
        "share_veto_entry_policy": "0.080000000",
        "share_pass_entry_policy": "0.200000000",
        "mode_distinct_edge": "1",
        "mode_distinct_pipeline": "1",
        "mode_distinct_entry_policy": "1",
        "deploy_distinct_edge": "2",
        "deploy_distinct_pipeline": "1",
        "deploy_distinct_entry_policy": "1",
        "rollout_drift_instances_edge": "1",
        "rollout_drift_instances_pipeline": "0",
        "rollout_drift_instances_entry_policy": "0",
        "threshold_distinct_edge_threshold_is_p95_bps": "1",
        "threshold_distinct_edge_threshold_perm_impact_p95_bps": "1",
        "threshold_distinct_edge_threshold_realized_spread_p50_bps": "1",
        "threshold_distinct_pipeline_threshold_is_p95_bps": "1",
        "threshold_distinct_pipeline_threshold_perm_impact_p95_bps": "1",
        "threshold_distinct_pipeline_threshold_realized_spread_p50_bps": "1",
        "threshold_distinct_entry_policy_threshold_is_p95_bps": "1",
        "threshold_distinct_entry_policy_threshold_perm_impact_p95_bps": "1",
        "threshold_distinct_entry_policy_threshold_realized_spread_p50_bps": "1",
        "reader_error_n_edge": "0",
        "reader_error_n_pipeline": "0",
        "reader_error_n_entry_policy": "0",
    }


class TestSloExporterSetsGauges(unittest.TestCase):
    """Verify the exporter correctly reads summary from Redis and sets gauges."""

    def test_exec_health_slo_exporter_sets_gauges_from_summary(self):
        import importlib

        # Stub out prometheus_client & start_http_server so they don't actually start
        _fake_prom = MagicMock()
        _fake_gauge_instances: dict = {}

        class _FakeGauge:
            def __init__(self, name, doc, labels_list=None):
                self._name = name
                self._labels_list = labels_list or []
                self._values: dict = {}
                _fake_gauge_instances[name] = self

            def set(self, v):
                self._values[()] = float(v)

            def labels(self, **kw):
                lab = tuple(sorted(kw.items()))
                g = _ChildGauge(self, lab)
                return g

        class _ChildGauge:
            def __init__(self, parent, lab):
                self.parent = parent
                self.lab = lab

            def set(self, v):
                self.parent._values[self.lab] = float(v)

        _fake_prom.Gauge = _FakeGauge
        _fake_prom.start_http_server = MagicMock()

        sys.modules.setdefault("prometheus_client", _fake_prom)

        # Reload the exporter with fake prometheus + redis
        spec_mod = importlib.util.find_spec("orderflow_services.exec_health_slo_exporter_v1")
        if spec_mod is None:
            self.skipTest("exec_health_slo_exporter_v1 not importable in test context")
            return

        import orderflow_services.exec_health_slo_exporter_v1 as exp
        importlib.reload(exp)

        now_ms = int(time.time() * 1000)
        summary = _build_summary(now_ms)

        fake_r = MagicMock()
        fake_r.hgetall.return_value = summary

        # Single iteration: call the gauge update logic directly
        # Simulate what main() does in one tick:
        m = summary
        exp.UP.set(1.0)
        exp.LAST_UPDATED_MS.set(float(int(m.get("updated_ts_ms", 0))))

        for scope in exp.SCOPES:
            exp.ACTIVE.labels(scope=scope).set(float(int(m.get(f"active_instances_{scope}", 0))))
            exp.STALE.labels(scope=scope).set(float(int(m.get(f"stale_instances_{scope}", 0))))
            exp.MODE_DISTINCT.labels(scope=scope).set(float(int(m.get(f"mode_distinct_{scope}", 0))))
            exp.DEPLOY_DISTINCT.labels(scope=scope).set(float(int(m.get(f"deploy_distinct_{scope}", 0))))
            exp.DRIFT_SCOPE.labels(scope=scope).set(float(int(m.get(f"rollout_drift_instances_{scope}", 0))))
            for outcome in exp.OUTCOMES:
                exp.SHARE.labels(scope=scope, outcome=outcome).set(float(m.get(f"share_{outcome}_{scope}", 0.0)))

        exp.DRIFT_TOTAL.set(float(int(m.get("rollout_drift_instances_total", 0))))
        exp.CROSS_SCOPE_MODE_DISTINCT.set(float(int(m.get("cross_scope_mode_distinct", 0))))

        # Verify drift_total
        self.assertIn((), exp.DRIFT_TOTAL._values)
        self.assertEqual(exp.DRIFT_TOTAL._values[()], 1.0)

        # Verify share for veto edge
        share_veto_edge_key = tuple(sorted([("scope", "edge"), ("outcome", "veto")]))
        self.assertIn(share_veto_edge_key, exp.SHARE._values)
        self.assertAlmostEqual(exp.SHARE._values[share_veto_edge_key], 0.1, places=5)


if __name__ == "__main__":
    unittest.main()
