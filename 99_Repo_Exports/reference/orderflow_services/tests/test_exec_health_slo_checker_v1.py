from __future__ import annotations
"""Tests for exec_health_slo_checker_v1 (P4)."""
import importlib
import json
import unittest


def _import():
    import orderflow_services.exec_health_slo_checker_v1 as m
    return m


def _make_row(scope, mode, deploy, thr_is=1.0, thr_imp=2.0, thr_rs=-999.0, *, now_ms=1_700_000_000_000, age_ms=0, total=10, apply=7, veto=1, pass_=2):
    return {
        "scope": scope,
        "last_mode": mode,
        "deploy_id": deploy,
        "updated_ts_ms": str(int(now_ms - age_ms)),
        "threshold_is_p95_bps": str(thr_is),
        "threshold_perm_impact_p95_bps": str(thr_imp),
        "threshold_realized_spread_p50_bps": str(thr_rs),
        "total_n": str(total),
        "apply_n": str(apply),
        "veto_n": str(veto),
        "pass_n": str(pass_),
        "reader_error_n": "0",
    }


class TestExecHealthSloCheckerBuilds(unittest.TestCase):
    def test_exec_health_slo_checker_builds_drift_and_share_summary(self):
        m = _import()
        now = 1_700_000_000_000
        stale = 90_000

        rows = [
            _make_row("edge", "tighten", "sha1", thr_is=1.0, now_ms=now, age_ms=1000, total=100, apply=70, veto=20, pass_=10),
            _make_row("edge", "tighten", "sha1", thr_is=1.0, now_ms=now, age_ms=2000, total=100, apply=70, veto=20, pass_=10),
            # drift: different mode
            _make_row("edge", "monitor", "sha2", thr_is=1.0, now_ms=now, age_ms=3000, total=100, apply=80, veto=5, pass_=15),
            _make_row("pipeline", "tighten", "sha1", thr_is=1.0, now_ms=now, age_ms=500),
            _make_row("entry_policy", "tighten", "sha1", thr_is=1.0, now_ms=now, age_ms=500),
        ]

        summary = m.build_summary(rows, now_ms=now, stale_ms=stale)

        # Basic structure assertions
        self.assertEqual(summary["schema_name"], "exec_health_slo_summary")
        self.assertEqual(int(summary["active_instances_total"]), 5)

        # Drift: one "edge" instance has mode=monitor while the other two have mode=tighten
        drift_edge = int(summary.get("rollout_drift_instances_edge", "-1"))
        self.assertGreaterEqual(drift_edge, 1, "expected at least 1 drift instance in edge scope")

        drift_total = int(summary["rollout_drift_instances_total"])
        self.assertGreaterEqual(drift_total, 1)

        # Share computed (edge total=300 apply=220 veto=45 pass=35)
        share_veto_edge = float(summary["share_veto_edge"])
        self.assertGreater(share_veto_edge, 0.0)

    def test_stale_rows_excluded(self):
        m = _import()
        now = 1_700_000_000_000
        stale = 90_000
        rows = [
            _make_row("edge", "tighten", "sha1", now_ms=now, age_ms=200_000),  # older than stale
        ]
        summary = m.build_summary(rows, now_ms=now, stale_ms=stale)
        self.assertEqual(int(summary["active_instances_total"]), 0)
        self.assertEqual(int(summary.get("stale_instances_edge", 0)), 1)

    def test_empty_rows(self):
        m = _import()
        summary = m.build_summary([], now_ms=1_700_000_000_000, stale_ms=90_000)
        self.assertEqual(int(summary["active_instances_total"]), 0)


if __name__ == "__main__":
    unittest.main()
