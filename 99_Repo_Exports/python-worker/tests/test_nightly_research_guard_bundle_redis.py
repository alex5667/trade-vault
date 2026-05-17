"""Tests: nightly_strategy_research_guard_bundle_v1 writes Redis hashes (not strings).

Verifies the contract expected by:
  - strategy_research_guard_state_exporter_v1 (HGETALL → _compute_state)
  - research_guard_blocker_v1 (HGETALL → evaluate_research_guard_gate)
  - research_guard_calibrator_service (_load_nightly_report via HGETALL)
"""
from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import MagicMock, patch


def _make_fake_redis(store: dict):
    """Minimal fake Redis that captures HSET calls and returns them via HGETALL."""

    class FakeRedis:
        def hset(self, key, mapping=None, **kwargs):
            store.setdefault(key, {}).update(mapping or {})

        def hgetall(self, key):
            return dict(store.get(key, {}))

        def get(self, key):
            return None  # ensure old GET path is never used

    class FakeRedisModule:
        @staticmethod
        def from_url(url, **kwargs):
            return FakeRedis()

    return FakeRedisModule()


def _stub_ml_modules():
    """Stub only the leaf ML modules that have heavy scipy/numpy dependencies.

    We do NOT stub `ml_analysis` or `ml_analysis.tools` themselves — those are
    real packages that need to load correctly. We only replace the compute modules
    that the bundle calls at runtime.
    """
    _LEAF_STUBS = {
        "ml_analysis.pbo_cscv": {
            "compute_pbo": lambda *a, **kw: {"pbo": 0.05, "cscv_splits": 4},
        },
        "ml_analysis.psr_dsr": {
            "probabilistic_sharpe_ratio": lambda *a, **kw: 0.97,
            "deflated_sharpe_ratio": lambda *a, **kw: 0.93,
        },
        "ml_analysis.reality_check": {
            "evaluate_rows": lambda rows, **kw: {
                "net_expectancy": 0.012,
                "precision_at_top_x": 0.58,
                "mean_r": 0.8,
                "downside_adjusted_return": 0.6,
                "hit_rate_conditioned_on_cost": 0.55,
                "primary_metric_value": 0.012,
            },
        },
    }
    for mod_name, attrs in _LEAF_STUBS.items():
        stub = types.ModuleType(mod_name)
        for attr, val in attrs.items():
            setattr(stub, attr, val)
        sys.modules[mod_name] = stub


class TestNightlyBundleRedisContract(unittest.TestCase):

    def setUp(self):
        _stub_ml_modules()
        # Import lazily after stubs are in place
        import importlib
        if "ml_analysis.tools.nightly_strategy_research_guard_bundle_v1" in sys.modules:
            del sys.modules["ml_analysis.tools.nightly_strategy_research_guard_bundle_v1"]
        self.bundle = importlib.import_module(
            "ml_analysis.tools.nightly_strategy_research_guard_bundle_v1"
        )

    def _run_main_with_dataset(self, dataset_rows, *, report_only="1"):
        import json
        import tempfile

        store: dict = {}
        fake_redis = _make_fake_redis(store)

        with (
            tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as ds_f,
            tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as rp_f,
        ):
            for row in dataset_rows:
                ds_f.write(json.dumps(row) + "\n")
            ds_path = ds_f.name
            rp_path = rp_f.name

        argv = [
            "prog",
            "--dataset", ds_path,
            "--redis-url", "redis://localhost:6379/0",
            "--report-path", rp_path,
        ]
        with (
            patch.object(sys, "argv", argv),
            patch.dict("os.environ", {"STRATEGY_RESEARCH_GUARD_REPORT_ONLY": report_only}),
            patch("redis.from_url", fake_redis.from_url),
        ):
            self.bundle.main()

        return store

    def test_summary_written_as_hash(self):
        store = self._run_main_with_dataset([])
        summary_key = "metrics:strategy_research_guard:last"
        self.assertIn(summary_key, store, "summary key must be written to Redis")
        summary = store[summary_key]
        self.assertIsInstance(summary, dict, "summary must be a hash (dict), not a string")

    def test_blocker_written_as_hash(self):
        store = self._run_main_with_dataset([])
        blocker_key = "cfg:research_guard:blocker:v1"
        self.assertIn(blocker_key, store, "blocker key must be written to Redis")
        blocker = store[blocker_key]
        self.assertIsInstance(blocker, dict, "blocker must be a hash (dict), not a string")

    def test_summary_has_updated_ts_ms(self):
        store = self._run_main_with_dataset([])
        summary = store["metrics:strategy_research_guard:last"]
        self.assertIn("updated_ts_ms", summary)
        self.assertGreater(int(summary["updated_ts_ms"]), 0)

    def test_blocker_has_updated_ts_ms(self):
        store = self._run_main_with_dataset([])
        blocker = store["cfg:research_guard:blocker:v1"]
        self.assertIn("updated_ts_ms", blocker)
        self.assertGreater(int(blocker["updated_ts_ms"]), 0)

    def test_blocker_has_report_only_field(self):
        store = self._run_main_with_dataset([], report_only="1")
        blocker = store["cfg:research_guard:blocker:v1"]
        self.assertIn("report_only", blocker)
        self.assertEqual(blocker["report_only"], "1")

    def test_blocker_fields_match_evaluate_gate_contract(self):
        """evaluate_research_guard_gate reads: blocked, report_only, reason, updated_ts_ms."""
        store = self._run_main_with_dataset([])
        blocker = store["cfg:research_guard:blocker:v1"]
        for field in ("blocked", "report_only", "reason", "updated_ts_ms"):
            self.assertIn(field, blocker, f"blocker hash must contain '{field}'")

    def test_summary_fields_match_compute_state_contract(self):
        """_compute_state reads: psr, dsr, pbo, updated_ts_ms from summary hash."""
        dataset = [
            {"variant": "default", "return": 0.01, "cost_bps": 2, "label": 1, "score": 0.7},
        ] * 10
        store = self._run_main_with_dataset(dataset)
        summary = store["metrics:strategy_research_guard:last"]
        for field in ("psr", "dsr", "pbo", "updated_ts_ms"):
            self.assertIn(field, summary, f"summary hash must contain '{field}'")

    def test_empty_dataset_does_not_crash(self):
        store = self._run_main_with_dataset([])
        self.assertIn("cfg:research_guard:blocker:v1", store)
        blocker = store["cfg:research_guard:blocker:v1"]
        # empty dataset → PSR=0 < 0.95 → blocker_active=True
        self.assertEqual(blocker["blocked"], "1")
        # REPORT-ONLY=1 means fail-open regardless
        self.assertEqual(blocker["report_only"], "1")

    def test_report_only_0_enforce_mode(self):
        dataset = [
            {"variant": "default", "return": 0.01, "cost_bps": 2, "label": 1, "score": 0.7},
        ] * 10
        store = self._run_main_with_dataset(dataset, report_only="0")
        blocker = store["cfg:research_guard:blocker:v1"]
        self.assertEqual(blocker["report_only"], "0")


class TestCalibServiceReadsHash(unittest.TestCase):
    """research_guard_calibrator_service._load_nightly_report must use HGETALL."""

    def test_hgetall_used_not_get(self):
        from services.research_guard_calibrator_service import _load_nightly_report

        mock_client = MagicMock()
        mock_client.hgetall.side_effect = lambda key: {
            "metrics:strategy_research_guard:last": {
                "psr": "0.97",
                "dsr": "0.93",
                "pbo": "0.05",
                "ece": "0.08",
                "brier": "0.20",
                "updated_ts_ms": "1700000000000",
                "ts_ms": "1700000000000",
            },
            "cfg:research_guard:blocker:v1": {
                "blocker_active": "0",
                "blocked": "0",
            },
        }.get(key, {})
        mock_client.get.return_value = None  # must NOT be called

        report = _load_nightly_report(mock_client)

        mock_client.get.assert_not_called()
        self.assertTrue(mock_client.hgetall.called)
        self.assertTrue(report.has_data)
        self.assertAlmostEqual(report.psr, 0.97, places=3)
        self.assertAlmostEqual(report.dsr, 0.93, places=3)
        self.assertAlmostEqual(report.pbo, 0.05, places=3)
        self.assertEqual(report.report_ts, 1700000000)

    def test_empty_hash_returns_no_data(self):
        from services.research_guard_calibrator_service import _load_nightly_report

        mock_client = MagicMock()
        mock_client.hgetall.return_value = {}
        report = _load_nightly_report(mock_client)
        self.assertFalse(report.has_data)


if __name__ == "__main__":
    unittest.main()
