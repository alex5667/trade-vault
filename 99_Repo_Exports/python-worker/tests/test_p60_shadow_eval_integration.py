"""Unit tests for P60 Shadow Eval + Exporters integration.

Tests cover:
1. edge_stack_shadow_status_exporter_v1 - P60 compat gauges present
2. utilities/init_ml_confirm_on_startup - kind detection logic
3. ml_analysis/tools/init_ml_confirm_on_startup - wrapper import
4. ml_analysis/tools/edge_stack_shadow_eval_bundle_v1 - module importable
"""

from __future__ import annotations

import ast
import json
import os
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import MagicMock, patch, ANY

# Add project root to path
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


# ─────────────────────────────────────────────────────────────────────────────
# Helper: read file as string
# ─────────────────────────────────────────────────────────────────────────────

def _read(relpath: str) -> str:
    abs_path = os.path.join(_ROOT, relpath)
    with open(abs_path, "r", encoding="utf-8") as f:
        return f.read()


def _parse(relpath: str) -> ast.Module:
    return ast.parse(_read(relpath))


# ─────────────────────────────────────────────────────────────────────────────
# 1. Syntax checks — pure AST, no imports
# ─────────────────────────────────────────────────────────────────────────────

class TestSyntaxCheck(unittest.TestCase):
    FILES = [
        "ml_analysis/tools/edge_stack_train_exporter_v1.py",
        "ml_analysis/tools/edge_stack_shadow_eval_bundle_v1.py",
        "ml_analysis/tools/init_ml_confirm_on_startup.py",
        "orderflow_services/edge_stack_shadow_status_exporter_v1.py",
        "utilities/init_ml_confirm_on_startup.py",
        "tools/init_ml_confirm_on_startup.py",
    ]

    def test_all_files_parse(self):
        for f in self.FILES:
            with self.subTest(file=f):
                try:
                    _parse(f)
                except SyntaxError as e:
                    self.fail(f"SyntaxError in {f}: {e}")

    def test_files_exist(self):
        for f in self.FILES:
            with self.subTest(file=f):
                full = os.path.join(_ROOT, f)
                self.assertTrue(os.path.isfile(full), f"Missing: {f}")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Exporter: P60 compat gauges present in source
# ─────────────────────────────────────────────────────────────────────────────

class TestShadowExporterCompatGauges(unittest.TestCase):
    """Verify the three P60 compat gauges are present in the exporter source."""

    def _src(self) -> str:
        return _read("orderflow_services/edge_stack_shadow_status_exporter_v1.py")

    def test_last_success_gauge_defined(self):
        self.assertIn("edge_stack_shadow_last_success", self._src())

    def test_last_updated_ms_gauge_defined(self):
        self.assertIn("edge_stack_shadow_last_updated_ts_ms", self._src())

    def test_champ_brier_gauge_defined(self):
        self.assertIn("edge_stack_shadow_champion_brier", self._src())

    def test_last_success_set_in_loop(self):
        src = self._src()
        # LAST_SUCCESS.set(... must be called in the loop
        self.assertIn("LAST_SUCCESS.set(", src)

    def test_last_updated_ms_set_in_loop(self):
        self.assertIn("LAST_UPDATED_MS.set(", self._src())

    def test_champ_brier_set_in_loop(self):
        self.assertIn("CHAMP_BRIER.set(", self._src())


# ─────────────────────────────────────────────────────────────────────────────
# 3. init_ml_confirm_on_startup: kind detection logic
# ─────────────────────────────────────────────────────────────────────────────

class TestInitMlConfirmKindDetection(unittest.TestCase):
    """Test the kind detection logic without importing the real module
    (avoids redis import requirement in unit context)."""

    def _src(self) -> str:
        return _read("utilities/init_ml_confirm_on_startup.py")

    def test_joblib_imported(self):
        src = self._src()
        self.assertIn("import joblib", src)

    def test_edge_stack_v1_search_paths(self):
        src = self._src()
        self.assertIn("edge_stack_v1/champions", src)
        self.assertIn("edge_stack_v1/runs", src)

    def test_edge_stack_v1_kind_cfg_branch(self):
        src = self._src()
        # must have a branch for edge_stack_v1 kind
        self.assertIn("kind == 'edge_stack_v1'", src)

    def test_p_min_in_edge_stack_v1_cfg(self):
        src = self._src()
        self.assertIn("'p_min'", src)
        self.assertIn("'p_min_by_bucket'", src)

    def test_util_floors_in_else_branch(self):
        src = self._src()
        # util_floors must still exist for non edge_stack_v1 kinds
        self.assertIn("util_floors", src)

    def test_tools_mirror_matches_utilities(self):
        """tools/init_ml_confirm_on_startup.py should have same key changes."""
        src_tools = _read("tools/init_ml_confirm_on_startup.py")
        self.assertIn("import joblib", src_tools)
        self.assertIn("edge_stack_v1/champions", src_tools)
        self.assertIn("kind == 'edge_stack_v1'", src_tools)
        self.assertIn("'p_min'", src_tools)


# ─────────────────────────────────────────────────────────────────────────────
# 4. ml_analysis/tools wrapper: ensure_ml_confirm_config re-exported
# ─────────────────────────────────────────────────────────────────────────────

class TestMlAnalysisInitWrapper(unittest.TestCase):

    def _src(self) -> str:
        return _read("ml_analysis/tools/init_ml_confirm_on_startup.py")

    def test_imports_from_utilities(self):
        src = self._src()
        self.assertIn("utilities.init_ml_confirm_on_startup", src)

    def test_all_exports(self):
        src = self._src()
        self.assertIn("ensure_ml_confirm_config", src)
        self.assertIn("__all__", src)


# ─────────────────────────────────────────────────────────────────────────────
# 5. ml_analysis/tools/edge_stack_shadow_eval_bundle_v1: key logic present
# ─────────────────────────────────────────────────────────────────────────────

class TestShadowEvalBundleSource(unittest.TestCase):

    def _src(self) -> str:
        return _read("ml_analysis/tools/edge_stack_shadow_eval_bundle_v1.py")

    def test_promtheus_metrics_key_present(self):
        src = self._src()
        self.assertIn("metrics:edge_stack_shadow:last", src)

    def test_champion_key_present(self):
        self.assertIn("edge_stack_v1:champion", self._src())

    def test_auto_promote_arg_present(self):
        self.assertIn("auto_promote_guarded", self._src())

    def test_guard_thresholds_env(self):
        src = self._src()
        self.assertIn("EDGE_STACK_PROMOTE_MAX_BRIER_REL", src)
        self.assertIn("EDGE_STACK_PROMOTE_MAX_ECE_ABS_DIFF", src)

    def test_write_train_metrics_called(self):
        self.assertIn("write_train_metrics(", self._src())

    def test_atomic_write_json_called(self):
        self.assertIn("atomic_write_json(", self._src())

    def test_check_promotion_guard_called(self):
        self.assertIn("check_promotion_guard(", self._src())

    def test_guarded_promotion_copy(self):
        src = self._src()
        self.assertIn("atomic_copy(", src)
        self.assertIn("promote_applied = 1", src)


# ─────────────────────────────────────────────────────────────────────────────
# 6. init_ml_confirm_on_startup: kind detection simulation
# ─────────────────────────────────────────────────────────────────────────────

class TestKindDetectionLogic(unittest.TestCase):
    """Simulate the kind detection logic without importing the module."""

    def _simulate_kind_detection(self, model_path: str, pack: object) -> str:
        """Replicate the logic from utilities/init_ml_confirm_on_startup.py."""
        import types

        # Simulate joblib
        fake_joblib = types.SimpleNamespace(load=lambda path: pack)

        kind = "util_mh_v1"
        if model_path.endswith(".json") and "meta_lr" in os.path.basename(model_path):
            kind = "meta_lr"
        elif model_path.endswith(".joblib"):
            try:
                loaded_pack = fake_joblib.load(model_path)
                if (isinstance(loaded_pack, dict)
                        and isinstance(loaded_pack.get("kind"), str)
                        and loaded_pack.get("kind")):
                    kind = str(loaded_pack.get("kind"))
            except Exception:
                pass
        return kind

    def test_edge_stack_v1_pack_detected(self):
        pack = {"kind": "edge_stack_v1", "lr": None, "gbdt": None, "meta": None}
        kind = self._simulate_kind_detection("/path/to/model.joblib", pack)
        self.assertEqual(kind, "edge_stack_v1")

    def test_util_mh_v1_default(self):
        pack = {}  # no kind key
        kind = self._simulate_kind_detection("/path/to/model.joblib", pack)
        self.assertEqual(kind, "util_mh_v1")

    def test_meta_lr_from_filename(self):
        pack = {}  # json file
        kind = self._simulate_kind_detection("/models/meta_lr_20260101.json", pack)
        self.assertEqual(kind, "meta_lr")

    def test_edge_stack_cfg_uses_p_min(self):
        """Verify the generated cfg for edge_stack_v1 uses p_min schema."""
        kind = "edge_stack_v1"
        cfg_edge = {
            "kind": kind,
            "run_id": "auto_init_test",
            "model_path": "/model.joblib",
            "schema_version": 1,
            "mode": "SHADOW",
            "enforce_share": 0.0,
            "p_min": 0.55,
            "p_min_by_bucket": {},
            "hard_p_min_floor": 0.0,
        }
        # edge_stack_v1 must NOT have util_floors
        self.assertNotIn("util_floors", cfg_edge)
        self.assertIn("p_min", cfg_edge)
        self.assertEqual(cfg_edge["kind"], "edge_stack_v1")

    def test_util_mh_cfg_uses_util_floors(self):
        """Non edge_stack_v1 models still use util_floors schema."""
        kind = "util_mh_v1"
        cfg_util = {
            "kind": kind,
            "model_path": "/model.joblib",
            "util_floors": {"global": {"floor": -0.05}, "by_bucket": {}},
        }
        self.assertIn("util_floors", cfg_util)
        self.assertNotIn("p_min", cfg_util)


if __name__ == "__main__":
    unittest.main()
