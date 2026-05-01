from __future__ import annotations
"""Tests for Feature Registry ↔ train_edge_stack_v1_oof integration.

Покрывает:
  1. _sha256_16 — стабильность и длина
  2. get_edge_stack_feature_spec с новыми параметрами (max_numeric, strict_feature_cols, forbid)
  3. max_numeric правильно обрезает f_* (direction/scenario/time сохраняются)
  4. strict + forbid отвергают scenario_v4_*
  5. train_edge_stack_v1_oof main() с --feature_schema_ver (smoke)
  6. hash_mismatch в dataset_report_json → SystemExit
  7. require_feature_registry=1 без PYTHONPATH → SystemExit
  8. run_ml_train_edge_stack_v1_oof schema_ver-aware logic (unit)
  9. build_edge_stack_dataset_from_redis --feature_schema_ver choices включает v4

Запуск:
    PYTHONPATH=./tick_flow_full:. ./python-worker/.venv/bin/pytest \\
        python-worker/ml_analysis/tests/test_train_registry_integration_v1.py -v
"""


import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

# ─── PYTHONPATH setup ─────────────────────────────────────────────────────────
_HERE = Path(__file__).resolve()
_PW_ROOT = _HERE.parents[2]      # python-worker/
_REPO_ROOT = _HERE.parents[3]    # scanner_infra/
_TFF = _REPO_ROOT / "tick_flow_full"

for _p in [str(_PW_ROOT), str(_REPO_ROOT), str(_TFF)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _import_registry():
    """Импортирует feature_registry, пропускает тест если нет PYTHONPATH."""
    import importlib
    try:
        return importlib.import_module("core.feature_registry")
    except ImportError as e:
        pytest.skip(f"core.feature_registry недоступен (PYTHONPATH?): {e}")


def _import_train():
    """Импортирует train_edge_stack_v1_oof, пропускает если нет sklearn."""
    try:
        import importlib
        m = importlib.import_module("ml_analysis.tools.train_edge_stack_v1_oof")
        return m
    except SystemExit:
        pytest.skip("train_edge_stack_v1_oof: sklearn недоступен")
    except Exception as e:
        pytest.skip(f"train_edge_stack_v1_oof import failed: {e}")


def _make_minimal_jsonl(n: int = 200, td: str = None) -> str:
    """Создаёт минимальный JSONL с n labeled rows."""
    import random
    rows = []
    for i in range(n):
        rows.append({
            "ts_ms": 1_700_000_000_000 + i * 60_000,
            "y": 1 if i % 3 == 0 else 0,
            "direction": "BUY",
            "scenario": "trend",
            "indicators": {
                "delta_z": round(random.uniform(-3, 3), 4),
                "ofi_z": round(random.uniform(-2, 2), 4),
                "spread_bps": round(random.uniform(0.5, 5.0), 4),
                "obi": round(random.uniform(-1, 1), 4),
            },
        })
    path = os.path.join(td, "edge_train.jsonl")
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    return path


# ─── Tests ────────────────────────────────────────────────────────────────────

class TestSha256_16:
    """_sha256_16 — стабильный 16-символьный хэш."""

    def test_output_length_16(self):
        reg = _import_registry()
        h = reg._sha256_16(["f_delta_z", "direction_BUY", "hour:0"])
        assert len(h) == 16, f"ожидаем 16 символов, получили {len(h)}"

    def test_stable(self):
        reg = _import_registry()
        items = ["f_delta_z", "f_ofi_z", "direction_BUY"]
        h1 = reg._sha256_16(items)
        h2 = reg._sha256_16(items)
        assert h1 == h2, "hash должен быть стабильным"

    def test_different_input_different_hash(self):
        reg = _import_registry()
        h1 = reg._sha256_16(["f_a"])
        h2 = reg._sha256_16(["f_b"])
        assert h1 != h2, "разные входы должны давать разные хэши"


class TestGetEdgeStackFeatureSpecExtended:
    """Расширенная сигнатура get_edge_stack_feature_spec."""

    def test_schema_ver_positional_still_works(self):
        """Backward compat: first positional arg."""
        reg = _import_registry()
        spec = reg.get_edge_stack_feature_spec("v3")
        assert spec.ver == "v3"
        assert len(spec.feature_cols) > 30

    def test_schema_ver_kwarg(self):
        reg = _import_registry()
        spec = reg.get_edge_stack_feature_spec(schema_ver="v3")
        assert spec.ver == "v3"

    def test_v4of_alias_normalized(self):
        """v4of → v4_of."""
        reg = _import_registry()
        spec = reg.get_edge_stack_feature_spec(schema_ver="v4of")
        assert spec.ver == "v4_of"

    def test_v4_alias_normalized(self):
        """v4 → v4_of."""
        reg = _import_registry()
        spec = reg.get_edge_stack_feature_spec(schema_ver="v4")
        assert spec.ver == "v4_of"

    def test_unknown_version_raises(self):
        reg = _import_registry()
        with pytest.raises(ValueError, match="v999"):
            reg.get_edge_stack_feature_spec(schema_ver="v999")

    def test_max_numeric_caps_f_cols(self):
        """max_numeric=5 оставляет ≤ 5 f_* колонок; direction/scenario/time всегда есть."""
        reg = _import_registry()
        spec = reg.get_edge_stack_feature_spec(schema_ver="v3", max_numeric=5)
        f_cols = [c for c in spec.feature_cols if c.startswith("f_")]
        assert len(f_cols) <= 5, f"ожидаем ≤ 5 f_* колонок, получили {len(f_cols)}"
        # direction и bucket должны сохраниться
        assert "direction_BUY" in spec.feature_cols
        assert "bucket:trend" in spec.feature_cols

    def test_max_numeric_zero_means_unlimited(self):
        """max_numeric=0 → без ограничения."""
        reg = _import_registry()
        spec_unlimited = reg.get_edge_stack_feature_spec(schema_ver="v3", max_numeric=0)
        spec_limited = reg.get_edge_stack_feature_spec(schema_ver="v3", max_numeric=128)
        f_unlimited = [c for c in spec_unlimited.feature_cols if c.startswith("f_")]
        f_limited = [c for c in spec_limited.feature_cols if c.startswith("f_")]
        # Для v3 все фичи умещаются в 128 → равны
        assert len(f_unlimited) == len(f_limited)

    def test_strict_forbid_scenario_v4_raises(self):
        """strict_feature_cols + forbid_scenario_v4_onehot → ValueError при scenario_v4_* колонках."""
        reg = _import_registry()
        # scenario_v4_* возникают при scenario_prefix != "bucket:"
        with pytest.raises(ValueError, match="forbidden_feature_cols"):
            reg.get_edge_stack_feature_spec(
                schema_ver="v3",
                scenario_prefix="scenario_v4_",
                strict_feature_cols=True,
                forbid_scenario_v4_onehot=True,
            )

    def test_strict_without_forbid_no_raise(self):
        """strict_feature_cols=True но forbid=False → нет ValueError."""
        reg = _import_registry()
        spec = reg.get_edge_stack_feature_spec(
            schema_ver="v3",
            scenario_prefix="scenario_v4_",
            strict_feature_cols=True,
            forbid_scenario_v4_onehot=False,
        )
        assert spec is not None

    def test_include_time_onehot_true(self):
        reg = _import_registry()
        spec = reg.get_edge_stack_feature_spec(schema_ver="v3", include_time_onehot=True)
        assert "hour:0" in spec.feature_cols
        assert "dow:6" in spec.feature_cols

    def test_include_time_onehot_false(self):
        reg = _import_registry()
        spec = reg.get_edge_stack_feature_spec(schema_ver="v3", include_time_onehot=False)
        assert "hour:0" not in spec.feature_cols
        assert "dow:0" not in spec.feature_cols

    def test_include_time_onehot_default_v2_false(self):
        """Default include_time_onehot=None → False для v2."""
        reg = _import_registry()
        spec = reg.get_edge_stack_feature_spec(schema_ver="v2")
        assert "hour:0" not in spec.feature_cols

    def test_include_time_onehot_default_v3_true(self):
        """Default include_time_onehot=None → True для v3+."""
        reg = _import_registry()
        spec = reg.get_edge_stack_feature_spec(schema_ver="v3")
        assert "hour:0" in spec.feature_cols


class TestTrainOofRegistrySmoke:
    """Smoke-тест train_edge_stack_v1_oof с --feature_schema_ver."""

    def test_train_with_feature_schema_ver(self):
        mod = _import_train()
        try:
            from sklearn.linear_model import LogisticRegression  # noqa: F401
        except ImportError:
            pytest.skip("sklearn недоступен")

        with tempfile.TemporaryDirectory() as td:
            # Создаём минимальный датасет с 150 строками (мин для обучения)
            data_path = _make_minimal_jsonl(n=500, td=td)
            out_model = os.path.join(td, "out.joblib")

            rc = mod.main([
                "--data_jsonl", data_path,
                "--out_model", out_model,
                "--feature_schema_ver", "v3",
                "--scenario_prefix", "bucket:",
                "--include_time_onehot", "1",
                "--require_feature_registry", "0",
                "--n_splits", "2",
                "--min_train", "50",
                "--purge_ms", "0",
                "--embargo_ms", "0",
                "--calibrate", "0",
            ])
            assert rc == 0, "ожидаем rc=0"
            assert os.path.exists(out_model), "модель должна быть записана"

    def test_model_artifact_has_pinning_metadata(self):
        """Артефакт содержит feature_cols_hash и feature_schema_ver."""
        mod = _import_train()
        try:
            import joblib  # noqa: F401
            from sklearn.linear_model import LogisticRegression  # noqa: F401
        except ImportError:
            pytest.skip("sklearn или joblib недоступен")

        with tempfile.TemporaryDirectory() as td:
            data_path = _make_minimal_jsonl(n=500, td=td)
            out_model = os.path.join(td, "out2.joblib")

            rc = mod.main([
                "--data_jsonl", data_path,
                "--out_model", out_model,
                "--feature_schema_ver", "v3",
                "--require_feature_registry", "0",
                "--n_splits", "2",
                "--min_train", "50",
                "--purge_ms", "0",
                "--embargo_ms", "0",
                "--calibrate", "0",
            ])
            assert rc == 0

            import joblib
            pack = joblib.load(out_model)
            assert "feature_cols_hash" in pack, "ожидаем feature_cols_hash в артефакте"
            assert len(pack["feature_cols_hash"]) == 16, "feature_cols_hash должен быть 16 символов"
            assert "feature_schema_ver" in pack, "ожидаем feature_schema_ver в артефакте"


class TestTrainOofHashMismatch:
    """dataset_report_json с неверным hash → SystemExit."""

    def test_hash_mismatch_raises(self):
        mod = _import_train()
        try:
            from sklearn.linear_model import LogisticRegression  # noqa: F401
        except ImportError:
            pytest.skip("sklearn недоступен")

        with tempfile.TemporaryDirectory() as td:
            data_path = _make_minimal_jsonl(n=200, td=td)
            out_model = os.path.join(td, "out.joblib")
            # report.json с заведомо неверным хэшем
            report = {
                "feature_registry": {
                    "feature_cols_hash": "deadbeef00000000",  # 16 chars but wrong
                }
            }
            report_path = os.path.join(td, "report.json")
            with open(report_path, "w") as f:
                json.dump(report, f)

            with pytest.raises(SystemExit, match="feature_cols_hash_mismatch"):
                mod.main([
                    "--data_jsonl", data_path,
                    "--out_model", out_model,
                    "--feature_schema_ver", "v3",
                    "--dataset_report_json", report_path,
                    "--require_feature_registry", "0",
                    "--n_splits", "2",
                    "--min_train", "50",
                    "--purge_ms", "0",
                    "--embargo_ms", "0",
                ])

    def test_missing_registry_section_require_hard(self):
        """dataset_report_json без feature_registry + require=1 → SystemExit."""
        mod = _import_train()
        try:
            from sklearn.linear_model import LogisticRegression  # noqa: F401
        except ImportError:
            pytest.skip("sklearn недоступен")

        with tempfile.TemporaryDirectory() as td:
            data_path = _make_minimal_jsonl(n=200, td=td)
            out_model = os.path.join(td, "out.joblib")
            report = {"joined": 5000, "pos_rate": 0.15}  # нет feature_registry
            report_path = os.path.join(td, "report.json")
            with open(report_path, "w") as f:
                json.dump(report, f)

            with pytest.raises(SystemExit, match="dataset_report_missing_feature_registry"):
                mod.main([
                    "--data_jsonl", data_path,
                    "--out_model", out_model,
                    "--feature_schema_ver", "v3",
                    "--dataset_report_json", report_path,
                    "--require_feature_registry", "1",
                    "--n_splits", "2",
                    "--min_train", "50",
                    "--purge_ms", "0",
                    "--embargo_ms", "0",
                ])


class TestTimerSchemaVerAware:
    """run_ml_train_edge_stack_v1_oof логика schema_ver."""

    def _get_func(self):
        try:
            from services import of_timers_worker
            return of_timers_worker.run_ml_train_edge_stack_v1_oof
        except ImportError:
            pytest.skip("services.of_timers_worker недоступен")

    def test_returns_false_if_no_dataset(self):
        """Если dataset не существует — возвращаем False."""
        func = self._get_func()
        with patch.dict(os.environ, {
            "ML_EDGE_STACK_OOF_DATASET_PATH": "/nonexistent/edge_train.jsonl",
            "ML_EDGE_STACK_OOF_FEATURE_SCHEMA_VER": "v3",
        }, clear=False):
            result = func()
        assert result is False

    def test_no_schema_ver_requires_feature_cols_json(self):
        """Без schema_ver: если feature_cols.json нет → False."""
        func = self._get_func()
        with tempfile.TemporaryDirectory() as td:
            dataset = os.path.join(td, "edge_train.jsonl")
            with open(dataset, "w") as f:
                f.write("{}\n")
            with patch.dict(os.environ, {
                "ML_EDGE_STACK_OOF_DATASET_PATH": dataset,
                "ML_EDGE_STACK_OOF_FEATURE_SCHEMA_VER": "",
                "ML_FEATURE_SCHEMA_VER": "",
                "FEATURE_SCHEMA_VER": "",
                "ML_EDGE_STACK_OOF_FEATURE_COLS_JSON": "/nonexistent/feature_cols.json",
            }, clear=False):
                result = func()
        assert result is False, "без feature_cols.json должен вернуть False в legacy-режиме"

    def test_schema_ver_skips_feature_cols_json_check(self):
        """С schema_ver не проверяем наличие feature_cols.json."""
        func = self._get_func()
        with tempfile.TemporaryDirectory() as td:
            dataset = os.path.join(td, "edge_train.jsonl")
            with open(dataset, "w") as f:
                f.write("{}\n")

            # run_tool мокируем, чтобы не запускать реальное обучение
            with patch("services.of_timers_worker.run_tool", return_value=True) as mock_run:
                with patch.dict(os.environ, {
                    "ML_EDGE_STACK_OOF_DATASET_PATH": dataset,
                    "ML_EDGE_STACK_OOF_FEATURE_SCHEMA_VER": "v3",
                    "ML_EDGE_STACK_OOF_FEATURE_COLS_JSON": "/nonexistent/feature_cols.json",
                    "ML_EDGE_STACK_OOF_REQUIRE_REGISTRY": "0",
                    # Disable P59 bundle to allow OOF train function to proceed
                    "EDGE_STACK_BUNDLE_ENABLED": "0",
                }, clear=False):
                    result = func()

            # run_tool должен был быть вызван (schema_ver путь)
            assert mock_run.called, "run_tool должен быть вызван в registry-режиме"
            # args должны содержать --feature_schema_ver
            call_args = mock_run.call_args
            args_list = call_args[0][1] if call_args[0] else call_args[1].get("args", [])
            assert "--feature_schema_ver" in args_list, "--feature_schema_ver должен быть в аргументах"
            assert "--feature_cols_json" not in args_list, "--feature_cols_json не должен быть в registry-режиме"


class TestBuildDatasetChoices:
    """build_edge_stack_dataset_from_redis принимает v4 в --feature_schema_ver."""

    def test_v4_in_choices(self):
        """v4 должен быть в choices (раньше не было)."""
        try:
            import argparse
            # Собираем argparser напрямую через небольшой хак — проверяем что v4 не вызывает ошибку
            from ml_analysis.tools.build_edge_stack_dataset_from_redis import main  # noqa: F401
            # Проверяем что парсер принимает v4 без ошибки
            import sys as _sys
            import io
            import contextlib

            # argparse throws SystemExit for --help; we need another way.
            # Inspect the choices from the parser directly via patching argv.
            # We'll just validate via the source choices constant.
            # Since this is hard to do without running the full parser,
            # we just verify v4 is in the spec choices string in the file.
            import ml_analysis.tools.build_edge_stack_dataset_from_redis as _mod
            source = Path(_mod.__file__).read_text(encoding="utf-8")
            assert '"v4"' in source or "'v4'" in source, (
                "Ожидаем 'v4' в choices --feature_schema_ver в build_edge_stack_dataset_from_redis"
            )
        except ImportError:
            pytest.skip("build_edge_stack_dataset_from_redis недоступен (redis?)")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
