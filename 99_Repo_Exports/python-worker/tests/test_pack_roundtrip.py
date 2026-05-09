#!/usr/bin/env python3
from __future__ import annotations

"""
test_pack_roundtrip.py

Тесты для проверки roundtrip сохранения/загрузки model pack:
  - joblib.dump/load работает корректно
  - все ключи присутствуют (lr/gbdt/meta/feature_cols)
  - модели можно использовать для предсказаний
"""

import json
import os
import tempfile

import joblib
import numpy as np
import pytest
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression

from utils.time_utils import get_ny_time_millis


def test_pack_roundtrip_basic():
    """Базовый тест roundtrip: сохранение и загрузка pack."""
    # Создаём тестовый pack
    feature_cols = ["f_delta_z", "f_ofi_z", "direction_LONG", "direction_SHORT"]

    lr = LogisticRegression(random_state=42, max_iter=100)
    gbdt = HistGradientBoostingClassifier(random_state=42, max_iter=10)
    meta = LogisticRegression(random_state=42, max_iter=100)

    # Обучаем на синтетических данных
    X = np.random.randn(100, len(feature_cols)).astype(np.float32)
    y = (np.random.rand(100) > 0.5).astype(np.int64)

    lr.fit(X, y)
    gbdt.fit(X, y)

    # Meta обучается на предсказаниях base моделей
    p_lr = lr.predict_proba(X)[:, 1]
    p_gbdt = gbdt.predict_proba(X)[:, 1]
    Z = np.vstack([p_lr, p_gbdt]).T
    meta.fit(Z, y)

    pack = {
        "schema_version": 1,
        "kind": "edge_stack_v1",
        "created_ms": get_ny_time_millis(),
        "feature_cols": feature_cols,
        "lr": lr,
        "gbdt": gbdt,
        "meta": meta,
    }

    # Сохранение
    with tempfile.NamedTemporaryFile(suffix=".joblib", delete=False) as f:
        path = f.name
        try:
            joblib.dump(pack, path)

            # Загрузка
            loaded = joblib.load(path)

            # Проверка структуры
            assert loaded["schema_version"] == 1
            assert loaded["kind"] == "edge_stack_v1"
            assert loaded["feature_cols"] == feature_cols
            assert "lr" in loaded
            assert "gbdt" in loaded
            assert "meta" in loaded
            assert "created_ms" in loaded

            # Проверка, что модели работают
            X_test = np.random.randn(10, len(feature_cols)).astype(np.float32)

            p_lr_test = loaded["lr"].predict_proba(X_test)[:, 1]
            p_gbdt_test = loaded["gbdt"].predict_proba(X_test)[:, 1]
            Z_test = np.vstack([p_lr_test, p_gbdt_test]).T
            p_meta_test = loaded["meta"].predict_proba(Z_test)[:, 1]

            assert len(p_meta_test) == 10
            assert np.all((p_meta_test >= 0) & (p_meta_test <= 1))
        finally:
            if os.path.exists(path):
                os.unlink(path)


def test_pack_optional_fields():
    """Проверка, что опциональные поля (transforms/scaler) сохраняются и загружаются."""
    feature_cols = ["f_delta_z", "f_ofi_z"]

    lr = LogisticRegression(random_state=42, max_iter=100)
    gbdt = HistGradientBoostingClassifier(random_state=42, max_iter=10)
    meta = LogisticRegression(random_state=42, max_iter=100)

    X = np.random.randn(50, len(feature_cols)).astype(np.float32)
    y = (np.random.rand(50) > 0.5).astype(np.int64)

    lr.fit(X, y)
    gbdt.fit(X, y)
    p_lr = lr.predict_proba(X)[:, 1]
    p_gbdt = gbdt.predict_proba(X)[:, 1]
    Z = np.vstack([p_lr, p_gbdt]).T
    meta.fit(Z, y)

    pack = {
        "schema_version": 1,
        "kind": "edge_stack_v1",
        "created_ms": get_ny_time_millis(),
        "feature_cols": feature_cols,
        "lr": lr,
        "gbdt": gbdt,
        "meta": meta,
        # Опциональные поля
        "feature_transforms": {"f_delta_z": {"type": "log1p"}},
        "robust_scaler": {"f_delta_z": {"center": 0.0, "scale": 1.0}},
        "session_cfg": {"timezone": "UTC"},
        "spread_bucket_edges": [2.0, 5.0, 10.0],
        "liq_cfg": {"quantiles": [0.25, 0.5, 0.75]},
    }

    with tempfile.NamedTemporaryFile(suffix=".joblib", delete=False) as f:
        path = f.name
        try:
            joblib.dump(pack, path)
            loaded = joblib.load(path)

            # Проверка опциональных полей
            assert loaded.get("feature_transforms") == pack["feature_transforms"]
            assert loaded.get("robust_scaler") == pack["robust_scaler"]
            assert loaded.get("session_cfg") == pack["session_cfg"]
            assert loaded.get("spread_bucket_edges") == pack["spread_bucket_edges"]
            assert loaded.get("liq_cfg") == pack["liq_cfg"]
        finally:
            if os.path.exists(path):
                os.unlink(path)


def test_calibrator_json_roundtrip():
    """Проверка roundtrip для calibrator.json."""
    calib_data = {
        "a": 1.234567,
        "b": -0.123456,
        "eps": 1e-6,
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
        path = f.name
        try:
            json.dump(calib_data, f, ensure_ascii=False, indent=2)
            f.flush()

            # Загрузка
            with open(path, encoding="utf-8") as f2:
                loaded = json.load(f2)

            assert abs(loaded["a"] - calib_data["a"]) < 1e-9
            assert abs(loaded["b"] - calib_data["b"]) < 1e-9
            assert abs(loaded["eps"] - calib_data["eps"]) < 1e-9
        finally:
            if os.path.exists(path):
                os.unlink(path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

