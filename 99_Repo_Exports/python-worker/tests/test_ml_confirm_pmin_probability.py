"""
Unit тесты для проверки p_min логики в edge_stack_v1.

Проверяет:
- dec.p_min ∈ [0,1]
- p_min_by_bucket приоритетнее floor-derived
- hard_p_min_floor как guardrail
"""

import pytest
from services.ml_confirm_gate import MLConfirmGate, MLConfirmDecision
import redis
from unittest.mock import Mock
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


class DummyEdgeStackModel:
    """Mock edge_stack_v1 модель."""
    feature_cols = [
        "f_spread_bps",
        "f_expected_slippage_bps",
        "f_exec_risk_norm",
        "direction_LONG",
        "scenario_v4_range_meanrev",
    ]


def make_fake_edge_stack_pack():
    """Создает fake edge_stack_v1 pack с фиксированными вероятностями."""
    # LR модель, которая всегда возвращает 0.6
    lr_pipe = Pipeline([
        ("scaler", StandardScaler(with_mean=False)),
        ("lr", LogisticRegression(solver="lbfgs", max_iter=100, random_state=42))
    ])
    # Обучаем на dummy данных
    X_dummy = np.array([[1.0, 2.0, 0.5, 1.0, 1.0], [2.0, 3.0, 0.6, 0.0, 0.0]])
    y_dummy = np.array([1, 0])
    lr_pipe.fit(X_dummy, y_dummy)
    
    # GBDT модель (используем LR как заглушку для простоты)
    gbdt_model = LogisticRegression(solver="lbfgs", max_iter=100, random_state=43)
    gbdt_model.fit(X_dummy, y_dummy)
    
    # Meta модель
    meta_model = LogisticRegression(solver="lbfgs", max_iter=100, random_state=44)
    Z_dummy = np.array([[0.6, 0.7], [0.4, 0.3]])
    y_meta = np.array([1, 0])
    meta_model.fit(Z_dummy, y_meta)
    
    return {
        "schema_version": 1,
        "kind": "edge_stack_v1",
        "feature_cols": DummyEdgeStackModel.feature_cols,
        "lr": lr_pipe,
        "gbdt": gbdt_model,
        "meta": meta_model,
    }


def test_pmin_in_range():
    """Проверка: dec.p_min ∈ [0,1]."""
    r = Mock(spec=redis.Redis)
    r.get = Mock(return_value=None)
    
    gate = MLConfirmGate(
        r=r,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger",
    )
    
    gate._cfg = {
        "kind": "edge_stack_v1",
        "run_id": "test_run_001",
        "model_path": "/tmp/test_model.joblib",
        "p_min": 0.55,
    }
    gate._model = make_fake_edge_stack_pack()
    
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={"spread_bps": 2.0, "expected_slippage_bps": 2.0},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    
    assert 0.0 <= dec.p_min <= 1.0, f"p_min={dec.p_min} вне диапазона [0,1]"
    assert dec.kind == "edge_stack_v1"


def test_pmin_by_bucket_priority():
    """Проверка: p_min_by_bucket приоритетнее p_min."""
    r = Mock(spec=redis.Redis)
    r.get = Mock(return_value=None)
    
    gate = MLConfirmGate(
        r=r,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger",
    )
    
    gate._cfg = {
        "kind": "edge_stack_v1",
        "run_id": "test_run_001",
        "model_path": "/tmp/test_model.joblib",
        "p_min": 0.50,  # глобальный
        "p_min_by_bucket": {
            "trend": 0.55,
            "range": 0.60,
            "other": 0.50,
            "news": 0.65
        },
    }
    gate._model = make_fake_edge_stack_pack()
    
    # Тест для range bucket
    dec_range = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={"spread_bps": 2.0, "expected_slippage_bps": 2.0},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    
    assert dec_range.bucket == "range"
    assert dec_range.p_min == pytest.approx(0.60, abs=1e-6), f"Ожидался p_min=0.60 для range, получен {dec_range.p_min}"
    
    # Тест для trend bucket
    dec_trend = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000000,
        direction="LONG",
        scenario="trend_continuation",
        indicators={"spread_bps": 2.0, "expected_slippage_bps": 2.0},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    
    assert dec_trend.bucket == "trend"
    assert dec_trend.p_min == pytest.approx(0.55, abs=1e-6), f"Ожидался p_min=0.55 для trend, получен {dec_trend.p_min}"


def test_hard_pmin_floor_guardrail():
    """Проверка: hard_p_min_floor как guardrail."""
    r = Mock(spec=redis.Redis)
    r.get = Mock(return_value=None)
    
    gate = MLConfirmGate(
        r=r,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger",
    )
    
    gate._cfg = {
        "kind": "edge_stack_v1",
        "run_id": "test_run_001",
        "model_path": "/tmp/test_model.joblib",
        "p_min": 0.30,  # низкий p_min
        "hard_p_min_floor": 0.40,  # guardrail выше
    }
    gate._model = make_fake_edge_stack_pack()
    
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={"spread_bps": 2.0, "expected_slippage_bps": 2.0},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    
    # p_min должен быть max(0.30, 0.40) = 0.40
    assert dec.p_min == pytest.approx(0.40, abs=1e-6), f"Ожидался p_min=0.40 (hard floor), получен {dec.p_min}"


def test_pmin_by_bucket_with_hard_floor():
    """Проверка: p_min_by_bucket + hard_p_min_floor."""
    r = Mock(spec=redis.Redis)
    r.get = Mock(return_value=None)
    
    gate = MLConfirmGate(
        r=r,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger",
    )
    
    gate._cfg = {
        "kind": "edge_stack_v1",
        "run_id": "test_run_001",
        "model_path": "/tmp/test_model.joblib",
        "p_min": 0.50,
        "p_min_by_bucket": {
            "range": 0.35,  # ниже hard floor
        },
        "hard_p_min_floor": 0.40,
    }
    gate._model = make_fake_edge_stack_pack()
    
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000000,
        direction="LONG",
        scenario="range_meanrev",
        indicators={"spread_bps": 2.0, "expected_slippage_bps": 2.0},
        rule_score=0.7,
        rule_have=2,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    
    # p_min должен быть max(0.35, 0.40) = 0.40
    assert dec.p_min == pytest.approx(0.40, abs=1e-6), f"Ожидался p_min=0.40 (hard floor override), получен {dec.p_min}"










