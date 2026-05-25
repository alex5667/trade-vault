from types import SimpleNamespace

import pytest

from services.orderflow.decision_snapshot import build_decision_snapshot


def test_decision_snapshot_contract_v1():
    """Гарантирует неизменность базовых полей контракта A2 (Decision Snapshot)."""

    # Исходные данные (имитируем сигнал из пайплайна)
    signal = {
        "symbol": "BTCUSDT",
        "sid": "crypto-of:BTCUSDT:1700000000000",
        "direction": "LONG",
        "decision_ts_ms": 1700000000000,
        "decision_bid": 100.0,
        "decision_ask": 100.1,
        "decision_mid": 100.05,
        "decision_spread_bps": 5.0,
        "tca_ready": True
    }

    indicators = {
        "delta_z": 2.5,
        "confidence": 0.85,
        "strong_gate_ok": 1,
        # ML Stage 4 fields (будущие или текущие)
        "ml_p_edge": 0.65,
        "ml_status": "ALLOW",
        "liqmap_5m_total_usd": 123456.0,
        "liqmap_5m_near_imb": -0.25,
        "liqmap_gate_rr": 1.8,
        "liqmap_gate_veto_reason": "too_close"
    }

    runtime = SimpleNamespace(symbol="BTCUSDT")

    # Вызов билдера
    snap = build_decision_snapshot(
        signal,
        runtime=runtime,
        indicators=indicators,
        schema_version=1,
        include_indicators=True
    )

    # 1. Проверка ключей для Join
    assert snap["sid"] == signal["sid"]
    assert snap["symbol"] == "BTCUSDT"
    assert snap["decision_ts_ms"] == 1700000000000

    # 2. Проверка микроструктуры (важно для TCA)
    assert snap["decision_mid"] == 100.05
    # (100.1 - 100.0) / 100.05 * 10000 = 9.995...
    assert snap["decision_spread_bps"] == pytest.approx(9.995, abs=1e-3)
    assert snap["tca_ready"] is True

    # 3. Проверка метаданных
    assert snap["schema_version"] == 1
    assert "producer" in snap

    # 4. Проверка фильтрации индикаторов (include_indicators=True)
    # Билдер должен возвращать только разрешенные индикаторы в indicators_small
    if "indicators_small" in snap:
        # delta_z в белом списке
        assert "delta_z" in snap["indicators_small"]
        # ml_p_edge НЕ в белом списке (по текущей реализации в decision_snapshot.py)
        # Это ожидаемое поведение: мы не раздуваем снапшот всем мусором.
        assert "ml_p_edge" not in snap["indicators_small"]
        # v9 liqmap train features are allowed because they are decision-time
        # replay inputs, but non-numeric diagnostic strings stay out.
        assert snap["indicators_small"]["liqmap_5m_total_usd"] == 123456.0
        assert snap["indicators_small"]["liqmap_5m_near_imb"] == -0.25
        assert snap["indicators_small"]["liqmap_gate_rr"] == 1.8
        assert "liqmap_gate_veto_reason" not in snap["indicators_small"]

def test_decision_snapshot_missing_critical_fields():
    """Проверяет fail-safe поведение при отсутствии цен."""
    signal = {"sid": "test", "direction": "SHORT"}
    snap = build_decision_snapshot(signal, runtime=None, indicators={})

    # Должен построиться без ошибок, но с None в ценах
    assert snap["sid"] == "test"
    assert snap["decision_mid"] is None
    assert snap["tca_ready"] is False
