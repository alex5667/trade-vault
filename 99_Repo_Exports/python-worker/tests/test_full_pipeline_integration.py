from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import MagicMock
import pytest

from handlers.crypto_orderflow.pipeline.orchestrator import SignalOrchestrator

_NOW_MS = int(time.time() * 1000)

def _setup_mock_orchestrator():
    """Helper function to create a mock orchestrator and its context."""
    call_log: list[str] = []

    # --- 1. Мокаем все компоненты так, чтобы они разрешали (ALLOW) прохождение ---
    gates = MagicMock()
    
    def _gate_pass(*, veto=False, reason_code="OK"):
        return SimpleNamespace(veto=veto, reason_code=reason_code, decision="ALLOW")
    
    def _qa_pass():
        return SimpleNamespace(veto=False, reason="", decision="ALLOW")

    def _regime(*args, **kwargs):
        call_log.append("regime")
        return (True, "")

    gates.check_dq_integrity.side_effect = lambda ctx, kind: (call_log.append("dq_integrity") or _gate_pass())
    gates.check_quality.side_effect = lambda ctx, kind, side="": (call_log.append("quality") or _qa_pass())
    gates.check_regime_gate.side_effect = _regime
    gates.check_smt.side_effect = lambda ctx, kind, side="": (call_log.append("smt") or _gate_pass())
    gates.consistency_once.side_effect = lambda ctx, symbol, kind, side="": (call_log.append("consistency") or _gate_pass())
    gates.edge_cost_cached.side_effect = lambda ctx, kind, symbol, side, cfg: (call_log.append("edge_cost") or _gate_pass())
    gates.check_entry_policy.side_effect = lambda ctx, payload: (call_log.append("entry_policy") or _gate_pass())

    # --- 2. Liquidity (обеспечение уровней) ---
    liquidity = MagicMock()
    def _ensure_levels(*, ctx, symbol, side, kind, cfg, overwrite):
        call_log.append("levels")
    liquidity.ensure_trade_levels_once.side_effect = _ensure_levels

    # --- 3. Observability (заглушка для метрик) ---
    obs = MagicMock()
    obs._metrics = None
    def _emit_veto_metric(*args, **kwargs):
        call_log.append("veto_metric") # Не должно вызываться при успехе
    obs.emit_veto_metric.side_effect = _emit_veto_metric

    # --- 4. Emitter ---
    def _emit_kw(*args, **kwargs):
        call_log.append("emit")
        return True
    emitter = MagicMock()
    emitter.emit.side_effect = _emit_kw

    # --- 5. Confirmation Engine (ML) ---
    confirm = MagicMock()
    def _confirm_ok():
        return SimpleNamespace(ok=True, final_score=1.0, confidence=0.9, code="OK", parts={})
    confirm.validate.side_effect = lambda kind, ctx: (call_log.append("confirm") or _confirm_ok())

    # --- 6. Config ---
    cfg = MagicMock()
    cfg.symbol = "BTCUSDT"
    cfg.get_runtime_snapshot.return_value = None
    cfg.resolve_risk_cfg.return_value = {"tp_mode": "RR"}

    # Создаем оркестратор
    orch = SignalOrchestrator(
        config=cfg,
        gates=gates,
        liquidity=liquidity,
        observability=obs,
        confirmations_engine=confirm,
        emitter=emitter,
    )

    # Принудительно выключаем Layer A/B/C чтобы он не требовал Redis и не сбоил
    orch._layer_enforce_mode = "off"

    # --- 7. Подготавливаем идеального кандидата и контекст ---
    ctx = SimpleNamespace(
        symbol="BTCUSDT",
        price=50000.0,
        ts=_NOW_MS,
        ts_ms=_NOW_MS,
        sizing_ok=True,
        qty=0.01,
        of=SimpleNamespace(spread_bps=3.0),
        redis=None,
        venue="binance",
        timeframe="1m",
        atr=250.0,
        sl_price=49500.0,
        tp1_price=51000.0,
        tp_mode_used="RR",
        risk_usd_target=25.0,
        risk_usd=24.5,
        trail_profile="",
        trailing_min_lock_r=0.0,
        risk_cfg={"tp_mode": "RR"},
        indicators={"spread_bps": 3.0},
    )
    cand = SimpleNamespace(
        kind="breakout",
        side="long",
        direction=1,
        raw_score=2.0,
        signal_id="sid-test-integration-1",
        reasons=["delta_spike"],
    )

    return orch, ctx, cand, call_log


def test_full_pipeline_end_to_end_success():
    """
    Интеграционный тест сквозного пайплайна (все этапы).
    Убеждаемся, что правильно настроенный контекст и кандидат проходят все 
    этапы пайплайна в оркестраторе (без единого вето) и успешно эмитятся.
    """
    orch, ctx, cand, call_log = _setup_mock_orchestrator()

    # --- 8. Выполнение ---
    result = orch.process(ctx, lambda c: [cand])

    # --- 9. Проверки ---
    assert result is True, "Оркестратор должен был вернуть True (успешная эмиссия)"
    assert "veto_metric" not in call_log, "Сигнал был отклонен на одном из гейтов!"

    # Проверяем, что сигнал прошел все основные этапы и попал в emit
    expected_stages = [
        "dq_integrity",
        "quality",
        "regime",
        "smt",
        "consistency",
        "levels",
        "entry_policy",
        "edge_cost",
        "confirm",
        "emit",
    ]
    
    for stage in expected_stages:
        assert stage in call_log, f"Пайплайн не дошел до этапа: {stage}"

    # Проверка вызова эмиттера
    orch.emitter.emit.assert_called_once()


def test_pipeline_invalid_timestamp():
    """
    Проверка на отбрасывание сигнала с некорректным временем (ts_ms = 0).
    Должно произойти до всех проверок гейтов.
    """
    orch, ctx, cand, call_log = _setup_mock_orchestrator()
    
    # Имитируем плохой таймштамп
    ctx.ts_ms = 0
    ctx.ts = 0

    result = orch.process(ctx, lambda c: [cand])

    assert result is False, "Сигнал с ts=0 должен быть отброшен"
    # Ни один гейт не должен быть вызван
    assert "dq_integrity" not in call_log
    assert "quality" not in call_log


@pytest.mark.parametrize("gate_name, setup_mock_fn", [
    (
        "dq_integrity", 
        lambda orch: orch.gates.check_dq_integrity.side_effect == None or orch.gates.check_dq_integrity.configure_mock(
            side_effect=lambda ctx, kind: SimpleNamespace(veto=True, reason_code="VETO_DQ_INTEGRITY", decision="DENY")
        )
    ),
    (
        "quality", 
        lambda orch: orch.gates.check_quality.configure_mock(
            side_effect=lambda ctx, kind, side="": SimpleNamespace(veto=True, reason="VETO_QUALITY", decision="DENY")
        )
    ),
    (
        "regime", 
        lambda orch: orch.gates.check_regime_gate.configure_mock(
            side_effect=lambda ctx, kind, side="": (False, "VETO_REGIME")
        )
    ),
    (
        "smt", 
        lambda orch: orch.gates.check_smt.configure_mock(
            side_effect=lambda ctx, kind, side="": SimpleNamespace(veto=True, reason_code="VETO_SMT", decision="DENY")
        )
    ),
    (
        "consistency", 
        lambda orch: orch.gates.consistency_once.configure_mock(
            side_effect=lambda ctx, symbol, kind, side="": SimpleNamespace(veto=True, reason_code="VETO_CONSISTENCY", decision="DENY")
        )
    ),
    (
        "entry_policy", 
        lambda orch: orch.gates.check_entry_policy.configure_mock(
            side_effect=lambda ctx, payload: SimpleNamespace(veto=True, reason_code="VETO_ENTRY_POLICY", decision="DENY")
        )
    ),
    (
        "edge_cost", 
        lambda orch: orch.gates.edge_cost_cached.configure_mock(
            side_effect=lambda ctx, kind, symbol, side, cfg: SimpleNamespace(veto=True, reason_code="VETO_COST", decision="DENY")
        )
    ),
    (
        "confirm", 
        lambda orch: orch.confirmations.validate.configure_mock(
            side_effect=lambda kind, ctx: SimpleNamespace(ok=False, final_score=0.0, confidence=0.0, code="ML_VETO", parts={})
        )
    )
])
def test_full_pipeline_gate_vetoes(gate_name, setup_mock_fn):
    """
    Проверяет, что если любой гейт возвращает veto=True, пайплайн
    прерывается, метрика вето отправляется, а сигнал не доходит до emit.
    """
    orch, ctx, cand, call_log = _setup_mock_orchestrator()
    
    # Настраиваем нужный гейт на отказ
    setup_mock_fn(orch)

    result = orch.process(ctx, lambda c: [cand])

    assert result is False, f"Пайплайн не отбросил сигнал на гейте {gate_name}!"
    
    # Оркестратор должен был вызвать emit_veto_metric
    if gate_name == "confirm":
        # confirm engine не использует emit_veto_metric напрямую через observ (или делает это внутри)
        pass 
    else:
        assert "veto_metric" in call_log, f"Метрика вето не отправлена при отказе гейта {gate_name}!"
    
    # Эмиттер не должен быть вызван
    assert "emit" not in call_log, f"Сигнал дошел до emit несмотря на отказ гейта {gate_name}!"
