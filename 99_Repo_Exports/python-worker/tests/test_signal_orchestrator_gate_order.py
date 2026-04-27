"""
Gate sequence contract test for SignalOrchestrator.

Цель: если кто-то переставит шаги gate-пайплайна — этот тест сломается.
Фиксирует контракт: quality → regime → smt → consistency → levels → cost →
                     confirm → sizing_check → entry_policy → emit

Это sentinel: при refactor orchestrator.process() порядок должен быть явно
пересмотрен и тест обновлён осознанно.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, call
import pytest

from handlers.crypto_orderflow.pipeline.orchestrator import SignalOrchestrator


def _gate_pass(*, veto: bool = False, reason_code: str = "OK") -> SimpleNamespace:
    return SimpleNamespace(veto=veto, reason_code=reason_code)


def _qa_pass() -> SimpleNamespace:
    return SimpleNamespace(veto=False, reason="")


def _confirm_ok() -> SimpleNamespace:
    return SimpleNamespace(ok=True, final_score=1.0, confidence=0.9, code="OK", parts={})


class TestGateSequenceOrder:
    """
    Записывает порядок вызовов gate-методов и эмиссии.
    Любое нарушение порядка — assertion failure.
    """

    def test_gate_sequence_contract(self):
        call_log: list[str] = []

        # --- Патческие gates ---
        gates = MagicMock()

        def _quality(ctx, kind, side=""):
            call_log.append("quality")
            return _qa_pass()

        def _regime(ctx, kind):
            call_log.append("regime")
            return (True, "")

        def _smt(ctx, kind, side):
            call_log.append("smt")
            return _gate_pass()

        def _consistency(*, ctx, symbol, kind, side):
            call_log.append("consistency")
            return _gate_pass()

        def _edge_cost(*, ctx, kind, symbol, side, cfg):
            call_log.append("edge_cost")
            return _gate_pass()

        def _entry_policy(ctx, payload):
            call_log.append("entry_policy")
            return _gate_pass()

        gates.check_quality.side_effect = _quality
        gates.check_regime_gate.side_effect = _regime
        gates.check_smt.side_effect = _smt
        gates.consistency_once.side_effect = _consistency
        gates.edge_cost_cached.side_effect = _edge_cost
        gates.check_entry_policy.side_effect = _entry_policy

        # --- Liquidity (levels) ---
        liquidity = MagicMock()

        def _ensure_levels(*, ctx, symbol, side, kind, cfg, overwrite):
            call_log.append("levels")

        liquidity.ensure_trade_levels_once.side_effect = _ensure_levels

        # --- Observability ---
        obs = MagicMock()
        obs._metrics = None

        # --- Emitter ---
        def _emit_kw(
            *,
            signal_id="", kind="", symbol="", side=None,
            ts_event_ms=0, ingest_time_ms=0, trace_id=None,
            quality_flags=None, source="python-worker",
            meta_schema_version=1, raw_score=0.0, final_score=0.0,
            confidence_pct=0.0, payload=None,
        ):
            call_log.append("emit")
            return True

        emitter = MagicMock()
        emitter.emit.side_effect = _emit_kw

        # --- Confirm ---
        confirm = MagicMock()
        confirm.validate.side_effect = lambda kind, ctx: (
            call_log.append("confirm") or _confirm_ok()
        )

        # --- Cfg ---
        cfg = MagicMock()
        cfg.symbol = "BTCUSDT"
        cfg.get_runtime_snapshot.return_value = None
        cfg.resolve_risk_cfg.return_value = {}

        orch = SignalOrchestrator(
            config=cfg,
            gates=gates,
            liquidity=liquidity,
            observability=obs,
            confirmations_engine=confirm,
            emitter=emitter,
        )

        ctx = SimpleNamespace(
            symbol="BTCUSDT",
            price=50000.0,
            ts=1_700_000_000_000,
            ts_ms=1_700_000_000_000,
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
            risk_cfg={},
        )
        cand = SimpleNamespace(
            kind="breakout",
            side="long",
            raw_score=2.0,
            signal_id="sid-gate-order",
            reasons=["delta_spike"],
        )

        result = orch.process(ctx, lambda c: [cand])

        assert result is True, f"Ожидался True, call_log={call_log}"

        # Контракт порядка (sizing_check — неявный, между confirm и entry_policy)
        EXPECTED = [
            "quality",
            "regime",
            "smt",
            "consistency",
            # levels может быть тут (step 3)
            "edge_cost",
            "confirm",
            # sizing_ok проверяется между confirm и entry_policy
            "entry_policy",
            "emit",
        ]

        # Убираем из log 'levels' так как это вспомогательный шаг, но проверяем что он ДО edge_cost
        log_without_levels = [x for x in call_log if x != "levels"]
        assert log_without_levels == EXPECTED, (
            f"\nОжидался порядок: {EXPECTED}\n"
            f"Получен (без levels): {log_without_levels}\n"
            f"Полный лог: {call_log}"
        )

        # levels должен быть ДО edge_cost
        if "levels" in call_log:
            idx_levels = call_log.index("levels")
            idx_edge = call_log.index("edge_cost")
            assert idx_levels < idx_edge, (
                f"levels (idx={idx_levels}) должен быть до edge_cost (idx={idx_edge})"
            )

    def test_first_veto_stops_processing_candidate(self):
        """При veto на первом gate — следующие gates НЕ вызываются."""
        call_log: list[str] = []

        gates = MagicMock()
        gates.check_quality.side_effect = lambda ctx, kind, side="": (
            call_log.append("quality") or SimpleNamespace(veto=True, reason="VETO_QUALITY")
        )
        # Остальные не должны вызываться
        gates.check_regime_gate.side_effect = lambda ctx, kind: (
            call_log.append("regime_SHOULD_NOT") or (True, "")
        )

        obs = MagicMock()
        obs._metrics = None
        obs.emit_veto_metric = MagicMock()

        orch = SignalOrchestrator(
            config=MagicMock(symbol="BTCUSDT",
                             get_runtime_snapshot=lambda: None,
                             resolve_risk_cfg=lambda: {}),
            gates=gates,
            liquidity=MagicMock(),
            observability=obs,
            confirmations_engine=MagicMock(),
            emitter=MagicMock(),
        )

        ctx = SimpleNamespace(
            symbol="BTCUSDT", price=50000.0, ts=1_700_000_000_000,
            ts_ms=1_700_000_000_000, sizing_ok=True, qty=0.01,
            of=SimpleNamespace(spread_bps=3.0), redis=None,
        )
        orch.process(ctx, lambda c: [SimpleNamespace(kind="b", side="long", raw_score=1.0, signal_id="x", reasons=[])])

        assert "quality" in call_log
        assert "regime_SHOULD_NOT" not in call_log, (
            "После quality veto не должен вызываться regime gate"
        )
