from __future__ import annotations

"""
Regression pack для SignalOrchestrator (prod-pipeline).

Coverage: все gate-шаги (T-01..T-22), sizing gate, payload contract,
notional cap, veto metrics, DLQ xadd, edge-gate events.

Мотивация: до этого файла существовало только 2 теста (RSLG veto/pass),
которые не покрывали ни sizing gate, ни полный gate-sequence,
ни multi-candidate батч, ни payload-schema.
"""

import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from handlers.crypto_orderflow.pipeline.orchestrator import SignalOrchestrator

# ---------------------------------------------------------------------------
# Фабрики
# ---------------------------------------------------------------------------

def _make_gate_dec(*, veto: bool = False, reason_code: str = "OK") -> SimpleNamespace:
    return SimpleNamespace(veto=veto, reason_code=reason_code)


def _make_qa(*, veto: bool = False, reason: str = "") -> SimpleNamespace:
    return SimpleNamespace(veto=veto, reason=reason)


def _make_smt(*, veto: bool = False, reason_code: str = "OK") -> SimpleNamespace:
    return SimpleNamespace(veto=veto, reason_code=reason_code)


def _make_confirm_ok(final_score: float = 1.0, confidence: float = 0.9) -> SimpleNamespace:
    return SimpleNamespace(ok=True, final_score=final_score, confidence=confidence, code="OK", parts={})


def _make_confirm_fail(code: str = "VETO_CONFIRM") -> SimpleNamespace:
    return SimpleNamespace(ok=False, final_score=0.0, confidence=0.0, code=code, parts={})


def _make_cand(
    kind: str = "breakout",
    side: str = "long",
    raw_score: float = 2.0,
    signal_id: str = "sid-001",
    reasons: list | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        kind=kind,
        side=side,
        raw_score=raw_score,
        signal_id=signal_id,
        reasons=reasons or ["delta_spike"],
    )


def _now_ms() -> int:
    """Current time in epoch ms — avoids _normalize_ts_ms rejecting stale timestamps."""
    return int(time.time() * 1000)


def _make_ctx(
    symbol: str = "BTCUSDT",
    price: float = 50000.0,
    ts: int | None = None,
    sizing_ok: bool = True,
    spread_bps: float = 3.0,
    redis: object | None = None,
) -> SimpleNamespace:
    _ts = ts if ts is not None else _now_ms()
    return SimpleNamespace(
        symbol=symbol,
        price=price,
        ts=_ts,
        ts_ms=_ts,
        sizing_ok=sizing_ok,
        qty=0.01,
        of=SimpleNamespace(spread_bps=spread_bps),
        redis=redis,
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


class _DummyCfg:
    def __init__(self, symbol: str = "BTCUSDT"):
        self.symbol = symbol

    def get_runtime_snapshot(self):
        return None

    def resolve_risk_cfg(self):
        return {"sl_mode": "atr"}


class _DummyObs:
    def __init__(self):
        self.veto_calls: list[dict] = []
        self.level_calls: list[str] = []

    def emit_veto_metric(self, *, kind: str, ctx, reason_code: str) -> None:
        self.veto_calls.append({"kind": kind, "reason_code": reason_code})

    def emit_level_mode_metric(self, tp_mode: str, ctx) -> None:
        self.level_calls.append(tp_mode)

    @property
    def _metrics(self):
        return None


class _DummyEmitter:
    """Accepts the new keyword-args emit() API introduced in orchestrator update."""
    def __init__(self, *, returns: bool = True):
        self._returns = returns
        self.emitted: list[dict] = []  # stores the 'payload' kwarg
        self.envelope_calls: list[dict] = []  # stores full call kwargs

    def emit(
        self,
        *,
        signal_id: str = "",
        kind: str = "",
        symbol="",
        side: str | None = None,
        ts_event_ms: int = 0,
        ingest_time_ms: int = 0,
        trace_id: str | None = None,
        quality_flags: list | None = None,
        source: str = "python-worker",
        meta_schema_version: int = 1,
        raw_score: float = 0.0,
        final_score: float = 0.0,
        confidence_pct: float = 0.0,
        payload: dict | None = None,
    ) -> bool:
        self.emitted.append(payload or {})
        self.envelope_calls.append({
            "signal_id": signal_id, "kind": kind, "symbol": symbol,
            "side": side, "ts_event_ms": ts_event_ms,
        })
        return self._returns


def _make_orchestrator(
    *,
    qa_veto: bool = False,
    regime_ok: bool = True,
    smt_veto: bool = False,
    consistency_veto: bool = False,
    cost_veto: bool = False,
    confirm_ok: bool = True,
    entry_policy_veto: bool = False,
    emit_returns: bool = True,
    symbol: str = "BTCUSDT",
) -> tuple[SignalOrchestrator, _DummyObs, _DummyEmitter]:
    gates = MagicMock()
    gates.check_quality.return_value = _make_qa(veto=qa_veto, reason="VETO_QUALITY" if qa_veto else "")
    gates.check_regime_gate.return_value = (
        regime_ok, ("" if regime_ok else "VETO_REGIME")
    )
    gates.check_smt.return_value = _make_smt(veto=smt_veto, reason_code="VETO_SMT" if smt_veto else "OK")
    gates.consistency_once.return_value = _make_gate_dec(
        veto=consistency_veto,
        reason_code="VETO_CONSISTENCY" if consistency_veto else "OK",
    )
    gates.edge_cost_cached.return_value = _make_gate_dec(
        veto=cost_veto,
        reason_code="VETO_COST" if cost_veto else "OK",
    )
    gates.check_entry_policy.return_value = _make_gate_dec(
        veto=entry_policy_veto,
        reason_code="VETO_ENTRY_POLICY" if entry_policy_veto else "OK",
    )

    liquidity = MagicMock()
    obs = _DummyObs()
    emitter = _DummyEmitter(returns=emit_returns)

    confirm = MagicMock()
    confirm.validate.return_value = (
        _make_confirm_ok() if confirm_ok else _make_confirm_fail()
    )

    orch = SignalOrchestrator(
        config=_DummyCfg(symbol=symbol),
        gates=gates,
        liquidity=liquidity,
        observability=obs,
        confirmations_engine=confirm,
        emitter=emitter,
    )
    return orch, obs, emitter


# ---------------------------------------------------------------------------
# T-01  Happy path — все gates PASS → emit вызван
# ---------------------------------------------------------------------------
class TestHappyPath:
    def test_T01_all_gates_pass_emit_called(self):
        orch, obs, emitter = _make_orchestrator()
        ctx = _make_ctx()
        result = orch.process(ctx, lambda c: [_make_cand()])

        assert result is True
        assert len(emitter.emitted) == 1
        assert emitter.emitted[0]["kind"] == "breakout"
        assert emitter.emitted[0]["symbol"] == "BTCUSDT"

    def test_T11_empty_candidates_returns_false(self):
        orch, _, emitter = _make_orchestrator()
        ctx = _make_ctx()
        result = orch.process(ctx, lambda c: [])

        assert result is False
        assert len(emitter.emitted) == 0


# ---------------------------------------------------------------------------
# T-02..T-09  Gate veto tests
# ---------------------------------------------------------------------------
class TestGateVetos:
    def test_T02_quality_veto_blocks_emit(self):
        orch, obs, emitter = _make_orchestrator(qa_veto=True)
        result = orch.process(_make_ctx(), lambda c: [_make_cand()])

        assert result is False
        assert len(emitter.emitted) == 0
        assert any(v["reason_code"] == "VETO_QUALITY" for v in obs.veto_calls)

    def test_T03_regime_gate_veto_blocks_emit(self):
        orch, obs, emitter = _make_orchestrator(regime_ok=False)
        result = orch.process(_make_ctx(), lambda c: [_make_cand()])

        assert result is False
        assert len(emitter.emitted) == 0
        assert any(v["reason_code"] == "VETO_REGIME" for v in obs.veto_calls)

    def test_T04_smt_veto_blocks_emit(self):
        orch, obs, emitter = _make_orchestrator(smt_veto=True)
        result = orch.process(_make_ctx(), lambda c: [_make_cand()])

        assert result is False
        assert any(v["reason_code"] == "VETO_SMT" for v in obs.veto_calls)

    def test_T05_consistency_veto_blocks_emit(self):
        orch, obs, emitter = _make_orchestrator(consistency_veto=True)
        result = orch.process(_make_ctx(), lambda c: [_make_cand()])

        assert result is False
        assert any(v["reason_code"] == "VETO_CONSISTENCY" for v in obs.veto_calls)

    def test_T06_edge_cost_veto_blocks_emit(self):
        orch, obs, emitter = _make_orchestrator(cost_veto=True)
        result = orch.process(_make_ctx(), lambda c: [_make_cand()])

        assert result is False
        assert any(v["reason_code"] == "VETO_COST" for v in obs.veto_calls)

    def test_T07_confirm_fail_blocks_emit(self):
        orch, obs, emitter = _make_orchestrator(confirm_ok=False)
        result = orch.process(_make_ctx(), lambda c: [_make_cand()])

        assert result is False
        assert any(v["reason_code"] == "VETO_CONFIRM" for v in obs.veto_calls)

    def test_T08_sizing_ok_false_veto_sizing(self):
        """GAP-1: sizing gate существует только в Orchestrator. Ключевой P0 тест."""
        orch, obs, emitter = _make_orchestrator()
        ctx = _make_ctx(sizing_ok=False)
        result = orch.process(ctx, lambda c: [_make_cand()])

        assert result is False
        assert len(emitter.emitted) == 0
        assert any(v["reason_code"] == "VETO_SIZING" for v in obs.veto_calls), (
            "VETO_SIZING должен быть задиспатчен когда ctx.sizing_ok=False"
        )

    def test_T09_entry_policy_veto_blocks_emit(self):
        orch, obs, emitter = _make_orchestrator(entry_policy_veto=True)
        result = orch.process(_make_ctx(), lambda c: [_make_cand()])

        assert result is False
        assert any(v["reason_code"] == "VETO_ENTRY_POLICY" for v in obs.veto_calls)


# ---------------------------------------------------------------------------
# T-10  Emit error — счётчик инкрементируется
# ---------------------------------------------------------------------------
class TestEmitError:
    def test_T10_emitter_raises_increments_error_counter(self):
        orch, _, _ = _make_orchestrator()
        # заменяем emitter на бросающий
        orch.emitter = MagicMock()
        orch.emitter.emit.side_effect = RuntimeError("redis timeout")

        with patch(
            "handlers.crypto_orderflow.pipeline.orchestrator.SIGNAL_EMIT_ERROR_TOTAL"
        ) as mock_counter:
            mock_counter.labels.return_value.inc = MagicMock()
            result = orch.process(_make_ctx(), lambda c: [_make_cand()])

        # Emit error — возвращаем False (any_sent so far остаётся False)
        assert result is False
        mock_counter.labels.assert_called_once_with(symbol="BTCUSDT")
        mock_counter.labels.return_value.inc.assert_called_once()


# ---------------------------------------------------------------------------
# T-12  Shadow-bug GAP-2: side_val не переопределяется между шагами
# ---------------------------------------------------------------------------
class TestSideValConsistency:
    def test_T12_side_val_consistent_between_smt_and_levels(self):
        """
        GAP-2: side_val присваивался дважды (L179, L222).
        Проверяем, что значение side, которое ушло в check_smt,
        совпадает с тем, что ушло в ensure_trade_levels_once.
        """
        smt_sides: list = []
        levels_sides: list = []

        gates = MagicMock()
        gates.check_quality.return_value = _make_qa()
        gates.check_regime_gate.return_value = (True, "")
        gates.check_entry_policy.return_value = _make_gate_dec()
        gates.edge_cost_cached.return_value = _make_gate_dec()

        def _check_smt(ctx, kind, side):
            smt_sides.append(side)
            return _make_smt()

        gates.check_smt.side_effect = _check_smt
        gates.consistency_once.return_value = _make_gate_dec()

        liquidity = MagicMock()

        def _ensure_levels(*, ctx, symbol, side, kind, cfg, overwrite):
            levels_sides.append(side)

        liquidity.ensure_trade_levels_once.side_effect = _ensure_levels

        obs = _DummyObs()
        emitter = _DummyEmitter()
        confirm = MagicMock()
        confirm.validate.return_value = _make_confirm_ok()

        orch = SignalOrchestrator(
            config=_DummyCfg(),
            gates=gates,
            liquidity=liquidity,
            observability=obs,
            confirmations_engine=confirm,
            emitter=emitter,
        )

        cand = _make_cand(side="short")
        orch.process(_make_ctx(), lambda c: [cand])

        assert len(smt_sides) == 1
        assert len(levels_sides) == 1
        assert smt_sides[0] == levels_sides[0], (
            f"side_val в SMT gate ({smt_sides[0]!r}) "
            f"≠ side_val в ensure_trade_levels ({levels_sides[0]!r})"
        )


# ---------------------------------------------------------------------------
# T-13  Multi-candidate: один прошёл, один вето
# ---------------------------------------------------------------------------
class TestMultiCandidate:
    def test_T13_mixed_batch_one_pass_one_veto(self):
        gates = MagicMock()

        call_count = {"n": 0}

        def _check_quality(ctx, kind, side=""):
            c = call_count["n"]
            call_count["n"] += 1
            # первый кандидат veto, второй pass
            return _make_qa(veto=(c == 0), reason="VETO_QUALITY" if c == 0 else "")

        gates.check_quality.side_effect = _check_quality
        gates.check_regime_gate.return_value = (True, "")
        gates.check_smt.return_value = _make_smt()
        gates.consistency_once.return_value = _make_gate_dec()
        gates.edge_cost_cached.return_value = _make_gate_dec()
        gates.check_entry_policy.return_value = _make_gate_dec()

        obs = _DummyObs()
        emitter = _DummyEmitter()
        confirm = MagicMock()
        confirm.validate.return_value = _make_confirm_ok()

        orch = SignalOrchestrator(
            config=_DummyCfg(),
            gates=gates,
            liquidity=MagicMock(),
            observability=obs,
            confirmations_engine=confirm,
            emitter=emitter,
        )

        cand1 = _make_cand(kind="breakout", signal_id="sid-001")
        cand2 = _make_cand(kind="reversal", signal_id="sid-002")
        result = orch.process(_make_ctx(), lambda c: [cand1, cand2])

        assert result is True
        assert len(emitter.emitted) == 1
        assert emitter.emitted[0]["kind"] == "reversal"
        assert len(obs.veto_calls) == 1
        assert obs.veto_calls[0]["reason_code"] == "VETO_QUALITY"


# ---------------------------------------------------------------------------
# T-14  Payload field contract
# ---------------------------------------------------------------------------
class TestPayloadFields:
    REQUIRED_KEYS = {
        "kind", "side", "symbol", "ts", "price", "raw_score",
        "final_score", "confidence", "reasons", "signal_id",
        "venue", "timeframe", "atr", "sl_price", "tp1_price",
        "tp_mode", "risk_usd_target", "risk_usd_actual",
        "qty", "trail_profile", "trailing_min_lock_r", "slq_used",
    }

    def test_T14_all_required_keys_present(self):
        orch, _, emitter = _make_orchestrator()
        orch.process(_make_ctx(), lambda c: [_make_cand()])

        assert len(emitter.emitted) == 1
        payload = emitter.emitted[0]
        missing = self.REQUIRED_KEYS - payload.keys()
        assert not missing, f"Payload missing keys: {missing}"

    def test_T14b_values_are_json_serializable(self):
        import json

        orch, _, emitter = _make_orchestrator()
        orch.process(_make_ctx(), lambda c: [_make_cand()])

        payload = emitter.emitted[0]
        # Не должно бросать
        json.dumps(payload)

    def test_T14c_ts_is_ctx_ts_not_ts_ms(self):
        """ts в payload берётся из ctx.ts (int), не ctx.ts_ms."""
        orch, _, emitter = _make_orchestrator()
        # Используем текущий timestamp чтобы _normalize_ts_ms не отклонил как too_old
        now = _now_ms()
        ctx = _make_ctx(ts=now)
        orch.process(ctx, lambda c: [_make_cand()])

        payload = emitter.emitted[0]
        assert payload["ts"] == now

    def test_T14d_qty_is_emitted(self):
        orch, _, emitter = _make_orchestrator()
        ctx = _make_ctx()
        ctx.qty = 0.005
        orch.process(ctx, lambda c: [_make_cand()])

        payload = emitter.emitted[0]
        assert payload["qty"] == 0.005


# ---------------------------------------------------------------------------
# T-15  Notional cap clamps qty
# ---------------------------------------------------------------------------
class TestNotionalCap:
    def test_T15_notional_cap_clamps_qty(self, monkeypatch):
        """
        RISK_MAX_NOTIONAL_USD=500, price=50000 → max_qty=0.01
        ctx.qty=0.5 → должно быть зажато до 0.01
        """
        monkeypatch.setenv("RISK_MAX_NOTIONAL_USD", "500")
        monkeypatch.setenv("RISK_MAX_QTY", "0")

        orch, _, emitter = _make_orchestrator()
        ctx = _make_ctx(price=50_000.0)
        ctx.qty = 0.5  # завышенный lot

        orch.process(ctx, lambda c: [_make_cand()])

        payload = emitter.emitted[0]
        expected_max = 500.0 / 50_000.0  # 0.01
        assert payload["qty"] <= expected_max + 1e-9, (
            f"qty={payload['qty']} не зажато до {expected_max}"
        )

    def test_T15b_no_cap_when_qty_within_limit(self, monkeypatch):
        monkeypatch.setenv("RISK_MAX_NOTIONAL_USD", "500")
        monkeypatch.setenv("RISK_MAX_QTY", "0")

        orch, _, emitter = _make_orchestrator()
        ctx = _make_ctx(price=50_000.0)
        ctx.qty = 0.005  # < 0.01 → не зажимать

        orch.process(ctx, lambda c: [_make_cand()])

        payload = emitter.emitted[0]
        assert abs(payload["qty"] - 0.005) < 1e-9

    def test_T15c_risk_max_qty_caps_qty(self, monkeypatch):
        """RISK_MAX_QTY=0.001 → жёстче любого notional cap."""
        monkeypatch.setenv("RISK_MAX_NOTIONAL_USD", "10000")
        monkeypatch.setenv("RISK_MAX_QTY", "0.001")

        orch, _, emitter = _make_orchestrator()
        ctx = _make_ctx(price=50_000.0)
        ctx.qty = 1.0

        orch.process(ctx, lambda c: [_make_cand()])
        payload = emitter.emitted[0]
        assert payload["qty"] <= 0.001 + 1e-9


# ---------------------------------------------------------------------------
# T-16  SIGNAL_BUILD_FAILED_TOTAL при ошибке _build_payload
# ---------------------------------------------------------------------------
class TestBuildFailedMetric:
    def test_T16_build_failed_counter_incremented(self):
        orch, _, emitter = _make_orchestrator()

        with patch.object(orch, "_build_payload", side_effect=ValueError("bad payload")), patch(
            "handlers.crypto_orderflow.pipeline.orchestrator.SIGNAL_BUILD_FAILED_TOTAL"
        ) as mock_cnt:
            mock_cnt.labels.return_value.inc = MagicMock()
            result = orch.process(_make_ctx(), lambda c: [_make_cand()])

        assert result is False
        mock_cnt.labels.assert_called_with(symbol="BTCUSDT")
        mock_cnt.labels.return_value.inc.assert_called()


# ---------------------------------------------------------------------------
# T-17  DLQ xadd при veto — проверяем параметры redis
# ---------------------------------------------------------------------------
class TestDLQPublish:
    def test_T17_dlq_xadd_called_on_veto(self):
        """Veto с redis-клиентом на ctx → xadd в DLQ."""
        redis_mock = MagicMock()

        orch, _, emitter = _make_orchestrator(qa_veto=True)
        ctx = _make_ctx(redis=redis_mock)

        orch.process(ctx, lambda c: [_make_cand()])

        redis_mock.xadd.assert_called()
        args, kwargs = redis_mock.xadd.call_args
        stream_key = args[0] if args else kwargs.get("name", "")
        assert "dlq" in str(stream_key).lower(), (
            f"Ожидался dlq stream, получен: {stream_key!r}"
        )

    def test_T17b_dlq_not_called_without_redis(self):
        """Без redis-клиента DLQ-паблиш не должен бросать исключение."""
        orch, _, emitter = _make_orchestrator(qa_veto=True)
        ctx = _make_ctx(redis=None)

        # Не должно бросать
        orch.process(ctx, lambda c: [_make_cand()])


# ---------------------------------------------------------------------------
# T-18/T-19  Edge gate events
# ---------------------------------------------------------------------------
class TestEdgeGateEvents:
    def test_T18_no_edge_event_when_mode_off(self, monkeypatch):
        monkeypatch.setenv("EDGE_GATE_EVENTS_MODE", "off")
        redis_mock = MagicMock()

        orch, _, emitter = _make_orchestrator()
        ctx = _make_ctx(redis=redis_mock)
        orch.process(ctx, lambda c: [_make_cand()])

        # Проверяем: xadd НЕ вызывался для edge_gate_events
        for c in redis_mock.xadd.call_args_list:
            stream = c.args[0] if c.args else c.kwargs.get("name", "")
            assert "edge_gate" not in str(stream).lower(), (
                f"Не ожидался edge_gate xadd при mode=off, получен: {stream!r}"
            )

    def test_T19_edge_event_published_when_mode_stream(self, monkeypatch):
        monkeypatch.setenv("EDGE_GATE_EVENTS_MODE", "redis_stream")
        monkeypatch.setenv("EDGE_GATE_SAMPLE_PASS", "1.0")  # 100% sampling
        redis_mock = MagicMock()

        orch, _, emitter = _make_orchestrator()
        ctx = _make_ctx(redis=redis_mock)
        orch.process(ctx, lambda c: [_make_cand()])

        edge_gate_calls = [
            c for c in redis_mock.xadd.call_args_list
            if "edge_gate" in str(c.args[0] if c.args else c.kwargs.get("name", "")).lower()
        ]
        assert len(edge_gate_calls) >= 1, "Ожидался xadd edge_gate_events при mode=redis_stream"


# ---------------------------------------------------------------------------
# T-20  slq_used берётся из ctx.risk_cfg
# ---------------------------------------------------------------------------
class TestSlqUsed:
    def test_T21_slq_used_from_risk_cfg(self):
        """
        slq_used читается из ctx.risk_cfg["slq_used"].
        Мокируем maybe_apply_slq_to_risk_cfg чтобы он не перезаписывал risk_cfg
        без slq_used (именно это делает реальная функция).
        """
        orch, _, emitter = _make_orchestrator()
        ctx = _make_ctx()
        ctx.risk_cfg = {"slq_used": 1}

        # maybe_apply_slq_to_risk_cfg импортируется локально внутри process() через:
        # 'from services.slq_risk_adjust import maybe_apply_slq_to_risk_cfg'
        # поэтому патчим по источнику, а не по модулю orchestrator
        with patch(
            "services.slq_risk_adjust.maybe_apply_slq_to_risk_cfg",
            side_effect=lambda redis, ctx, symbol, side, cfg: dict(cfg, slq_used=1),
        ):
            orch.process(ctx, lambda c: [_make_cand()])

        payload = emitter.emitted[0]
        assert payload["slq_used"] == 1

    def test_T21b_slq_used_default_zero(self):
        orch, _, emitter = _make_orchestrator()
        ctx = _make_ctx()
        ctx.risk_cfg = {}

        orch.process(ctx, lambda c: [_make_cand()])

        payload = emitter.emitted[0]
        assert payload["slq_used"] == 0



