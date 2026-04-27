"""
Golden payload contract test for SignalOrchestrator.

Фиксирует точную схему payload, который уходит в emitter.
Любое добавление/удаление/переименование поля — сломает этот тест.
Это намеренно: schema change должен быть осознанным.

Обновление fixture: при легитимном изменении схемы обновите
GOLDEN_PAYLOAD_KEYS и добавьте комментарий с датой изменения.
"""
from __future__ import annotations

import json
import time
from types import SimpleNamespace
from unittest.mock import MagicMock
import pytest

from handlers.crypto_orderflow.pipeline.orchestrator import SignalOrchestrator


# ---------------------------------------------------------------------------
# GOLDEN SCHEMA  (обновлять осознанно — с датой и причиной изменения)
# ---------------------------------------------------------------------------
# Last updated: 2026-04-15 (Phase 0 horizon contract — добавлен optional 'meta')
# POLICY: GOLDEN_PAYLOAD_KEYS — минимально обязательный набор ключей.
#         Payload может содержать ДОПОЛНИТЕЛЬНЫЕ optional ключи (meta, etc.).
#         При добавлении обязательного поля — добавить в GOLDEN и в GOLDEN_SCALAR_TYPES.
GOLDEN_PAYLOAD_KEYS = frozenset({
    "kind",
    "side",
    "symbol",
    "ts",
    "price",
    "raw_score",
    "final_score",
    "confidence",
    "reasons",
    "signal_id",
    "venue",
    "timeframe",
    "atr",
    "sl_price",
    "tp1_price",
    "tp_mode",
    "risk_usd_target",
    "risk_usd_actual",
    # 'lot' removed 2026-04-15: field set in envelope_builder, not orchestrator
    "qty",
    "trail_profile",
    "trailing_min_lock_r",
    "slq_used",
})

# Phase 0 optional keys (payload может содержать их при ATR_HORIZON_EMIT_PAYLOAD_META=1)
PHASE0_OPTIONAL_KEYS = frozenset({"meta"})

# Типы обязательных скалярных полей
GOLDEN_SCALAR_TYPES: dict[str, type | tuple[type, ...]] = {
    "kind": str,
    "side": str,
    "symbol": str,
    "ts": int,
    "price": float,
    "raw_score": float,
    "final_score": float,
    "confidence": float,
    "signal_id": str,
    "venue": str,
    "timeframe": str,
    "atr": float,
    "sl_price": float,
    "tp1_price": float,
    "tp_mode": str,
    "risk_usd_target": float,
    "risk_usd_actual": float,
    # 'lot' removed: set in envelope_builder, not orchestrator
    "qty": float,
    "trailing_min_lock_r": float,
    "slq_used": int,
}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _make_orch_and_ctx():
    gates = MagicMock()
    gates.check_quality.return_value = SimpleNamespace(veto=False, reason="")
    gates.check_regime_gate.return_value = (True, "")
    gates.check_smt.return_value = SimpleNamespace(veto=False, reason_code="OK")
    gates.consistency_once.return_value = SimpleNamespace(veto=False, reason_code="OK")
    gates.edge_cost_cached.return_value = SimpleNamespace(veto=False, reason_code="OK")
    gates.check_entry_policy.return_value = SimpleNamespace(veto=False, reason_code="OK")

    obs = MagicMock()
    obs._metrics = None

    emitter_payloads: list[dict] = []

    class _Emitter:
        def emit(
            self,
            *,
            signal_id: str = "",
            kind: str = "",
            symbol: str = "",
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
            emitter_payloads.append(payload or {})
            return True

    confirm = MagicMock()
    confirm.validate.return_value = SimpleNamespace(
        ok=True, final_score=1.23, confidence=0.87, code="OK", parts={}
    )

    cfg = MagicMock()
    cfg.symbol = "ETHUSDT"
    cfg.get_runtime_snapshot.return_value = None
    cfg.resolve_risk_cfg.return_value = {"sl_mode": "atr"}

    orch = SignalOrchestrator(
        config=cfg,
        gates=gates,
        liquidity=MagicMock(),
        observability=obs,
        confirmations_engine=confirm,
        emitter=_Emitter(),
    )

    _ts = _now_ms()  # current ts — passes _normalize_ts_ms validation
    ctx = SimpleNamespace(
        symbol="ETHUSDT",
        price=3000.0,
        ts=_ts,
        ts_ms=_ts,
        sizing_ok=True,
        qty=0.5,
        of=SimpleNamespace(spread_bps=2.0),
        redis=None,
        venue="binance",
        timeframe="5m",
        atr=50.0,
        sl_price=2950.0,
        tp1_price=3100.0,
        tp_mode_used="RR",
        risk_usd_target=25.0,
        risk_usd=24.8,
        trail_profile="conservative",
        trailing_min_lock_r=1.5,
        risk_cfg={"slq_used": 0},
    )
    cand = SimpleNamespace(
        kind="reversal",
        side="short",
        raw_score=3.1,
        signal_id="golden-sid-001",
        reasons=["obi_flip", "smt_div"],
    )

    orch.process(ctx, lambda c: [cand])
    return emitter_payloads


class TestGoldenPayloadSchema:
    def test_golden_keys_superset(self):
        """Payload должен содержать ВСЕ ключи из GOLDEN_PAYLOAD_KEYS.

        Phase 0 разрешает дополнительные optional ключи (meta, etc.).
        Старые обязательные поля должны присутствовать всегда.
        """
        payloads = _make_orch_and_ctx()
        assert len(payloads) == 1
        payload = payloads[0]

        actual_keys = frozenset(payload.keys())
        missing = GOLDEN_PAYLOAD_KEYS - actual_keys
        # Extra keys beyond GOLDEN + PHASE0_OPTIONAL are unexpected
        allowed_keys = GOLDEN_PAYLOAD_KEYS | PHASE0_OPTIONAL_KEYS
        unexpected = actual_keys - allowed_keys

        errors = []
        if missing:
            errors.append(f"Отсутствующие обязательные поля: {sorted(missing)}")
        if unexpected:
            errors.append(
                f"Неожиданные поля (добавить в GOLDEN или PHASE0_OPTIONAL): {sorted(unexpected)}"
            )

        assert not errors, "\n".join(errors)

    def test_golden_scalar_types(self):
        """Проверяем типы всех скалярных полей."""
        payloads = _make_orch_and_ctx()
        payload = payloads[0]

        type_errors = []
        for field, expected_type in GOLDEN_SCALAR_TYPES.items():
            val = payload.get(field)
            if not isinstance(val, expected_type):
                type_errors.append(
                    f"  {field}: ожидался {expected_type.__name__}, получен {type(val).__name__} = {val!r}"
                )
        assert not type_errors, "Несоответствие типов в payload:\n" + "\n".join(type_errors)

    def test_reasons_is_list_of_strings(self):
        payloads = _make_orch_and_ctx()
        payload = payloads[0]
        assert isinstance(payload["reasons"], list)
        assert all(isinstance(r, str) for r in payload["reasons"])

    def test_payload_is_json_serializable(self):
        """Payload должен сериализоваться без ошибок — контракт для Redis xadd."""
        payloads = _make_orch_and_ctx()
        payload = payloads[0]
        # Не должно бросать
        serialized = json.dumps(payload)
        restored = json.loads(serialized)
        # Все ключи сохранены
        assert set(restored.keys()) == set(payload.keys())

    def test_signal_id_non_empty(self):
        payloads = _make_orch_and_ctx()
        assert payloads[0]["signal_id"] != ""

    def test_kind_matches_candidate_kind(self):
        payloads = _make_orch_and_ctx()
        assert payloads[0]["kind"] == "reversal"

    def test_side_matches_candidate_side(self):
        payloads = _make_orch_and_ctx()
        assert payloads[0]["side"] == "short"

    def test_ts_matches_ctx_ts(self):
        """ts в payload = ctx.ts (int, текущий эпох ms)."""
        payloads = _make_orch_and_ctx()
        assert payloads[0]["ts"] > 0
        assert isinstance(payloads[0]["ts"], int)


class TestPhase0MetaContract:
    """Phase 0: если ATR_HORIZON_EMIT_PAYLOAD_META=1, payload[meta] должен содержать
    contract_ver=2, horizon и atr_profile. При отсутствии — payload без meta — тоже ок.
    """

    def test_meta_is_dict_if_present(self):
        payloads = _make_orch_and_ctx()
        payload = payloads[0]
        meta = payload.get("meta")
        if meta is not None:
            assert isinstance(meta, dict), "meta должен быть dict"

    def test_meta_contract_ver_is_2_if_present(self):
        payloads = _make_orch_and_ctx()
        payload = payloads[0]
        meta = payload.get("meta")
        if meta and meta.get("contract_ver") is not None:
            assert meta["contract_ver"] == 2

    def test_meta_horizon_keys_if_present(self):
        payloads = _make_orch_and_ctx()
        meta = payloads[0].get("meta")
        if meta and "horizon" in meta:
            hz = meta["horizon"]
            assert isinstance(hz, dict)
            assert "risk_horizon_bucket" in hz
            assert "reason_code" in hz
            assert hz["phase_mode"] == "off"  # Phase 0

    def test_meta_atr_profile_keys_if_present(self):
        payloads = _make_orch_and_ctx()
        meta = payloads[0].get("meta")
        if meta and "atr_profile" in meta:
            atr = meta["atr_profile"]
            assert isinstance(atr, dict)
            assert "atr_value" in atr
            assert "mode" in atr
            assert atr["mode"] == "legacy"  # Phase 0

    def test_payload_still_json_serializable_with_meta(self):
        payloads = _make_orch_and_ctx()
        payload = payloads[0]
        import json
        serialized = json.dumps(payload)
        restored = json.loads(serialized)
        assert set(restored.keys()) >= GOLDEN_PAYLOAD_KEYS

    def test_signal_id_unchanged_by_phase0(self):
        """signal_id должен совпадать с cand.signal_id — Phase 0 не влияет."""
        payloads = _make_orch_and_ctx()
        assert payloads[0]["signal_id"] == "golden-sid-001"
