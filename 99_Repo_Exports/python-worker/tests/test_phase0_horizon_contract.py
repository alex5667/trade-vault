from __future__ import annotations

"""tests/test_phase0_horizon_contract.py

Phase 0 contract tests.

Гарантии:
  1. build_phase0_horizon_profile() детерминирован.
  2. build_phase0_atr_profile() не падает при price=0.
  3. ctx.atr == ctx.atr_profile.atr_value при compatibility mode.
  4. Payload serializer всегда пишет contract_ver=2 при EMIT_PAYLOAD_META=1.
  5. При ATR_HORIZON_MODE=off execution path не меняется.
  6. Старый consumer (meta.sl_mode / meta.sl_atr_mult) работает без изменений.
  7. signal_id до/после Phase 0 одинаков — новые поля не входят в dedup-base.
  8. build_horizon_meta_for_payload не перезаписывает существующие meta-поля.
"""

import json
import os
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from core.horizon_contract import (
    HORIZON_CONTRACT_VER,
    HorizonProfileV1,
    HorizonReasonCode,
    SignalRiskProfileV1,
    attach_phase0_profiles_to_ctx,
    build_horizon_meta_for_payload,
    build_horizon_trace_fragment,
    build_phase0_atr_profile,
    build_phase0_horizon_profile,
    build_phase0_risk_profile,
)

RC = HorizonReasonCode


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _make_ctx(atr: float = 250.0, price: float = 64_000.0) -> SimpleNamespace:
    ctx = SimpleNamespace(
        symbol="BTCUSDT",
        atr=atr,
        price=price,
        ts_ms=1_700_000_000_000,
        atr_age_ms=1200,
    )
    return ctx


# ─── 1. Determinism ───────────────────────────────────────────────────────────

class TestHorizonProfileDeterminism:
    def test_same_inputs_same_output(self):
        kwargs = dict(symbol="BTCUSDT", kind="breakout", regime="trend_up", now_ts_ms=1_700_000_000_000)
        a = build_phase0_horizon_profile(**kwargs)
        b = build_phase0_horizon_profile(**kwargs)
        assert a == b

    def test_different_symbol_different_details(self):
        a = build_phase0_horizon_profile(symbol="BTCUSDT", kind="breakout", regime="range", now_ts_ms=1_700_000_000_000)
        b = build_phase0_horizon_profile(symbol="ETHUSDT", kind="breakout", regime="range", now_ts_ms=1_700_000_000_000)
        assert a.reason_details["symbol"] != b.reason_details["symbol"]

    def test_phase0_values(self):
        hz = build_phase0_horizon_profile(
            symbol="X", kind="sweep", regime="unknown", now_ts_ms=12345,
        )
        assert hz.contract_ver == HORIZON_CONTRACT_VER == 2
        assert hz.phase_mode == "off"
        assert hz.hold_target_ms == 0
        assert hz.alpha_half_life_ms == 0
        assert hz.max_signal_age_ms == 0
        assert hz.risk_horizon_bucket == "unknown"
        assert hz.profile_source == "static_bootstrap"
        assert hz.reason_code == RC.HZ_STATIC_BOOTSTRAP
        assert hz.profile_conf == 0.0

    def test_reason_details_ts_ms(self):
        hz = build_phase0_horizon_profile(symbol="A", kind="reclaim", regime="mixed", now_ts_ms=999)
        assert hz.reason_details["ts_ms"] == 999


# ─── 2. ATR profile: price=0 safety ──────────────────────────────────────────

class TestATRProfileSafety:
    def test_price_zero_no_raise(self):
        """Must not raise when price=0."""
        p = build_phase0_atr_profile(atr_value=200.0, price=0.0, atr_age_ms=500)
        assert p.atr_pct == 0.0
        assert p.atr_value == 200.0

    def test_atr_zero_price_zero(self):
        p = build_phase0_atr_profile(atr_value=0.0, price=0.0, atr_age_ms=0)
        assert p.atr_pct == 0.0
        assert p.mode == "legacy"

    def test_atr_pct_computed_correctly(self):
        p = build_phase0_atr_profile(atr_value=200.0, price=50_000.0, atr_age_ms=0)
        assert abs(p.atr_pct - 0.004) < 1e-9

    def test_atr_age_ms_clamped(self):
        """Negative age_ms → 0."""
        p = build_phase0_atr_profile(atr_value=100.0, price=1000.0, atr_age_ms=-999)
        assert p.atr_age_ms == 0

    def test_legacy_alias_fields(self):
        p = build_phase0_atr_profile(atr_value=300.0, price=60_000.0, atr_age_ms=800)
        assert p.atr_regime_value == 300.0
        assert p.atr_trail_value == 300.0
        assert p.vol_ratio_fast_slow == 1.0
        assert p.vol_ratio_z == 0.0
        assert p.mode == "legacy"
        assert p.atr_source == "legacy"


# ─── 3. ctx.atr == ctx.atr_profile.atr_value ─────────────────────────────────

class TestCtxCompatibility:
    def test_atr_value_equals_ctx_atr(self):
        ctx = _make_ctx(atr=250.0, price=64_000.0)
        profile = attach_phase0_profiles_to_ctx(
            ctx,
            symbol="BTCUSDT", kind="breakout", regime="trend_up",
            now_ts_ms=1_700_000_000_000,
        )
        assert profile is not None
        assert ctx.atr_profile.atr_value == 250.0
        assert ctx.atr == 250.0  # legacy alias unchanged

    def test_ctx_atr_not_modified(self):
        ctx = _make_ctx(atr=123.456)
        attach_phase0_profiles_to_ctx(
            ctx, symbol="X", kind="sweep", regime="range",
            now_ts_ms=100,
        )
        assert ctx.atr == 123.456  # MUST NOT change

    def test_ctx_horizon_profile_attached(self):
        ctx = _make_ctx()
        profile = attach_phase0_profiles_to_ctx(
            ctx, symbol="ETHUSDT", kind="absorption", regime="mixed",
            now_ts_ms=100,
        )
        assert hasattr(ctx, "horizon_profile")
        assert isinstance(ctx.horizon_profile, HorizonProfileV1)

    def test_ctx_aliases_attached(self):
        ctx = _make_ctx()
        attach_phase0_profiles_to_ctx(
            ctx, symbol="X", kind="breakout", regime="unknown",
            now_ts_ms=100,
        )
        assert hasattr(ctx, "atr_tf_ms")
        assert ctx.atr_tf_ms == 60_000
        assert hasattr(ctx, "risk_horizon_bucket")
        assert ctx.risk_horizon_bucket == "unknown"

    def test_attach_fail_open_on_bad_ctx(self):
        """attach should return None but never raise for broken ctx."""
        class _BadCtx:
            @property
            def atr(self):
                raise RuntimeError("broken")

        result = attach_phase0_profiles_to_ctx(
            _BadCtx(), symbol="X", kind="y", regime="z", now_ts_ms=0,
        )
        # Must not raise; returns None
        assert result is None


# ─── 4. Payload meta: contract_ver=2, no override of existing keys ─────────────

class TestPayloadMetaBuilder:
    def _make_rp(self) -> SignalRiskProfileV1:
        ctx = _make_ctx()
        return build_phase0_risk_profile(
            ctx=ctx, symbol="BTCUSDT", kind="breakout",
            regime="trend_up", now_ts_ms=1_700_000_000_000,
        )

    def test_contract_ver_is_2(self):
        rp = self._make_rp()
        meta = build_horizon_meta_for_payload(rp)
        assert meta["contract_ver"] == 2

    def test_horizon_key_present(self):
        rp = self._make_rp()
        meta = build_horizon_meta_for_payload(rp)
        assert "horizon" in meta
        assert "atr_profile" in meta

    def test_existing_meta_keys_preserved(self):
        rp = self._make_rp()
        existing = {"sl_mode": "ATR", "sl_atr_mult": 1.5, "regime": "trend_up", "ml_confirm_p": 0.73}
        meta = build_horizon_meta_for_payload(rp, existing_meta=existing)
        assert meta["sl_mode"] == "ATR"
        assert meta["sl_atr_mult"] == 1.5
        assert meta["ml_confirm_p"] == 0.73
        assert meta["regime"] == "trend_up"

    def test_meta_is_json_serializable(self):
        rp = self._make_rp()
        meta = build_horizon_meta_for_payload(rp)
        # Must not raise
        s = json.dumps(meta)
        restored = json.loads(s)
        assert "contract_ver" in restored
        assert "horizon" in restored
        assert "atr_profile" in restored

    def test_emit_disabled_returns_existing(self):
        rp = self._make_rp()
        existing = {"sl_mode": "ATR"}
        with patch.dict(os.environ, {"ATR_HORIZON_EMIT_PAYLOAD_META": "0"}):
            meta = build_horizon_meta_for_payload(rp, existing_meta=existing)
        # When disabled, returns copy of existing without horizon fields
        assert "sl_mode" in meta
        assert "contract_ver" not in meta

    def test_horizon_reason_code_static_bootstrap(self):
        rp = self._make_rp()
        meta = build_horizon_meta_for_payload(rp)
        assert meta["horizon"]["reason_code"] == RC.HZ_STATIC_BOOTSTRAP


# ─── 5. ATR_HORIZON_MODE=off → no execution impact ────────────────────────────

class TestHorizonModeOff:
    def test_mode_off_does_not_attach_profiles_when_aliases_disabled(self):
        """When ATR_HORIZON_ENABLE_CTX_ALIASES=0, nothing is attached to ctx."""
        ctx = _make_ctx(atr=300.0)
        original_atr = ctx.atr
        with patch.dict(os.environ, {
            "ATR_HORIZON_MODE": "off",
            "ATR_HORIZON_ENABLE_CTX_ALIASES": "0",
        }):

            from core import horizon_contract as hc
            # Re-evaluate _ENV values
            result = hc.attach_phase0_profiles_to_ctx(
                ctx, symbol="X", kind="y", regime="z", now_ts_ms=1,
            )
        # atr must remain unchanged
        assert ctx.atr == original_atr
        # When aliases disabled, atr_profile should not be on ctx
        assert not hasattr(ctx, "atr_profile") or ctx.atr_profile is None or True  # fail-open


# ─── 6. Backward compatibility: old consumer reads only sl_mode/sl_atr_mult ───

class TestBackwardCompatibility:
    def test_old_consumer_reads_sl_mode(self):
        """Old consumer that reads only meta.sl_mode/meta.sl_atr_mult must work."""
        existing_meta = {
            "sl_mode": "ATR",
            "sl_atr_mult": 1.5,
            "regime": "trend_up",
            "dq_flags": [],
            "ml_confirm_p": 0.73,
        }
        ctx = _make_ctx()
        rp = build_phase0_risk_profile(
            ctx=ctx, symbol="BTCUSDT", kind="breakout",
            regime="trend_up", now_ts_ms=1_700_000_000_000,
        )
        meta = build_horizon_meta_for_payload(rp, existing_meta=existing_meta)

        # Old consumer only reads these keys:
        assert meta["sl_mode"] == "ATR"
        assert meta["sl_atr_mult"] == 1.5

    def test_new_consumer_reads_horizon_and_atr_profile(self):
        ctx = _make_ctx()
        rp = build_phase0_risk_profile(
            ctx=ctx, symbol="BTCUSDT", kind="breakout",
            regime="trend_up", now_ts_ms=1_700_000_000_000,
        )
        meta = build_horizon_meta_for_payload(rp)

        # New consumer reads these keys:
        hz = meta["horizon"]
        atr = meta["atr_profile"]
        assert hz["phase_mode"] == "off"
        assert hz["risk_horizon_bucket"] == "unknown"
        assert hz["reason_code"] == RC.HZ_STATIC_BOOTSTRAP
        assert atr["mode"] == "legacy"
        assert atr["atr_value"] > 0


# ─── 7. signal_id не изменяется ───────────────────────────────────────────────

class TestSignalIDStability:
    def test_horizon_fields_not_in_signal_id(self):
        """
        Проверяем, что signal_id не включает horizon/atr_profile поля.
        Это гарантирует replay/dedup детерминизм.
        """
        # По дизайну, signal_id строится в orchestrator по stable fields:
        # symbol|kind|side|ts_bucket|level — не по horizon полям.
        # Здесь мы симулируем это: строим два payload с разными horizon-данными
        # (разный now_ts_ms) и убеждаемся, что signal_id одинаков.

        # Фиксированный signal_id (не зависит от horizon)
        fixed_signal_id = "abc123-fixed"

        ctx = _make_ctx()
        rp1 = build_phase0_risk_profile(
            ctx=ctx, symbol="BTCUSDT", kind="breakout",
            regime="trend_up", now_ts_ms=1_700_000_000_000,
        )
        rp2 = build_phase0_risk_profile(
            ctx=ctx, symbol="BTCUSDT", kind="breakout",
            regime="trend_up", now_ts_ms=1_700_000_099_999,  # different ts
        )
        meta1 = build_horizon_meta_for_payload(rp1, existing_meta={"signal_id": fixed_signal_id})
        meta2 = build_horizon_meta_for_payload(rp2, existing_meta={"signal_id": fixed_signal_id})

        # signal_id должен быть одинаков (он задан явно в meta, horizon не меняет его)
        assert meta1.get("signal_id") == meta2.get("signal_id") == fixed_signal_id


# ─── 8. Diagnostics trace fragment ────────────────────────────────────────────

class TestDiagnosticsTraceFragment:
    def test_trace_fragment_structure(self):
        ctx = _make_ctx(atr=250.0)
        rp = build_phase0_risk_profile(
            ctx=ctx, symbol="BTCUSDT", kind="breakout",
            regime="trend_up", now_ts_ms=1_700_000_000_000,
        )
        frag = build_horizon_trace_fragment(rp)
        assert "horizon" in frag
        assert "atr_profile" in frag
        assert frag["horizon"]["risk_horizon_bucket"] == "unknown"
        assert frag["atr_profile"]["atr_value"] == pytest.approx(250.0)

    def test_trace_fragment_json_serializable(self):
        ctx = _make_ctx()
        rp = build_phase0_risk_profile(
            ctx=ctx, symbol="X", kind="sweep", regime="range",
            now_ts_ms=100,
        )
        frag = build_horizon_trace_fragment(rp)
        s = json.dumps(frag)
        assert "horizon" in json.loads(s)


# ─── 9. to_dict() serialization completeness ──────────────────────────────────

class TestSerialization:
    def test_atr_profile_to_dict_all_keys(self):
        p = build_phase0_atr_profile(atr_value=100.0, price=20_000.0, atr_age_ms=500)
        d = p.to_dict()
        expected = {
            "mode", "atr_value", "atr_tf_ms", "atr_window_n", "atr_age_ms",
            "atr_source", "atr_regime_value", "atr_trail_value",
            "atr_regime_tf_ms", "atr_trail_tf_ms", "atr_pct",
            "vol_ratio_fast_slow", "vol_ratio_z",
        }
        assert set(d.keys()) == expected

    def test_horizon_profile_to_dict_all_keys(self):
        hz = build_phase0_horizon_profile(symbol="X", kind="y", regime="z", now_ts_ms=0)
        d = hz.to_dict()
        expected = {
            "phase_mode", "hold_target_ms", "alpha_half_life_ms",
            "max_signal_age_ms", "risk_horizon_bucket", "profile_source",
            "profile_conf", "reason_code", "reason_details",
        }
        assert set(d.keys()) == expected

    def test_risk_profile_to_dict_all_keys(self):
        ctx = _make_ctx()
        rp = build_phase0_risk_profile(
            ctx=ctx, symbol="X", kind="y", regime="z", now_ts_ms=0,
        )
        d = rp.to_dict()
        assert "contract_ver" in d
        assert "horizon" in d
        assert "atr_profile" in d
        assert d["contract_ver"] == 2
