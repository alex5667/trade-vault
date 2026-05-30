"""Regression suite for 2026-05-29 stop-floor stack hardening.

Covers four structural fixes:

  #1  fees-aware unified ATR floor is now evaluated for every trail profile
      (was rocket_v1-only — protective_only / range setups bypassed the
      "SL covers fees+spread+TP buffer" check entirely).

  #2  When ``atr <= 0`` inside ``_calculate_levels`` the SL_ATR_MULT_FLOOR
      previously silently fell through. The new path emits telemetry always
      and, under ``SL_ATR_FLOOR_VETO_ON_ZERO_ATR=1``, collapses ``stop_dist``
      to zero so the downstream profitability gate vetoes the signal.

  #3  Meme-symbol relaxation (``×0.05`` on unified_th) now has an absolute
      bps floor (``ATR_UNIFIED_MEME_ABS_FLOOR_BPS``, default 20 bps =
      2 × FEES_BPS_RT) so the 95 % relax cannot let SL fall below roundtrip
      fee coverage.

  #4  bounded_sl: default ``BOUNDED_SL_MIN_SAMPLES`` lowered 30 → 15 and a
      group-level (global p50) fallback is consulted when the per-symbol
      sample count is too thin.
"""

from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


# --------------------------------------------------------------------------- #
# Fix #1 + #3 — check_atr_floor unified threshold across profiles + meme floor
# --------------------------------------------------------------------------- #

from handlers.crypto_orderflow.components.gates import GateOrchestrator
from core.gates.decision import GateDecisionV1


def _make_gate_with_atr_floor(thr_bps: float, atr_bps: float) -> GateOrchestrator:
    """GateOrchestrator wired with a stub AtrFloorGate that returns a fixed
    base-tier result (ALLOW + thr/atr in notes)."""
    fake_inner = MagicMock()
    fake_inner.evaluate.return_value = SimpleNamespace(
        decision="ALLOW",
        reason_code="OK",
        notes={"thr": thr_bps, "atr": atr_bps},
    )
    return GateOrchestrator(
        entry_policy=None,
        cost_gate=None,
        atr_floor_gate=fake_inner,
    )


def _ctx(symbol: str, trail_profile: str, atr_bps_exec: float = 0.0):
    return SimpleNamespace(
        symbol=symbol,
        ts_ms=1_700_000_000_000,
        indicators={
            "trail_profile": trail_profile,
            "atr_bps_exec": atr_bps_exec,
        },
        config={"tp_ratio": "0.5,0.3,0.2", "tp_rr": "1.3,2.0,2.7"},
    )


def test_fix1_non_rocket_unified_threshold_shadow_does_not_veto(monkeypatch):
    """protective_only profile gets the unified threshold computed and
    'would_veto' annotated, but does NOT veto by default (shadow on)."""
    monkeypatch.setenv("ATR_UNIFIED_GATE_ALL_PROFILES_ENABLED", "1")
    monkeypatch.setenv("ATR_UNIFIED_GATE_NON_ROCKET_SHADOW", "1")
    monkeypatch.setenv("FEES_BPS_RT", "10.0")
    monkeypatch.setenv("TP_BPS_BUFFER", "5.0")
    monkeypatch.setenv("SL_ATR_MULT_FLOOR", "0.78")

    orch = _make_gate_with_atr_floor(thr_bps=0.0, atr_bps=10.0)
    dec = orch.check_atr_floor(
        ctx=_ctx("BTCUSDT", trail_profile="protective_only", atr_bps_exec=10.0),
        kind="of",
    )
    assert dec.decision == "ALLOW", "shadow path must not veto"
    assert dec.notes["unified_gate_profile"] == "non_rocket"
    assert dec.notes["unified_th_would_veto"] == 1
    assert dec.notes["unified_th_shadow"] == 1
    # threshold must be > current atr (otherwise the test is uninformative)
    assert dec.notes["effective_th"] > 10.0


def test_fix1_non_rocket_unified_threshold_enforce_does_veto(monkeypatch):
    monkeypatch.setenv("ATR_UNIFIED_GATE_ALL_PROFILES_ENABLED", "1")
    monkeypatch.setenv("ATR_UNIFIED_GATE_NON_ROCKET_SHADOW", "0")
    monkeypatch.setenv("FEES_BPS_RT", "10.0")
    monkeypatch.setenv("TP_BPS_BUFFER", "5.0")
    monkeypatch.setenv("SL_ATR_MULT_FLOOR", "0.78")

    orch = _make_gate_with_atr_floor(thr_bps=0.0, atr_bps=10.0)
    dec = orch.check_atr_floor(
        ctx=_ctx("BTCUSDT", trail_profile="protective_only", atr_bps_exec=10.0),
        kind="of",
    )
    assert dec.decision == "DENY"
    assert dec.reason_code == "VETO_ATR_UNIFIED"


def test_fix1_rocket_v1_keeps_enforce_semantics(monkeypatch):
    """rocket_v1 path must keep its pre-existing enforce-by-default behaviour."""
    monkeypatch.setenv("ATR_UNIFIED_GATE_ALL_PROFILES_ENABLED", "1")
    monkeypatch.setenv("ATR_UNIFIED_GATE_NON_ROCKET_SHADOW", "1")
    monkeypatch.setenv("FEES_BPS_RT", "10.0")
    monkeypatch.setenv("TP_BPS_BUFFER", "5.0")

    orch = _make_gate_with_atr_floor(thr_bps=0.0, atr_bps=5.0)
    dec = orch.check_atr_floor(
        ctx=_ctx("BTCUSDT", trail_profile="rocket_v1", atr_bps_exec=5.0),
        kind="of",
    )
    assert dec.decision == "DENY"
    assert dec.reason_code == "VETO_ATR_UNIFIED"


def test_fix1_master_kill_switch_disables_unified(monkeypatch):
    monkeypatch.setenv("ATR_UNIFIED_GATE_ALL_PROFILES_ENABLED", "0")

    orch = _make_gate_with_atr_floor(thr_bps=0.0, atr_bps=1.0)
    dec = orch.check_atr_floor(
        ctx=_ctx("BTCUSDT", trail_profile="protective_only"),
        kind="of",
    )
    assert dec.decision == "ALLOW"
    assert "unified_th" not in dec.notes


def test_fix3_meme_abs_floor_shadow_annotates_but_does_not_apply(monkeypatch):
    """PEPE-family relaxation × 0.05 can drop effective_th below fees coverage.
    Shadow mode must annotate `meme_abs_floor_would_apply` but not raise the
    effective threshold."""
    monkeypatch.setenv("ATR_UNIFIED_GATE_ALL_PROFILES_ENABLED", "1")
    monkeypatch.setenv("ATR_UNIFIED_GATE_NON_ROCKET_SHADOW", "0")
    monkeypatch.setenv("FEES_BPS_RT", "10.0")
    monkeypatch.setenv("TP_BPS_BUFFER", "5.0")
    monkeypatch.setenv("ATR_UNIFIED_MEME_ABS_FLOOR_BPS", "20.0")
    monkeypatch.setenv("ATR_UNIFIED_MEME_ABS_FLOOR_SHADOW", "1")

    orch = _make_gate_with_atr_floor(thr_bps=0.0, atr_bps=5.0)
    dec = orch.check_atr_floor(
        ctx=_ctx("PEPEUSDT", trail_profile="rocket_v1", atr_bps_exec=5.0),
        kind="of",
    )
    # relaxed_th = unified_th * 0.05 which is far below 20 bps abs floor
    assert dec.notes["meme_relaxed_th"] < dec.notes["meme_abs_floor_bps"]
    assert dec.notes.get("meme_abs_floor_would_apply") == 1
    assert "meme_abs_floor_applied" not in dec.notes


def test_fix3_meme_abs_floor_enforce_raises_effective_threshold(monkeypatch):
    monkeypatch.setenv("ATR_UNIFIED_GATE_ALL_PROFILES_ENABLED", "1")
    monkeypatch.setenv("ATR_UNIFIED_GATE_NON_ROCKET_SHADOW", "0")
    monkeypatch.setenv("FEES_BPS_RT", "10.0")
    monkeypatch.setenv("TP_BPS_BUFFER", "5.0")
    monkeypatch.setenv("ATR_UNIFIED_MEME_ABS_FLOOR_BPS", "20.0")
    monkeypatch.setenv("ATR_UNIFIED_MEME_ABS_FLOOR_SHADOW", "0")

    # atr=15 bps would pass the ×0.05 relax (effective_th ≈ 1 bps)
    # but should now fail the 20-bps absolute floor
    orch = _make_gate_with_atr_floor(thr_bps=0.0, atr_bps=15.0)
    dec = orch.check_atr_floor(
        ctx=_ctx("PEPEUSDT", trail_profile="rocket_v1", atr_bps_exec=15.0),
        kind="of",
    )
    assert dec.decision == "DENY"
    assert dec.reason_code == "VETO_ATR_UNIFIED"
    assert dec.notes["meme_abs_floor_applied"] == 1
    assert dec.notes["effective_th"] >= 20.0


# --------------------------------------------------------------------------- #
# Fix #4 — bounded_sl: lowered MIN_SAMPLES + group fallback
# --------------------------------------------------------------------------- #

from signals.bounded_sl import resolve_mae_floor_bps, _reset_cache_for_tests  # noqa: E402
import signals.bounded_sl as _bsl  # noqa: E402


@pytest.fixture(autouse=True)
def _wipe_bsl_cache():
    _reset_cache_for_tests()
    yield
    _reset_cache_for_tests()


def test_fix4_default_min_samples_is_15(monkeypatch):
    """The default lowered from 30 → 15. With 20 samples (was rejected
    pre-fix), the per-symbol p75 is now trusted."""
    monkeypatch.delenv("BOUNDED_SL_MIN_SAMPLES", raising=False)

    monkeypatch.setattr(
        _bsl,
        "_read_priors_from_redis",
        lambda sym: {
            "p50_mae_bps_30d": 30.0,
            "p75_mae_bps_30d": 60.0,
            "p90_mae_bps_30d": 90.0,
            "sample_count": 20.0,
        },
    )
    monkeypatch.setattr(_bsl, "_read_group_priors_from_redis", lambda: {})

    floor_bps, meta = resolve_mae_floor_bps("ALTCOIN")
    assert floor_bps == 60.0
    assert meta["source"] == 2.0  # per-symbol
    assert meta["sample_count"] == 20.0


def test_fix4_group_fallback_used_when_symbol_thin(monkeypatch):
    """Thin per-symbol prior (5 samples < min 15) falls back to global p50."""
    monkeypatch.delenv("BOUNDED_SL_MIN_SAMPLES", raising=False)
    monkeypatch.setenv("BOUNDED_SL_GROUP_FALLBACK_ENABLED", "1")
    monkeypatch.setenv("BOUNDED_SL_GROUP_MIN_SAMPLES", "50")

    monkeypatch.setattr(
        _bsl,
        "_read_priors_from_redis",
        lambda sym: {
            "p50_mae_bps_30d": 0.0,
            "p75_mae_bps_30d": 50.0,
            "p90_mae_bps_30d": 80.0,
            "sample_count": 5.0,
        },
    )
    monkeypatch.setattr(
        _bsl,
        "_read_group_priors_from_redis",
        lambda: {
            "p50_mae_bps_30d": 35.0,
            "p75_mae_bps_30d": 70.0,
            "p90_mae_bps_30d": 110.0,
            "sample_count": 250.0,
        },
    )

    floor_bps, meta = resolve_mae_floor_bps("THINSYMUSDT")
    assert floor_bps == 35.0  # group p50, not symbol p75
    assert meta["source"] == 3.0
    assert meta["group_sample_count"] == 250.0
    assert meta["group_p50_bps_raw"] == 35.0


def test_fix4_group_fallback_disabled_returns_zero_when_thin(monkeypatch):
    monkeypatch.delenv("BOUNDED_SL_MIN_SAMPLES", raising=False)
    monkeypatch.setenv("BOUNDED_SL_GROUP_FALLBACK_ENABLED", "0")

    monkeypatch.setattr(
        _bsl,
        "_read_priors_from_redis",
        lambda sym: {
            "p50_mae_bps_30d": 0.0,
            "p75_mae_bps_30d": 50.0,
            "p90_mae_bps_30d": 80.0,
            "sample_count": 5.0,
        },
    )
    # group reader must NOT be called when fallback is off
    monkeypatch.setattr(
        _bsl,
        "_read_group_priors_from_redis",
        lambda: pytest.fail("group reader must be skipped when fallback disabled"),
    )

    floor_bps, meta = resolve_mae_floor_bps("THINSYMUSDT")
    assert floor_bps == 0.0
    assert meta["source"] == 0.0


def test_fix4_group_fallback_requires_min_samples(monkeypatch):
    """Group must also clear its own min_samples or the fallback stays zero."""
    monkeypatch.delenv("BOUNDED_SL_MIN_SAMPLES", raising=False)
    monkeypatch.setenv("BOUNDED_SL_GROUP_FALLBACK_ENABLED", "1")
    monkeypatch.setenv("BOUNDED_SL_GROUP_MIN_SAMPLES", "50")

    monkeypatch.setattr(
        _bsl,
        "_read_priors_from_redis",
        lambda sym: {
            "p50_mae_bps_30d": 0.0,
            "p75_mae_bps_30d": 0.0,
            "p90_mae_bps_30d": 0.0,
            "sample_count": 0.0,
        },
    )
    monkeypatch.setattr(
        _bsl,
        "_read_group_priors_from_redis",
        lambda: {
            "p50_mae_bps_30d": 35.0,
            "p75_mae_bps_30d": 70.0,
            "p90_mae_bps_30d": 110.0,
            "sample_count": 10.0,  # below 50 min
        },
    )

    floor_bps, meta = resolve_mae_floor_bps("RAREUSDT")
    assert floor_bps == 0.0
    assert meta["group_sample_count"] == 10.0


# --------------------------------------------------------------------------- #
# Fix #2 — sl_floor_atr_invalid: telemetry always; veto under env flag
# --------------------------------------------------------------------------- #
#
# We exercise the inline logic added inside _calculate_levels (signal_pipeline)
# without spinning up the whole orchestrator. The relevant fragment is small
# enough to reproduce here verbatim — keeping the test focused on Fix #2's
# behavioural contract (indicator vs stop_dist collapse).

def _sl_floor_branch(atr: float, stop_dist: float, sl_atr_floor: float, indicators: dict) -> float:
    """Mirror of the production fragment after the 2026-05-29 patch."""
    if atr > 0 and stop_dist > 0:
        _actual = stop_dist / atr
        if _actual < sl_atr_floor:
            indicators["sl_atr_mult_floored"] = 1
            indicators["sl_atr_mult_original"] = round(_actual, 4)
            stop_dist = atr * sl_atr_floor
    elif atr <= 0:
        indicators["sl_floor_atr_invalid"] = 1
        indicators["sl_floor_atr_invalid_reason"] = "atr_zero_or_negative"
        indicators["sl_floor_atr_invalid_atr_raw"] = atr
        if (os.getenv("SL_ATR_FLOOR_VETO_ON_ZERO_ATR", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}:
            indicators["sl_floor_atr_invalid_enforced"] = 1
            stop_dist = 0.0
    return stop_dist


def test_fix2_atr_zero_shadow_only_annotates(monkeypatch):
    monkeypatch.delenv("SL_ATR_FLOOR_VETO_ON_ZERO_ATR", raising=False)
    ind: dict = {}
    new_dist = _sl_floor_branch(atr=0.0, stop_dist=0.5, sl_atr_floor=0.78, indicators=ind)
    assert new_dist == 0.5  # unchanged in shadow mode
    assert ind["sl_floor_atr_invalid"] == 1
    assert ind["sl_floor_atr_invalid_reason"] == "atr_zero_or_negative"
    assert "sl_floor_atr_invalid_enforced" not in ind


def test_fix2_atr_zero_enforce_collapses_stop_dist(monkeypatch):
    monkeypatch.setenv("SL_ATR_FLOOR_VETO_ON_ZERO_ATR", "1")
    ind: dict = {}
    new_dist = _sl_floor_branch(atr=0.0, stop_dist=0.5, sl_atr_floor=0.78, indicators=ind)
    assert new_dist == 0.0  # downstream lot_risk<=0 path will veto
    assert ind["sl_floor_atr_invalid"] == 1
    assert ind["sl_floor_atr_invalid_enforced"] == 1


def test_fix2_atr_negative_treated_same_as_zero(monkeypatch):
    monkeypatch.setenv("SL_ATR_FLOOR_VETO_ON_ZERO_ATR", "1")
    ind: dict = {}
    new_dist = _sl_floor_branch(atr=-0.1, stop_dist=0.5, sl_atr_floor=0.78, indicators=ind)
    assert new_dist == 0.0
    assert ind["sl_floor_atr_invalid"] == 1
    assert ind["sl_floor_atr_invalid_atr_raw"] == -0.1


def test_fix2_atr_positive_keeps_original_floor_behaviour():
    """Sanity: legacy positive-atr path is untouched by Fix #2."""
    ind: dict = {}
    new_dist = _sl_floor_branch(atr=1.0, stop_dist=0.3, sl_atr_floor=0.78, indicators=ind)
    assert new_dist == 0.78  # floored: 1.0 * 0.78
    assert ind["sl_atr_mult_floored"] == 1
    assert ind["sl_atr_mult_original"] == 0.3
    assert "sl_floor_atr_invalid" not in ind
