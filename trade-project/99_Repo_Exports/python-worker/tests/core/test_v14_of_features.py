from __future__ import annotations

"""
Unit tests for core/v14_of_features.build_og_payload.

Covers:
  - fail-open: all 16 keys present, zero-valued when no inputs
  - population from ofc (have/need/score/contrib/gate_bits/ok)
  - fallback to dec when ofc absent
  - strong-need via ofc.evidence
  - reason_code stable hash determinism + locality (different reason → different bucket)
  - weak_progress from indicators
  - keys match schema v14_of declared og_* keys
"""

from types import SimpleNamespace
from typing import Any

from core.ml_feature_schema_v14_of import V14_OF_NUMERIC_KEYS
from core.v14_of_features import _OG_KEYS, build_og_payload, og_keys


def _ofc(**kw: Any) -> Any:
    """Build a minimal OFConfirmV3-like object."""
    defaults = dict(
        v=3, symbol="BTCUSDT", ts_ms=1_700_000_000_000,
        direction="LONG", scenario="reversal",
        ok=0, score=0.0, have=0, need=0, gate_bits=0,
        reason="", evidence={}, contrib={},
    )
    defaults.update(kw)
    return SimpleNamespace(**defaults)


def _dec(**kw: Any) -> Any:
    defaults: dict[str, Any] = dict(have=0, need=0, need_reason="")
    defaults.update(kw)
    return SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# Fail-open
# ---------------------------------------------------------------------------

def test_empty_inputs_fail_open():
    out = build_og_payload()
    assert set(out.keys()) == set(_OG_KEYS)
    assert len(out) == 16
    assert all(v == 0.0 for v in out.values())


def test_only_indicators_no_ofc_dec():
    """Indicators alone should still surface weak_progress + score_minus_threshold."""
    out = build_og_payload(indicators={"weak_progress": 1, "legacy_of_score_min": 0.5})
    assert out["og_weak_progress_any"] == 1.0
    # ofc.score absent → score_minus_threshold = 0 - 0.5 = -0.5
    assert out["og_score_minus_threshold"] == -0.5
    # everything else stays 0.0
    assert out["og_have"] == 0.0
    assert out["og_need"] == 0.0


# ---------------------------------------------------------------------------
# Population from ofc
# ---------------------------------------------------------------------------

def test_have_need_from_ofc():
    ofc = _ofc(have=3, need=2, ok=1)
    out = build_og_payload(ofc=ofc)
    assert out["og_have"] == 3.0
    assert out["og_need"] == 2.0
    assert out["og_have_minus_need"] == 1.0
    assert out["og_ok"] == 1.0


def test_have_need_from_dec_when_ofc_missing():
    """If ofc is None, fall back to dec."""
    dec = _dec(have=2, need=3)
    out = build_og_payload(dec=dec)
    assert out["og_have"] == 2.0
    assert out["og_need"] == 3.0
    assert out["og_have_minus_need"] == -1.0


def test_score_minus_threshold():
    ofc = _ofc(score=0.7)
    out = build_og_payload(ofc=ofc, indicators={"legacy_of_score_min": 0.4})
    assert abs(out["og_score_minus_threshold"] - 0.3) < 1e-9


def test_contrib_keys_mapped():
    ofc = _ofc(contrib={
        "z": 0.4, "weak_progress": 0.1, "reclaim": 0.2,
        "obi_stable": 0.3, "iceberg_strict": 0.15, "absorption": 0.05,
    })
    out = build_og_payload(ofc=ofc)
    assert out["og_contrib_z"] == 0.4
    assert out["og_contrib_wp"] == 0.1
    assert out["og_contrib_reclaim"] == 0.2
    assert out["og_contrib_obi"] == 0.3
    assert out["og_contrib_iceberg"] == 0.15
    assert out["og_contrib_absorption"] == 0.05


def test_contrib_partial_dict_fail_open():
    """Missing contrib subkeys must default to 0.0 without raising."""
    ofc = _ofc(contrib={"z": 0.7})  # only one key
    out = build_og_payload(ofc=ofc)
    assert out["og_contrib_z"] == 0.7
    assert out["og_contrib_wp"] == 0.0
    assert out["og_contrib_reclaim"] == 0.0


def test_gate_bits_popcount():
    # gate_bits = 0b1011 → 3 bits set
    ofc = _ofc(gate_bits=0b1011)
    out = build_og_payload(ofc=ofc)
    assert out["og_gate_bits_count"] == 3.0

    ofc2 = _ofc(gate_bits=0)
    assert build_og_payload(ofc=ofc2)["og_gate_bits_count"] == 0.0

    ofc3 = _ofc(gate_bits=0xF)
    assert build_og_payload(ofc=ofc3)["og_gate_bits_count"] == 4.0


# ---------------------------------------------------------------------------
# Strong-need via evidence (set by of_confirm_engine after compute_strong_need_same_tick)
# ---------------------------------------------------------------------------

def test_strong_need_from_evidence():
    ofc = _ofc(evidence={
        "strong_need_reversal": 3,
        "strong_need_continuation": 4,
        "strong_need_reason": "ESCALATED",
    })
    out = build_og_payload(ofc=ofc)
    assert out["og_strong_need_rev"] == 3.0
    assert out["og_strong_need_cont"] == 4.0


def test_strong_need_missing_evidence_fail_open():
    ofc = _ofc(evidence={})
    out = build_og_payload(ofc=ofc)
    assert out["og_strong_need_rev"] == 0.0
    assert out["og_strong_need_cont"] == 0.0


# ---------------------------------------------------------------------------
# Reason code
# ---------------------------------------------------------------------------

def test_reason_code_empty_is_zero():
    out = build_og_payload(ofc=_ofc(reason=""))
    assert out["og_reason_code_id"] == 0.0


def test_reason_code_deterministic_across_calls():
    """Same reason string must hash to same bucket every time."""
    r = "ESCALATED_PRESSURE_HI"
    out_a = build_og_payload(ofc=_ofc(reason=r))
    out_b = build_og_payload(ofc=_ofc(reason=r))
    assert out_a["og_reason_code_id"] == out_b["og_reason_code_id"]


def test_reason_code_in_bucket_range():
    for reason in ["BASE", "ESCALATED", "EXTREME", "rev_dz_strong", "cont_obi_weak"]:
        out = build_og_payload(ofc=_ofc(reason=reason))
        v = out["og_reason_code_id"]
        assert 0.0 <= v < 64.0, f"reason {reason!r} → {v} outside [0, 64)"
        # Integer-valued (no fractional bits introduced by hashing)
        assert v == float(int(v))


def test_reason_code_distinct_strings_likely_distinct_buckets():
    """Birthday-bound sanity: 5 distinct reasons fit in 64 buckets with high probability."""
    reasons = ["BASE", "ESCALATED", "EXTREME", "rev_dz_strong", "cont_obi_weak"]
    buckets = {build_og_payload(ofc=_ofc(reason=r))["og_reason_code_id"] for r in reasons}
    # We don't require all distinct (collisions possible), but expect at least 3 distinct.
    assert len(buckets) >= 3, f"expected ≥3 distinct buckets for 5 reasons; got {buckets}"


def test_reason_code_falls_back_through_dec_then_evidence():
    """Lookup order: ofc.reason → dec.need_reason → evidence['strong_need_reason']."""
    # ofc has empty reason, dec has it
    dec = _dec(need_reason="from_dec")
    out_dec = build_og_payload(ofc=_ofc(reason=""), dec=dec)
    assert out_dec["og_reason_code_id"] != 0.0

    # only evidence has reason
    ofc = _ofc(reason="", evidence={"strong_need_reason": "from_evidence"})
    out_ev = build_og_payload(ofc=ofc)
    assert out_ev["og_reason_code_id"] != 0.0


# ---------------------------------------------------------------------------
# weak_progress
# ---------------------------------------------------------------------------

def test_weak_progress_from_indicators():
    out = build_og_payload(indicators={"weak_progress": 1})
    assert out["og_weak_progress_any"] == 1.0

    out_0 = build_og_payload(indicators={"weak_progress": 0})
    assert out_0["og_weak_progress_any"] == 0.0


def test_weak_progress_handles_bool():
    out = build_og_payload(indicators={"weak_progress": True})
    assert out["og_weak_progress_any"] == 1.0


# ---------------------------------------------------------------------------
# Robustness: malformed inputs must not raise
# ---------------------------------------------------------------------------

def test_ofc_with_none_attrs_does_not_raise():
    ofc = SimpleNamespace(have=None, need=None, ok=None, score=None,
                          gate_bits=None, contrib=None, evidence=None, reason=None)
    out = build_og_payload(ofc=ofc)
    assert len(out) == 16
    assert all(isinstance(v, float) for v in out.values())


def test_contrib_non_dict_does_not_raise():
    ofc = _ofc(contrib="not_a_dict")  # type: ignore[arg-type]
    out = build_og_payload(ofc=ofc)
    # contrib keys all stay 0.0 (fail-open)
    assert out["og_contrib_z"] == 0.0
    assert out["og_contrib_wp"] == 0.0


def test_indicators_with_string_value_does_not_raise():
    out = build_og_payload(indicators={"weak_progress": "1", "legacy_of_score_min": "0.3"})
    assert out["og_weak_progress_any"] == 1.0
    assert out["og_score_minus_threshold"] == -0.3


# ---------------------------------------------------------------------------
# Schema parity: helper keys must equal schema declaration
# ---------------------------------------------------------------------------

def test_og_keys_match_schema_v14_of_declaration():
    """All og_* keys produced by build_og_payload must be present in V14_OF_NUMERIC_KEYS."""
    helper_keys = set(og_keys())
    schema_keys = set(V14_OF_NUMERIC_KEYS)
    missing = helper_keys - schema_keys
    assert not missing, f"helper emits keys not in v14_of schema: {sorted(missing)}"


def test_og_keys_complete_count():
    assert len(og_keys()) == 16


def test_v14_og_subset_count_in_schema():
    """v14_of schema must contain exactly the 16 og_* keys from build_og_payload."""
    schema_og = {k for k in V14_OF_NUMERIC_KEYS if k.startswith("og_")}
    helper_og = set(og_keys())
    assert schema_og == helper_og
