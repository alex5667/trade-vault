import math
import sys
import os
from types import SimpleNamespace
import pytest

# Adjust path to find services
# Prioritize python-worker/services to avoid conflict with scanner_infra/services
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../python-worker/services")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../python-worker")))

def _import_scorer():
    try:
        # Direct import since we added services/ to path
        import signal_confidence
        return signal_confidence.ConfidenceScorer, signal_confidence.ConfidenceConfig
    except ImportError:
        try:
            from services.signal_confidence import ConfidenceScorer, ConfidenceConfig
            return ConfidenceScorer, ConfidenceConfig
        except ImportError:
             raise ImportError("Could not import ConfidenceScorer from services.signal_confidence or signal_confidence")

ConfidenceScorer, ConfidenceConfig = _import_scorer()


def make_ctx(**kwargs):
    # ctx is accessed via getattr(), so SimpleNamespace is enough
    if "confirmations" not in kwargs:
        kwargs["confirmations"] = []
    return SimpleNamespace(**kwargs)


def test_confidence_clamped_and_finite():
    scorer = ConfidenceScorer(cfg=ConfidenceConfig(min_conf=0.05, max_conf=0.98))
    ctx = make_ctx(delta_z=0.0, obi=0.0, confirmations=[])
    conf, parts = scorer.score(kind="custom", side="LONG", ctx=ctx)

    assert math.isfinite(conf)
    assert scorer.cfg.min_conf <= conf <= scorer.cfg.max_conf
    assert 0.0 <= parts["confidence01"] <= 1.0


def test_alias_delta_z_is_supported():
    scorer = ConfidenceScorer(cfg=ConfidenceConfig(min_conf=0.05, max_conf=0.98))
    # Provide only delta_z (legacy pipelines often use this key)
    ctx = make_ctx(delta_z=5.0, obi=0.0, confirmations=[])
    conf, parts = scorer.score(kind="custom", side="LONG", ctx=ctx)

    # With z=5 and no penalties, confidence01 should be high (base=0.75)
    # The default main_z_thr is 3.0. z=5 is well above.
    assert parts["confidence01"] > 0.60
    assert conf > scorer.cfg.min_conf


def test_alias_obi_is_supported():
    scorer = ConfidenceScorer(cfg=ConfidenceConfig(min_conf=0.05, max_conf=0.98))
    # Provide only 'obi' (TickProcessor uses indicators["obi"])
    ctx = make_ctx(delta_z=0.0, obi=0.90, obi_sustained=True, confirmations=[])
    conf, parts = scorer.score(kind="custom", side="LONG", ctx=ctx)

    assert parts["confidence01"] > 0.15
    assert conf > scorer.cfg.min_conf


def test_micro_bonus_applied_once_and_bounded():
    scorer = ConfidenceScorer(cfg=ConfidenceConfig(min_conf=0.05, max_conf=0.98))

    ctx_base = make_ctx(
        delta_z=5.0,  # s_z=1 => base=0.75
        obi=0.0,
        confirmations=[],
        micro_bonus_cap=0.10,
    )
    _, parts0 = scorer.score(kind="custom", side="LONG", ctx=ctx_base)
    base0 = parts0["confidence01"]

    # Strong microstructure evidence via BOTH confirmations and ctx fields.
    # Old (buggy) implementations could apply multiple bonus blocks and exceed cap.
    ctx_micro = make_ctx(
        delta_z=5.0,
        obi=0.0,
        micro_bonus_cap=0.10,

        # confirmations channel
        confirmations=[
            "obi_stable=10",
            "obi_q=1",
            "ofi_stable=10",
            "ofi_q=1",
            "cvd_reclaim=1",
            "cvdR=2.0",
        ],

        # ctx fields channel (should not lead to double counting)
        obi_stable_secs=10.0,
        obi_stability_score=1.0,
        ofi_stable_secs=10.0,
        ofi_stability_score=1.0,
    )
    _, parts1 = scorer.score(kind="custom", side="LONG", ctx=ctx_micro)
    base1 = parts1["confidence01"]

    # base1 should be higher than base0, but not by more than micro_bonus_cap (0.10)
    # also we need to ensuring we didn't apply bonuses twice.
    # If double counted, the bonus might be > 0.10 if cap logic was per-block.
    # But new logic has single micro_bonus accumulation and SINGLE cap application.
    
    assert base1 >= base0
    assert (base1 - base0) <= 0.10 + 1e-9


def test_micro_bonus_cap_respected():
    scorer = ConfidenceScorer(cfg=ConfidenceConfig(min_conf=0.05, max_conf=0.98))

    ctx0 = make_ctx(delta_z=5.0, confirmations=[], micro_bonus_cap=0.03)
    _, p0 = scorer.score(kind="custom", side="LONG", ctx=ctx0)

    ctx1 = make_ctx(
        delta_z=5.0,
        micro_bonus_cap=0.03,
        confirmations=["obi_stable=10", "obi_q=1", "ofi_stable=10", "ofi_q=1", "cvdR=2.0"],
        obi_stable_secs=10.0,
        obi_stability_score=1.0,
        ofi_stable_secs=10.0,
        ofi_stability_score=1.0,
    )
    _, p1 = scorer.score(kind="custom", side="LONG", ctx=ctx1)

    assert (p1["confidence01"] - p0["confidence01"]) <= 0.03 + 1e-9


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_inputs_are_sanitized(bad):
    scorer = ConfidenceScorer(cfg=ConfidenceConfig(min_conf=0.05, max_conf=0.98))
    # Passing NaN/Inf as delta_z or obi
    ctx = make_ctx(delta_z=bad, obi=bad, confirmations=[])
    conf, parts = scorer.score(kind="custom", side="LONG", ctx=ctx)

    assert math.isfinite(conf)
    assert math.isfinite(parts["confidence01"])
    # With invalid inputs sanitized (z=0, obi=0), we expect base confidence (or min).
    # Since z=0 (default) < threshold, score is low.
    # Should be close to min_conf.
    assert conf == pytest.approx(scorer.cfg.min_conf, abs=1e-2)
