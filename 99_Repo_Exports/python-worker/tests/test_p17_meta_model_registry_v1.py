from __future__ import annotations
"""
P1-7 — MetaModelRegistry: shadow-first promotion gate tests.

Verifies:
  1. challenger_is_shadow_only() = True when n < min_shadow_samples
  2. effective_ab_share() returns 0.0 while shadow-only, configured value after
  3. promo_readiness() blocked while n < threshold
  4. promo_readiness() blocked when challenger doesn't beat champion by delta
  5. try_promote() succeeds when criteria pass → champion path updated
  6. try_promote() fails when criteria NOT met → champion path unchanged
  7. record_shadow() increments correct tracker
  8. maybe_auto_promote() respects auto_promote=False gate
"""

import os
import sys
import tempfile
import time

import pytest

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.meta_model_registry import MetaModelRegistry, PromotionPolicy, _BrierTracker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_policy(**kwargs) -> PromotionPolicy:
    defaults = dict(
        min_shadow_samples=10,
        brier_delta_min=0.01,
        auto_promote=False,
        shadow_enforce_challenger=True,
    )
    defaults.update(kwargs)
    return PromotionPolicy(**defaults)


def _make_registry(challenger_exists: bool = True, **policy_kwargs) -> MetaModelRegistry:
    """Returns a registry with a fake (tmp) challenger file."""
    policy = _make_policy(**policy_kwargs)
    if challenger_exists:
        # Create a temp file to satisfy os.path.exists()
        tf = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        tf.close()
        challenger_path = tf.name
    else:
        challenger_path = "/nonexistent/challenger.json"
    return MetaModelRegistry(
        champion_path="/fake/champion.json",
        challenger_path=challenger_path,
        policy=policy,
    )


# ---------------------------------------------------------------------------
# _BrierTracker unit tests
# ---------------------------------------------------------------------------

class TestBrierTracker:
    def test_empty_brier_is_none(self):
        t = _BrierTracker()
        assert t.brier is None
        assert t.n == 0

    def test_perfect_predictions(self):
        t = _BrierTracker()
        for _ in range(10):
            t.record(1.0, 1.0)
        assert t.n == 10
        assert t.brier == pytest.approx(0.0)

    def test_worst_predictions(self):
        t = _BrierTracker()
        for _ in range(4):
            t.record(1.0, 0.0)  # err=1 each
        assert t.brier == pytest.approx(1.0)

    def test_reset(self):
        t = _BrierTracker()
        t.record(0.8, 1.0)
        t.reset()
        assert t.n == 0
        assert t.brier is None


# ---------------------------------------------------------------------------
# Shadow-enforcement tests
# ---------------------------------------------------------------------------

class TestShadowEnforcement:
    def test_shadow_only_true_when_below_quota(self):
        reg = _make_registry(min_shadow_samples=10)
        assert reg.challenger_is_shadow_only() is True

    def test_shadow_only_false_after_quota(self):
        reg = _make_registry(min_shadow_samples=3)
        for _ in range(3):
            reg.record_shadow("challenger", 0.6, 1.0)
        assert reg.challenger_is_shadow_only() is False

    def test_shadow_only_false_when_no_challenger(self):
        reg = _make_registry(challenger_exists=False)
        assert reg.challenger_is_shadow_only() is False

    def test_shadow_only_false_when_enforce_disabled(self):
        reg = _make_registry(shadow_enforce_challenger=False)
        assert reg.challenger_is_shadow_only() is False

    def test_effective_ab_share_zero_during_shadow(self):
        reg = _make_registry(min_shadow_samples=100)
        # 0 samples → share must be 0
        assert reg.effective_ab_share(0.5) == 0.0

    def test_effective_ab_share_restored_after_quota(self):
        reg = _make_registry(min_shadow_samples=2)
        reg.record_shadow("challenger", 0.7, 1.0)
        reg.record_shadow("challenger", 0.8, 1.0)
        assert reg.effective_ab_share(0.3) == pytest.approx(0.3)


# ---------------------------------------------------------------------------
# Promotion readiness tests
# ---------------------------------------------------------------------------

class TestPromoReadiness:
    def test_no_challenger_not_ready(self):
        reg = _make_registry(challenger_exists=False)
        ready, reason, stats = reg.promo_readiness()
        assert ready is False
        assert reason == "no_challenger"

    def test_insufficient_shadow_samples(self):
        reg = _make_registry(min_shadow_samples=50)
        for _ in range(10):
            reg.record_shadow("challenger", 0.3, 0.0)
        ready, reason, _ = reg.promo_readiness()
        assert ready is False
        assert "insufficient_shadow_samples" in reason

    def test_challenger_not_better_enough(self):
        reg = _make_registry(min_shadow_samples=5, brier_delta_min=0.05)
        # Champion: Brier ≈ 0.04  (good)
        for _ in range(5):
            reg.record_shadow("champion", 0.8, 1.0)
        # Challenger: Brier ≈ 0.04 as well (NOT better by 0.05)
        for _ in range(5):
            reg.record_shadow("challenger", 0.8, 1.0)
        ready, reason, _ = reg.promo_readiness()
        assert ready is False
        assert "not_better_enough" in reason

    def test_promotion_ready_when_criteria_pass(self):
        reg = _make_registry(min_shadow_samples=5, brier_delta_min=0.05)
        # Champion: Brier = 0.25  (bad: predicts 0.5 for all 1s)
        for _ in range(5):
            reg.record_shadow("champion", 0.5, 1.0)
        # Challenger: Brier ≈ 0.04  (good)
        for _ in range(5):
            reg.record_shadow("challenger", 0.8, 1.0)
        ready, reason, stats = reg.promo_readiness()
        assert ready is True
        assert reason == "criteria_passed"
        assert stats["challenger_n"] == 5

    def test_champion_untracked_quota_met(self):
        """When champion has no tracked data, challenger only needs quota."""
        reg = _make_registry(min_shadow_samples=3, brier_delta_min=0.05)
        for _ in range(3):
            reg.record_shadow("challenger", 0.9, 1.0)
        # Champion tracker is empty
        ready, reason, _ = reg.promo_readiness()
        assert ready is True
        assert reason == "champion_untracked_challenger_quota_met"


# ---------------------------------------------------------------------------
# Promotion execution tests
# ---------------------------------------------------------------------------

class TestPromotion:
    def test_try_promote_succeeds(self):
        reg = _make_registry(min_shadow_samples=2, brier_delta_min=0.05)
        for _ in range(5):
            reg.record_shadow("champion", 0.5, 1.0)
        old_challenger_path = reg.challenger_path
        for _ in range(2):
            reg.record_shadow("challenger", 0.95, 1.0)
        promoted, reason, stats = reg.try_promote()
        assert promoted is True
        assert reason == "promoted"
        assert reg.champion_path == old_challenger_path
        assert reg.challenger_path == ""
        assert stats["new_champion"] == old_challenger_path

    def test_try_promote_fails_when_not_ready(self):
        reg = _make_registry(min_shadow_samples=100)
        old_path = reg.champion_path
        promoted, reason, _ = reg.try_promote()
        assert promoted is False
        assert reg.champion_path == old_path  # unchanged

    def test_promotion_resets_champion_tracker(self):
        reg = _make_registry(min_shadow_samples=2, brier_delta_min=0.01)
        for _ in range(5):
            reg.record_shadow("champion", 0.5, 1.0)  # Brier=0.25
        for _ in range(2):
            reg.record_shadow("challenger", 0.95, 1.0)  # Brier≈0.0025
        reg.try_promote()
        # After promotion, champion tracker reset
        assert reg._champion_brier.n == 0

    def test_promotion_log_written(self):
        reg = _make_registry(min_shadow_samples=2, brier_delta_min=0.01)
        for _ in range(5):
            reg.record_shadow("champion", 0.5, 1.0)
        for _ in range(2):
            reg.record_shadow("challenger", 0.95, 1.0)
        reg.try_promote()
        log = reg.promotion_log()
        assert len(log) == 1
        assert log[0]["event"] == "model_promoted"
        assert log[0]["ts_ms"] > 0

    def test_maybe_auto_promote_disabled_by_default(self):
        reg = _make_registry(min_shadow_samples=2, brier_delta_min=0.01, auto_promote=False)
        for _ in range(5):
            reg.record_shadow("champion", 0.5, 1.0)
        for _ in range(2):
            reg.record_shadow("challenger", 0.95, 1.0)
        promoted, reason, _ = reg.maybe_auto_promote()
        assert promoted is False
        assert reason == "auto_promote_disabled"

    def test_maybe_auto_promote_enabled(self):
        reg = _make_registry(min_shadow_samples=2, brier_delta_min=0.01, auto_promote=True)
        for _ in range(5):
            reg.record_shadow("champion", 0.5, 1.0)
        for _ in range(2):
            reg.record_shadow("challenger", 0.95, 1.0)
        promoted, reason, _ = reg.maybe_auto_promote()
        assert promoted is True


# ---------------------------------------------------------------------------
# status() and to_json() smoke test
# ---------------------------------------------------------------------------

class TestStatus:
    def test_status_dict_keys(self):
        reg = _make_registry(min_shadow_samples=10)
        s = reg.status()
        required_keys = {
            "champion_path", "challenger_path", "has_challenger",
            "challenger_is_shadow_only", "promo_ready", "promo_reason",
            "challenger_shadow_n", "challenger_shadow_needed",
        }
        assert required_keys <= set(s.keys())

    def test_to_json_valid(self):
        import json as _json
        reg = _make_registry()
        j = reg.to_json()
        parsed = _json.loads(j)
        assert "champion_path" in parsed


# ---------------------------------------------------------------------------
# record_shadow routing
# ---------------------------------------------------------------------------

class TestRecordShadow:
    def test_record_champion_slot(self):
        reg = _make_registry()
        reg.record_shadow("champion", 0.7, 1.0)
        assert reg._champion_brier.n == 1

    def test_record_challenger_slot(self):
        reg = _make_registry()
        reg.record_shadow("challenger", 0.7, 1.0)
        assert reg._challenger_brier.n == 1

    def test_record_unknown_slot_goes_to_challenger(self):
        reg = _make_registry()
        # Unknown slot treated as challenger
        reg.record_shadow("unknown_arm", 0.5, 0.0)
        assert reg._challenger_brier.n == 1
