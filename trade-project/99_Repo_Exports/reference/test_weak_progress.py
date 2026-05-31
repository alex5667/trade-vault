"""
Unit tests for weak progress scoring system.
"""

import unittest
from datetime import datetime
from signal_scoring.ctx import SignalContext
from signal_scoring.weak_progress import (
    WeakProgressConfig,
    get_weak_progress_config,
    compute_progress_score,
    apply_weak_progress_and_fade_filters,
    validate_signal_for_weak_progress,
    compute_weak_progress,
    PATTERN_WP_CONFIG
)


class TestWeakProgressSystem(unittest.TestCase):
    """Test weak progress scoring and filtering."""

    def test_compute_weak_progress(self):
        """Test weak progress calculation."""
        # Strong progress (high range relative to ATR)
        wp1 = compute_weak_progress(high=1.1, low=1.0, atr=0.02)
        self.assertAlmostEqual(wp1, 5.0, places=1)  # (0.1 / 0.02) = 5.0

        # Weak progress (low range relative to ATR)
        wp2 = compute_weak_progress(high=1.01, low=1.0, atr=0.02)
        self.assertAlmostEqual(wp2, 0.5, places=1)  # (0.01 / 0.02) = 0.5

        # Edge case: zero ATR
        wp3 = compute_weak_progress(high=1.1, low=1.0, atr=0.0)
        self.assertAlmostEqual(wp3, 0.1 / 1e-6, places=0)  # Uses epsilon

    def test_pattern_config_loading(self):
        """Test pattern configuration loading."""
        # Known pattern
        cfg1 = get_weak_progress_config("breakout_R1")
        self.assertEqual(cfg1.family, "continuation")
        self.assertEqual(cfg1.cont_strong_min, 0.8)

        # Fade pattern
        cfg2 = get_weak_progress_config("fade_PDH")
        self.assertEqual(cfg2.family, "fade")
        self.assertEqual(cfg2.fade_weak_max, 0.3)

        # Unknown pattern (should infer from name)
        cfg3 = get_weak_progress_config("some_breakout_signal")
        self.assertEqual(cfg3.family, "continuation")

        cfg4 = get_weak_progress_config("some_fade_signal")
        self.assertEqual(cfg4.family, "fade")

    def test_progress_score_continuation(self):
        """Test progress score for continuation patterns."""
        cfg = WeakProgressConfig(
            family="continuation",
            cont_strong_min=0.7,
            cont_weak_max=0.3,
            bonus_cont_strong=12,
            penalty_cont_weak=15
        )

        # Strong progress -> bonus
        ctx1 = SignalContext(
            ts=datetime.now(),
            symbol="XAUUSD",
            side="buy",
            session="asia",
            regime="trend",
            weak_progress=0.8
        )
        score1 = compute_progress_score(ctx1, cfg)
        self.assertEqual(score1, 12)

        # Weak progress -> penalty
        ctx2 = SignalContext(
            ts=datetime.now(),
            symbol="XAUUSD",
            side="buy",
            session="asia",
            regime="trend",
            weak_progress=0.2
        )
        score2 = compute_progress_score(ctx2, cfg)
        self.assertEqual(score2, -15)

        # Moderate progress -> neutral
        ctx3 = SignalContext(
            ts=datetime.now(),
            symbol="XAUUSD",
            side="buy",
            session="asia",
            regime="trend",
            weak_progress=0.5
        )
        score3 = compute_progress_score(ctx3, cfg)
        self.assertEqual(score3, 0)

    def test_progress_score_fade(self):
        """Test progress score for fade patterns."""
        cfg = WeakProgressConfig(
            family="fade",
            fade_weak_max=0.35,
            bonus_fade_weak=10,
            penalty_fade_strong=10
        )

        # Weak progress -> bonus
        ctx1 = SignalContext(
            ts=datetime.now(),
            symbol="XAUUSD",
            side="sell",
            session="asia",
            regime="range",
            weak_progress=0.2
        )
        score1 = compute_progress_score(ctx1, cfg)
        self.assertEqual(score1, 10)

        # Strong progress -> penalty
        ctx2 = SignalContext(
            ts=datetime.now(),
            symbol="XAUUSD",
            side="sell",
            session="asia",
            regime="range",
            weak_progress=0.8
        )
        score2 = compute_progress_score(ctx2, cfg)
        self.assertEqual(score2, -10)

    def test_fade_filters(self):
        """Test fade pattern preconditions and confirmation."""
        from signal_scoring.weak_progress.filters import (
            fade_preconditions_passed,
            fade_confirmation_passed
        )

        cfg = WeakProgressConfig(
            family="fade",
            fade_weak_max=0.35,
            fade_min_delta_z=1.8,
            fade_min_volume_z=1.5,
            fade_confirm_delta_z=1.5
        )

        # Good fade signal
        ctx1 = SignalContext(
            ts=datetime.now(),
            symbol="XAUUSD",
            side="sell",
            session="asia",
            regime="range",
            weak_progress=0.2,
            delta_spike_z=2.5,
            reverse_delta_spike_z=2.0
        )
        self.assertTrue(fade_preconditions_passed(ctx1, cfg))
        self.assertTrue(fade_confirmation_passed(ctx1, cfg))

        # Weak progress too strong
        ctx2 = SignalContext(
            ts=datetime.now(),
            symbol="XAUUSD",
            side="sell",
            session="asia",
            regime="range",
            weak_progress=0.5,  # Too strong for fade
            delta_spike_z=2.5,
            reverse_delta_spike_z=2.0
        )
        self.assertFalse(fade_preconditions_passed(ctx2, cfg))

        # Insufficient impulse
        ctx3 = SignalContext(
            ts=datetime.now(),
            symbol="XAUUSD",
            side="sell",
            session="asia",
            regime="range",
            weak_progress=0.2,
            delta_spike_z=1.0,  # Too weak
            reverse_delta_spike_z=2.0
        )
        self.assertFalse(fade_preconditions_passed(ctx3, cfg))

        # No confirmation
        ctx4 = SignalContext(
            ts=datetime.now(),
            symbol="XAUUSD",
            side="sell",
            session="asia",
            regime="range",
            weak_progress=0.2,
            delta_spike_z=2.5,
            reverse_delta_spike_z=None  # No confirmation
        )
        self.assertFalse(fade_confirmation_passed(ctx4, cfg))

    def test_apply_weak_progress_filters(self):
        """Test complete weak progress filtering and scoring."""
        # Continuation pattern
        cfg_cont = WeakProgressConfig(
            family="continuation",
            cont_strong_min=0.7,
            bonus_cont_strong=12
        )

        ctx_cont = SignalContext(
            ts=datetime.now(),
            symbol="XAUUSD",
            side="buy",
            session="asia",
            regime="trend",
            pattern_name="breakout_R1",
            weak_progress=0.8
        )

        final_conf_cont = apply_weak_progress_and_fade_filters(ctx_cont, cfg_cont, base_conf=70)
        self.assertEqual(final_conf_cont, 82)  # 70 + 12 bonus
        self.assertEqual(ctx_cont.progress_score_component, 12)

        # Fade pattern - good conditions
        cfg_fade = WeakProgressConfig(
            family="fade",
            fade_weak_max=0.35,
            fade_min_delta_z=1.8,
            fade_confirm_delta_z=1.5,
            bonus_fade_weak=10
        )

        ctx_fade_good = SignalContext(
            ts=datetime.now(),
            symbol="XAUUSD",
            side="sell",
            session="asia",
            regime="range",
            pattern_name="fade_PDH",
            weak_progress=0.2,
            delta_spike_z=2.5,
            reverse_delta_spike_z=2.0
        )

        final_conf_fade = apply_weak_progress_and_fade_filters(ctx_fade_good, cfg_fade, base_conf=65)
        self.assertEqual(final_conf_fade, 75)  # 65 + 10 bonus
        self.assertEqual(ctx_fade_good.progress_score_component, 10)

        # Fade pattern - bad conditions (rejected)
        ctx_fade_bad = SignalContext(
            ts=datetime.now(),
            symbol="XAUUSD",
            side="sell",
            session="asia",
            regime="range",
            pattern_name="fade_PDH",
            weak_progress=0.5,  # Too strong for fade
            delta_spike_z=2.5,
            reverse_delta_spike_z=2.0
        )

        final_conf_bad = apply_weak_progress_and_fade_filters(ctx_fade_bad, cfg_fade, base_conf=65)
        self.assertEqual(final_conf_bad, 0)  # Rejected
        self.assertEqual(ctx_fade_bad.progress_score_component, -100)

    def test_validation_function(self):
        """Test signal validation function."""
        ctx = SignalContext(
            ts=datetime.now(),
            symbol="XAUUSD",
            side="sell",
            session="asia",
            regime="range",
            pattern_name="fade_PDH",
            weak_progress=0.2,
            delta_spike_z=2.5,
            reverse_delta_spike_z=2.0
        )

        validation = validate_signal_for_weak_progress(ctx)

        self.assertEqual(validation["pattern_family"], "fade")
        self.assertEqual(validation["weak_progress"], 0.2)
        self.assertTrue(validation["fade_preconditions"])
        self.assertTrue(validation["fade_confirmation"])
        self.assertTrue(validation["is_valid"])
        self.assertGreater(validation["progress_score"], 0)


if __name__ == "__main__":
    unittest.main()
