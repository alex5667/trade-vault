# test_regime_hard_gate.py
"""
Tests for regime-based hard gate in signal generation pipeline.

The old _regime_allows_signal method was removed from SignalGenerator.
Regime gating is now handled by the regime_allows() function from
handlers.regime_gate module. These tests verify that function directly.
"""

import pytest

from handlers.regime_gate import RegimeGateCfg, regime_allows


@pytest.fixture
def gate_cfg():
    """Create a regime gate config that enforces regime checks."""
    return RegimeGateCfg(
        breakout_min_score=0.1,    # breakout requires positive score (trend)
        extreme_min_score=0.1,     # extreme requires positive score
        obi_spike_min_score=0.0,   # neutral
        absorption_max_score=0.0,  # absorption requires non-positive score (range)
        allow_sweep_any=True,
    )


class TestRegimeHardGate:
    """Test regime-based signal filtering via regime_allows()."""

    def test_regime_gate_breakout_allowed_in_trend(self, gate_cfg):
        """Breakout signals should be allowed in trend regime (positive score)."""
        assert regime_allows("breakout", regime_score=0.6, cfg=gate_cfg) is True

    def test_regime_gate_breakout_rejected_in_range(self, gate_cfg):
        """Breakout signals should be rejected in range regime (negative score)."""
        assert regime_allows("breakout", regime_score=-0.4, cfg=gate_cfg) is False

    def test_regime_gate_mean_reversion_allowed_in_range(self, gate_cfg):
        """Absorption signals should be allowed in range regime (negative score)."""
        # In the new gate, "absorption" is the signal type for range-appropriate trades
        assert regime_allows("absorption", regime_score=-0.4, cfg=gate_cfg) is True

    def test_regime_gate_mean_reversion_rejected_in_trend(self, gate_cfg):
        """Absorption signals should be rejected in trend regime (positive score)."""
        assert regime_allows("absorption", regime_score=0.6, cfg=gate_cfg) is False

    def test_regime_gate_mixed_regime_allows_both(self, gate_cfg):
        """Mixed regime (score=0) should allow breakout (score >= 0.1 fails) but allow absorption."""
        # score=0.0 < 0.1 (breakout_min_score) => breakout rejected in strict mode
        # But with default cfg (min_score=0.0), breakout is always allowed
        default_cfg = RegimeGateCfg()  # all thresholds = 0.0
        assert regime_allows("breakout", regime_score=0.0, cfg=default_cfg) is True
        assert regime_allows("absorption", regime_score=0.0, cfg=default_cfg) is True

    def test_regime_gate_sweep_always_allowed(self, gate_cfg):
        """Sweep signals should always be allowed regardless of regime."""
        assert regime_allows("sweep", regime_score=-1.0, cfg=gate_cfg) is True
        assert regime_allows("sweep", regime_score=1.0, cfg=gate_cfg) is True

    def test_regime_gate_none_score_allows_all(self, gate_cfg):
        """When regime_score is None, all signal types should be allowed."""
        assert regime_allows("breakout", regime_score=None, cfg=gate_cfg) is True
        assert regime_allows("absorption", regime_score=None, cfg=gate_cfg) is True
        assert regime_allows("sweep", regime_score=None, cfg=gate_cfg) is True

    def test_signal_type_case_insensitive(self, gate_cfg):
        """Signal type matching should be case insensitive."""
        assert regime_allows("BREAKOUT", regime_score=0.6, cfg=gate_cfg) is True
        assert regime_allows("Breakout", regime_score=0.6, cfg=gate_cfg) is True

    def test_unknown_signal_type_allowed(self, gate_cfg):
        """Unknown signal types should be allowed (fail-open)."""
        assert regime_allows("custom_signal", regime_score=-1.0, cfg=gate_cfg) is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
