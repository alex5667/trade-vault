"""
Unit tests for P61 ML Confirm Live Rollout Binding
"""

import pytest

# Add parent directory to path for imports
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from services.orderflow.tick_decision_engine import _ml_should_enforce, _stable_hash01


class TestP61MLRollout:
    """Test suite for P61 ML confirm rollout functionality"""

    def test_stable_hash_deterministic(self):
        """Hash should be deterministic and in [0,1]"""
        sid = "test_signal_123"
        h1 = _stable_hash01(sid)
        h2 = _stable_hash01(sid)
        assert h1 == h2, "Hash should be deterministic"
        assert 0.0 <= h1 <= 1.0, f"Hash should be in [0,1], got {h1}"

    def test_stable_hash_different_inputs(self):
        """Different inputs should produce different hashes"""
        h1 = _stable_hash01("signal_1")
        h2 = _stable_hash01("signal_2")
        assert h1 != h2, "Different inputs should produce different hashes"

    def test_stable_hash_empty_string(self):
        """Empty string should produce valid hash"""
        h = _stable_hash01("")
        assert 0.0 <= h <= 1.0, f"Empty string hash should be in [0,1], got {h}"

    def test_ml_should_enforce_shadow(self):
        """Shadow mode should never enforce"""
        assert not _ml_should_enforce("shadow", "sig1", 0.5)
        assert not _ml_should_enforce("SHADOW", "sig1", 0.5)
        assert not _ml_should_enforce("Shadow", "sig1", 0.5)

    def test_ml_should_enforce_full(self):
        """Full mode should always enforce"""
        assert _ml_should_enforce("full", "sig1", 0.5)
        assert _ml_should_enforce("FULL", "sig1", 0.5)
        assert _ml_should_enforce("enforce", "sig1", 0.5)
        assert _ml_should_enforce("on", "sig1", 0.5)
        assert _ml_should_enforce("1", "sig1", 0.5)
        assert _ml_should_enforce("true", "sig1", 0.5)

    def test_ml_should_enforce_off(self):
        """Off modes should never enforce"""
        assert not _ml_should_enforce("off", "sig1", 0.5)
        assert not _ml_should_enforce("disabled", "sig1", 0.5)
        assert not _ml_should_enforce("false", "sig1", 0.5)
        assert not _ml_should_enforce("0", "sig1", 0.5)
        assert not _ml_should_enforce("none", "sig1", 0.5)

    def test_ml_should_enforce_canary_zero_rate(self):
        """Canary mode with 0% rate should never enforce"""
        # Test multiple signals to ensure none are enforced
        for i in range(100):
            assert not _ml_should_enforce("canary", f"sig{i}", 0.0)

    def test_ml_should_enforce_canary_full_rate(self):
        """Canary mode with 100% rate should always enforce"""
        # Test multiple signals to ensure all are enforced
        for i in range(100):
            assert _ml_should_enforce("canary", f"sig{i}", 1.0)

    def test_ml_should_enforce_canary_deterministic(self):
        """Canary mode should be deterministic for same signal"""
        result1 = _ml_should_enforce("canary", "sig1", 0.5)
        result2 = _ml_should_enforce("canary", "sig1", 0.5)
        assert result1 == result2, "Canary decision should be deterministic"

    def test_ml_should_enforce_canary_rate_respected(self):
        """Canary mode should approximately respect the given rate"""
        # Test with 10% rate over 1000 signals
        rate = 0.10
        num_signals = 1000
        enforced_count = sum(
            1 for i in range(num_signals)
            if _ml_should_enforce("canary", f"sig{i}", rate)
        )

        # Allow 20% tolerance (8% to 12% enforcement)
        expected = num_signals * rate
        tolerance = expected * 0.20
        assert abs(enforced_count - expected) < tolerance, \
            f"Expected ~{expected} enforced, got {enforced_count}"

    def test_ml_should_enforce_canary_aliases(self):
        """Test canary mode aliases"""
        sid = "test_sig"
        rate = 0.5
        result1 = _ml_should_enforce("canary", sid, rate)
        result2 = _ml_should_enforce("canary_enforce", sid, rate)
        result3 = _ml_should_enforce("canary-only", sid, rate)
        assert result1 == result2 == result3, "Canary aliases should behave identically"

    def test_ml_should_enforce_invalid_mode(self):
        """Invalid mode should default to shadow (no enforcement)"""
        assert not _ml_should_enforce("invalid_mode", "sig1", 0.5)
        assert not _ml_should_enforce("", "sig1", 0.5)
        assert not _ml_should_enforce(None, "sig1", 0.5)  # type: ignore[arg-type]

    def test_ml_should_enforce_rate_clamping(self):
        """Rates outside [0,1] should be clamped"""
        # Rate > 1 should be clamped to 1 (always enforce)
        assert _ml_should_enforce("canary", "sig1", 1.5)
        assert _ml_should_enforce("canary", "sig1", 100.0)

        # Rate < 0 should be clamped to 0 (never enforce)
        assert not _ml_should_enforce("canary", "sig1", -0.5)
        assert not _ml_should_enforce("canary", "sig1", -100.0)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
