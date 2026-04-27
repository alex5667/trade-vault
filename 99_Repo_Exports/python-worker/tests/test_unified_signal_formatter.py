from core.unified_signal_formatter import UnifiedSignalFormatter
import math
import pytest

def test_normalize_confidence_ratio():
    pct, ratio = UnifiedSignalFormatter.normalize_confidence_pct(0.85)
    assert round(pct, 2) == 85.00
    assert round(ratio, 4) == 0.8500

def test_normalize_confidence_percent():
    pct, ratio = UnifiedSignalFormatter.normalize_confidence_pct(87)
    assert round(pct, 2) == 87.00
    assert round(ratio, 4) == 0.8700

def test_normalize_confidence_invalid():
    pct, ratio = UnifiedSignalFormatter.normalize_confidence_pct(None)
    assert pct == 0.0
    assert ratio == 0.0
    
    pct, ratio = UnifiedSignalFormatter.normalize_confidence_pct(float("nan"))
    assert pct == 0.0
    assert ratio == 0.0

def test_clamp_p_values():
    # Helper to mock a signal object or just test the mix builder if static
    # _build_mix_dict is static but takes a Signal object.
    # We can mock a simple object with indicators.
    
    class MockSignal:
        def __init__(self, indicators):
            self.indicators = indicators
            
    f = UnifiedSignalFormatter
    
    # Test strict clamping > 0.99
    sig = MockSignal({"p_delta": 1.7, "p_speed": -2.2})
    mix = f._build_mix_dict(sig, [])
    assert mix["p_delta"] == 0.99
    # Logic: if v < 0: v = -v. Then clamp.
    # So -2.2 -> 2.2 -> 0.99.
    assert mix["p_speed"] == 0.99

    # Test normal values
    sig = MockSignal({"p_delta": 0.5, "p_speed": 0.3})
    mix = f._build_mix_dict(sig, [])
    assert mix["p_delta"] == 0.5
    assert mix["p_speed"] == 0.3

def test_speed_fallback_uses_alias_delta_z():
    class MockSignal:
        def __init__(self, indicators):
            self.indicators = indicators
            
    f = UnifiedSignalFormatter
    # Fallback logic: abs(z) / 6.0
    # 6.0 / 6.0 = 1.0 -> clamp 0.99
    sig = MockSignal({"delta_z": 6.0})
    mix = f._build_mix_dict(sig, [])
    assert mix.get("p_speed") == 0.99
    
    # 3.0 / 6.0 = 0.5
    sig = MockSignal({"delta_z": 3.0})
    mix = f._build_mix_dict(sig, [])
    assert mix.get("p_speed") == 0.5
    
    # Verify legacy z_delta also works
    sig = MockSignal({"z_delta": 3.0})
    mix = f._build_mix_dict(sig, [])
    assert mix.get("p_speed") == 0.5
    
    # Verify priority: p_speed > z_delta
    sig = MockSignal({"p_speed": 0.8, "delta_z": 100.0})
    mix = f._build_mix_dict(sig, [])
    assert mix.get("p_speed") == 0.8

def test_safe_float_and_clamp():
    f = UnifiedSignalFormatter
    assert f._safe_float(None) != f._safe_float(None) # nan != nan
    assert math.isnan(f._safe_float(None))
    assert f._safe_float("1.5") == 1.5
    
    assert f._clamp01(1.5) == 0.99
    assert f._clamp01(-0.5) == 0.5
    assert f._clamp01(float("nan")) == 0.0
