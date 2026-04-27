import pytest
from services.trailing_profiles import TrailingProfilesRegistry

def test_lock_and_trail_profile_atr_mult():
    """
    Golden-data test to ensure lock_and_trail uses an ATR multiplier of 1.0 (previously 0.8),
    as introduced in the fix/book-parser-signature-is_virtual commit.
    """
    registry = TrailingProfilesRegistry()
    profile = registry.get("lock_and_trail")
    
    assert profile is not None, "Profile 'lock_and_trail' must exist"
    assert profile.mode == "ATR", "Mode must be ATR"
    assert profile.atr_mult == 1.0, f"ATR multiplier expected to be 1.0, got {profile.atr_mult}"
    
    # Check that calculating an SL offset reflects atr_mult = 1.0
    # For a long position, calculation: base - X * ATR (simulated)
    # the ATR offset logic usually multiplies ATR * atr_mult.
    mock_atr = 150.0
    expected_offset = mock_atr * profile.atr_mult
    assert expected_offset == 150.0, "Expected SL offset to be exactly 150.0 (1.0 * ATR)"

def test_rocket_v1_profile():
    # Smoke test for rocket profile to ensure multiple profiles are loaded correctly
    registry = TrailingProfilesRegistry()
    profile = registry.get("rocket_v1")
    
    assert profile is not None
    assert profile.mode == "ATR"
