from signal_scoring.reason_registry import (
    _REASON_CODE_U16,
    _ALLOWED_DUP_U16,
    _U16_TO_CANONICAL,
    reason_code_to_u16,
    u16_to_reason_code,
)


def test_reason_registry_has_no_unexpected_duplicates() -> None:
    """Test that only explicitly allowed u16 values have duplicates (aliases)."""
    inv = {}
    for rc, u in _REASON_CODE_U16.items():
        inv.setdefault(int(u), []).append(rc)

    bad = {u: rcs for u, rcs in inv.items() if len(rcs) > 1 and u not in _ALLOWED_DUP_U16}
    assert not bad, f"Unexpected duplicate u16 values: {bad}"


def test_canonical_decode_is_deterministic() -> None:
    """Test that u16_to_reason_code always returns canonical names."""
    # Test all registered codes
    for rc, u16 in _REASON_CODE_U16.items():
        decoded = u16_to_reason_code(u16)
        assert decoded in _REASON_CODE_U16, f"Decoded '{decoded}' not in registry"
        assert _REASON_CODE_U16[decoded] == u16, f"Decoded '{decoded}' has wrong u16"

    # Test canonical preferences
    assert u16_to_reason_code(255) == "VETO_UNKNOWN"  # canonical for aliases

    # Test unknown codes
    assert u16_to_reason_code(99999) == "VETO_UNKNOWN"


def test_reason_code_to_u16_and_back() -> None:
    """Test round-trip conversion for all registered codes."""
    for code in _REASON_CODE_U16.keys():
        u16 = reason_code_to_u16(code)
        decoded = u16_to_reason_code(u16)
        # Should decode to canonical form
        assert _REASON_CODE_U16[decoded] == u16


def test_empty_string_defaults_to_veto_unknown() -> None:
    """Test that empty reason_code defaults to VETO_UNKNOWN."""
    assert reason_code_to_u16("") == 255
    assert reason_code_to_u16(None) == 255
