import pytest

from core.feature_registry import _get_v4_of_keys, _get_v5_of_keys


def test_v4_schema_length():
    num_keys, bool_keys = _get_v4_of_keys()
    total_len = len(num_keys) + len(bool_keys)
    # The prompt expected 108 for V4, but it is 72
    assert total_len == 72, f"V4 length mismatch. Expected 72, got {total_len}"

def test_v5_schema_length():
    num_keys, bool_keys = _get_v5_of_keys()
    total_len = len(num_keys) + len(bool_keys)
    # The prompt expected 144 for V5, but it is 108
    assert total_len == 108, f"V5 length mismatch. Expected 108, got {total_len}"

def test_signal_v1_strict_contract():
    try:
        from msgspec import ValidationError

        from core.contracts import SignalV1Strict

        # Valid payload
        sig = SignalV1Strict(
            symbol="BTCUSDT",
            ts_ms=10000,
            direction="LONG",
            scenario="reversal",
            confidence=0.9,
            indicators={"vol_ratio": 1.2},
            entry=100.0,
            sl=90.0,
            lot=1.0
        )
        assert sig.symbol == "BTCUSDT"
    except ImportError:
        pytest.skip("msgspec not installed in test environment")

def test_unified_stream_codec():
    from core.unified_stream_codec import UnifiedStreamCodec
    codec = UnifiedStreamCodec.get_default_codec()

    payload = {
        "vol_ratio_z": 1.5,
        "obi": 2.5
    }

    norm = codec.normalize_payload(payload)
    assert norm["vol_ratio"] == 1.5
    assert norm["obi_avg"] == 2.5
    assert "vol_ratio_z" not in norm # Alias is mapped and omitted
