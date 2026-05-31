"""Unit tests for meta ENFORCE progressive rollout with canary-share.

Tests deterministic hash-based canary selection and rollout mechanics.
"""


from core.of_confirm_engine import _hash01


def test_hash01_deterministic():
    """Test that _hash01 is deterministic for same input."""
    key = "enf_v1:test_sid_123"
    h1 = _hash01(key)
    h2 = _hash01(key)
    assert h1 == h2, "Hash should be deterministic"


def test_hash01_range():
    """Test that _hash01 returns values in [0, 1) range."""
    for i in range(100):
        key = f"test_key_{i}"
        h = _hash01(key)
        assert 0.0 <= h < 1.0, f"Hash should be in [0, 1) range, got {h}"


def test_hash01_distribution():
    """Test that _hash01 has reasonable distribution."""
    keys = [f"test_key_{i}" for i in range(1000)]
    hashes = [_hash01(k) for k in keys]

    # Check that we get a spread of values
    min_h = min(hashes)
    max_h = max(hashes)
    assert min_h < 0.1, "Should have some low values"
    assert max_h > 0.9, "Should have some high values"

    # Check that distribution is roughly uniform
    # Count values in each quartile
    q1 = sum(1 for h in hashes if 0.0 <= h < 0.25)
    q2 = sum(1 for h in hashes if 0.25 <= h < 0.50)
    q3 = sum(1 for h in hashes if 0.50 <= h < 0.75)
    q4 = sum(1 for h in hashes if 0.75 <= h < 1.0)

    # Each quartile should have roughly 250 values (allow ±50)
    assert 200 <= q1 <= 300, f"Q1 should have ~250 values, got {q1}"
    assert 200 <= q2 <= 300, f"Q2 should have ~250 values, got {q2}"
    assert 200 <= q3 <= 300, f"Q3 should have ~250 values, got {q3}"
    assert 200 <= q4 <= 300, f"Q4 should have ~250 values, got {q4}"


def test_hash01_different_inputs():
    """Test that _hash01 produces different values for different inputs."""
    h1 = _hash01("key1")
    h2 = _hash01("key2")
    h3 = _hash01("key3")

    # Very unlikely all three are the same
    assert not (h1 == h2 == h3), "Different inputs should produce different hashes"


def test_canary_share_selection():
    """Test canary share selection logic."""
    # Test with share=0.10: should select ~10% of signals
    share = 0.10
    keys = [f"enf_v1:sid_{i}" for i in range(1000)]
    selected = sum(1 for k in keys if _hash01(k) < share)

    # Should be roughly 10% (allow ±2%)
    assert 80 <= selected <= 120, f"With share=0.10, should select ~100 signals, got {selected}"

    # Test with share=0.50: should select ~50% of signals
    share = 0.50
    selected = sum(1 for k in keys if _hash01(k) < share)
    assert 450 <= selected <= 550, f"With share=0.50, should select ~500 signals, got {selected}"


def test_canary_share_deterministic():
    """Test that same sid always gets same selection for same salt."""
    salt = "enf_v1"
    sid = "test_sid_123"
    key = f"{salt}:{sid}"

    # Same key should always produce same hash
    h1 = _hash01(key)
    h2 = _hash01(key)
    assert h1 == h2

    # Different salt should produce different hash
    key2 = f"enf_v2:{sid}"
    h3 = _hash01(key2)
    assert h1 != h3, "Different salt should produce different hash"

