#!/usr/bin/env python3
from __future__ import annotations

"""
Test for SID normalization and deterministic sampling.

Tests:
  - SID normalization to canonical format: crypto-of:{symbol}:{ts_ms}
  - Deterministic sampling (stable by sid, no RNG)
  - Enforce/canary routing with sid+run_id
"""


import pytest

from services.ml_confirm import (
    _canonical_sid,
    _make_sid,
    _normalize_sid,
    _should_sample,
    _stable_hash_u64,
    _stable_sample,
    _stable_u01,
)


class TestSIDNormalization:
    """Test SID normalization to canonical format"""

    def test_make_sid(self):
        """Test creating canonical SID"""
        assert _make_sid("BTCUSDT", 1234567890) == "crypto-of:BTCUSDT:1234567890"
        assert _make_sid("ETHUSDT", 9876543210) == "crypto-of:ETHUSDT:9876543210"
        assert _make_sid("btcusdt", 1234567890) == "crypto-of:BTCUSDT:1234567890"  # Uppercase

    def test_normalize_already_canonical(self):
        """Test normalization of already canonical SID"""
        sid = "crypto-of:BTCUSDT:1234567890"
        result = _normalize_sid(sid, symbol="BTCUSDT", ts_ms=1234567890)
        assert result == sid

    def test_normalize_legacy_pipe_format(self):
        """Test normalization of legacy {symbol}|{ts}|{dir} format"""
        sid = "BTCUSDT|1234567890|LONG"
        result = _normalize_sid(sid, symbol="BTCUSDT", ts_ms=1234567890)
        assert result == "crypto-of:BTCUSDT:1234567890"

    def test_normalize_canonical_with_suffix(self):
        """Test normalization of canonical with extra suffix"""
        sid = "crypto-of:BTCUSDT:1234567890:extra:suffix"
        result = _normalize_sid(sid, symbol="BTCUSDT", ts_ms=1234567890)
        assert result == "crypto-of:BTCUSDT:1234567890"  # Only first 3 tokens

    def test_normalize_empty_generates(self):
        """Test normalization of empty SID generates canonical format"""
        result = _normalize_sid("", symbol="BTCUSDT", ts_ms=1234567890)
        assert result == "crypto-of:BTCUSDT:1234567890"

    def test_normalize_none_generates(self):
        """Test normalization of None SID generates canonical format"""
        result = _normalize_sid(None, symbol="ETHUSDT", ts_ms=9876543210)
        assert result == "crypto-of:ETHUSDT:9876543210"

    def test_canonical_sid_from_indicators(self):
        """Test _canonical_sid extracts and normalizes from indicators"""
        indicators = {"sid": "BTCUSDT|1234567890|LONG"}
        result = _canonical_sid(indicators, symbol="BTCUSDT", ts_ms=1234567890)
        assert result == "crypto-of:BTCUSDT:1234567890"

    def test_canonical_sid_signal_id_fallback(self):
        """Test _canonical_sid uses signal_id as fallback"""
        indicators = {"signal_id": "ETHUSDT:9876543210"}
        result = _canonical_sid(indicators, symbol="ETHUSDT", ts_ms=9876543210)
        assert result == "crypto-of:ETHUSDT:9876543210"

    def test_canonical_sid_generates_when_missing(self):
        """Test _canonical_sid generates when missing from indicators"""
        indicators = {}
        result = _canonical_sid(indicators, symbol="BTCUSDT", ts_ms=1234567890)
        assert result == "crypto-of:BTCUSDT:1234567890"


class TestDeterministicSampling:
    """Test deterministic sampling (stable by sid, no RNG)"""

    def test_stable_hash_u64(self):
        """Test stable hash generation"""
        h1 = _stable_hash_u64("test")
        h2 = _stable_hash_u64("test")
        assert h1 == h2  # Same input -> same hash

        h3 = _stable_hash_u64("different")
        assert h1 != h3  # Different input -> different hash

    def test_stable_u01(self):
        """Test stable uniform [0,1) value generation"""
        u1 = _stable_u01("test", salt="")
        u2 = _stable_u01("test", salt="")
        assert u1 == u2  # Same input -> same value
        assert 0.0 <= u1 < 1.0  # In range [0, 1)

        u3 = _stable_u01("different", salt="")
        assert u1 != u3  # Different input -> different value

    def test_stable_u01_with_salt(self):
        """Test stable_u01 with salt produces different values"""
        u1 = _stable_u01("test", salt="salt1")
        u2 = _stable_u01("test", salt="salt2")
        assert u1 != u2  # Different salt -> different value

    def test_should_sample(self):
        """Test _should_sample function"""
        key = "crypto-of:BTCUSDT:1234567890"

        # Rate 1.0 always returns True
        assert _should_sample(key, rate=1.0, salt="test") is True

        # Rate 0.0 always returns False
        assert _should_sample(key, rate=0.0, salt="test") is False

        # Same key+salt -> same decision
        result1 = _should_sample(key, rate=0.5, salt="test")
        result2 = _should_sample(key, rate=0.5, salt="test")
        assert result1 == result2

        # Different salt -> may be different
        result3 = _should_sample(key, rate=0.5, salt="different")
        # May be same or different, but should be deterministic
        assert _should_sample(key, rate=0.5, salt="different") == result3

    def test_stable_sample_deterministic(self):
        """Test deterministic sampling - same sid always gets same decision"""
        sid = "crypto-of:BTCUSDT:1234567890"
        prob = 0.5
        salt = "test_salt"

        # Same sid + salt -> same decision
        result1 = _stable_sample(prob, key=sid, salt=salt)
        result2 = _stable_sample(prob, key=sid, salt=salt)
        assert result1 == result2

    def test_stable_sample_prob_1_0(self):
        """Test sampling with probability 1.0 always returns True"""
        sid = "crypto-of:BTCUSDT:1234567890"
        assert _stable_sample(1.0, key=sid, salt="test") is True

    def test_stable_sample_prob_0_0(self):
        """Test sampling with probability 0.0 always returns False"""
        sid = "crypto-of:BTCUSDT:1234567890"
        assert _stable_sample(0.0, key=sid, salt="test") is False

    def test_stable_sample_different_salt(self):
        """Test different salt produces different sampling decisions"""
        sid = "crypto-of:BTCUSDT:1234567890"
        prob = 0.5

        result1 = _stable_sample(prob, key=sid, salt="salt1")
        result2 = _stable_sample(prob, key=sid, salt="salt2")

        # May be same or different, but should be deterministic
        # (same salt -> same result)
        assert _stable_sample(prob, key=sid, salt="salt1") == result1
        assert _stable_sample(prob, key=sid, salt="salt2") == result2

    def test_stable_sample_distribution(self):
        """Test sampling distribution over many sids"""
        prob = 0.1
        salt = "distribution_test"
        results = []
        for i in range(1000):
            sid = f"crypto-of:SYMBOL{i}:{i}"
            results.append(_stable_sample(prob, key=sid, salt=salt))

        # Should have roughly 10% True (with some variance)
        true_count = sum(results)
        assert 50 < true_count < 150  # Roughly 10% of 1000 = 100, allow variance


class TestEnforceRouting:
    """Test enforce/canary routing with sid+run_id"""

    def test_stable_u01_with_run_id(self):
        """Test stable bucket assignment with sid+run_id"""
        sid = "crypto-of:BTCUSDT:1234567890"
        run_id = "run_123"

        bucket1 = _stable_u01(f"{sid}|{run_id}")
        bucket2 = _stable_u01(f"{sid}|{run_id}")
        assert bucket1 == bucket2  # Same sid+run_id -> same bucket

        # Different run_id -> different bucket
        bucket3 = _stable_u01(f"{sid}|run_456")
        assert bucket1 != bucket3

        # Different sid -> different bucket
        sid2 = "crypto-of:ETHUSDT:1234567890"
        bucket4 = _stable_u01(f"{sid2}|{run_id}")
        assert bucket1 != bucket4

    def test_enforce_routing_deterministic(self):
        """Test enforce routing is deterministic for same sid+run_id"""
        sid = "crypto-of:BTCUSDT:1234567890"
        run_id = "run_123"
        enforce_share = 0.1

        bucket = _stable_u01(f"{sid}|{run_id}")
        should_enforce = bucket < enforce_share

        # Same sid+run_id -> same decision
        bucket2 = _stable_u01(f"{sid}|{run_id}")
        should_enforce2 = bucket2 < enforce_share
        assert should_enforce == should_enforce2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

