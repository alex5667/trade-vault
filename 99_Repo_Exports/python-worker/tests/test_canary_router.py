"""Tests for canary router (deterministic routing)."""

from __future__ import annotations

import pytest

from core.canary_router import CanaryPolicy, parse_symbol_set, _stable_u01


def test_stable_u01():
    """Test deterministic hash function."""
    s1 = "test_symbol_123"
    s2 = "test_symbol_123"
    s3 = "different"
    
    u1 = _stable_u01(s1)
    u2 = _stable_u01(s2)
    u3 = _stable_u01(s3)
    
    # Same input -> same output
    assert u1 == u2
    # Different input -> likely different output
    assert u1 != u3
    
    # Range check
    assert 0.0 <= u1 < 1.0
    assert 0.0 <= u2 < 1.0
    assert 0.0 <= u3 < 1.0


def test_parse_symbol_set():
    """Test symbol set parsing."""
    assert parse_symbol_set("") == set()
    assert parse_symbol_set("BTCUSDT") == {"BTCUSDT"}
    assert parse_symbol_set("BTCUSDT,ETHUSDT") == {"BTCUSDT", "ETHUSDT"}
    assert parse_symbol_set("BTCUSDT, ETHUSDT, SOLUSDT") == {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
    assert parse_symbol_set("btcusdt,ethusdt") == {"BTCUSDT", "ETHUSDT"}  # uppercase


def test_canary_policy_enforce_symbols():
    """Test enforce_symbols (always ENFORCE)."""
    policy = CanaryPolicy(enforce_share=0.0, enforce_symbols={"BTCUSDT", "ETHUSDT"})
    
    # Symbols in enforce_symbols -> always True
    assert policy.should_enforce(sid="s1", symbol="BTCUSDT", ts_ms=1000) is True
    assert policy.should_enforce(sid="s2", symbol="ETHUSDT", ts_ms=2000) is True
    
    # Other symbols -> False (enforce_share=0.0)
    assert policy.should_enforce(sid="s3", symbol="SOLUSDT", ts_ms=3000) is False


def test_canary_policy_enforce_share():
    """Test enforce_share routing."""
    # 100% share -> always True
    policy = CanaryPolicy(enforce_share=1.0)
    assert policy.should_enforce(sid="s1", symbol="BTCUSDT", ts_ms=1000) is True
    assert policy.should_enforce(sid="s2", symbol="ETHUSDT", ts_ms=2000) is True
    
    # 0% share -> always False
    policy = CanaryPolicy(enforce_share=0.0)
    assert policy.should_enforce(sid="s1", symbol="BTCUSDT", ts_ms=1000) is False
    assert policy.should_enforce(sid="s2", symbol="ETHUSDT", ts_ms=2000) is False
    
    # 50% share -> deterministic routing
    policy = CanaryPolicy(enforce_share=0.5)
    results = [policy.should_enforce(sid=f"s{i}", symbol="BTCUSDT", ts_ms=1000+i) for i in range(100)]
    # Should have some True and some False (not all same)
    assert any(results) and not all(results)


def test_canary_policy_sample_key_mode_sid():
    """Test sid-based routing (default)."""
    policy = CanaryPolicy(enforce_share=0.5, sample_key_mode="sid")
    
    # Same sid -> same result
    r1 = policy.should_enforce(sid="stable_sid", symbol="BTCUSDT", ts_ms=1000)
    r2 = policy.should_enforce(sid="stable_sid", symbol="BTCUSDT", ts_ms=2000)
    assert r1 == r2  # deterministic per sid
    
    # Different sid -> may differ
    r3 = policy.should_enforce(sid="other_sid", symbol="BTCUSDT", ts_ms=1000)
    # May or may not be same, but deterministic


def test_canary_policy_sample_key_mode_symbol_ts():
    """Test symbol_ts-based routing (timebucket)."""
    policy = CanaryPolicy(enforce_share=0.5, sample_key_mode="symbol_ts", timebucket_sec=60)
    
    # Same timebucket -> same result
    r1 = policy.should_enforce(sid="s1", symbol="BTCUSDT", ts_ms=1000)
    r2 = policy.should_enforce(sid="s2", symbol="BTCUSDT", ts_ms=50000)  # same bucket (0-60s)
    assert r1 == r2
    
    # Different timebucket -> may differ
    r3 = policy.should_enforce(sid="s3", symbol="BTCUSDT", ts_ms=61000)  # next bucket
    # May or may not be same, but deterministic per bucket


def test_canary_policy_clamp_share():
    """Test share clamping to [0,1]."""
    # Negative -> 0.0
    policy = CanaryPolicy(enforce_share=-0.5)
    assert policy.should_enforce(sid="s1", symbol="BTCUSDT", ts_ms=1000) is False
    
    # > 1.0 -> 1.0
    policy = CanaryPolicy(enforce_share=1.5)
    assert policy.should_enforce(sid="s1", symbol="BTCUSDT", ts_ms=1000) is True

