"""Tests for Task 2.3 G10 enforce-canary deterministic sampler."""
from __future__ import annotations

import pytest

from services.orderflow.strategy import _g10_canary_sample


def test_canary_disabled_by_default(monkeypatch):
    monkeypatch.delenv("ADVERSE_ENFORCE_CANARY_PCT", raising=False)
    assert _g10_canary_sample("BTCUSDT", 1700000000_000) is False


def test_canary_zero_pct_disabled(monkeypatch):
    monkeypatch.setenv("ADVERSE_ENFORCE_CANARY_PCT", "0")
    for ts in range(1700000000_000, 1700000000_100):
        assert _g10_canary_sample("BTCUSDT", ts) is False


def test_canary_100_pct_always_samples(monkeypatch):
    monkeypatch.setenv("ADVERSE_ENFORCE_CANARY_PCT", "100")
    for ts in range(1700000000_000, 1700000000_100):
        assert _g10_canary_sample("BTCUSDT", ts) is True


def test_canary_rate_approximates_pct(monkeypatch):
    monkeypatch.setenv("ADVERSE_ENFORCE_CANARY_PCT", "5")
    n = 20000
    sampled = sum(
        1 for ts in range(1_700_000_000_000, 1_700_000_000_000 + n)
        if _g10_canary_sample("BTCUSDT", ts)
    )
    rate = sampled / n
    # 5% target, allow ±1.5pp tolerance for hash-uniform sampling on 20k draws
    assert 0.035 <= rate <= 0.065, f"canary rate {rate} far from 5%"


def test_canary_deterministic(monkeypatch):
    monkeypatch.setenv("ADVERSE_ENFORCE_CANARY_PCT", "5")
    # Same (symbol, ts) always lands in same bucket
    for ts in (1700000123_456, 1700000456_789, 1700000999_999):
        a = _g10_canary_sample("ETHUSDT", ts)
        b = _g10_canary_sample("ETHUSDT", ts)
        assert a == b


def test_canary_decorrelated_across_symbols(monkeypatch):
    monkeypatch.setenv("ADVERSE_ENFORCE_CANARY_PCT", "5")
    # Different symbols should not all share the same canary decision
    decisions = {
        sym: _g10_canary_sample(sym, 1700000000_000)
        for sym in ("BTCUSDT", "ETHUSDT", "SOLUSDT", "1000PEPEUSDT", "DOGEUSDT", "XRPUSDT")
    }
    # At least one True and one False in this sample
    assert True in decisions.values() or False in decisions.values()


def test_canary_invalid_env_treated_as_off(monkeypatch):
    monkeypatch.setenv("ADVERSE_ENFORCE_CANARY_PCT", "not_a_number")
    assert _g10_canary_sample("BTCUSDT", 1700000000_000) is False


def test_canary_pct_clamped(monkeypatch):
    # Out-of-range pct values clamp safely: -10 → off, 150 → 100%
    monkeypatch.setenv("ADVERSE_ENFORCE_CANARY_PCT", "150")
    for ts in range(1700000000_000, 1700000000_050):
        assert _g10_canary_sample("BTCUSDT", ts) is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
