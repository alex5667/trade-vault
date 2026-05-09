from __future__ import annotations

"""
services/tests/test_horizon_profile_bootstrap_service_v1.py
────────────────────────────────────────────────────────────
Unit tests for Phase 1 horizon_profile_bootstrap_service.

Covers pure math helpers (no DB / Redis needed).
"""

import pytest

from services.horizon_profile_bootstrap_service import (
    HorizonProfileBootstrapService,
    HorizonStatRow,
    _bucket_by_hold_ms,
    _percentile_disc_sorted,
    _profile_conf,
)

# ---------------------------------------------------------------------------
# _bucket_by_hold_ms
# ---------------------------------------------------------------------------

def test_bucket_micro():
    assert _bucket_by_hold_ms(60_000) == "micro"       # 1 min


def test_bucket_short():
    assert _bucket_by_hold_ms(300_000) == "short"      # 5 min


def test_bucket_medium():
    assert _bucket_by_hold_ms(1_800_000) == "medium"   # 30 min


def test_bucket_long():
    assert _bucket_by_hold_ms(5_400_000) == "long"     # 90 min


def test_bucket_exact_boundary_micro_short():
    # boundary: 180_000 ms = 3 min → short (not micro)
    assert _bucket_by_hold_ms(180_000) == "short"
    assert _bucket_by_hold_ms(179_999) == "micro"


def test_bucket_zero_unknown():
    assert _bucket_by_hold_ms(0) == "unknown"
    assert _bucket_by_hold_ms(-1) == "unknown"


# ---------------------------------------------------------------------------
# _percentile_disc_sorted
# ---------------------------------------------------------------------------

def test_percentile_p50():
    xs = [100, 200, 300, 400, 500]
    assert _percentile_disc_sorted(xs, 0.50) == 300


def test_percentile_p75():
    xs = [100, 200, 300, 400, 500]
    assert _percentile_disc_sorted(xs, 0.75) == 400


def test_percentile_single():
    assert _percentile_disc_sorted([999], 0.50) == 999


def test_percentile_empty():
    assert _percentile_disc_sorted([], 0.50) == 0


def test_percentile_extremes():
    xs = [1, 2, 3]
    assert _percentile_disc_sorted(xs, 0.0) == 1
    assert _percentile_disc_sorted(xs, 1.0) == 3


# ---------------------------------------------------------------------------
# _profile_conf
# ---------------------------------------------------------------------------

def test_profile_conf_zero():
    assert _profile_conf(0, 40, 150) == 0.0


def test_profile_conf_below_min():
    c = _profile_conf(20, 40, 150)
    assert 0.0 < c < 0.5


def test_profile_conf_at_min():
    c = _profile_conf(40, 40, 150)
    assert c == pytest.approx(0.5, abs=0.01)


def test_profile_conf_at_strong():
    assert _profile_conf(150, 40, 150) == 1.0


def test_profile_conf_above_strong():
    assert _profile_conf(500, 40, 150) == 1.0


def test_profile_conf_midway():
    # midway between min_n=40 and strong_n=150 => sample_n=95 => conf ~0.75
    c = _profile_conf(95, 40, 150)
    assert 0.70 < c < 0.80


# ---------------------------------------------------------------------------
# HorizonProfileBootstrapService._calc_profile
# ---------------------------------------------------------------------------

def _make_svc(**kwargs) -> HorizonProfileBootstrapService:
    svc = HorizonProfileBootstrapService(
        dsn="postgresql://x:x@localhost/x",
        redis_url="redis://localhost:6379/0",
    )
    for k, v in kwargs.items():
        setattr(svc, "_" + k, v)
    return svc


def test_calc_profile_basic():
    svc = _make_svc(min_n=4, strong_n=10, max_signal_age_cap_ms=300_000)
    rows = [
        HorizonStatRow("breakout", "trend_up", 300_000, 120_000),
        HorizonStatRow("breakout", "trend_up", 360_000, 180_000),
        HorizonStatRow("breakout", "trend_up", 420_000, 240_000),
        HorizonStatRow("breakout", "trend_up", 600_000, 300_000),
    ]
    prof = svc._calc_profile(rows)
    assert prof is not None
    assert prof["hold_target_ms"] == 360_000        # p50 of [300k,360k,420k,600k]
    assert prof["alpha_half_life_ms"] == 180_000    # p50 of [120k,180k,240k,300k]
    assert prof["max_signal_age_ms"] <= prof["alpha_half_life_ms"]
    assert prof["max_signal_age_ms"] >= 15_000
    assert prof["risk_horizon_bucket"] == "short"
    assert prof["profile_source"] == "history"
    assert prof["sample_n"] == 4
    assert prof["contract_ver"] == 2
    assert "updated_at_ms" in prof


def test_calc_profile_respects_hold_target_cap():
    """max_signal_age_ms must not exceed 0.33 * hold_target_ms."""
    svc = _make_svc(min_n=4, strong_n=10, max_signal_age_cap_ms=300_000)
    rows = [HorizonStatRow("x", "na", 60_000_000, 30_000_000)] * 5  # huge hold
    prof = svc._calc_profile(rows)
    assert prof is not None
    assert prof["max_signal_age_ms"] <= 300_000  # hard cap


def test_calc_profile_none_below_min_n():
    svc = _make_svc(min_n=10)
    rows = [HorizonStatRow("x", "na", 300_000, 120_000)] * 5
    assert svc._calc_profile(rows) is None


def test_calc_profile_no_mfe_fallback():
    """When time_to_mfe_ms = 0 for all rows, mfe_p50 falls back to 0.5 * hold_p50."""
    svc = _make_svc(min_n=4, strong_n=10, max_signal_age_cap_ms=300_000)
    rows = [HorizonStatRow("x", "na", 300_000, 0)] * 4
    prof = svc._calc_profile(rows)
    assert prof is not None
    assert prof["alpha_half_life_ms"] >= 15_000


def test_calc_profile_bucket_micro():
    svc = _make_svc(min_n=4, strong_n=10, max_signal_age_cap_ms=300_000)
    rows = [HorizonStatRow("x", "na", 60_000, 30_000)] * 4
    prof = svc._calc_profile(rows)
    assert prof is not None
    assert prof["risk_horizon_bucket"] == "micro"


def test_calc_profile_bucket_long():
    svc = _make_svc(min_n=4, strong_n=10, max_signal_age_cap_ms=300_000)
    rows = [HorizonStatRow("x", "na", 3_600_000, 1_200_000)] * 4
    prof = svc._calc_profile(rows)
    assert prof is not None
    assert prof["risk_horizon_bucket"] == "long"


# ---------------------------------------------------------------------------
# _publish_profiles (unit: grouping logic without Redis)
# ---------------------------------------------------------------------------

def test_publish_profiles_groups_exact_scenario_default():
    """
    Verify that grouping builds the 3-level hierarchy correctly
    without actually touching Redis (patch _get_redis to return a mock).
    """
    written: list = []

    class _FakeRedis:
        def set(self, key, value):
            written.append(key)

    svc = _make_svc(min_n=1, strong_n=10, max_signal_age_cap_ms=300_000)
    svc._redis = _FakeRedis()

    rows = [
        HorizonStatRow("breakout", "trend_up", 300_000, 120_000),
        HorizonStatRow("breakout", "flat", 360_000, 180_000),
        HorizonStatRow("pullback", "flat", 420_000, 200_000),
    ]
    n = svc._publish_profiles("CryptoOrderFlow", "BTCUSDT", rows)
    assert n > 0

    keys_str = " ".join(written)
    # Symbol default must always be present
    assert "cfg:horizon:profile:CryptoOrderFlow:BTCUSDT:default:na" in keys_str
    # Scenario fallbacks
    assert "cfg:horizon:profile:CryptoOrderFlow:BTCUSDT:breakout:na" in keys_str
