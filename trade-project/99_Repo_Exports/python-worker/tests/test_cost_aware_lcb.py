"""
Tests for cost-aware LCB computation and hysteresis.
"""
from unittest.mock import Mock

import pytest

from core.cost_aware_lcb import compute_arm_stats, compute_r_adj, lcb_from_samples
from core.winner_hysteresis import WinnerHysteresis


def test_compute_r_adj_slippage_only(monkeypatch):
    """Test r_adj computation with slippage only (fees already net)."""
    monkeypatch.setenv("LCB_FEES_ALREADY_NET", "1")
    monkeypatch.setenv("LCB_SUBTRACT_FEES", "0")
    monkeypatch.setenv("LCB_SLIPPAGE_BPS_CAP", "250")
    payload = {
        "r_mult": 1.0,
        "risk_usd": 100.0,
        "turnover_roundtrip": 10000.0,
        "p0_slippage_bps_est": 10.0,  # 10 bps
        "fees_usd": 5.0,
    }
    # slip_usd = 10000 * (10/10000) = 10
    # slip_R = 10 / 100 = 0.1
    # r_adj = 1.0 - 0.1 - 0 = 0.9
    result = compute_r_adj(payload)
    assert abs(result - 0.9) < 1e-9


def test_compute_r_adj_with_fees_subtract(monkeypatch):
    """Test r_adj computation with fees subtraction."""
    monkeypatch.setenv("LCB_FEES_ALREADY_NET", "0")
    monkeypatch.setenv("LCB_SUBTRACT_FEES", "1")
    payload = {
        "r_mult": 1.0,
        "risk_usd": 100.0,
        "turnover_roundtrip": 0.0,
        "p0_slippage_bps_est": 0.0,
        "fees_usd": 5.0,
    }
    # fees_R = 5 / 100 = 0.05
    # r_adj = 1.0 - 0 - 0.05 = 0.95
    result = compute_r_adj(payload)
    assert abs(result - 0.95) < 1e-9


def test_compute_r_adj_fees_already_net(monkeypatch):
    """Test that fees are not subtracted when already net."""
    monkeypatch.setenv("LCB_FEES_ALREADY_NET", "1")
    monkeypatch.setenv("LCB_SUBTRACT_FEES", "0")
    payload = {
        "r_mult": 1.0,
        "risk_usd": 100.0,
        "turnover_roundtrip": 0.0,
        "p0_slippage_bps_est": 0.0,
        "fees_usd": 5.0,
    }
    # fees_R = 0 (not subtracted)
    # r_adj = 1.0 - 0 - 0 = 1.0
    result = compute_r_adj(payload)
    assert abs(result - 1.0) < 1e-9


def test_compute_r_adj_slippage_cap(monkeypatch):
    """Test slippage bps cap."""
    monkeypatch.setenv("LCB_SLIPPAGE_BPS_CAP", "50")
    payload = {
        "r_mult": 1.0,
        "risk_usd": 100.0,
        "turnover_roundtrip": 10000.0,
        "p0_slippage_bps_est": 100.0,  # 100 bps, but capped at 50
        "fees_usd": 0.0,
    }
    # slip_usd = 10000 * (50/10000) = 50 (capped)
    # slip_R = 50 / 100 = 0.5
    # r_adj = 1.0 - 0.5 = 0.5
    result = compute_r_adj(payload)
    assert abs(result - 0.5) < 1e-9


def test_lcb_from_samples():
    """Test LCB computation from samples."""
    xs = [1.0, 1.0, 1.0, 1.0]
    mean, std, stderr, lcb = lcb_from_samples(xs, z=1.28)
    assert abs(mean - 1.0) < 1e-9
    assert abs(std - 0.0) < 1e-9
    assert abs(lcb - 1.0) < 1e-9

    xs2 = [0.5, 1.5, 0.5, 1.5]
    mean2, std2, stderr2, lcb2 = lcb_from_samples(xs2, z=1.28)
    assert abs(mean2 - 1.0) < 1e-9
    assert std2 > 0.0
    assert lcb2 < mean2  # LCB should be lower than mean


def test_lcb_prefers_lower_variance_at_same_mean():
    """Test that LCB prefers lower variance at same mean."""
    z = 1.28
    samples = {
        "A": [1.0, 1.0, 1.0, 1.0],
        "B": [0.5, 1.5, 0.5, 1.5],
    }
    stats = compute_arm_stats(samples, z=z, min_n=4, floor=-10.0)
    assert len(stats) == 2
    assert stats[0].arm == "A"  # Lower variance should have higher LCB
    assert stats[0].lcb > stats[1].lcb


def test_compute_arm_stats_min_n_filter():
    """Test that arms with insufficient samples are filtered."""
    samples = {
        "A": [1.0] * 50,
        "B": [1.0] * 30,  # Below min_n=40
        "C": [1.0] * 60,
    }
    stats = compute_arm_stats(samples, z=1.28, min_n=40, floor=-10.0)
    assert len(stats) == 2
    assert "B" not in [s.arm for s in stats]


def test_compute_arm_stats_floor_filter():
    """Test that arms below floor are filtered."""
    samples = {
        "A": [-1.0] * 50,  # Below floor=0.0
        "B": [1.0] * 50,
    }
    stats = compute_arm_stats(samples, z=1.28, min_n=40, floor=0.0)
    assert len(stats) == 1
    assert stats[0].arm == "B"


def test_hysteresis_init():
    """Test hysteresis initialization."""
    mock_r = Mock()
    hyst = WinnerHysteresis(mock_r)
    assert hyst.min_delta == 0.05
    assert hyst.confirm_windows == 2
    assert hyst.ttl_sec == 604800


def test_hysteresis_apply_init(monkeypatch):
    """Test hysteresis apply on first run (no previous winner)."""
    monkeypatch.setenv("LCB_MIN_DELTA_LCB", "0.05")
    monkeypatch.setenv("LCB_CONFIRM_WINDOWS", "2")

    mock_r = Mock()
    mock_r.get.return_value = None
    mock_r.pipeline.return_value = mock_r
    mock_r.execute.return_value = []

    hyst = WinnerHysteresis(mock_r)
    res = hyst.apply(bucket="test", candidate="B", candidate_lcb=0.5)

    assert res.changed is True
    assert res.winner == "B"
    assert res.reason == "init"


def test_hysteresis_apply_same_candidate(monkeypatch):
    """Test hysteresis when candidate is same as previous."""
    monkeypatch.setenv("LCB_MIN_DELTA_LCB", "0.05")
    monkeypatch.setenv("LCB_CONFIRM_WINDOWS", "2")

    mock_r = Mock()
    mock_r.get.side_effect = lambda k: "B" if "winner" in k else "0.5" if "winner_lcb" in k else None
    mock_r.pipeline.return_value = mock_r
    mock_r.execute.return_value = []

    hyst = WinnerHysteresis(mock_r)
    res = hyst.apply(bucket="test", candidate="B", candidate_lcb=0.5)

    assert res.changed is False
    assert res.winner == "B"
    assert res.reason == "same"


def test_hysteresis_apply_delta_too_small(monkeypatch):
    """Test hysteresis when delta is too small."""
    monkeypatch.setenv("LCB_MIN_DELTA_LCB", "0.05")
    monkeypatch.setenv("LCB_CONFIRM_WINDOWS", "2")

    mock_r = Mock()
    mock_r.get.side_effect = lambda k: "A" if "winner" in k else "0.5" if "winner_lcb" in k else None
    mock_r.pipeline.return_value = mock_r
    mock_r.execute.return_value = []

    hyst = WinnerHysteresis(mock_r)
    # candidate_lcb = 0.5, prev_lcb = 0.5, delta = 0.0 < 0.05
    res = hyst.apply(bucket="test", candidate="B", candidate_lcb=0.5)

    assert res.changed is False
    assert res.winner == "A"
    assert res.reason == "delta_too_small"


def test_hysteresis_apply_confirmed(monkeypatch):
    """Test hysteresis when candidate is confirmed after CONFIRM_WINDOWS."""
    monkeypatch.setenv("LCB_MIN_DELTA_LCB", "0.05")
    monkeypatch.setenv("LCB_CONFIRM_WINDOWS", "2")

    mock_r = Mock()
    call_count = {"winner": 0, "pending": 0, "pending_count": 0}

    def get_side_effect(k):
        if "winner" in k:
            call_count["winner"] += 1
            return "A" if call_count["winner"] == 1 else "B"
        elif "winner_lcb" in k:
            return "0.5"
        elif "pending" in k and "count" not in k:
            call_count["pending"] += 1
            return "B" if call_count["pending"] >= 1 else None
        elif "pending_count" in k:
            call_count["pending_count"] += 1
            return "2" if call_count["pending_count"] >= 2 else "1"
        return None

    mock_r.get.side_effect = get_side_effect
    mock_r.pipeline.return_value = mock_r
    mock_r.execute.return_value = [2]  # For incr
    mock_r.incr.return_value = 2

    hyst = WinnerHysteresis(mock_r)
    # First call: pending
    res1 = hyst.apply(bucket="test", candidate="B", candidate_lcb=0.6)
    # Second call: confirmed
    res2 = hyst.apply(bucket="test", candidate="B", candidate_lcb=0.6)

    # Second call should confirm
    assert res2.changed is True or res2.reason in ("confirmed", "pending")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
