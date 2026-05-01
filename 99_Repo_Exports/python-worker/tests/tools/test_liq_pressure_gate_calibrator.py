from __future__ import annotations
"""
Unit tests for tools/liq_pressure_gate_calibrator.py
"""

import pytest
from tools.liq_pressure_gate_calibrator import (
    MODE_LADDER,
    _next_mode,
    _should_propose,
    compute_stats,
)


# ---------------------------------------------------------------------------
# _next_mode
# ---------------------------------------------------------------------------

def test_next_mode_ladder():
    assert _next_mode("off")     == "boost"
    assert _next_mode("boost")   == "penalty"
    assert _next_mode("penalty") == "both"
    assert _next_mode("both")    == "enforce"
    assert _next_mode("enforce") is None


def test_next_mode_unknown_treated_as_off():
    # Unknown mode → treat as index 0 (off), next = boost
    assert _next_mode("unknown_xyz") == "boost"


def test_next_mode_case_insensitive():
    assert _next_mode("BOOST")   == "penalty"
    assert _next_mode("Penalty") == "both"


# ---------------------------------------------------------------------------
# _should_propose
# ---------------------------------------------------------------------------

def test_should_propose_positive():
    stats = {
        "boost_hits":   15,
        "boost_r_mean": 0.30,
        "pass_r_mean":  0.10,
    }
    ok, msg = _should_propose(stats, min_boost_hits=10, min_r_delta=0.05)
    assert ok is True
    assert "boost_hits=15" in msg


def test_should_propose_too_few_hits():
    stats = {
        "boost_hits":   5,
        "boost_r_mean": 0.99,
        "pass_r_mean":  0.00,
    }
    ok, msg = _should_propose(stats, min_boost_hits=10, min_r_delta=0.05)
    assert ok is False
    assert "min=10" in msg


def test_should_propose_delta_too_small():
    stats = {
        "boost_hits":   20,
        "boost_r_mean": 0.12,
        "pass_r_mean":  0.10,  # delta = 0.02 < 0.05
    }
    ok, msg = _should_propose(stats, min_boost_hits=10, min_r_delta=0.05)
    assert ok is False
    assert "Δ R̄" in msg


def test_should_propose_negative_delta():
    stats = {
        "boost_hits":   30,
        "boost_r_mean": -0.20,
        "pass_r_mean":   0.10,  # boost performs worse
    }
    ok, _ = _should_propose(stats, min_boost_hits=10, min_r_delta=0.05)
    assert ok is False


# ---------------------------------------------------------------------------
# compute_stats
# ---------------------------------------------------------------------------

def _make_decisions(boost_sids, pass_sids, veto_sids):
    """Create minimal decision dict for test fixtures."""
    out = {}
    for sid in boost_sids:
        out[sid] = {
            "liq_boost": 0.05, "liq_pen": 0.0, "liq_veto": 0,
            "liq_reason": "bst", "liq_q_align": 1, "liq_ofi_align": 1,
            "symbol": "BTCUSDT", "direction": "LONG", "ts_ms": 0,
        }
    for sid in pass_sids:
        out[sid] = {
            "liq_boost": 0.0, "liq_pen": 0.0, "liq_veto": 0,
            "liq_reason": "", "liq_q_align": 0, "liq_ofi_align": 0,
            "symbol": "BTCUSDT", "direction": "LONG", "ts_ms": 0,
        }
    for sid in veto_sids:
        out[sid] = {
            "liq_boost": 0.0, "liq_pen": 0.1, "liq_veto": 1,
            "liq_reason": "VETO", "liq_q_align": -1, "liq_ofi_align": -1,
            "symbol": "BTCUSDT", "direction": "LONG", "ts_ms": 0,
        }
    return out


def _make_trades(sids_r_map):
    """sid → r_mult"""
    return {
        sid: {"r_mult": r, "symbol": "BTCUSDT", "direction": "LONG"}
        for sid, r in sids_r_map.items()
    }


def test_compute_stats_basic():
    decisions = _make_decisions(
        boost_sids=["b1", "b2", "b3"],
        pass_sids=["p1", "p2"],
        veto_sids=["v1"],
    )
    trades = _make_trades({
        "b1": 0.50, "b2": 0.40, "b3": 0.30,
        "p1": 0.10, "p2": 0.05,
        "v1": -0.80,
    })
    stats = compute_stats(decisions, trades)

    assert stats["total_decisions"] == 6
    assert stats["total_joined"]    == 6
    assert stats["boost_hits"]      == 3
    assert stats["pass_hits"]       == 2
    assert stats["veto_hits"]       == 1

    assert round(stats["boost_r_mean"], 2) == pytest.approx((0.50 + 0.40 + 0.30) / 3, abs=0.01)
    assert round(stats["pass_r_mean"],  2) == pytest.approx((0.10 + 0.05) / 2,        abs=0.01)
    assert round(stats["veto_r_mean"],  2) == pytest.approx(-0.80, abs=0.01)


def test_compute_stats_no_trade_match():
    """Decisions with no matching trade should not be counted."""
    decisions = _make_decisions(boost_sids=["x1", "x2"], pass_sids=[], veto_sids=[])
    trades = _make_trades({})  # empty
    stats = compute_stats(decisions, trades)

    assert stats["total_joined"] == 0
    assert stats["boost_hits"]   == 0


def test_compute_stats_by_symbol():
    decisions = _make_decisions(boost_sids=["b1"], pass_sids=["p1"], veto_sids=[])
    decisions["b1"]["symbol"] = "ETHUSDT"
    decisions["p1"]["symbol"] = "BTCUSDT"
    trades = _make_trades({"b1": 0.20, "p1": -0.05})

    stats = compute_stats(decisions, trades)
    assert "ETHUSDT" in stats["by_symbol"]
    assert "BTCUSDT" in stats["by_symbol"]
    assert stats["by_symbol"]["ETHUSDT"]["boost_count"] == 1
    assert stats["by_symbol"]["BTCUSDT"]["pass_count"]  == 1


def test_mode_ladder_complete():
    """Full ladder traversal."""
    current = "off"
    ladder = [current]
    while True:
        nxt = _next_mode(current)
        if nxt is None:
            break
        ladder.append(nxt)
        current = nxt
    assert ladder == MODE_LADDER
