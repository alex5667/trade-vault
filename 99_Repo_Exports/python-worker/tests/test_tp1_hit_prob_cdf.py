"""Unit tests for core/tp1_hit_prob_cdf.py (Plan 3 Phase 2)."""

from __future__ import annotations

import pytest

from core.tp1_hit_prob_cdf import (
    ALL,
    BucketKey,
    build_phit_buckets,
    build_phit_recommendations,
    compute_phit_curve,
    is_curve_calibrated,
    lookup_phit_curve,
    parse_trade_for_phit,
)


# ---------------------------------------------------------------------------
# parse_trade_for_phit
# ---------------------------------------------------------------------------


def test_parse_trade_full_fields() -> None:
    p = parse_trade_for_phit({
        "symbol": "btcusdt",
        "kind": "OF",
        "entry_regime": "Trending",
        "direction": "LONG",
        "mfe_r": "1.5",
        "is_virtual": "0",
    })
    assert p is not None
    assert p["symbol"] == "BTCUSDT"
    assert p["kind"] == "of"
    assert p["regime"] == "trending"
    assert p["direction"] == "LONG"
    assert p["mfe_r"] == pytest.approx(1.5)
    assert p["is_virtual"] is False


def test_parse_trade_derives_mfe_from_pnl() -> None:
    p = parse_trade_for_phit({
        "symbol": "ETHUSDT", "kind": "of", "regime": "range",
        "side": "SELL", "mfe_pnl": "5.0", "one_r_money": "2.0",
    })
    assert p is not None
    assert p["direction"] == "SHORT"
    assert p["mfe_r"] == pytest.approx(2.5)


def test_parse_trade_handles_missing_dims() -> None:
    p = parse_trade_for_phit({"mfe_r": "0.5"})
    assert p is not None
    assert p["symbol"] == ALL
    assert p["kind"] == ALL
    assert p["regime"] == ALL
    assert p["direction"] == ALL


# ---------------------------------------------------------------------------
# compute_phit_curve
# ---------------------------------------------------------------------------


def test_compute_phit_curve_ecdf_complement() -> None:
    # 10 samples: 5 at 0.5, 3 at 1.0, 2 at 2.0
    samples = [0.5] * 5 + [1.0] * 3 + [2.0] * 2
    grid = [0.5, 1.0, 1.5, 2.0]
    curve = compute_phit_curve(samples, grid)
    # P(mfe>=0.5) = 10/10, P(mfe>=1.0)=5/10, P(mfe>=1.5)=2/10, P(mfe>=2.0)=2/10
    assert curve["0.50"] == pytest.approx(1.0)
    assert curve["1.00"] == pytest.approx(0.5)
    assert curve["1.50"] == pytest.approx(0.2)
    assert curve["2.00"] == pytest.approx(0.2)


def test_compute_phit_curve_empty_samples() -> None:
    assert compute_phit_curve([], [0.5, 1.0]) == {}
    assert compute_phit_curve([1.0], []) == {}


def test_compute_phit_curve_strict_monotone_decreasing_on_uniform() -> None:
    samples = list(range(1, 101))  # 1..100
    grid = [10, 25, 50, 75]
    curve = compute_phit_curve(samples, grid)
    vals = [curve["10.00"], curve["25.00"], curve["50.00"], curve["75.00"]]
    # monotonically non-increasing
    assert all(vals[i] >= vals[i + 1] for i in range(len(vals) - 1))


# ---------------------------------------------------------------------------
# is_curve_calibrated
# ---------------------------------------------------------------------------


def test_calibrated_monotone_decreasing() -> None:
    assert is_curve_calibrated({"0.65": 0.9, "1.00": 0.6, "1.50": 0.3}) is True


def test_uncalibrated_when_flat() -> None:
    # all p_hit identical → spread=0 < min_spread
    assert is_curve_calibrated({"0.65": 0.5, "1.00": 0.5, "1.50": 0.5}) is False


def test_uncalibrated_when_non_monotone() -> None:
    # p_hit increases at 1.00 well beyond MONOTONE_TOL
    assert is_curve_calibrated({"0.65": 0.3, "1.00": 0.9, "1.50": 0.1}) is False


def test_uncalibrated_when_too_few_points() -> None:
    assert is_curve_calibrated({"1.00": 0.5}) is False


def test_uncalibrated_when_value_out_of_range() -> None:
    assert is_curve_calibrated({"0.65": 1.2, "1.00": 0.3}) is False


# ---------------------------------------------------------------------------
# build_phit_buckets / build_phit_recommendations
# ---------------------------------------------------------------------------


def _trade(symbol: str, kind: str, regime: str, direction: str, mfe_r: float) -> dict:
    return {
        "symbol": symbol, "kind": kind, "regime": regime,
        "direction": direction, "mfe_r": mfe_r, "is_virtual": False,
    }


def test_build_buckets_creates_all_six_fallback_levels() -> None:
    trades = [_trade("BTCUSDT", "of", "range", "LONG", 0.5)]
    buckets = build_phit_buckets(trades)
    expected_keys = {
        BucketKey("BTCUSDT", "of", "range", "LONG").encode(),
        BucketKey(ALL, "of", "range", "LONG").encode(),
        BucketKey("BTCUSDT", ALL, "range", "LONG").encode(),
        BucketKey("BTCUSDT", "of", ALL, "LONG").encode(),
        BucketKey("BTCUSDT", "of", "range", ALL).encode(),
        BucketKey(ALL, ALL, ALL, ALL).encode(),
    }
    assert set(buckets.keys()) == expected_keys
    # each bucket got the single mfe_r sample
    for b in buckets.values():
        assert b.n_total == 1


def test_build_buckets_excludes_virtual_when_requested() -> None:
    trades = [_trade("BTCUSDT", "of", "range", "LONG", 0.5)]
    trades[0]["is_virtual"] = True
    buckets = build_phit_buckets(trades, include_virtual=False)
    assert buckets == {}


def test_build_recommendations_passes_only_when_calibrated_and_enough_samples() -> None:
    # 250 samples with strict descending curve via uniform 0..2.0
    trades = [
        _trade("BTCUSDT", "of", "range", "LONG", v * 2.0 / 250)
        for v in range(250)
    ]
    buckets = build_phit_buckets(trades)
    grid = [0.5, 1.0, 1.5]
    recs = build_phit_recommendations(buckets, grid=grid, min_samples=200)
    specific = recs[BucketKey("BTCUSDT", "of", "range", "LONG").encode()]
    assert specific["n_total"] == 250
    assert specific["passes"] == 1
    assert specific["calibration_ok"] == 1


def test_build_recommendations_fails_when_below_min_samples() -> None:
    trades = [_trade("BTCUSDT", "of", "range", "LONG", 1.0)]
    buckets = build_phit_buckets(trades)
    recs = build_phit_recommendations(buckets, grid=[0.5, 1.0, 1.5], min_samples=200)
    for r in recs.values():
        assert r["passes"] == 0


# ---------------------------------------------------------------------------
# lookup_phit_curve fallback
# ---------------------------------------------------------------------------


def test_lookup_uses_most_specific_when_passing() -> None:
    state = {
        BucketKey("BTCUSDT", "of", "range", "LONG").encode(): {
            "n_total": 300, "curve": {"1.00": 0.7}, "calibration_ok": 1, "passes": 1,
        },
        BucketKey(ALL, ALL, ALL, ALL).encode(): {
            "n_total": 9999, "curve": {"1.00": 0.5}, "calibration_ok": 1, "passes": 1,
        },
    }
    res = lookup_phit_curve(
        state, symbol="BTCUSDT", kind="of", regime="range", direction="LONG",
    )
    assert res is not None
    assert res["curve"]["1.00"] == pytest.approx(0.7)


def test_lookup_falls_back_to_global() -> None:
    state = {
        BucketKey(ALL, ALL, ALL, ALL).encode(): {
            "n_total": 9999, "curve": {"1.00": 0.5}, "calibration_ok": 1, "passes": 1,
        },
    }
    res = lookup_phit_curve(
        state, symbol="DOGEUSDT", kind="of", regime="range", direction="LONG",
    )
    assert res is not None
    assert res["curve"]["1.00"] == pytest.approx(0.5)


def test_lookup_requires_pass() -> None:
    state = {
        BucketKey("BTCUSDT", "of", "range", "LONG").encode(): {
            "n_total": 100, "curve": {"1.00": 0.7}, "calibration_ok": 0, "passes": 0,
        },
    }
    res = lookup_phit_curve(
        state, symbol="BTCUSDT", kind="of", regime="range", direction="LONG",
        require_pass=True,
    )
    assert res is None


def test_lookup_returns_none_when_empty() -> None:
    assert lookup_phit_curve(
        {}, symbol="X", kind="y", regime="z", direction="LONG"
    ) is None
