from __future__ import annotations

from core.eq_pools import EQPoolTracker
from core.sweep_detector import SweepDetector
from core.swing_detector import SwingPoint
from core.microbar import MicroBar


def _sp(kind: str, ts: int, price: float, cvd: float = 0.0) -> SwingPoint:
    return SwingPoint(
        kind=kind,
        ts_ms=ts,
        price=price,
        cvd=cvd,
        bar_start_ts_ms=ts - 1000,
        bar_end_ts_ms=ts,
    )


def _bar(ts: int, o: float, h: float, l: float, c: float) -> MicroBar:
    return MicroBar(
        symbol="BTCUSDT",
        tf_ms=1000,
        start_ts_ms=ts - 1000,
        end_ts_ms=ts,
        open=o,
        high=h,
        low=l,
        close=c,
        vol=1.0,
        delta_sum=0.0,
        cvd_close=0.0,
        vwap=c,
        tick_count=10,
    )


def test_eq_pool_clustering_by_bp():
    tr = EQPoolTracker(symbol="BTCUSDT", eq_tol_bp=10.0, eq_tol_atr_mult=0.0, eq_min_touches=2)
    # two highs within 10bp should cluster
    tr.on_swing(_sp("high", 1000, 100.00), atr=0.0)
    # 100.05 is +5bp away
    tr.on_swing(_sp("high", 2000, 100.05), atr=0.0)

    eqh = tr.pools(kind="EQH", only_mature=True)
    assert len(eqh) == 1
    assert eqh[0].touches == 2
    assert abs(eqh[0].level - 100.025) < 1e-6


def test_sweep_eqh_immediate_confirm():
    tr = EQPoolTracker(symbol="BTCUSDT", eq_tol_bp=10.0, eq_tol_atr_mult=0.0, eq_min_touches=2)
    tr.on_swing(_sp("high", 1000, 100.0), atr=0.0)
    tr.on_swing(_sp("high", 2000, 100.0), atr=0.0)
    pools = tr.pools(only_mature=True)

    sw = SweepDetector(confirm_bars=3, cooldown_ms=0, valid_ms=120000)
    # bar raids above level and closes back below => immediate EQH_SWEEP
    b = _bar(ts=3000, o=100.0, h=101.0, l=99.8, c=99.9)
    out = sw.update_bar(b, pools)
    assert len(out) == 1
    assert out[0].kind == "EQH_SWEEP"
    assert out[0].direction_bias == "SHORT"


def test_sweep_eql_pending_then_confirm():
    tr = EQPoolTracker(symbol="BTCUSDT", eq_tol_bp=10.0, eq_tol_atr_mult=0.0, eq_min_touches=2)
    tr.on_swing(_sp("low", 1000, 100.0), atr=0.0)
    tr.on_swing(_sp("low", 2000, 100.0), atr=0.0)
    pools = tr.pools(only_mature=True)

    sw = SweepDetector(confirm_bars=2, cooldown_ms=0, valid_ms=120000)

    # breach below, but close still below => pending
    b1 = _bar(ts=3000, o=100.0, h=100.2, l=99.0, c=99.5)
    out1 = sw.update_bar(b1, pools)
    assert out1 == []

    # next bar closes back above => confirm EQL_SWEEP
    b2 = _bar(ts=4000, o=99.6, h=100.5, l=99.4, c=100.2)
    out2 = sw.update_bar(b2, pools)
    assert len(out2) == 1
    assert out2[0].kind == "EQL_SWEEP"
    assert out2[0].direction_bias == "LONG"
