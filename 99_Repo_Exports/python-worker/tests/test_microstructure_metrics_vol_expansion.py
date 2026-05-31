"""Unit tests for vol_expansion_score / vol_compression_score in microstructure_metrics_v2."""
import pytest
from core.microstructure_metrics_v2 import OHLCBar, compute_all, _gk_vol_subset, _BAR_FAST_WINDOW


def _bar(o: float, h: float, l: float, c: float) -> OHLCBar:
    return OHLCBar(o=o, h=h, l=l, c=c, volume=100.0)


def _make_bars(n: int, h_spread: float, base: float = 100.0) -> list[OHLCBar]:
    return [_bar(base, base + h_spread, base - h_spread, base + 0.01) for _ in range(n)]


def test_vol_expansion_score_positive_when_fast_vol_exceeds_slow() -> None:
    slow = _make_bars(8, h_spread=0.5)
    fast = _make_bars(5, h_spread=3.0)
    result = compute_all(
        prices=[b.c for b in slow + fast],
        signed_vols=[1.0] * 13,
        bars=slow + fast,
    )
    assert result.get("vol_expansion_score", 0.0) > 0.0, "vol expansion should be > 0"
    assert result.get("vol_compression_score", 1.0) == 0.0, "compression should be 0 when expanding"
    assert result.get("vol_ratio_fast_slow", 0.0) > 1.0


def test_vol_compression_score_positive_when_fast_vol_below_slow() -> None:
    slow = _make_bars(8, h_spread=3.0)
    fast = _make_bars(5, h_spread=0.3)
    result = compute_all(
        prices=[b.c for b in slow + fast],
        signed_vols=[1.0] * 13,
        bars=slow + fast,
    )
    assert result.get("vol_compression_score", 0.0) > 0.0, "compression should be > 0"
    assert result.get("vol_expansion_score", 1.0) == 0.0, "expansion should be 0 when compressing"
    assert result.get("vol_ratio_fast_slow", 1.0) < 1.0


def test_vol_expansion_absent_with_insufficient_bars() -> None:
    bars = _make_bars(_BAR_FAST_WINDOW, h_spread=1.0)
    result = compute_all(
        prices=[b.c for b in bars],
        signed_vols=[1.0] * len(bars),
        bars=bars,
    )
    assert "vol_expansion_score" not in result, "should not appear with < BAR_FAST_WINDOW+1 bars"


def test_vol_expansion_absent_with_no_bars() -> None:
    result = compute_all(
        prices=[100.0, 101.0, 100.5],
        signed_vols=[1.0, -1.0, 1.0],
        bars=[],
    )
    assert result.get("vol_expansion_score") is None


def test_gk_vol_subset_uses_trailing_n_bars() -> None:
    slow = _make_bars(10, h_spread=0.1)
    fast = _make_bars(5, h_spread=5.0)
    all_bars = slow + fast
    v_fast = _gk_vol_subset(all_bars, _BAR_FAST_WINDOW)
    v_all = _gk_vol_subset(all_bars, len(all_bars))
    assert v_fast > v_all, "trailing fast window should have higher vol than full slow window"
