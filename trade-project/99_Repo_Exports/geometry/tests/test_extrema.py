"""
Unit tests for geometry/extrema.py — LocalExtremaService
"""
import pytest
from geometry.extrema import LocalExtremaConfig, LocalExtremaService, _LARGE_BAR_COUNT


T0 = 1_700_000_000_000  # arbitrary base timestamp ms


def _feed_prices(svc: LocalExtremaService, prices: list[float], base_ts: int = T0) -> list:
    """Feed a list of prices and collect non-None events."""
    events = []
    for i, p in enumerate(prices):
        ev = svc.feed(base_ts + i * 60_000, p)
        if ev is not None:
            events.append(ev)
    return events


class TestLocalExtremaServiceBasic:

    def test_window_not_full_returns_none(self) -> None:
        """No event while the window is not yet filled."""
        cfg = LocalExtremaConfig(lookback_left=2, lookback_right=2)
        svc = LocalExtremaService(cfg)
        # window_size = 5; feed 4 bars — should all be None
        for i in range(4):
            assert svc.feed(T0 + i * 1000, 100.0 + i) is None

    def test_local_max_detected(self) -> None:
        """Clear local maximum is detected after window fills."""
        cfg = LocalExtremaConfig(lookback_left=2, lookback_right=2,
                                 min_bars_between_extremes=1, min_move_bps=0.0)
        svc = LocalExtremaService(cfg)
        # Pattern: 1, 2, 3, 2, 1  → 3 is local max
        prices = [1.0, 2.0, 3.0, 2.0, 1.0]
        events = _feed_prices(svc, prices)
        assert len(events) == 1
        ev = events[0]
        assert ev.kind == "high"
        assert ev.price == pytest.approx(3.0)

    def test_local_min_detected(self) -> None:
        """Clear local minimum is detected after window fills."""
        cfg = LocalExtremaConfig(lookback_left=2, lookback_right=2,
                                 min_bars_between_extremes=1, min_move_bps=0.0)
        svc = LocalExtremaService(cfg)
        # Pattern: 5, 4, 3, 4, 5  → 3 is local min
        prices = [5.0, 4.0, 3.0, 4.0, 5.0]
        events = _feed_prices(svc, prices)
        assert len(events) == 1
        ev = events[0]
        assert ev.kind == "low"
        assert ev.price == pytest.approx(3.0)

    def test_flat_window_no_extremum(self) -> None:
        """Flat prices produce no event (no strict inequality)."""
        cfg = LocalExtremaConfig(lookback_left=2, lookback_right=2,
                                 min_bars_between_extremes=1, min_move_bps=0.0)
        svc = LocalExtremaService(cfg)
        events = _feed_prices(svc, [100.0] * 10)
        assert events == []

    def test_invalid_price_ignored(self) -> None:
        """Prices ≤ 0 are skipped; bars_total and bars_since still increment."""
        cfg = LocalExtremaConfig(lookback_left=2, lookback_right=2,
                                 min_bars_between_extremes=1, min_move_bps=0.0)
        svc = LocalExtremaService(cfg)
        assert svc.feed(T0, 0.0) is None
        assert svc.feed(T0, -1.0) is None
        assert svc._bars_total == 2


class TestLocalExtremaFilters:

    def test_min_bars_between_extremes_filter(self) -> None:
        """Two extrema too close together: second is suppressed."""
        cfg = LocalExtremaConfig(lookback_left=2, lookback_right=2,
                                 min_bars_between_extremes=10, min_move_bps=0.0)
        svc = LocalExtremaService(cfg)
        # First max at bar 2 (centre of 0..4), then another max right after
        prices = [1.0, 2.0, 3.0, 2.0, 1.0,   # first max confirmed at bar 4
                  0.5, 4.0, 0.5, 0.0, 0.0]    # second max candidate — too close
        events = _feed_prices(svc, prices)
        # Only the first max should fire
        assert len(events) == 1
        assert events[0].kind == "high"

    def test_min_move_bps_filter(self) -> None:
        """Move too small (< min_move_bps) → event suppressed."""
        cfg = LocalExtremaConfig(lookback_left=2, lookback_right=2,
                                 min_bars_between_extremes=1,
                                 min_move_bps=500.0)  # 5% move required
        svc = LocalExtremaService(cfg)
        # First extremum at price 50_000.0 (no previous → always passes)
        prices1 = [49_000.0, 49_500.0, 50_000.0, 49_500.0, 49_000.0]
        events1 = _feed_prices(svc, prices1)
        assert len(events1) == 1
        assert events1[0].price == pytest.approx(50_000.0)

        # Second extremum at 50_010 → move from 50_000 = 2 bps << 500 bps → suppressed
        prices2 = [49_998.0, 50_005.0, 50_010.0, 50_005.0, 49_998.0]
        events2 = _feed_prices(svc, prices2)
        assert events2 == []

        # Third extremum at 52_600 → move from 50_000 = 520 bps >> 500 bps → fires
        prices3 = [51_000.0, 52_000.0, 52_600.0, 52_000.0, 51_000.0]
        events3 = _feed_prices(svc, prices3)
        assert len(events3) == 1
        assert events3[0].move_from_prev_bps is not None
        assert events3[0].move_from_prev_bps > 500.0


class TestLocalExtremaReset:

    def test_reset_clears_state(self) -> None:
        cfg = LocalExtremaConfig(lookback_left=2, lookback_right=2,
                                 min_bars_between_extremes=1, min_move_bps=0.0)
        svc = LocalExtremaService(cfg)
        _feed_prices(svc, [1.0, 2.0, 3.0, 2.0, 1.0])
        svc.reset()
        assert len(svc._window) == 0
        assert svc._last_extreme_price is None
        assert svc._bars_total == 0
        assert svc._bars_since_last_extreme == _LARGE_BAR_COUNT

    def test_first_event_bars_since_prev_is_none(self) -> None:
        """First detected extremum has bars_since_prev=None (no previous)."""
        cfg = LocalExtremaConfig(lookback_left=2, lookback_right=2,
                                 min_bars_between_extremes=1, min_move_bps=0.0)
        svc = LocalExtremaService(cfg)
        events = _feed_prices(svc, [1.0, 2.0, 3.0, 2.0, 1.0])
        assert len(events) == 1
        assert events[0].bars_since_prev is None


class TestLocalExtremaMoveTracking:

    def test_move_bps_calculated_correctly(self) -> None:
        """move_from_prev_bps is correct relative move in basis points."""
        cfg = LocalExtremaConfig(lookback_left=2, lookback_right=2,
                                 min_bars_between_extremes=1, min_move_bps=0.0)
        svc = LocalExtremaService(cfg)
        # First extremum at price 100.0
        _feed_prices(svc, [98.0, 99.0, 100.0, 99.0, 98.0])
        # Second extremum at price 90.0 → move = 10/100 * 10000 = 1000 bps
        events2 = _feed_prices(svc, [92.0, 91.0, 90.0, 91.0, 92.0])
        assert len(events2) == 1
        assert events2[0].move_from_prev_bps == pytest.approx(1000.0, rel=1e-3)
