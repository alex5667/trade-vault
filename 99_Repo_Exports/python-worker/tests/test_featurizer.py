"""
Unit tests for signals/featurizer.py
"""

import pytest
from signals.featurizer import (
    Rolling,
    classify_delta,
    obi_from_book,
    make_features,
    compute_rolling_metrics
)


class TestRolling:
    """Test Rolling window class."""
    
    def test_empty(self):
        """Test empty rolling window."""
        r = Rolling(size=10)
        assert len(r) == 0
        assert r.mean() is None
        assert r.std() is None
    
    def test_single_value(self):
        """Test single value."""
        r = Rolling(size=10)
        r.add(5.0)
        assert len(r) == 1
        assert r.mean() == 5.0
        assert r.std() is None  # Need 2+ values for std
    
    def test_mean_std(self):
        """Test mean and std computation."""
        r = Rolling(size=10)
        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        for v in values:
            r.add(v)
        
        assert len(r) == 5
        assert r.mean() == pytest.approx(3.0)
        assert r.std() == pytest.approx(1.4142, rel=0.01)
    
    def test_window_limit(self):
        """Test that window respects size limit."""
        r = Rolling(size=3)
        for i in range(10):
            r.add(float(i))
        
        assert len(r) == 3
        assert r.buf == [7.0, 8.0, 9.0]
    
    def test_rolling_stats(self):
        """Test rolling statistics update correctly."""
        r = Rolling(size=3)
        r.add(1.0)
        r.add(2.0)
        r.add(3.0)
        
        m1 = r.mean()
        
        r.add(4.0)  # Window now [2, 3, 4]
        m2 = r.mean()
        
        assert m1 == pytest.approx(2.0)
        assert m2 == pytest.approx(3.0)


class TestClassifyDelta:
    """Test delta classification."""
    
    def test_buy_aggressive(self):
        """Test aggressive buy (last >= ask)."""
        tick = {"bid": 100.0, "ask": 101.0, "last": 101.0, "volume": 1.5}
        delta = classify_delta(tick)
        assert delta == 1.5
    
    def test_sell_aggressive(self):
        """Test aggressive sell (last <= bid)."""
        tick = {"bid": 100.0, "ask": 101.0, "last": 100.0, "volume": 2.0}
        delta = classify_delta(tick)
        assert delta == -2.0
    
    def test_bid_ask_fallback(self):
        """Test fallback to bid-ask comparison."""
        tick = {"bid": 100.0, "ask": 101.0, "last": 100.5, "volume": 1.0}
        delta = classify_delta(tick)
        # ask > bid -> positive
        assert delta == 1.0
    
    def test_no_volume(self):
        """Test tick with no volume."""
        tick = {"bid": 100.0, "ask": 101.0, "last": 100.5}
        delta = classify_delta(tick)
        assert delta == 0.0


class TestOBIFromBook:
    """Test OBI calculation from order book."""
    
    def test_balanced_book(self):
        """Test balanced book (OBI ~ 0)."""
        book = {
            "bids": [[100.0, 10.0], [99.0, 10.0]],
            "asks": [[101.0, 10.0], [102.0, 10.0]]
        }
        obi = obi_from_book(book, depth=2)
        assert obi == pytest.approx(0.0)
    
    def test_bid_heavy(self):
        """Test bid-heavy book (OBI > 0)."""
        book = {
            "bids": [[100.0, 20.0], [99.0, 20.0]],
            "asks": [[101.0, 10.0], [102.0, 10.0]]
        }
        obi = obi_from_book(book, depth=2)
        assert obi > 0.0
        # (40 - 20) / (40 + 20) = 0.333...
        assert obi == pytest.approx(0.333, rel=0.01)
    
    def test_ask_heavy(self):
        """Test ask-heavy book (OBI < 0)."""
        book = {
            "bids": [[100.0, 10.0], [99.0, 10.0]],
            "asks": [[101.0, 30.0], [102.0, 30.0]]
        }
        obi = obi_from_book(book, depth=2)
        assert obi < 0.0
        # (20 - 60) / (20 + 60) = -0.5
        assert obi == pytest.approx(-0.5)
    
    def test_empty_book(self):
        """Test empty book."""
        book = {"bids": [], "asks": []}
        obi = obi_from_book(book, depth=5)
        assert obi == 0.0
    
    def test_none_book(self):
        """Test None book."""
        obi = obi_from_book(None, depth=5)
        assert obi is None
    
    def test_depth_limit(self):
        """Test depth limiting."""
        book = {
            "bids": [[100.0, 10.0], [99.0, 10.0], [98.0, 10.0]],
            "asks": [[101.0, 5.0], [102.0, 5.0], [103.0, 5.0]]
        }
        # depth=2: (20 - 10) / (20 + 10) = 0.333
        obi = obi_from_book(book, depth=2)
        assert obi == pytest.approx(0.333, rel=0.01)


class TestMakeFeatures:
    """Test feature extraction."""
    
    def test_basic_features(self):
        """Test basic feature extraction."""
        tick = {
            "ts": 1234567890000,
            "bid": 100.0,
            "ask": 101.0,
            "last": 100.5,
            "volume": 1.5
        }
        
        rdelta = Rolling(size=10)
        features = make_features(tick, None, rdelta)
        
        assert features["ts"] == 1234567890000
        assert features["mid"] == pytest.approx(100.5)
        assert features["spread"] == pytest.approx(1.0)
        assert features["delta"] == 1.5  # aggressive buy
        assert features["obi"] is None  # no book
    
    def test_with_book(self):
        """Test feature extraction with order book."""
        tick = {
            "ts": 1234567890000,
            "bid": 100.0,
            "ask": 101.0,
            "last": 100.5,
            "volume": 1.0
        }
        
        book = {
            "bids": [[100.0, 20.0]],
            "asks": [[101.0, 10.0]]
        }
        
        rdelta = Rolling(size=10)
        features = make_features(tick, book, rdelta)
        
        assert features["obi"] is not None
        assert features["obi"] == pytest.approx(0.333, rel=0.01)
    
    def test_zscore(self):
        """Test z-score computation."""
        rdelta = Rolling(size=10)
        
        # Add some values to build statistics
        for i in range(5):
            tick = {"bid": 100.0, "ask": 101.0, "last": 100.5, "volume": 1.0, "ts": i}
            make_features(tick, None, rdelta)
        
        # Add outlier
        tick = {"bid": 100.0, "ask": 101.0, "last": 101.0, "volume": 10.0, "ts": 100}
        features = make_features(tick, None, rdelta)
        
        # Z-score should be significantly positive
        assert features["delta_z"] > 2.0


class TestComputeRollingMetrics:
    """Test rolling metrics computation."""
    
    def test_empty_list(self):
        """Test empty list."""
        m, s = compute_rolling_metrics([])
        assert m == 0.0
        assert s == 1.0
    
    def test_single_value(self):
        """Test single value."""
        m, s = compute_rolling_metrics([5.0])
        assert m == 0.0
        assert s == 1.0
    
    def test_normal_case(self):
        """Test normal case."""
        deltas = [1.0, 2.0, 3.0, 4.0, 5.0]
        m, s = compute_rolling_metrics(deltas)
        assert m == pytest.approx(3.0)
        assert s == pytest.approx(1.4142, rel=0.01)
    
    def test_window_limiting(self):
        """Test window limiting."""
        deltas = list(range(1, 201))  # 200 values
        m, s = compute_rolling_metrics(deltas, window=10)
        
        # Should only use last 10 values
        # mean of [191, 192, ..., 200] = 195.5
        assert m == pytest.approx(195.5)

