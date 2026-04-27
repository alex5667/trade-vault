"""
Unit tests for Weak Progress Detector (History).

Tests:
- History buffer management (maxlen enforcement)
- Recent window counting (last N bars)
- Weak fraction calculation
- Sample creation with correct metrics
"""

import unittest
from types import SimpleNamespace
from core.weak_progress_detector import WeakProgressDetector, WeakProgressSample


class TestWeakProgressDetector(unittest.TestCase):
    """Test weak progress history tracking logic."""
    
    def test_history_buffer_maxlen(self):
        """Test history buffer respects maxlen."""
        detector = WeakProgressDetector(maxlen=10, recent_window=3)
        
        # Add more samples than maxlen
        for i in range(20):
            bar = SimpleNamespace(
                open=100.0, high=100.1, low=100.0, close=100.05,
                delta_sum=10.0, end_ts_ms=1000 + i, fp_bucket_px=0.01
            )
            detector.update(bar, atr=1.0, delta_abs=10.0)
        
        # Buffer should only contain last 10 samples
        self.assertEqual(len(detector._samples), 10)
    
    def test_recent_weak_count(self):
        """Test recent window weak bar counting."""
        detector = WeakProgressDetector(
            maxlen=50,
            recent_window=5,
            range_max_atr=0.30,
            body_max_atr=0.35,
            eff_max=0.02,
        )
        
        # Add 10 bars: first 5 weak, last 5 strong
        for i in range(5):
            # Weak bar (tight range, small body)
            bar = SimpleNamespace(
                open=100.0, high=100.05, low=100.0, close=100.02,
                delta_sum=100.0, end_ts_ms=1000 + i, fp_bucket_px=0.01
            )
            detector.update(bar, atr=1.0, delta_abs=100.0)
        
        for i in range(5, 10):
            # Strong bar (wide range, large body)
            bar = SimpleNamespace(
                open=100.0, high=101.0, low=100.0, close=100.8,
                delta_sum=100.0, end_ts_ms=1000 + i, fp_bucket_px=0.01
            )
            detector.update(bar, atr=1.0, delta_abs=100.0)
        
        # Recent 5 should be all strong
        recent_cnt = detector.recent_weak_count()
        self.assertEqual(recent_cnt, 0)
    
    def test_recent_weak_fraction(self):
        """Test recent weak fraction calculation."""
        detector = WeakProgressDetector(
            maxlen=50,
            recent_window=10,
            range_max_atr=0.30,
            body_max_atr=0.35,
            eff_max=0.02,
        )
        
        # Add 10 bars: 3 weak, 7 strong
        for i in range(3):
            # Weak bar
            bar = SimpleNamespace(
                open=100.0, high=100.05, low=100.0, close=100.02,
                delta_sum=100.0, end_ts_ms=1000 + i, fp_bucket_px=0.01
            )
            detector.update(bar, atr=1.0, delta_abs=100.0)
        
        for i in range(3, 10):
            # Strong bar
            bar = SimpleNamespace(
                open=100.0, high=101.0, low=100.0, close=100.8,
                delta_sum=100.0, end_ts_ms=1000 + i, fp_bucket_px=0.01
            )
            detector.update(bar, atr=1.0, delta_abs=100.0)
        
        # Fraction should be 3/10 = 0.3
        frac = detector.recent_weak_frac()
        self.assertAlmostEqual(frac, 0.3, places=2)
    
    def test_sample_metrics(self):
        """Test sample creation with correct metrics."""
        detector = WeakProgressDetector(
            maxlen=50,
            recent_window=5,
            range_max_atr=0.30,
            body_max_atr=0.35,
            eff_max=0.02,
        )
        
        # Create bar with known metrics
        bar = SimpleNamespace(
            open=100.0,
            high=100.2,  # range = 0.2
            low=100.0,
            close=100.1,  # body = 0.1
            delta_sum=50.0,
            end_ts_ms=1000,
            fp_bucket_px=0.01
        )
        
        atr = 1.0
        sample = detector.update(bar, atr=atr, delta_abs=50.0)
        
        # Check metrics
        self.assertAlmostEqual(sample.range_atr, 0.2, places=2)
        self.assertAlmostEqual(sample.body_atr, 0.1, places=2)
        # eff = (body/tick_px) / |delta_sum| = (0.1/0.01) / 50 = 10/50 = 0.2
        self.assertAlmostEqual(sample.eff, 0.2, places=2)
        
        # Should be weak (range < 0.30, body < 0.35)
        self.assertTrue(sample.weak)
    
    def test_empty_buffer(self):
        """Test detector with no data."""
        detector = WeakProgressDetector()
        
        # Recent count/frac should be 0
        self.assertEqual(detector.recent_weak_count(), 0)
        self.assertEqual(detector.recent_weak_frac(), 0.0)
    
    def test_weak_efficiency_criterion(self):
        """Test weak progress detection via efficiency."""
        detector = WeakProgressDetector(
            maxlen=50,
            recent_window=5,
            range_max_atr=0.10,  # Very tight (won't trigger)
            body_max_atr=0.10,   # Very tight (won't trigger)
            eff_max=0.05,        # Efficiency threshold
        )
        
        # Bar with low efficiency (high delta, small move)
        bar = SimpleNamespace(
            open=100.0,
            high=100.5,  # range = 0.5 (not weak by range)
            low=100.0,
            close=100.02,  # body = 0.02 (not weak by body with tight threshold)
            delta_sum=1000.0,  # High delta
            end_ts_ms=1000,
            fp_bucket_px=0.01
        )
        
        atr = 1.0
        sample = detector.update(bar, atr=atr, delta_abs=1000.0)
        
        # eff = (0.02/0.01) / 1000 = 2/1000 = 0.002 < 0.05
        self.assertLess(sample.eff, 0.05)
        self.assertTrue(sample.weak)  # Should be weak via efficiency


    def test_push_compat(self):
        """Test expert compatibility with manual push."""
        detector = WeakProgressDetector(
            maxlen=50,
            recent_window=5,
            range_max_atr=0.30,
            body_max_atr=0.35,
            eff_max=0.02,
        )
        
        # Helper class from expert snippet
        class WP:
            def __init__(self, weak_any):
                self.weak_any = weak_any
                self.range_atr = 0.2
                self.body_atr = 0.2
                self.eff = 0.01

        ts = 0
        for w in [1,0,1,1,0,1]:
            detector.push(WP(bool(w)), ts_ms=ts)
            ts += 1000

        # Last 5: 0,1,1,0,1 => 3 weak
        self.assertEqual(detector.recent_weak_count(), 3)


if __name__ == '__main__':
    unittest.main()
