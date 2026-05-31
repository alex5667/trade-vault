
import sys
import os
import unittest
import logging

# Add python-worker to path
sys.path.append(os.path.join(os.getcwd(), 'python-worker'))

from core.crypto_orderflow_detectors import DeltaSpikeDetector, classify_signed_qty

# Setup logging
logging.basicConfig(level=logging.DEBUG)

class TestDeltaSpikeDetector(unittest.TestCase):
    def test_classify_signed_qty(self):
        # Test BUY
        tick_buy = {"qty": 1.0, "side": "buy"}
        self.assertEqual(classify_signed_qty(tick_buy), 1.0)
        
        # Test SELL
        tick_sell = {"qty": 1.0, "side": "sell"}
        self.assertEqual(classify_signed_qty(tick_sell), -1.0)
        
        # Test Binance is_buyer_maker=True (SELL)
        tick_binance_sell = {"qty": 1.0, "is_buyer_maker": True}
        self.assertEqual(classify_signed_qty(tick_binance_sell), -1.0)
        
        # Test Binance is_buyer_maker=False (BUY)
        tick_binance_buy = {"qty": 1.0, "is_buyer_maker": False}
        self.assertEqual(classify_signed_qty(tick_binance_buy), 1.0)

    def test_detector_trigger(self):
        detector = DeltaSpikeDetector(window=20, z_threshold=2.0, min_abs_volume=0.0)
        
        # 1. Fill buffer with noise (mean=0, std_dev small)
        for i in range(15):
             # Alternating +1, -1 => mean ~ 0
            val = 1.0 if i % 2 == 0 else -1.0
            tick = {"qty": 1.0, "side": "buy" if val > 0 else "sell"}
            event = detector.push(tick)
            # Should be None initially (buffer < 10) or small z-score
            if event:
                print(f"Triggered early at {i}: {event}")
        
        # 2. Inject a spike
        # Current buffer has small variance. A large value should trigger Z-score.
        spike_tick = {"qty": 10.0, "side": "buy"} # Delta = +10.0
        event = detector.push(spike_tick)
        
        print(f"Spike event: {event}")
        self.assertIsNotNone(event, "Detector should trigger on spike")
        self.assertEqual(event['type'], 'delta_spike')
        self.assertGreater(event['z'], 2.0)
        self.assertEqual(event['delta'], 10.0)

if __name__ == '__main__':
    unittest.main()
