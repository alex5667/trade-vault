
import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Add parent dir to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from l2_microstructure_engine import L2MicrostructureEngine, WallObs
from gpu.l2_processor import L2GPUProcessor
from contexts import SimpleL2Snapshot, L2Level, BucketState

class MockConfig:
    def __init__(self):
        self.wall_hist_m = 5
        self.obi_samples_maxlen = 100
        self.obi20_samples_maxlen = 100
        self.obi_band_mode = "bps"
        self.obi_band_5_bps = 5
        self.obi_band_20_bps = 20
        self.gpu_offload_enabled = True # Enable GPU for test

class TestL2GPUIntegration(unittest.TestCase):
    def setUp(self):
        self.config = MockConfig()
        self.gpu_processor = L2GPUProcessor("TEST", batch_size=10)
        self.engine = L2MicrostructureEngine(self.config, gpu_processor=self.gpu_processor)

    def test_gpu_offload_called(self):
        # Mock the process_l2_snapshot method
        self.gpu_processor.process_l2_snapshot = MagicMock(return_value={
            'microprice': 100.5,
            'imbalance': 0.5
        })

        # Create a snapshot
        bids = [L2Level(price=99.0, size=1.0), L2Level(price=98.0, size=2.0)]
        asks = [L2Level(price=101.0, size=1.0), L2Level(price=102.0, size=2.0)]
        snap = SimpleL2Snapshot(bids=bids, asks=asks)
        
        st = BucketState.empty()
        
        # Run update
        self.engine.update(snap, 123456789, st)
        
        # Verify GPU processor was called
        self.gpu_processor.process_l2_snapshot.assert_called_once()
        
        # Verify we passed correct data structure (list of tuples)
        args, _ = self.gpu_processor.process_l2_snapshot.call_args
        self.assertEqual(len(args[0]), 2) # 2 bids
        self.assertEqual(len(args[1]), 2) # 2 asks
        self.assertEqual(args[0][0], (99.0, 1.0))

    def test_gpu_processor_fallback(self):
        # Use real L2GPUProcessor (which falls back to CPU if no CuPy)
        # We can't easily force it to use GPU if no GPU, but we can check it returns valid dict
        
        bids = [L2Level(price=100.0, size=1.0)]
        asks = [L2Level(price=101.0, size=1.0)]
        snap = SimpleL2Snapshot(bids=bids, asks=asks)
        st = BucketState.empty()
        
        # Spy on the method
        with patch.object(self.gpu_processor, 'process_l2_snapshot', wraps=self.gpu_processor.process_l2_snapshot) as mock_method:
            self.engine.update(snap, 123456789, st)
            mock_method.assert_called_once()
            
            # Check result of the real method call (indirectly via what it returns, 
            # but since engine doesn't use it yet, we just ensure no exception)

if __name__ == '__main__':
    unittest.main()
