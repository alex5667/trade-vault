
import unittest
import os
from core.instrument_config import OrderFlowConfig

class TestGPUConfig(unittest.TestCase):
    def test_gpu_offload_from_env(self):
        # Case 1: GPU_OFFLOAD_ENABLED=1
        os.environ["GPU_OFFLOAD_ENABLED"] = "1"
        cfg = OrderFlowConfig.from_env("BTCUSDT")
        self.assertTrue(cfg.gpu_offload_enabled)
        
        # Case 2: GPU_OFFLOAD_ENABLED=false
        os.environ["GPU_OFFLOAD_ENABLED"] = "false"
        cfg = OrderFlowConfig.from_env("BTCUSDT")
        self.assertFalse(cfg.gpu_offload_enabled)
        
        # Case 3: GPU_ENABLED=true (fallback)
        del os.environ["GPU_OFFLOAD_ENABLED"]
        os.environ["GPU_ENABLED"] = "true"
        cfg = OrderFlowConfig.from_env("BTCUSDT")
        self.assertTrue(cfg.gpu_offload_enabled)

        # Case 4: Default (False if neither set)
        del os.environ["GPU_ENABLED"]
        cfg = OrderFlowConfig.from_env("BTCUSDT")
        self.assertFalse(cfg.gpu_offload_enabled)

if __name__ == '__main__':
    unittest.main()
