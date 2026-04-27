
import unittest
import os
import json
import tempfile
import sys
from unittest.mock import MagicMock, patch

# Mock missing dependency
sys.modules["services.cancellation_spike_gate"] = MagicMock()
sys.modules["services.ml_confirm_gate"] = MagicMock()
from core.of_confirm_engine import OFConfirmEngine
from core.meta_model_lr import MetaModelLR
from core.meta_features_v1 import META_FEATURE_SCHEMA_VERSION

class TestOFConfirmEngineMetaV1(unittest.TestCase):
    def setUp(self):
        self.engine = OFConfirmEngine()
        # Mock runtime and indicators
        self.runtime = MagicMock()
        self.indicators = {"sid": "test_sid"}
        self.cfg = {"meta_model_enable": 1, "meta_model_mode": "ENFORCE", "meta_p_min": 0.5, "meta_model_reload_sec": 0}
        
    def test_meta_features_integration_and_schema_enforce(self):
        # Create a temporary model file with correct schema version
        model_data = {
            "features": ["base_score", "obi"],
            "intercept": -1.0,
            "coef": [0.5, 0.5],
            "threshold": 0.5,
            "schema_version": META_FEATURE_SCHEMA_VERSION
        }
        
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            json.dump(model_data, f)
            model_path = f.name
            
        try:
            self.cfg["meta_model_path"] = model_path
            
            # Run build
            # We need to mock compute_obi_flags and others to return valid values or just mock them out
            with patch("core.of_confirm_engine.compute_obi_flags", return_value=(1, 1, 1.0, 0.5)), \
                 patch("core.of_confirm_engine.compute_iceberg_flags", return_value=(1, 0, 0, 0.0)), \
                 patch("core.of_confirm_engine.compute_ofi_flags", return_value=(1, 1.0, 1.0, 1, 1.0, 1.0)), \
                 patch("core.of_confirm_engine.compute_reclaim_recent", return_value=(0, 0)), \
                 patch("core.of_confirm_engine.compute_sweep_recent", return_value=False), \
                 patch("core.of_confirm_engine.compute_absorption_flags", return_value=(0, 0.0)), \
                 patch("core.of_confirm_engine.compute_absorption_level_score", return_value=(0, 0.0, "neutral", 0)), \
                 patch("core.of_confirm_engine.compute_fp_edge_absorb", return_value=(0, 0.0, 0, "neutral")):
                 
                 of_confirm, dec = self.engine.build(
                     symbol="BTCUSDT",
                     tf="1m",
                     direction="long",
                     tick_ts_ms=1000,
                     price=100.0,
                     delta_z=1.0,
                     runtime=self.runtime,
                     cfg=self.cfg,
                     indicators=self.indicators
                 )
                 
                 # Check evidence for meta fields
                 ev = of_confirm.evidence
                 self.assertEqual(ev["meta_feature_schema"], META_FEATURE_SCHEMA_VERSION)
                 self.assertEqual(ev["meta_schema_mismatch"], 0)
                 self.assertEqual(ev["meta_mode"], "ENFORCE")
                 
                 # Now test mismatch
                 # Create model with OLD schema or NO schema
                 model_data["schema_version"] = "old_v0"
                 with open(model_path, "w") as f:
                     json.dump(model_data, f)
                     
                 # Force reload by updating mtime and last check
                 self.engine._meta_model_last_check_ms = 0
                 
                 of_confirm, dec = self.engine.build(
                     symbol="BTCUSDT",
                     tf="1m",
                     direction="long",
                     tick_ts_ms=2000, # different time to trigger
                     price=100.0,
                     delta_z=1.0,
                     runtime=self.runtime,
                     cfg=self.cfg,
                     indicators=self.indicators
                 )
                 
                 ev = of_confirm.evidence
                 self.assertEqual(ev["meta_schema_mismatch"], 1)
                 self.assertEqual(ev["meta_mode"], "SHADOW") # Should downgrade
                 self.assertEqual(ev["meta_schema_reason"], "meta_schema_mismatch")

        finally:
            os.remove(model_path)

if __name__ == "__main__":
    unittest.main()
