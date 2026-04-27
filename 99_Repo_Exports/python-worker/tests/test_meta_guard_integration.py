import json
import os
import unittest
from tempfile import NamedTemporaryFile
from core.of_confirm_engine import OFConfirmEngine

class TestMetaGuardIntegration(unittest.TestCase):
    def setUp(self):
        self.tmp = NamedTemporaryFile(delete=False, suffix=".json")
        self.tmp_path = self.tmp.name
        self.tmp.close()
        
        # Set environment variable before instantiating engine
        os.environ["META_FREEZE_FILE"] = self.tmp_path
        os.environ["META_FREEZE_FILE_TTL_SEC"] = "0"

    def tearDown(self):
        if os.path.exists(self.tmp_path):
            os.remove(self.tmp_path)
        if "META_FREEZE_FILE" in os.environ:
            del os.environ["META_FREEZE_FILE"]

    def test_engine_respects_freeze(self):
        # 1. Setup engine and freeze state
        with open(self.tmp_path, "w") as f:
            json.dump({"freeze": 1, "comment": "test_freeze"}, f)
            
        engine = OFConfirmEngine()
        
        # 2. Mock inputs
        # We only need enough to trigger the meta guard section in build()
        res, _ = engine.build(
            symbol="BTCUSDT",
            tf="1m",
            direction="BUY",
            tick_ts_ms=1600000000000,
            price=50000.0,
            delta_z=1.0,
            runtime=None,
            cfg={"meta_model_enable": "1", "meta_model_path": "any"},
            indicators={"sid": "test_sid"}
        )
        
        # 3. Verify evidence and decision
        evidence = res.evidence
        self.assertEqual(evidence["meta_guard_freeze"], 1)
        self.assertEqual(evidence["meta_veto"], 1)
        self.assertEqual(evidence["meta_reason"], "meta_guard_freeze")
        self.assertEqual(res.ok, 0)

    def test_engine_respects_caps(self):
        # 1. Setup engine and caps
        with open(self.tmp_path, "w") as f:
            json.dump({
                "freeze": 0, 
                "ab_share_cap": 0.0, # Kill challenger
                "enforce_share_cap": 0.0 # Kill enforce
            }, f)
            
        engine = OFConfirmEngine()
        
        # 2. Mock inputs
        res, _ = engine.build(
            symbol="BTCUSDT",
            tf="1m",
            direction="BUY",
            tick_ts_ms=1600000000000,
            price=50000.0,
            delta_z=1.0,
            runtime=None,
            cfg={
                "meta_model_enable": "1", 
                "meta_model_path": "any", 
                "meta_ab_challenger_share": "1.0", # Want 100% challenger
                "meta_enforce_share": "1.0" # Want 100% enforce
            },
            indicators={"sid": "test_sid"}
        )
        
        # 3. Verify caps applied in evidence
        evidence = res.evidence
        self.assertEqual(evidence["meta_guard_ab_cap"], 0.0)
        self.assertEqual(evidence["meta_guard_enforce_cap"], 0.0)
        
        # meta_ab_share should be capped at 0.0 from 1.0
        self.assertEqual(evidence["meta_ab_share"], 0.0)
        # meta_enforce_share should be capped at 0.0 from 1.0
        self.assertEqual(evidence["meta_enforce_share"], 0.0)

if __name__ == "__main__":
    unittest.main()
