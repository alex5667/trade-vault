import os
import json
import time
import unittest
from unittest.mock import patch, MagicMock
from prometheus_client import REGISTRY

# Adjust path to import the module under test
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from orderflow_services import conf_cal_live_status_exporter_v1

class TestConfCalLiveStatusExporter(unittest.TestCase):
    def setUp(self):
        self.test_dir = "/tmp/test_conf_cal_exporter"
        os.makedirs(self.test_dir, exist_ok=True)
        self.status_path = os.path.join(self.test_dir, "status.json")
        self.proof_path = os.path.join(self.test_dir, "proof.json")
        self.reports_dir = self.test_dir
        
        # Reset counters/gauges if possible, or just rely on them being global
        # Prometheus client keeps them global. We can verify values.

    def tearDown(self):
        if os.path.exists(self.status_path):
            os.remove(self.status_path)
        if os.path.exists(self.proof_path):
            os.remove(self.proof_path)
        if os.path.exists(self.test_dir):
            os.rmdir(self.test_dir)

    def test_proof_read_missing_file(self):
        """Test that missing proof file is handled gracefully."""
        exporter = conf_cal_live_status_exporter_v1.Exporter(self.reports_dir)
        exporter.proof_path = self.proof_path
        
        # Ensure file does not exist
        if os.path.exists(self.proof_path):
            os.remove(self.proof_path)
            
        exporter._step_proof(int(time.time() * 1000))
        
        # Check metric value
        self.assertEqual(conf_cal_live_status_exporter_v1.conf_cal_proof_read_ok.collect()[0].samples[0].value, 0.0)

    def test_proof_read_valid_file(self):
        """Test reading a valid proof file."""
        exporter = conf_cal_live_status_exporter_v1.Exporter(self.reports_dir)
        exporter.proof_path = self.proof_path
        
        now_sec = int(time.time())
        proof_data = {
            "ts": now_sec - 10,
            "evidence_ts": now_sec - 20,
            "valid": True,
            "canary_share": 0.5,
            "source": {"status_age_sec": 5.5}
        }
        
        with open(self.proof_path, "w") as f:
            json.dump(proof_data, f)
            
        exporter._step_proof(now_sec * 1000)
        
        self.assertEqual(conf_cal_live_status_exporter_v1.conf_cal_proof_read_ok.collect()[0].samples[0].value, 1.0)
        self.assertEqual(conf_cal_live_status_exporter_v1.conf_cal_proof_valid.collect()[0].samples[0].value, 1.0)
        self.assertEqual(conf_cal_live_status_exporter_v1.conf_cal_proof_canary_share.collect()[0].samples[0].value, 0.5)
        self.assertEqual(conf_cal_live_status_exporter_v1.conf_cal_proof_age_sec.collect()[0].samples[0].value, 10.0)
        self.assertEqual(conf_cal_live_status_exporter_v1.conf_cal_proof_evidence_age_sec.collect()[0].samples[0].value, 20.0)
        self.assertEqual(conf_cal_live_status_exporter_v1.conf_cal_proof_status_age_sec.collect()[0].samples[0].value, 5.5)

    def test_proof_read_invalid_json(self):
        """Test reading an invalid proof file."""
        exporter = conf_cal_live_status_exporter_v1.Exporter(self.reports_dir)
        exporter.proof_path = self.proof_path
        
        with open(self.proof_path, "w") as f:
            f.write("invalid json")
            
        initial_errors = conf_cal_live_status_exporter_v1.conf_cal_proof_read_errors_total.collect()[0].samples[0].value
        
        exporter._step_proof(int(time.time() * 1000))
        
        new_errors = conf_cal_live_status_exporter_v1.conf_cal_proof_read_errors_total.collect()[0].samples[0].value
        self.assertEqual(new_errors, initial_errors + 1) # Json load failure in _load_json returns None, which sets read_ok=0 and increments error.
        
        # Wait, looking at logic:
        # proof = _load_json(self.proof_path)
        # if not isinstance(proof, dict): ... errors.inc()
        
        # _load_json handles exception and returns None.
        # So it should increment.
        
        # Let's re-verify _load_json behavior in source.
        # It returns None on exception.
        # Logic: if not isinstance(proof, dict): ... inc()
        # So yes, it should increment.
        
        # Correction: The previous assertion was +0, but it should be +1.
        
    def test_proof_read_malformed_fields(self):
         """Test reading a proof file with missing/malformed fields."""
         exporter = conf_cal_live_status_exporter_v1.Exporter(self.reports_dir)
         exporter.proof_path = self.proof_path
         
         proof_data = {
             "ts": "not-an-int", # Should handle gracefully
             "valid": "true", # Should be bool but bool("true") is True
         }
         
         with open(self.proof_path, "w") as f:
             json.dump(proof_data, f)
             
         exporter._step_proof(int(time.time() * 1000))
         
         self.assertEqual(conf_cal_live_status_exporter_v1.conf_cal_proof_read_ok.collect()[0].samples[0].value, 1.0)
         # ts defaults to 0
         self.assertEqual(conf_cal_live_status_exporter_v1.conf_cal_proof_ts_sec.collect()[0].samples[0].value, 0.0)

if __name__ == "__main__":
    unittest.main()
