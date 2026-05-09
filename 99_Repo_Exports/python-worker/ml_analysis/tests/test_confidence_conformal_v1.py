
import unittest

# Ensure we can import services
try:
    from services.orderflow import confidence_conformal_v1 as cc
except ImportError:
    # If running from ml_analysis/tests, try adjusting path
# [AUTOGRAVITY CLEANUP]     sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
    from services.orderflow import confidence_conformal_v1 as cc

class TestConfidenceConformal(unittest.TestCase):
    def test_predict_set_basic(self):
        cm = cc.ConformalModel(
            schema_version="conf_conformal_v1",
            alpha=0.10,
            global_qhat=0.20,   # => p<=0.2 include 0, p>=0.8 include 1
            buckets={},
            trained_ts_ms=0,
            source_path="",
        )
        out_low = cm.predict_set(0.10, 0.20)
        self.assertEqual(out_low["cp_set_size"], 1)
        self.assertEqual(out_low["cp_in_set0"], 1)
        self.assertEqual(out_low["cp_in_set1"], 0)

        out_high = cm.predict_set(0.90, 0.20)
        self.assertEqual(out_high["cp_set_size"], 1)
        self.assertEqual(out_high["cp_in_set0"], 0)
        self.assertEqual(out_high["cp_in_set1"], 1)

        out_mid = cm.predict_set(0.50, 0.20)
        self.assertEqual(out_mid["cp_set_size"], 2)
        self.assertEqual(out_mid["cp_abstain"], 1)

if __name__ == '__main__':
    unittest.main()
