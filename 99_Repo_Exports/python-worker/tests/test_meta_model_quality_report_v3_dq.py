import unittest

import numpy as np


class TestMetaModelQualityReportV3DQ(unittest.TestCase):
    def test_parse_thresholds_sorted_desc(self) -> None:
        from tools.meta_model_quality_report_v3 import _parse_thresholds

        xs = _parse_thresholds("0.7, 0.9,0.8")
        self.assertEqual(xs, [0.9, 0.8, 0.7])

        # Test defaults
        self.assertEqual(_parse_thresholds(""), [0.9, 0.8, 0.7, 0.6, 0.5])
        self.assertEqual(_parse_thresholds("invalid"), [0.9, 0.8, 0.7, 0.6, 0.5])

    def test_dq_bucket_mapping(self) -> None:
        from tools.meta_model_quality_report_v3 import _dq_bucket

        thr = [0.9, 0.8, 0.7]
        self.assertEqual(_dq_bucket(0.95, thr), "dq0")
        self.assertEqual(_dq_bucket(0.85, thr), "dq1")
        self.assertEqual(_dq_bucket(0.75, thr), "dq2")
        self.assertEqual(_dq_bucket(0.65, thr), "dq3")
        self.assertEqual(_dq_bucket(None, thr), "na")
        self.assertEqual(_dq_bucket(float("nan"), thr), "na")
        self.assertEqual(_dq_bucket(float("inf"), thr), "na")

    def test_pearson_corr(self) -> None:
        from tools.meta_model_quality_report_v3 import _pearson_corr

        x = np.asarray([0.0, 1.0, 2.0], dtype=float)
        y = np.asarray([0.0, 1.0, 2.0], dtype=float)
        self.assertAlmostEqual(_pearson_corr(x, y), 1.0, places=6)

        y2 = np.asarray([2.0, 1.0, 0.0], dtype=float)
        self.assertAlmostEqual(_pearson_corr(x, y2), -1.0, places=6)

        # degenerate inputs should be safe
        self.assertEqual(_pearson_corr(np.asarray([1.0]), np.asarray([2.0])), 0.0)
        self.assertEqual(_pearson_corr(np.asarray([1.0, 1.0]), np.asarray([2.0, 3.0])), 0.0)

if __name__ == "__main__":
    unittest.main()
