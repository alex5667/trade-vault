import unittest

from tools.bench_tick_ingest_latency_ab import parse_histogram, hist_delta, quantile_from_buckets


SAMPLE1 = """
# TYPE tick_ingest_process_ms histogram
tick_ingest_process_ms_bucket{le="1.0",symbol="BTCUSDT"} 10
tick_ingest_process_ms_bucket{le="5.0",symbol="BTCUSDT"} 20
tick_ingest_process_ms_bucket{le="+Inf",symbol="BTCUSDT"} 30
tick_ingest_process_ms_count{symbol="BTCUSDT"} 30
tick_ingest_process_ms_sum{symbol="BTCUSDT"} 60
"""

SAMPLE2 = """
# TYPE tick_ingest_process_ms histogram
tick_ingest_process_ms_bucket{le="1.0",symbol="BTCUSDT"} 30
tick_ingest_process_ms_bucket{le="5.0",symbol="BTCUSDT"} 70
tick_ingest_process_ms_bucket{le="+Inf",symbol="BTCUSDT"} 100
tick_ingest_process_ms_count{symbol="BTCUSDT"} 100
tick_ingest_process_ms_sum{symbol="BTCUSDT"} 260
"""


class TestBenchTickIngestLatencyAB(unittest.TestCase):
    def test_parse_and_delta_and_quantile(self):
        h1 = parse_histogram(SAMPLE1, "tick_ingest_process_ms", symbol="BTCUSDT")
        h2 = parse_histogram(SAMPLE2, "tick_ingest_process_ms", symbol="BTCUSDT")
        d = hist_delta(h1, h2)
        self.assertEqual(d.count, 70.0)
        self.assertAlmostEqual(d.buckets[float("inf")], 70.0)
        p50 = quantile_from_buckets(d, 0.50)
        p95 = quantile_from_buckets(d, 0.95)
        self.assertIsNotNone(p50)
        self.assertIsNotNone(p95)
        self.assertGreaterEqual(p50, 0.0)
        self.assertLessEqual(p50, 5.0)
        self.assertLessEqual(p95, 5.0)


if __name__ == "__main__":
    unittest.main()
