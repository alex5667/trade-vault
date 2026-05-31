import unittest

from core.tick_time import TickTimePolicy, apply_tick_time_policy


class TestTickTimePolicy(unittest.TestCase):
    def test_ok(self):
        pol = TickTimePolicy(max_future_ms=5000, max_past_ms=120000, max_reorder_ms=1500, clamp_soft_future=True, allow_soft_reorder=True)
        ts, decision, meta = apply_tick_time_policy(tick_ts_ms=1000, ingest_now_ms=1000, prev_ts_ms=900, policy=pol)
        self.assertEqual(decision, "ok")
        self.assertEqual(ts, 1000)

    def test_drop_past(self):
        pol = TickTimePolicy(max_future_ms=5000, max_past_ms=100, max_reorder_ms=1500, clamp_soft_future=True, allow_soft_reorder=True)
        ts, decision, meta = apply_tick_time_policy(tick_ts_ms=0, ingest_now_ms=1000, prev_ts_ms=900, policy=pol)
        self.assertEqual(decision, "drop_missing")
        ts, decision, meta = apply_tick_time_policy(tick_ts_ms=800, ingest_now_ms=1000, prev_ts_ms=0, policy=pol)
        self.assertEqual(decision, "drop_past")
        self.assertEqual(ts, 0)

    def test_future_clamp_and_drop(self):
        pol = TickTimePolicy(max_future_ms=100, max_past_ms=120000, max_reorder_ms=1500, clamp_soft_future=True, allow_soft_reorder=True)
        ts, decision, meta = apply_tick_time_policy(tick_ts_ms=1050, ingest_now_ms=1000, prev_ts_ms=900, policy=pol)
        self.assertEqual(decision, "clamp_future")
        self.assertEqual(ts, 1000)
        ts, decision, meta = apply_tick_time_policy(tick_ts_ms=1200, ingest_now_ms=1000, prev_ts_ms=900, policy=pol)
        self.assertEqual(decision, "drop_future")
        self.assertEqual(ts, 0)

    def test_reorder_soft_and_hard(self):
        pol = TickTimePolicy(max_future_ms=5000, max_past_ms=120000, max_reorder_ms=50, clamp_soft_future=True, allow_soft_reorder=True)
        ts, decision, meta = apply_tick_time_policy(tick_ts_ms=980, ingest_now_ms=1000, prev_ts_ms=1000, policy=pol)
        self.assertEqual(decision, "reorder_soft")
        self.assertEqual(ts, 1001)
        ts, decision, meta = apply_tick_time_policy(tick_ts_ms=800, ingest_now_ms=1000, prev_ts_ms=1000, policy=pol)
        self.assertEqual(decision, "reorder_hard")
        self.assertEqual(ts, 0)


if __name__ == "__main__":
    unittest.main()

