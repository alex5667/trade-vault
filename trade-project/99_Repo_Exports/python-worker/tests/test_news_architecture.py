import time
import unittest

from contexts import NewsFeatures, OrderflowSignalContext
from news_pipeline.calendar_store_worker import CalendarStoreWorker, importance_to_grade_id
from news_pipeline.enricher_shadow import NewsEnricherShadow
from news_pipeline.feature_store_worker import NewsFeatureStoreWorker
from tests.fake_redis import FakeRedis
from utils.time_utils import get_ny_time_millis


class TestNewsArchitecture(unittest.TestCase):
    def setUp(self):
        self.r = FakeRedis()

    def test_enricher_zero_io_cached(self):
        """Verify NewsEnricherShadow zero-IO attach with cache hit/miss."""
        enricher = NewsEnricherShadow(redis=self.r, cache_ttl_ms=1000)
        ctx = OrderflowSignalContext(symbol="BTCUSDT")

        # 1) Attach to empty cache -> should get empty features but register interest
        enricher.attach(ctx)
        self.assertIsInstance(ctx.news, NewsFeatures)
        self.assertEqual(ctx.news.news_risk, 0.0)
        self.assertIn(("BTCUSDT", "crypto"), enricher._wanted)

        # 2) Manually populate cache to simulate background refresh
        nf = NewsFeatures(news_risk=0.8, ref="news:analysis:123")
        now_ms = get_ny_time_millis()
        enricher._cache[("BTCUSDT", "crypto")] = (now_ms, nf)

        # 3) Attach again -> should get cached features
        ctx2 = OrderflowSignalContext(symbol="BTCUSDT")
        enricher.attach(ctx2)
        self.assertEqual(ctx2.news.news_risk, 0.8)
        self.assertEqual(ctx2.news.ref, "news:analysis:123")

    def test_feature_store_ema_logic(self):
        """Verify NewsFeatureStoreWorker EMA logic and Redis updates."""
        worker = NewsFeatureStoreWorker(redis=self.r)

        # Send first news
        fields1 = {
            "uid": "n1",
            "symbol": "ETHUSDT",
            "risk": "0.5",
            "surprise": "0.2",
            "ts_ms": str(get_ny_time_millis())
        }
        worker.handle_message("msg1", fields1)

        agg = self.r.hgetall("news:agg:ETHUSDT")
        self.assertEqual(float(agg["risk_ema"]), 0.5)
        self.assertEqual(agg["ref"], "news:analysis:n1")

        # Send second news (lower risk) -> should take max(decayed, new)
        time.sleep(0.1) # small decay
        fields2 = {
            "uid": "n2",
            "symbol": "ETHUSDT",
            "risk": "0.1",
            "surprise": "-0.5",
            "ts_ms": str(get_ny_time_millis())
        }
        worker.handle_message("msg2", fields2)

        agg2 = self.r.hgetall("news:agg:ETHUSDT")
        # risk_ema should be ~0.5 (decayed slightly but > 0.1)
        self.assertGreater(float(agg2["risk_ema"]), 0.4)
        # but ref should remain n1 if n1 risk was higher?
        # Actually in our code: if risk_new >= (prev_risk * d): replace.
        # Here risk_new (0.1) < prev_risk*d (~0.5), so ref remains news:analysis:n1
        self.assertEqual(agg2["ref"], "news:analysis:n1")

    def test_calendar_store_mapping(self):
        """Verify CalendarStoreWorker Go-field parsing and scope mapping."""
        worker = CalendarStoreWorker(redis=self.r)

        now_ms = get_ny_time_millis()
        future_ts = now_ms + 3600_000 # +1h

        # Go emits importance 0..3
        fields = {
            "uid": "cal-1",
            "event_ts_ms": str(future_ts),
            "country": "US",
            "currency": "USD",
            "importance": "3", # High
            "title": "Fed Interest Rate Decision"
        }

        worker.handle_message("msg-c1", fields)

        # USD High importance should map to all major scopes including crypto
        for scope in ["fx", "crypto", "metals"]:
            agg = self.r.hgetall(f"calendar:agg:{scope}")
            self.assertEqual(int(agg["next_ts_ms"]), future_ts)
            self.assertEqual(int(agg["event_grade_id"]), 4) # 3 maps to 4
            self.assertGreater(int(agg["event_tminus_sec"]), 3500)

    def test_importance_mapping(self):
        self.assertEqual(importance_to_grade_id(0), 0)
        self.assertEqual(importance_to_grade_id(1), 1)
        self.assertEqual(importance_to_grade_id(2), 2)
        self.assertEqual(importance_to_grade_id(3), 4)

if __name__ == "__main__":
    unittest.main()
