import unittest

from services.orderflow.utils import redis_stream_id_ts_ms, _parse_tick_payload
from services.orderflow.tick_dedup import TickDeduper, tick_uid


class TestTickContractAndDedup(unittest.TestCase):
    def test_redis_stream_id_ts_ms(self):
        self.assertEqual(redis_stream_id_ts_ms("1700000000000-0"), 1700000000000)
        self.assertEqual(redis_stream_id_ts_ms(b"1700000000001-42"), 1700000000001)
        self.assertEqual(redis_stream_id_ts_ms(""), 0)

    def test_parse_tick_trade_id_variants(self):
        t1 = _parse_tick_payload({"symbol": "BTCUSDT", "ts_ms": 1700000000000, "price": "1", "qty": "1", "trade_id": "123"})
        self.assertEqual(int(t1.get("trade_id") or 0), 123)
        t2 = _parse_tick_payload({"symbol": "BTCUSDT", "ts_ms": 1700000000000, "price": "1", "qty": "1", "t": "456"})
        self.assertEqual(int(t2.get("trade_id") or 0), 456)
        t3 = _parse_tick_payload({"symbol": "BTCUSDT", "ts_ms": 1700000000000, "price": "1", "qty": "1", "a": "789"})
        self.assertEqual(int(t3.get("trade_id") or 0), 789)

    def test_tick_deduper(self):
        d = TickDeduper(max_items=3, max_age_ms=1000)
        tick = {"ts_ms": 1, "price": 10.0, "qty": 1.0, "side": "BUY", "is_buyer_maker": False}
        uid = tick_uid(tick)
        self.assertFalse(d.seen(uid, 100))
        self.assertTrue(d.seen(uid, 200))
        # TTL eviction
        self.assertFalse(d.seen(uid, 2000))


if __name__ == "__main__":
    unittest.main()







