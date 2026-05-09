import unittest

from utils.time_utils import get_ny_time_millis


class TestSanity(unittest.TestCase):
    def test_decision_trace_wrapper_imports(self):
        # should re-export the v2 implementation
        from common import decision_trace as dt

        self.assertTrue(hasattr(dt, "ensure_trace"))
        self.assertTrue(hasattr(dt, "trace_gate"))
        self.assertTrue(hasattr(dt, "trace_enabled"))

        ctx = {}
        tr = dt.ensure_trace(ctx)
        self.assertIsNotNone(tr)

    def test_parse_tick_has_event_ts(self):
        from services.orderflow.utils import _parse_tick_payload

        payload = {
            "symbol": "BTCUSDT",
            "ts_ms": 1700000000123,
            "price": 100.0,
            "qty": 1.0,
            "side": "BUY",
        }
        t = _parse_tick_payload(payload)
        self.assertEqual(t.get("ts_ms"), 1700000000123)
        self.assertEqual(t.get("event_ts_ms"), 1700000000123)

    def test_fields_to_dict_supports_pairs_and_flat(self):
        from core.redis_stream_consumer import _fields_to_dict

        self.assertEqual(_fields_to_dict({b"a": b"1"}).get("a"), b"1")
        self.assertEqual(_fields_to_dict([(b"a", b"1"), (b"b", b"2")]).get("b"), b"2")
        self.assertEqual(_fields_to_dict([b"a", b"1", b"b", b"2"]).get("a"), b"1")

    def test_msgid_ms_extracts_timestamp(self):
        from services.crypto_orderflow_service import CryptoOrderflowService

        # Redis stream id format: <ms>-<seq>
        self.assertEqual(CryptoOrderflowService._msgid_ms("1700000000123-0"), 1700000000123)
        self.assertEqual(CryptoOrderflowService._msgid_ms("1700000000456-1"), 1700000000456)
        self.assertEqual(CryptoOrderflowService._msgid_ms("invalid"), 0)
        self.assertEqual(CryptoOrderflowService._msgid_ms(""), 0)

    def test_coerce_event_ts_ms_prefers_payload_if_sane(self):
        import os

        from services.crypto_orderflow_service import CryptoOrderflowService

        # Mock service with reasonable skew
        os.environ["CRYPTO_OF_MAX_TS_SKEW_MS"] = "3600000"  # 1 hour
        service = CryptoOrderflowService("redis://localhost:6379/0")
        service._max_ts_skew_ms = 3600000

        now_ms = get_ny_time_millis()
        payload_ts = now_ms - 1000  # 1 second ago (sane)
        msg_id = f"{now_ms - 5000}-0"  # 5 seconds ago

        result = service._coerce_event_ts_ms(msg_id=msg_id, payload_ts_ms=payload_ts, now_ms=now_ms)
        self.assertEqual(result, payload_ts)

    def test_coerce_event_ts_ms_falls_back_to_msgid(self):
        import os

        from services.crypto_orderflow_service import CryptoOrderflowService

        os.environ["CRYPTO_OF_MAX_TS_SKEW_MS"] = "1000"  # 1 second
        service = CryptoOrderflowService("redis://localhost:6379/0")
        service._max_ts_skew_ms = 1000

        now_ms = get_ny_time_millis()
        payload_ts = now_ms - 7200000  # 2 hours ago (poisoned, exceeds skew)
        msg_id = f"{now_ms - 5000}-0"  # 5 seconds ago (sane)

        result = service._coerce_event_ts_ms(msg_id=msg_id, payload_ts_ms=payload_ts, now_ms=now_ms)
        # Should use msg_id ms
        self.assertEqual(result, now_ms - 5000)

    def test_coerce_event_ts_ms_falls_back_to_wall_clock(self):
        import os

        from services.crypto_orderflow_service import CryptoOrderflowService

        os.environ["CRYPTO_OF_MAX_TS_SKEW_MS"] = "1000"
        service = CryptoOrderflowService("redis://localhost:6379/0")
        service._max_ts_skew_ms = 1000

        now_ms = get_ny_time_millis()
        payload_ts = 0  # Invalid
        msg_id = "invalid-0"  # Invalid msg_id

        result = service._coerce_event_ts_ms(msg_id=msg_id, payload_ts_ms=payload_ts, now_ms=now_ms)
        # Should use wall clock (within 100ms tolerance)
        self.assertAlmostEqual(result, now_ms, delta=100)


if __name__ == "__main__":
    unittest.main()

