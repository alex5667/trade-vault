import asyncio
import unittest

from services.orderflow.decision_snapshot import build_decision_snapshot_event, publish_decision_snapshot


class _DummyPublisher:
    def __init__(self):
        self.calls = []

    async def xadd_json(self, *args, **kwargs):
        # Support both (sink=..., payload=..., symbol=...) and (stream=..., payload=..., symbol=...)
        self.calls.append((args, kwargs))
        return True


class DecisionSnapshotSmokeTests(unittest.TestCase):
    def test_build_event_basic_mid_spread(self):
        signal = {
            "symbol": "BTCUSDT",
            "signal_id": "sid-1",
            "sid": "sid-1",
            "direction": "LONG",
            "side": "long",
            "ts_emit_ms": 1700000000123,
            "best_bid": 100.0,
            "best_ask": 100.1,
            "micro": {"spread_bps": 10.0},
        }
        evt = build_decision_snapshot_event(signal=signal, indicators={}, runtime=None, schema_version=1, include_indicators=False)
        self.assertEqual(evt["sid"], "sid-1")
        self.assertEqual(evt["symbol"], "BTCUSDT")
        self.assertAlmostEqual(evt["decision_mid"], 100.05, places=8)
        self.assertTrue(evt["decision_spread_bps"] > 0)
        self.assertTrue(isinstance(evt["book_sanity_flags"], list))

    def test_crossed_book_sets_flag(self):
        signal = {
            "symbol": "BTCUSDT",
            "sid": "sid-2",
            "signal_id": "sid-2",
            "direction": "SHORT",
            "ts_emit_ms": 1700000000123,
            "best_bid": 100.2,
            "best_ask": 100.1,
        }
        evt = build_decision_snapshot_event(signal=signal, indicators={}, runtime=None)
        self.assertIn("crossed_bbo", evt["book_sanity_flags"])
        self.assertFalse(evt["tca_ready"])

    def test_publish_smoke(self):
        pub = _DummyPublisher()
        evt = {"sid": "sid-3", "symbol": "BTCUSDT", "decision_ts_ms": 1}
        asyncio.run(publish_decision_snapshot(publisher=pub, stream="events:decision_snapshot", maxlen=10, symbol="BTCUSDT", evt=evt))
        self.assertTrue(pub.calls)
