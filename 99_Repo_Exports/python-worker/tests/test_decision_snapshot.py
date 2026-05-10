"""Tests for decision_snapshot (A2): build + publish contract."""
import unittest
from types import SimpleNamespace
from core.redis_keys import RedisStreams as RS


class DummyStreamSink:
    """Minimal StreamSink stub matching the real API."""
    def __init__(self, name, field, maxlen):
        self.name = name
        self.field = field
        self.maxlen = maxlen


class DummyPublisher:
    """Captures xadd_json calls without touching Redis."""
    def __init__(self):
        self.calls = []

    async def xadd_json(self, sink, payload, symbol):
        self.calls.append((sink.name, payload, symbol))


class TestDecisionSnapshot(unittest.IsolatedAsyncioTestCase):
    async def test_build_and_publish(self):
        from services.orderflow.decision_snapshot import build_decision_snapshot, publish_decision_snapshot

        runtime = SimpleNamespace(symbol='BTCUSDT', config={'venue': 'binance'})
        ctx = {
            'symbol': 'BTCUSDT',
            'signal_id': 'sid1',
            'sid': 'sid1',
            'direction': 'LONG',
            'decision_ts_ms': 1700000000000,
            'decision_bid': 100.0,
            'decision_ask': 101.0,
            'decision_mid': 100.5,
            'decision_spread_bps': 99.5,
            'tca_ready': True,
            'book_sanity_flags': [],
        }

        snap = build_decision_snapshot(ctx, runtime=runtime, indicators={}, schema_version=1)
        self.assertEqual(snap['sid'], 'sid1')
        self.assertEqual(snap['symbol'], 'BTCUSDT')
        self.assertEqual(snap['decision_ts_ms'], 1700000000000)
        self.assertTrue(snap['tca_ready'])
        self.assertEqual(snap['schema_version'], 1)
        self.assertEqual(snap['direction'], 'LONG')
        self.assertEqual(snap['side'], 'long')

        pub = DummyPublisher()
        await publish_decision_snapshot(
            publisher=pub,
            snapshot=snap,
            stream=RS.DECISION_SNAPSHOT,
            maxlen=1000,
            symbol='BTCUSDT',
        )
        self.assertEqual(len(pub.calls), 1)
        self.assertEqual(pub.calls[0][0], RS.DECISION_SNAPSHOT)
        self.assertEqual(pub.calls[0][2], 'BTCUSDT')

    async def test_publish_fail_open(self):
        """Publisher errors must not raise (caller wraps in try/except but snapshot itself is ok)."""
        from services.orderflow.decision_snapshot import build_decision_snapshot

        ctx = {
            'signal_id': 'sid_x',
            'sid': 'sid_x',
            'direction': 'SHORT',
            'decision_ts_ms': 1700000000001,
        }
        runtime = SimpleNamespace(symbol='ETHUSDT', config={})
        snap = build_decision_snapshot(ctx, runtime=runtime, indicators={})
        self.assertEqual(snap['sid'], 'sid_x')
        self.assertEqual(snap['direction'], 'SHORT')

    def test_build_decision_snapshot_missing_prices(self):
        """Missing prices should not crash; result fields should be None."""
        from services.orderflow.decision_snapshot import build_decision_snapshot

        ctx = {'sid': 'abc', 'direction': 'LONG', 'decision_ts_ms': 1000}
        runtime = SimpleNamespace(symbol='SOLUSDT', config={})
        snap = build_decision_snapshot(ctx, runtime=runtime, indicators={})
        self.assertIsNone(snap['decision_bid'])
        self.assertIsNone(snap['decision_ask'])
        self.assertIsNone(snap['decision_mid'])


if __name__ == '__main__':
    unittest.main()
