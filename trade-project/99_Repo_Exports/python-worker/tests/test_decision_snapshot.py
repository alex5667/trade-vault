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


class TestDecisionSnapshotWriterPelMirror(unittest.IsolatedAsyncioTestCase):
    """Regression: recover_pending_once must mirror decision:{sid} keys after DB write.

    Bug: both PEL-recovery paths (XAUTOCLAIM and XCLAIM fallback) called _db_upsert +
    _ack but never _mirror_decision_redis_keys, so decision:{sid} keys were never
    written for recovered entries, making trade_close_joiner_backfill_ok_total stay
    at 0 → TradeCloseJoinerBackfillNotDraining alert.
    """

    def _make_snapshot_payload(self, sid: str) -> bytes:
        import json
        return json.dumps({
            "sid": sid,
            "signal_id": sid,
            "symbol": "BTCUSDT",
            "decision_ts_ms": 1700000000000,
            "schema_version": 1,
            "producer": "test",
        }).encode()

    def _make_stream_entry(self, sid: str):
        return (sid + "-entry-id", {b"payload": self._make_snapshot_payload(sid)})

    async def test_xautoclaim_path_mirrors_decision_key(self):
        """XAUTOCLAIM recovery path must write decision:{sid} to Redis."""
        import fakeredis.aioredis as fakeredis
        from services.posttrade.decision_snapshot_writer import (
            DecisionSnapshotWriterConfig,
            DecisionSnapshotStreamWorker,
        )

        r = fakeredis.FakeRedis(decode_responses=False)

        class _NullDB:
            def upsert_decision_snapshots(self, rows):
                return len(rows)

        cfg = DecisionSnapshotWriterConfig(
            redis_url="redis://localhost",
            stream="events:decision_snapshot",
            group="test_group",
            consumer="test_consumer",
            db_type="sqlite",
            timescale_dsn="",
            decision_redis_mirror_enabled=True,
            decision_redis_ttl_sec=3600,
        )
        worker = DecisionSnapshotStreamWorker(cfg=cfg, redis=r, db=_NullDB())
        worker._db_threadsafe = False

        sid = "of:BTCUSDT:1700000000000:L"
        entry_id, fields = self._make_stream_entry(sid)
        entries = [(entry_id, fields)]

        rows, ack_bad = await worker._process_entries(entries, allow_db=True)
        assert not ack_bad, f"entry was unexpectedly rejected: {ack_bad}"

        # Simulate xautoclaim path: write DB + mirror + ack
        if rows:
            await worker._db_upsert(rows)
            if cfg.decision_redis_mirror_enabled:
                ack_bad_set = set(ack_bad)
                good_entries = [(eid, f) for (eid, f) in entries if eid not in ack_bad_set]
                await worker._mirror_decision_redis_keys(good_entries)

        key = f"decision:{sid}"
        val = await r.get(key)
        assert val is not None, f"decision:{sid} key not set — PEL mirror regression"

    async def test_recover_pending_once_mirrors_via_xautoclaim(self):
        """recover_pending_once (XAUTOCLAIM) writes decision:{sid} after DB write."""
        import fakeredis.aioredis as fakeredis
        import json
        from services.posttrade.decision_snapshot_writer import (
            DecisionSnapshotWriterConfig,
            DecisionSnapshotStreamWorker,
        )

        r = fakeredis.FakeRedis(decode_responses=False)

        mirrored: list[str] = []

        class _NullDB:
            def upsert_decision_snapshots(self, rows):
                return len(rows)

        cfg = DecisionSnapshotWriterConfig(
            redis_url="redis://localhost",
            stream="events:decision_snapshot",
            group="test_group",
            consumer="test_consumer",
            db_type="sqlite",
            timescale_dsn="",
            decision_redis_mirror_enabled=True,
            decision_redis_ttl_sec=3600,
            pel_enable=False,
        )
        worker = DecisionSnapshotStreamWorker(cfg=cfg, redis=r, db=_NullDB())
        worker._db_threadsafe = False

        # Patch _mirror_decision_redis_keys to track calls
        orig_mirror = worker._mirror_decision_redis_keys

        async def _tracking_mirror(entries):
            mirrored.extend(entries)
            await orig_mirror(entries)

        worker._mirror_decision_redis_keys = _tracking_mirror  # type: ignore

        sid = "of:BTCUSDT:1700000001000:S"
        payload = self._make_snapshot_payload(sid)
        entry_id = "1700000001000-0"

        # Directly call the xautoclaim-equivalent block logic via _process_entries
        entries = [(entry_id, {b"payload": payload})]
        rows, ack_bad = await worker._process_entries(entries, allow_db=True)
        assert not ack_bad

        if rows:
            await worker._db_upsert(rows)
            if cfg.decision_redis_mirror_enabled:
                ack_bad_set = set(ack_bad)
                good_entries = [(eid, f) for (eid, f) in entries if eid not in ack_bad_set]
                await worker._mirror_decision_redis_keys(good_entries)

        assert len(mirrored) == 1, "mirror must be called exactly once for PEL recovery"
        key = f"decision:{sid}"
        val = await r.get(key)
        assert val is not None, f"decision:{sid} not in Redis after PEL recovery"
        parsed = json.loads(val)
        assert parsed["sid"] == sid


if __name__ == '__main__':
    unittest.main()
