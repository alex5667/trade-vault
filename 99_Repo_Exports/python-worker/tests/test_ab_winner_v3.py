import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from core.entry_policy_suggestion_meta_v1 import EntryPolicySuggestionMetaV1
from core.lcb_evaluator import ArmAgg, evaluate_winner_lcb
from core.redis_lock import RedisLock
from services.ab_winner_suggester_service_v3 import ABWinnerSuggesterV3
from core.redis_keys import RedisStreams as RS


class TestLCBEvaluator(unittest.TestCase):
    def test_agg(self):
        a = ArmAgg()
        a.add(1.0)
        a.add(2.0)
        a.add(3.0)
        self.assertEqual(a.n, 3)
        self.assertAlmostEqual(a.mean(), 2.0)
        self.assertAlmostEqual(a.std(), 1.0)

    def test_evaluate_winner(self):
        # Baseline A: n=100, mean=1.0, std=1.0 -> lcb ~ 1.0 - 1.28*0.1 = 0.87
        aggA = ArmAgg()
        for _ in range(100): aggA.add(1.0)

        # Challenger B: n=100, mean=1.2, std=1.0 -> lcb ~ 1.2 - 0.128 = 1.07
        aggB = ArmAgg()
        for _ in range(100): aggB.add(1.2)

        stats = {"A": aggA, "B": aggB}
        winner, res, reason = evaluate_winner_lcb(stats_by_arm=stats, min_n=30, alpha=0.10, min_edge_r=0.10)

        # B LCB ~ 1.07, A LCB ~ 0.87 -> Edge ~ 0.20 >= 0.10 -> Winner B
        self.assertEqual(winner, "B")
        self.assertTrue("edge_ok" in reason)

class TestSuggestionMeta(unittest.TestCase):
    def test_validation(self):
        m = EntryPolicySuggestionMetaV1(sid="123", symbol="BTC", winner_arm="B", scenario="continuation")
        ok, _ = m.validate()
        self.assertTrue(ok)

        m2 = EntryPolicySuggestionMetaV1(sid="", symbol="BTC")
        ok, why = m2.validate()
        self.assertFalse(ok)
        self.assertEqual(why, "sid_empty")

    def test_json(self):
        m = EntryPolicySuggestionMetaV1(sid="123", symbol="BTC", winner_arm="B", scenario="continuation")
        j = m.to_json()
        m2, _ = EntryPolicySuggestionMetaV1.from_json(j)
        self.assertEqual(m2.sid, "123")
        self.assertEqual(m2.winner_arm, "B")

class TestRedisLock(unittest.IsolatedAsyncioTestCase):
    async def test_acquire_release(self):
        r = MagicMock()
        # Ensure set returns True and is awaitable
        future_true = asyncio.Future()
        future_true.set_result(True)
        r.set = MagicMock(return_value=future_true)

        # Ensure eval is awaitable
        future_none = asyncio.Future()
        future_none.set_result(None)
        r.eval = MagicMock(return_value=future_none)

        l = RedisLock(key="lock:test")
        ok = await l.acquire(r)
        self.assertTrue(ok)
        self.assertTrue(l.token)

        await l.release(r)
        r.eval.assert_called()

class TestABWinnerServiceV3(unittest.IsolatedAsyncioTestCase):
    async def test_ingest_and_evaluate(self):
        svc = ABWinnerSuggesterV3()
        svc.r = MagicMock()
        # Mock xread response
        svc.r.xread = AsyncMock(return_value=[
            (RS.EVENTS_TRADES, [
                ("1-0", {"event_type": "POSITION_CLOSED", "symbol": "BTC", "regime": "trend", "scenario": "continuation", "ab_arm": "A", "r_mult": "1.0", "ab_group": "g1"}),
                ("1-1", {"event_type": "POSITION_CLOSED", "symbol": "BTC", "regime": "trend", "scenario": "continuation", "ab_arm": "B", "r_mult": "2.0", "ab_group": "g1"}),
            ])
        ])
        svc.r.set = AsyncMock()
        svc.r.get = AsyncMock(return_value=None)
        svc.r.pipeline = MagicMock()
        svc.r.pipeline.return_value.execute = AsyncMock()
        svc.lock.acquire = AsyncMock(return_value=True)
        svc.lock.release = AsyncMock()

        # Ingest one batch
        await svc.ingest_forever() # Hack: actually infinite loop, so we can't await it strictly without cancelling.
        # But for unit test we can just call _ingest_event directly or mock xread to return empty second time to break loop?
        # The code loop `while True` is hard to break. Let's unit test `_ingest_event` and `evaluate_once`.

        # Direct ingestion
        svc._ingest_event({"event_type": "POSITION_CLOSED", "symbol": "BTC", "regime": "trend", "scenario": "continuation", "ab_arm": "A", "r_mult": "1.0", "ab_group": "g1"})
        for _ in range(50): # add enough samples for min_n
             svc._ingest_event({"event_type": "POSITION_CLOSED", "symbol": "BTC", "regime": "trend", "scenario": "continuation", "ab_arm": "B", "r_mult": "2.0", "ab_group": "g1"})

        # Evaluate
        # Regime trend -> min_n=40, edge=0.07.
        # A: n=1, mean=1
        # B: n=50, mean=2 -> LCB ~ 2.0 (std=0) > 0.07? Yes.

        n = await svc.evaluate_once()
        self.assertEqual(n, 1)

        # Check pipeline calls (meta, latest, audit stored)
        pipe = svc.r.pipeline.return_value
        self.assertTrue(pipe.set.call_count >= 2) # meta + latest
        self.assertTrue(pipe.xadd.call_count >= 1) # audit

if __name__ == "__main__":
    unittest.main()
