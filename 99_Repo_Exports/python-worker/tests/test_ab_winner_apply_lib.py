import json
import unittest
from unittest.mock import AsyncMock

from services.ab_winner_apply_lib import apply_sid_if_ready
from core.redis_keys import RedisStreams as RS


class TestABWinnerApplyLib(unittest.IsolatedAsyncioTestCase):
    async def test_apply_calls_eval_with_expected_keys(self):
        r = AsyncMock()
        sid = "abcd1234"
        meta_prefix = "cfg:suggestions:entry_policy:meta"
        approvals_prefix = "cfg:suggestions:entry_policy:approvals"
        applied_prefix = "cfg:suggestions:entry_policy:applied"

        meta = {
            "symbol": "ETHUSDT",
            "regime": "thin",
            "group": "thin",
            "winner_arm": "B",
        }
        r.get = AsyncMock(return_value=json.dumps(meta))
        # Lua result: applied, reason, approvals_n
        r.eval = AsyncMock(return_value=[1, "applied", 2])
        r.xadd = AsyncMock()

        res = await apply_sid_if_ready(
            r=r,
            sid=sid,
            meta_prefix=meta_prefix,
            approvals_prefix=approvals_prefix,
            applied_prefix=applied_prefix,
            approvals_required=2,
            lock_sec=60,
            active_ttl_sec=0,
            applied_ttl_sec=3600,
            audit_stream=RS.ENTRY_AUDIT,
            by="test",
        )
        self.assertTrue(res.applied)
        self.assertEqual(res.winner, "B")

        # ensure eval called once with 4 keys
        # unittest.mock AsyncMock await_args returns (args, kwargs)
        self.assertEqual(r.eval.await_count, 1)
        args, kwargs = r.eval.await_args
        # args: (script, numkeys, k1,k2,k3,k4, argv...)
        self.assertEqual(int(args[1]), 5)
        self.assertTrue(str(args[2]).endswith(f"{approvals_prefix}:{sid}"))
        self.assertTrue(str(args[3]).endswith(f"{applied_prefix}:{sid}"))
        self.assertIn("cfg:entry_policy:active_arm:ETHUSDT:thin:thin", str(args[4]))
        self.assertIn("cfg:entry_policy:active_arm_lock:ETHUSDT:thin:thin", str(args[5]))
        self.assertIn("cfg:entry_policy:active_arm_override_unlock:ETHUSDT:thin:thin", str(args[6]))


    async def test_apply_skips_when_no_meta(self):
        r = AsyncMock()
        r.get = AsyncMock(return_value=None)
        res = await apply_sid_if_ready(
            r=r,
            sid="sid",
            meta_prefix="cfg:suggestions:entry_policy:meta",
            approvals_prefix="cfg:suggestions:entry_policy:approvals",
            applied_prefix="cfg:suggestions:entry_policy:applied",
            approvals_required=2,
            lock_sec=60,
            active_ttl_sec=0,
            applied_ttl_sec=3600,
            audit_stream=RS.ENTRY_AUDIT,
        )
        self.assertTrue(res.skipped)
        self.assertEqual(res.reason, "no_meta")
