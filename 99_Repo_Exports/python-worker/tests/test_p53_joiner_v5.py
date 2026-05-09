import asyncio
import json
import unittest
from unittest.mock import AsyncMock

from domain.evidence_keys import MetaKeys

# Mocking parts of the worker to test _handle_close
from services.orderflow.tools.trade_close_joiner_worker_v5 import _handle_close


class TestJoinerV5(unittest.TestCase):

    def setUp(self):
        self.r = AsyncMock()
        self.decision_prefix = "decision:"
        self.trades_closed_stream = "trades:closed"
        self.close_wait_stream = "trades:close_wait"
        self.ml_replay_stream = "ml_replay_inputs_v1"

    def test_handle_close_success(self):
        sid = "sid_123"
        close_payload = {
            "sid": sid,
            "position_id": "pos_456",
            "r_mult": 1.5,
            "close_ts_ms": 1700000000000,
            "meta_enforce_cov_bucket": "A",
            "meta_enforce_applied": "1"
        }
        decision = {
            "dq_state": "ok",
            "drift_state": "warn",
            "actual_action": "emit",
            "actual_reason_code": "ML_PASS",
            "rule_reason_code_top1": "RC_01",
            "ts_ms": 1699999999000
        }
        self.r.get.return_value = json.dumps(decision)
        self.r.set.return_value = True # dedup ok

        loop = asyncio.get_event_loop()
        ok, reason = loop.run_until_complete(_handle_close(
            self.r,
            close_payload,
            decision_prefix=self.decision_prefix,
            trades_closed_stream=self.trades_closed_stream,
            trades_closed_maxlen=100,
            close_wait_stream=self.close_wait_stream,
            close_wait_maxlen=100,
            dedup_ttl_sec=3600,
            ml_replay_stream=self.ml_replay_stream,
            ml_replay_maxlen=100,
            write_ml_replay=True
        ))

        self.assertTrue(ok)
        self.assertEqual(reason, "ok")

        # Verify xadd for trades:closed
        self.r.xadd.assert_any_call(self.trades_closed_stream, unittest.mock.ANY, maxlen=100, approximate=True)
        call_args = self.r.xadd.call_args_list[0]
        fields = call_args[0][1]
        payload = json.loads(fields["payload"])

        self.assertEqual(payload["dq_state"], "ok")
        self.assertEqual(payload["drift_state"], "warn")
        self.assertEqual(payload["drift_mode"], "warn")
        self.assertEqual(payload[MetaKeys.ENFORCE_COV_BUCKET], "A")
        self.assertEqual(payload[MetaKeys.ENFORCE_APPLIED], "1")
        self.assertEqual(payload["rule_reason_code_top1"], "RC_01")

    def test_handle_close_missing_decision(self):
        sid = "sid_missing"
        close_payload = {"sid": sid, "r_mult": 1.0}
        self.r.get.return_value = None

        loop = asyncio.get_event_loop()
        ok, reason = loop.run_until_complete(_handle_close(
            self.r,
            close_payload,
            decision_prefix=self.decision_prefix,
            trades_closed_stream=self.trades_closed_stream,
            trades_closed_maxlen=100,
            close_wait_stream=self.close_wait_stream,
            close_wait_maxlen=100,
            dedup_ttl_sec=3600,
            ml_replay_stream=self.ml_replay_stream,
            ml_replay_maxlen=100,
            write_ml_replay=False
        ))

        self.assertFalse(ok)
        self.assertEqual(reason, "missing_decision")

        # Verify push to close_wait
        self.r.xadd.assert_called_once_with(self.close_wait_stream, unittest.mock.ANY, maxlen=100, approximate=True)

if __name__ == "__main__":
    unittest.main()
