#!/usr/bin/env python3
"""test_meta_cov_ops_eventlog_v1.py

Unit tests for meta_cov_ops_eventlog_v1
"""

import unittest
from unittest.mock import MagicMock

from tools import meta_cov_ops_eventlog_v1

class TestMetaCovOpsEventLog(unittest.TestCase):

    def setUp(self):
        self.mock_redis = MagicMock()

    def test_write_event_success(self):
        # Setup
        self.mock_redis.xadd.return_value = b"123-0"
        
        # Action
        res = meta_cov_ops_eventlog_v1.write_event(
            self.mock_redis, "test_stream", "run_1", "test_event", {"foo": "bar", "n": 10}
        )
        
        # Verify
        self.assertEqual(res, "123-0")
        self.mock_redis.xadd.assert_called_once()
        args, kwargs = self.mock_redis.xadd.call_args
        stream_key, fields = args
        self.assertEqual(stream_key, "test_stream")
        self.assertEqual(fields["event"], "test_event")
        self.assertEqual(fields["run_id"], "run_1")
        self.assertEqual(fields["foo"], "bar")
        self.assertEqual(fields["n"], 10)
        # Check ts_ms presence
        self.assertIn("ts_ms", fields)

    def test_write_event_no_redis(self):
        res = meta_cov_ops_eventlog_v1.write_event(None, "s", "r", "e", {})
        self.assertIsNone(res)

    def test_write_cfg2_snapshot(self):
        # Action
        meta_cov_ops_eventlog_v1.write_cfg2_snapshot(
            self.mock_redis, "cfg_key", {"k1": "v1", "k2": 123}
        )
        
        # Verify
        self.mock_redis.hset.assert_called_once()
        args, kwargs = self.mock_redis.hset.call_args
        self.assertEqual(args[0], "cfg_key")
        mapping = kwargs["mapping"]
        self.assertEqual(mapping["k1"], "v1")
        self.assertEqual(mapping["k2"], 123)

if __name__ == "__main__":
    unittest.main()
