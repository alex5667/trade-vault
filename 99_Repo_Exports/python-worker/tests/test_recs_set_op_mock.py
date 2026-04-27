
import unittest
from unittest.mock import MagicMock, call
import sys
import os

# Add path to services
sys.path.append(os.path.join(os.getcwd(), "python-worker"))

from services.recs_callback_worker_v2 import apply_ops, rollback_ops, op_preview_diff

class TestRecsSetOp(unittest.TestCase):
    def setUp(self):
        self.mock_redis = MagicMock()
        self.mock_pipeline = MagicMock()
        self.mock_redis.pipeline.return_value = self.mock_pipeline
        self.maxDiff = None

    def test_apply_ops_set(self):
        bundle_id = "test_bundle"
        actor = {"who": "tester"}
        ttl = 60
        ops = [
            {"op": "SET", "key": "mykey", "value": "myval"}
        ]
        bundle = {"id": bundle_id, "ops": ops}

        # Mock Redis get for old value (None)
        self.mock_redis.get.return_value = None

        applied = apply_ops(self.mock_redis, bundle, ttl, actor)

        self.assertEqual(applied, 1)
        # Verify SET called on pipeline
        self.mock_pipeline.set.assert_called_with("mykey", "myval")
        # Verify audit push
        self.mock_redis.rpush.assert_called()
        args, _ = self.mock_redis.rpush.call_args
        self.assertEqual(args[0], f"recs:audit:{bundle_id}")
        import json
        audit_entry = json.loads(args[1])
        self.assertEqual(audit_entry["op"], "SET")
        self.assertEqual(audit_entry["key"], "mykey")
        self.assertEqual(audit_entry["new"], "myval")
        self.assertEqual(audit_entry["old_null"], 1)

    def test_apply_ops_set_overwrite(self):
        bundle_id = "test_bundle"
        actor = {"who": "tester"}
        ttl = 60
        ops = [
            {"op": "SET", "key": "mykey", "value": "newval"}
        ]
        bundle = {"id": bundle_id, "ops": ops}

        # Mock Redis get for old value
        self.mock_redis.get.return_value = "oldval"

        applied = apply_ops(self.mock_redis, bundle, ttl, actor)

        self.assertEqual(applied, 1)
        self.mock_pipeline.set.assert_called_with("mykey", "newval")
        
        args, _ = self.mock_redis.rpush.call_args
        import json
        audit_entry = json.loads(args[1])
        self.assertEqual(audit_entry["op"], "SET")
        self.assertEqual(audit_entry["old"], "oldval")
        self.assertEqual(audit_entry["old_null"], 0)

    def test_preview_diff_set(self):
        bundle = {
            "id": "b1",
            "ops": [
                {"op": "SET", "key": "k1", "value": "v1"}
            ]
        }
        self.mock_redis.get.return_value = "old_v1"
        
        preview = op_preview_diff(self.mock_redis, bundle)
        self.assertIn("SET k1: old_v1 -> v1", preview)

    def test_rollback_ops_set_delete(self):
        bundle_id = "b1"
        ttl = 60
        actor = "tester"
        
        # Mock audit log response
        audit_entry = {
            "op": "SET",
            "key": "k1",
            "field": "",
            "old": "",
            "old_null": 1,
            "new": "v1"
        }
        import json
        self.mock_redis.llen.return_value = 1
        self.mock_redis.lindex.return_value = json.dumps(audit_entry)

        rollback_ops(self.mock_redis, bundle_id, ttl, actor)
        
        # Should delete k1 because old_null was 1
        self.mock_pipeline.delete.assert_called_with("k1")

    def test_rollback_ops_set_restore(self):
        bundle_id = "b1"
        ttl = 60
        actor = "tester"
        
        audit_entry = {
            "op": "SET",
            "key": "k1",
            "field": "",
            "old": "old_val",
            "old_null": 0,
            "new": "v1"
        }
        import json
        self.mock_redis.llen.return_value = 1
        self.mock_redis.lindex.return_value = json.dumps(audit_entry)

        rollback_ops(self.mock_redis, bundle_id, ttl, actor)
        
        # Should set k1 to old_val
        self.mock_pipeline.set.assert_called_with("k1", "old_val")

if __name__ == "__main__":
    unittest.main()
