import json
import unittest

from tools.redis_window import _merge_payload_fields


class TestRedisWindow(unittest.TestCase):
    def test_merge_payload_fields_simple(self):
        fields = {
            "ts_ms": "100",
            "payload": json.dumps({"foo": "bar", "baz": 123}),
            "indicators": json.dumps({"ind1": 1.0})
        }
        merged = _merge_payload_fields(fields)
        self.assertEqual(merged.get("foo"), "bar")
        self.assertEqual(merged.get("baz"), 123)
        self.assertEqual(merged.get("ind1"), 1.0)
        self.assertEqual(merged.get("ts_ms"), "100")

    def test_merge_priority(self):
        # Top-level wins over payload, payload wins over indicators
        fields = {
            "foo": "top",
            "payload": json.dumps({"foo": "payload", "bar": "payload"}),
            "indicators": json.dumps({"bar": "indicators", "baz": "indicators"})
        }
        merged = _merge_payload_fields(fields)
        self.assertEqual(merged.get("foo"), "top")
        self.assertEqual(merged.get("bar"), "payload")
        self.assertEqual(merged.get("baz"), "indicators")

    def test_nested_indicators_in_payload(self):
        # payload.indicators should be merged
        fields = {
            "payload": json.dumps({
                "p1": 1,
                "indicators": json.dumps({"i1": 2})
            })
        }
        merged = _merge_payload_fields(fields)
        self.assertEqual(merged.get("p1"), 1)
        self.assertEqual(merged.get("i1"), 2)

    def test_ignore_non_scalar(self):
        fields = {
            "payload": json.dumps({"nested": {"a": 1}, "list": [1, 2]})
        }
        merged = _merge_payload_fields(fields)
        self.assertNotIn("nested", merged)
        self.assertNotIn("list", merged)

if __name__ == '__main__':
    unittest.main()
