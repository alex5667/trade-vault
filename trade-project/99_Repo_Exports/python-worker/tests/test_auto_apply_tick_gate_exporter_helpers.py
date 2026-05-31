import unittest

from tools.auto_apply_tick_gate_exporter import normalize_reason, parse_json_maybe


class TestExporterHelpers(unittest.TestCase):
    def test_parse_json_maybe(self):
        self.assertEqual(parse_json_maybe(None), {})
        self.assertEqual(parse_json_maybe({"a": 1})["a"], 1)
        self.assertEqual(parse_json_maybe('{"x":2}')["x"], 2)
        self.assertEqual(parse_json_maybe("not_json"), {})

    def test_normalize_reason_collapse(self):
        allow = {"skew", "unknown_side"}
        self.assertEqual(normalize_reason("skew", "collapse", allow), "skew")
        self.assertEqual(normalize_reason("random_big_reason_name", "collapse", allow), "__other__")

    def test_normalize_reason_allow(self):
        allow = {"skew"}
        self.assertEqual(normalize_reason("skew", "allow", allow), "skew")
        self.assertEqual(normalize_reason("x", "allow", allow), "__other__")

    def test_normalize_reason_skip(self):
        self.assertEqual(normalize_reason("skew", "skip", None), "")


if __name__ == "__main__":
    unittest.main()
