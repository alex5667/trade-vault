import unittest

from services.orderflow.metric_labels import symbol_label, should_emit


class TestMetricLabels(unittest.TestCase):
    def test_symbol_label_no_allowlist(self):
        self.assertEqual(symbol_label("btcusdt", None, "collapse"), "BTCUSDT")
        self.assertEqual(symbol_label("", None, "collapse"), "__empty__")

    def test_symbol_label_allowlist_collapse(self):
        allow = {"BTCUSDT", "ETHUSDT"}
        self.assertEqual(symbol_label("BTCUSDT", allow, "collapse"), "BTCUSDT")
        self.assertEqual(symbol_label("XRPUSDT", allow, "collapse"), "__other__")

    def test_symbol_label_allowlist_skip(self):
        allow = {"BTCUSDT"}
        self.assertIsNone(symbol_label("ETHUSDT", allow, "skip"))
        self.assertEqual(symbol_label("BTCUSDT", allow, "skip"), "BTCUSDT")

    def test_should_emit(self):
        self.assertTrue(should_emit(1000, 0, 0))
        self.assertTrue(should_emit(1000, 0, -1))
        self.assertFalse(should_emit(1100, 1000, 200))
        self.assertTrue(should_emit(1200, 1000, 200))


if __name__ == "__main__":
    unittest.main()
