import unittest
from core.snapshot_builder import SnapshotBuilder

class TestSnapshotBuilderKey(unittest.TestCase):
    def test_key_method_supports_both_placeholders(self):
        # snapshot_builder._key не использует зависимости при вызове, поэтому передаем моки или None
        builder = SnapshotBuilder(r=None, cfg=None, logger=None)
        
        # 1. Проверяем legacy формат {SYMBOL}
        res_legacy = builder._key("stream:tick_{SYMBOL}", "BTCUSDT")
        self.assertEqual(res_legacy, "stream:tick_BTCUSDT")
        
        # 2. Проверяем новый canonical формат {symbol}
        res_canonical = builder._key("stream:tick_{symbol}", "ETHUSDT")
        self.assertEqual(res_canonical, "stream:tick_ETHUSDT")
        
        # 3. На всякий случай проверяем оба сразу
        res_both = builder._key("test:{SYMBOL}:{symbol}", "SOLUSDT")
        self.assertEqual(res_both, "test:SOLUSDT:SOLUSDT")

if __name__ == "__main__":
    unittest.main()
