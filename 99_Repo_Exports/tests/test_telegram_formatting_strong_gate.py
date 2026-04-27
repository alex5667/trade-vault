import unittest
from core.crypto_signal_formatter import CryptoSignal, CryptoSignalFormatter

class TestTelegramFormatting(unittest.TestCase):
    def test_strong_signal_formatting(self):
        signal = CryptoSignal(
            sid="test-sid",
            symbol="BTCUSDT",
            side="LONG",
            entry=50000.0,
            sl=49000.0,
            tp_levels=[51000.0, 52000.0],
            lot=0.1,
            atr=100.0,
            confidence=0.8,
            ts=1678886400000,
            source="Test",
            config_params={"strong_gate_ok": 1}
        )
        msg = CryptoSignalFormatter.format_telegram_message(signal)
        self.assertIn("✅ <b>Strong (Сильный)</b>", msg)
        self.assertIn("Соответствует жестким критериям гейта", msg)

    def test_weak_signal_formatting(self):
        signal = CryptoSignal(
            sid="test-sid",
            symbol="BTCUSDT",
            side="LONG",
            entry=50000.0,
            sl=49000.0,
            tp_levels=[51000.0, 52000.0],
            lot=0.1,
            atr=100.0,
            confidence=0.8,
            ts=1678886400000,
            source="Test",
            config_params={"strong_gate_ok": 0}
        )
        msg = CryptoSignalFormatter.format_telegram_message(signal)
        self.assertIn("⚠️ <b>Weak (Слабый)</b>", msg)
        self.assertIn("Не дотягивает до жестких критериев", msg)

    def test_no_gate_info_formatting(self):
        signal = CryptoSignal(
            sid="test-sid",
            symbol="BTCUSDT",
            side="LONG",
            entry=50000.0,
            sl=49000.0,
            tp_levels=[51000.0, 52000.0],
            lot=0.1,
            atr=100.0,
            confidence=0.8,
            ts=1678886400000,
            source="Test",
            config_params={}
        )
        msg = CryptoSignalFormatter.format_telegram_message(signal)
        self.assertNotIn("Strong (Сильный)", msg)
        self.assertNotIn("Weak (Слабый)", msg)

if __name__ == '__main__':
    unittest.main()
