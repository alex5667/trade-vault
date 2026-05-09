import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.telegram_analyzer import TelegramMessageAnalyzer


class TestTelegramAnalyzer(unittest.TestCase):

    @patch('urllib.request.urlopen')
    def test_analysis_success(self, mock_urlopen):
        # Mock Ollama response
        mock_response = MagicMock()
        mock_response.read.return_value = '{"message": {"content": "<think>Testing analysis</think>Это тестовый анализ для торговой системы."}}'.encode()
        mock_response.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_response

        original_text = "Critical error in signal generator: signal_emit_p99 > 8ms"
        result = TelegramMessageAnalyzer.analyze(original_text)

        self.assertIn(original_text, result)
        self.assertIn("--- DeepSeek Analysis ---", result)
        self.assertIn("Это тестовый анализ", result)
        self.assertNotIn("<think>", result)
        self.assertNotIn("Testing analysis", result)

    @patch('urllib.request.urlopen')
    def test_analysis_fail_open(self, mock_urlopen):
        # Mock error
        mock_urlopen.side_effect = Exception("Ollama is down")

        original_text = "Critical error in signal generator: signal_emit_p99 > 8ms"
        result = TelegramMessageAnalyzer.analyze(original_text)

        self.assertEqual(result, original_text)

    def test_short_message_skip(self):
        original_text = "Ok"
        result = TelegramMessageAnalyzer.analyze(original_text)
        self.assertEqual(result, original_text)

if __name__ == '__main__':
    unittest.main()
