
import asyncio
import unittest
from unittest.mock import MagicMock, AsyncMock

# Minimal mock of what's needed for SignalPipeline
class MockPublisher:
    def __init__(self):
        self.r = MagicMock()
        self.r.xadd = AsyncMock()

# Import the actual class to test
import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'python-worker'))
from services.orderflow.signal_pipeline import SignalPipeline

class TestSignalPipelineReport(unittest.IsolatedAsyncioTestCase):
    async def test_send_report_with_runtime(self):
        publisher = MockPublisher()
        pipeline = SignalPipeline(publisher, "notify:test")
        
        runtime = MagicMock()
        runtime.symbol = "BTCUSDT"
        
        # Call with keyword argument as in strategy.py
        await pipeline.send_telegram_report(text="test report", runtime=runtime)
        
        # Verify xadd was called with the correct symbol
        call_args = publisher.r.xadd.call_args
        fields = call_args.kwargs['fields']
        self.assertEqual(fields['symbol'], "BTCUSDT")
        self.assertEqual(fields['text'], "test report")
        self.assertEqual(fields['type'], "report")

    async def test_send_report_standard(self):
        publisher = MockPublisher()
        pipeline = SignalPipeline(publisher, "notify:test")
        
        await pipeline.send_telegram_report(text="test report", symbol="ETHUSDT")
        
        call_args = publisher.r.xadd.call_args
        fields = call_args.kwargs['fields']
        self.assertEqual(fields['symbol'], "ETHUSDT")

if __name__ == "__main__":
    unittest.main()
