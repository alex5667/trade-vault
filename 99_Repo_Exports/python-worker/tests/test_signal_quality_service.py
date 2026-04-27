
import unittest
from unittest.mock import MagicMock, patch, call
from services.signal_quality_service import SignalQualityService

class TestSignalQualityService(unittest.TestCase):
    def setUp(self):
        self.mock_redis = MagicMock()
        with patch('services.signal_quality_service.get_redis', return_value=self.mock_redis):
            self.service = SignalQualityService()
            
    def test_process_win(self):
        fields = {
            "sid": "TEST:LONG:100",
            "symbol": "BTCUSD",
            "result": "WIN",
            "r_multiple": "2.5",
            "pnl": "50.0"
        }
        
        pipe = MagicMock()
        self.mock_redis.pipeline.return_value = pipe
        
        result = self.service.process_closed_trade("trades:closed", "1-0", fields)
        assert result is True
        
        # Verify pipeline
        assert pipe.execute.called
        
        # Check calls for 'global' slice
        # hincrby(key, field, val)
        # We expect:
        # hincrby(signal_quality:global, count, 1)
        # hincrbyfloat(signal_quality:global, sum_r, 2.5)
        # hincrbyfloat(signal_quality:global, sum_pnl, 50.0)
        # hincrby(signal_quality:global, wins, 1)
        
        # And same for signal_quality:symbol:BTCUSD
        
        # Let's verify at least one call structure roughly
        calls = pipe.hincrby.call_args_list
        global_count = call("signal_quality:global", "count", 1)
        symbol_count = call("signal_quality:symbol:BTCUSD", "count", 1)
        assert global_count in calls
        assert symbol_count in calls
        
        global_wins = call("signal_quality:global", "wins", 1)
        assert global_wins in calls

        float_calls = pipe.hincrbyfloat.call_args_list
        global_r = call("signal_quality:global", "sum_r", 2.5)
        assert global_r in float_calls

    def test_process_loss(self):
        fields = {
            "sid": "TEST:SHORT:100",
            "symbol": "ETHUSD",
            "result": "LOSS",
            "r_multiple": "-1.0",
            "pnl": "-10.0"
        }
        
        pipe = MagicMock()
        self.mock_redis.pipeline.return_value = pipe
        
        result = self.service.process_closed_trade("trades:closed", "2-0", fields)
        assert result is True
        
        calls = pipe.hincrby.call_args_list
        assert call("signal_quality:global", "losses", 1) in calls
        assert call("signal_quality:symbol:ETHUSD", "losses", 1) in calls
        
        float_calls = pipe.hincrbyfloat.call_args_list
        assert call("signal_quality:global", "sum_r", -1.0) in float_calls

    def test_missing_data(self):
        fields = {"result": "WIN"} # Missing sid/symbol
        result = self.service.process_closed_trade("trades:closed", "3-0", fields)
        assert result is True # Ack invalid
        assert not self.mock_redis.pipeline.called
