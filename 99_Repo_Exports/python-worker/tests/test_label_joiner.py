
import json
import unittest
from unittest.mock import MagicMock, patch

from core.decision_record import DecisionRecord
from services.label_joiner import LabelJoinerService
from core.redis_keys import RedisStreams as RS


class TestLabelJoinerService(unittest.TestCase):
    def setUp(self):
        self.mock_redis = MagicMock()
        with patch('services.label_joiner.get_redis', return_value=self.mock_redis):
            self.service = LabelJoinerService()

        # Mock decision store inside service
        self.service.decision_store = MagicMock()

    def test_process_trade_event_success_win(self):
        # Setup decision
        decision = DecisionRecord(
            sid="TEST:LONG:100",
            symbol="TEST",
            ts=1000,
            rule_score=0.9,
            final_permit=True
        )
        self.service.decision_store.load_decision.return_value = decision

        # Setup trade event
        trade_payload = {
            "sid": "TEST:LONG:100",
            "entry_price": 100.0,
            "exit_price": 110.0,
            "total_pnl": 10.0,
            "direction": "LONG",
            "sl": 90.0,
            "exit_ts_ms": 2000
        }
        fields = {
            "type": "POSITION_CLOSED",
            "data": json.dumps(trade_payload)
        }

        # Execute
        result = self.service.process_trade_event(RS.EVENTS_TRADES, "1-0", fields)

        # Verify
        assert result is True

        # Verify publish to trades:closed
        assert self.mock_redis.xadd.called
        # Check calls
        calls = self.mock_redis.xadd.call_args_list

        # Expect 2 calls: trades:closed and ml_replay_inputs_v1
        trades_closed_call = [c for c in calls if c[0][0] == "trades:closed"]
        ml_replay_call = [c for c in calls if c[0][0] == "ml_replay_inputs_v1"]

        assert len(trades_closed_call) == 1
        assert len(ml_replay_call) == 1

        # Check content of trades:closed
        args, kwargs = trades_closed_call[0]
        data = args[1]
        assert data["sid"] == "TEST:LONG:100"
        assert data["result"] == "WIN"
        assert float(data["r_multiple"]) == 1.0  # (110-100)/(100-90) = 1.0

    def test_process_trade_event_loss(self):
        decision = DecisionRecord(sid="TEST:SHORT:100", symbol="TEST", ts=1000)
        self.service.decision_store.load_decision.return_value = decision

        trade_payload = {
            "sid": "TEST:SHORT:100",
            "entry_price": 100.0,
            "exit_price": 110.0, # Price went up, short loss
            "total_pnl": -10.0,
            "direction": "SHORT",
            "sl": 110.0, # 10 risk
            "exit_ts_ms": 2000
        }
        fields = {
            "type": "POSITION_CLOSED",
            "data": json.dumps(trade_payload)
        }

        result = self.service.process_trade_event(RS.EVENTS_TRADES, "2-0", fields)
        assert result is True

        trades_closed_call = [c for c in self.mock_redis.xadd.call_args_list if c[0][0] == "trades:closed"]
        args, _ = trades_closed_call[0]
        data = args[1]
        assert data["result"] == "LOSS"
        # R = (100 - 110) / (110 - 100) = -10 / 10 = -1.0
        assert float(data["r_multiple"]) == -1.0

    def test_generic_event_ignored(self):
        fields = {"type": "ORDER_FILLED", "data": "{}"}
        result = self.service.process_trade_event(RS.EVENTS_TRADES, "3-0", fields)
        assert result is True
        assert not self.service.decision_store.load_decision.called

    def test_decision_not_found(self):
        self.service.decision_store.load_decision.return_value = None
        trade_payload = {"sid": "MISSING", "direction": "LONG"}
        fields = {"type": "POSITION_CLOSED", "data": json.dumps(trade_payload)}

        result = self.service.process_trade_event(RS.EVENTS_TRADES, "4-0", fields)
        assert result is True # Should ACK
        assert not self.mock_redis.xadd.called
