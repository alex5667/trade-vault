
import sys
import os
import json
import unittest
from unittest.mock import MagicMock, patch

# Add path
# [AUTOGRAVITY CLEANUP] sys.path.append("/home/alex/front/trade/scanner_infra/python-worker")

from handlers.crypto_orderflow.pipeline.orchestrator import SignalOrchestrator
from edge_gate_ingestor import PostgresWriter

class TestEdgeGatePipeline(unittest.TestCase):
    def test_orchestrator_publishing(self):
        """Verify orchestrator publishes correctly formatted event to Redis Stream."""
        # Setup Mocks
        ctx = MagicMock()
        ctx.redis = MagicMock()
        ctx.symbol = "BTCUSDT"
        ctx.ts = 1700000000000
        
        cand = MagicMock()
        cand.signal_id = "sig-123"
        cand.kind = "test-kind"
        
        cost_decision = MagicMock()
        cost_decision.passed = False
        cost_decision.veto_reason = "VETO_TEST"
        cost_decision.expected_edge_bps = 5.0
        cost_decision.required_edge_bps = 10.0
        cost_decision.edge_ratio = 0.5
        cost_decision.cost_multiplier = 3.0
        cost_decision.fees_bps = 4.0
        cost_decision.slippage_bps = 4.0
        cost_decision.buffer_bps = 0.0
        cost_decision.total_costs_bps = 8.0
        
        # Override ENV to force sync publishing (mock 100% sample)
        with patch.dict(os.environ, {
            "EDGE_GATE_EVENTS_MODE": "stream",
            "EDGE_GATE_SAMPLE_VETO": "1.0",
            "EDGE_GATE_EVENTS_STREAM": "test:stream"
        }):
            orch = SignalOrchestrator(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
            # Directly call the helper to test logic
            orch._maybe_publish_edge_event(ctx, cand, cost_decision, "test-kind")
            
            # Verify Redis XADD
            ctx.redis.xadd.assert_called_once()
            args, kwargs = ctx.redis.xadd.call_args
            
            stream = args[0]
            fields = args[1]
            
            self.assertEqual(stream, "test:stream")
            self.assertEqual(fields["signal_id"], "sig-123")
            self.assertEqual(fields["gate_name"], "edge_cost")
            self.assertEqual(fields["passed"], "0")
            self.assertEqual(fields["veto_code"], "VETO_TEST")
            self.assertEqual(float(fields["margin_bps"]), -5.0) # 5 - 10

    @patch("edge_gate_ingestor.psycopg2")
    def test_ingestor_batch_write(self, mock_psycopg2):
        """Verify proper sql generation and execution in ingestor."""
        writer = PostgresWriter("dsn")
        
        mock_conn = writer.pool.getconn.return_value
        mock_cursor = mock_conn.cursor.return_value
        
        events = [
            {
                "signal_id": "s1", "symbol": "BTC", "gate_name": "g1", "gate_version": 2, "stage": "pre",
                "ts_ms": 100, "passed": True, "veto_code": None, "edge_source": "src",
                "exp_bps": 10.0, "req_bps": 5.0, "margin_bps": 5.0, "edge_ratio": 2.0,
                "k": 1.0, "fees_bps": 1.0, "slip_bps": 1.0, "buf_bps": 0.0, "total_costs_bps": 2.0,
                "ctx_obj": {}
            }
        ]
        
        # Patch execute_values to check logic
        with patch("edge_gate_ingestor.extras.execute_values") as mock_exec:
            writer.write_batch(events)
            
            self.assertTrue(mock_exec.called)
            # Check SQL contains ON CONFLICT
            sql = mock_exec.call_args[0][1]
            self.assertIn("ON CONFLICT", sql)
            self.assertIn("INSERT INTO edge_gate_events", sql)

if __name__ == '__main__':
    unittest.main()
