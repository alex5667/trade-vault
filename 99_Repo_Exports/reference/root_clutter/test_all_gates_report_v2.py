import sys
import unittest
from unittest.mock import MagicMock, patch

# Adjust path
sys.path.append("/home/alex/front/trade/scanner_infra/python-worker")

# Pre-mock dependencies to avoid ImportError or AttributeError during import of periodic_reporter
sys.modules["services.reporting_service"] = MagicMock()
sys.modules["services.trade_metrics_service"] = MagicMock()
sys.modules["services.trailing_edge_analyzer"] = MagicMock()
sys.modules["services.pnl_math"] = MagicMock()
sys.modules["core.redis_client"] = MagicMock()
sys.modules["common.log"] = MagicMock()
sys.modules["handlers.crypto_orderflow.utils.log_sampler"] = MagicMock()
sys.modules["services.edge_gate_reporter"] = MagicMock() # Optional import
sys.modules["domain.normalizers"] = MagicMock()
sys.modules["services.trade_closed_hydrator"] = MagicMock()
sys.modules["infra.redis_repo"] = MagicMock()

# Configure mocks
tm_module = sys.modules["services.trade_metrics_service"]
tm_module.TradeMetricsService = MagicMock()

# Now import the target module
from services.periodic_reporter import PeriodicReporter

# Test class
class TestAllGatesReport(unittest.TestCase):
    @patch("services.periodic_reporter.get_redis")
    @patch("services.periodic_reporter.RedisTradeRepository") 
    def test_all_gates_logic(self, mock_repo, mock_get_redis):
        # Setup Reporter
        reporter = PeriodicReporter()
        
        # Mock TradeMetricsService instance on the reporter
        mock_tm = MagicMock()
        reporter.tm = mock_tm
        
        # When new_metrics is called, return a fresh dict so we can verify population
        def new_metrics_side_effect():
            return {"total_trades": 0, "wins": 0, "total_pnl": 0.0, "wins_strict": 0}
        mock_tm.new_metrics.side_effect = new_metrics_side_effect
        
        # Mock accumulate_trade to actually update the dict (simplified)
        def accumulate_side_effect(m, t):
            m["total_trades"] += 1
            m["total_pnl"] += float(t.get("pnl", 0))
            return True
        mock_tm.accumulate_trade.side_effect = accumulate_side_effect
        
        # Mock finalize to do nothing
        mock_tm.finalize.return_value = None

        # Sample Trades
        # 1. Matches logic: Virtual + VETO Passed + ML Passed
        t1 = {"order_id": "1", "is_virtual": "1", "v_gate_status": "passed", "smt_leader_confirm": "0", "pnl": "10"}
        # 2. Blocked by ML: Virtual + VETO Passed + ML Blocked
        t2 = {"order_id": "2", "is_virtual": "1", "v_gate_status": "blocked", "smt_leader_confirm": "0", "pnl": "5"}
        # 3. Blocked by VETO: Virtual + VETO Failed + ML Passed
        # Veto logic: countertrend + confirmed leader + high coherence
        # Let's say we mock the SMT logic check or assume the logic inside _generate checks these fields.
        # To trigger 'is_vetoed=True', we need: 
        # side != ld_norm, stm_conf=1, stm_coh>=0.65
        t3 = {
            "order_id": "3", 
            "is_virtual": "1", 
            "v_gate_status": "passed", 
            "pnl": "20",
            "smt_leader_confirm": "1",
            "smt_coh": "0.9",
            "smt_leader_dir": "UP",
            "side": "SHORT" # Countertrend to UP
        }
        
        # Mock the trades iterator
        reporter._iter_recent_trades_window = MagicMock(return_value=[t1, t2, t3])
        
        # Mock _send_report to intercept the metrics
        reporter._send_report = MagicMock()
        
        # Mock normalizers that are imported inside the method
        with patch("domain.normalizers.strategy_from_source") as mock_sfs,              patch("domain.normalizers.canon_source", return_value="src"),              patch("domain.normalizers.canon_symbol", return_value="sym"):
            
            # RUN
            reporter._generate_and_send_report_internal("test_src", "test_sym", window_seconds=3600)
            
            # VERIFY
            # Check if _send_report was called
            self.assertTrue(reporter._send_report.called)
            args, _ = reporter._send_report.call_args
            metrics = args[2] # 3rd arg is metrics
            
            # Check keys
            self.assertIn("shadow_all_gates", metrics)
            m_all_gates = metrics["shadow_all_gates"]
            
            # t1 should be included
            # t2 (ML blocked) should be excluded
            # t3 (SMT vetoed) should be excluded
            
            print(f"Captured m_all_gates: {m_all_gates}")
            
            self.assertEqual(m_all_gates["total_trades"], 1, "Should only have 1 trade (t1)")
            self.assertEqual(m_all_gates["total_pnl"], 10.0, "PnL should be 10.0 (from t1)")
            
            print("TEST PASSED: Logic for m_all_gates verified.")

if __name__ == '__main__':
    unittest.main()
