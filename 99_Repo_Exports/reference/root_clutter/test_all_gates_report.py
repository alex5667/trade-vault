import os
import sys
import logging
from unittest.mock import MagicMock, patch

# Adjust path to include python-worker
sys.path.append("/home/alex/front/trade/scanner_infra/python-worker")

# Mock Redis and other dependencies
with patch("redis.Redis") as mock_redis,      patch("services.reporting_service.ReportingService") as mock_reporting,      patch("services.trade_metrics_service.TradeMetricsService") as mock_tm_cls:

    # Setup mocks
    mock_r = mock_redis.return_value
    mock_r.get.return_value = None
    
    # Mock TradeMetricsService instance
    mock_tm = mock_tm_cls.return_value
    mock_tm.new_metrics.side_effect = lambda: {"total_trades": 0, "wins": 0, "total_pnl": 0.0}
    mock_tm.accumulate_trade.return_value = True

    from services.periodic_reporter import PeriodicReporter

    # Instantiate reporter
    reporter = PeriodicReporter()
    reporter.tm = mock_tm  # Inject mock TM

    # Mock _iter_recent_trades_window to return sample trades
    sample_trades = [
        # Trade 1: Passed ML, SMT allowed (should be in all_gates)
        {"order_id": "1", "is_virtual": "1", "v_gate_status": "passed", "smt_leader_confirm": "0", "pnl": "10"},
        # Trade 2: Failed ML, SMT allowed
        {"order_id": "2", "is_virtual": "1", "v_gate_status": "blocked", "smt_leader_confirm": "0", "pnl": "-5"},
         # Trade 3: Passed ML, SMT vetoed
        {"order_id": "3", "is_virtual": "1", "v_gate_status": "passed", "smt_leader_confirm": "1", "smt_coh": "0.9", "smt_leader_dir": "UP", "side": "SHORT", "pnl": "20"},
    ]
    reporter._iter_recent_trades_window = MagicMock(return_value=sample_trades)
    
    # Mock _send_report to just print what it receives
    original_send_report = reporter._send_report
    reporter._send_report = MagicMock()

    # Trigger report generation
    print("Running report generation...")
    reporter._generate_and_send_report_internal("test_source", "TESTUSDT", window_seconds=3600)

    # Verify calls
    print("\nVerifying results...")
    
    # Check if m_all_gates was created and populated
    # We can check specific calls to accumulate_trade
    # Trade 1: Passed ML + SMT(allowed) -> Should be in m_all_gates
    # Trade 2: Failed ML -> Should NOT be in m_all_gates
    # Trade 3: Passed ML + SMT(vetoed) -> Should NOT be in m_all_gates
    
    # We can verify by looking at the 'metrics' passed to _send_report
    args, _ = reporter._send_report.call_args
    metrics = args[2]
    
    # Check for shadow_all_gates existence
    if "shadow_all_gates" in metrics:
        print("SUCCESS: 'shadow_all_gates' found in metrics.")
        # In a real run, TradeMetricsService would aggregate this. 
        # Since we mocked TM, we just check the key exists and was passed.
    else:
        print("FAILURE: 'shadow_all_gates' NOT found in metrics.")

    # We can also verify the logic by inspecting the code or running with a smarter mock 
    # that actually updates the dict. Let's do a simple check.
    
