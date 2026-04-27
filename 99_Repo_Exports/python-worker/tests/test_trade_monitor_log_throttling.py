from __future__ import annotations
import pytest
from unittest.mock import MagicMock
from services.trade_monitor import TradeMonitorService
from domain.models import SignalNorm

def test_trade_monitor_log_throttling(caplog):
    # Setup mock dependencies
    redis_mock = MagicMock()
    repo_mock = MagicMock()
    
    mon = TradeMonitorService(redis_client=redis_mock, repo=repo_mock)
    
    # Helper to mock signal processing parts
    mon._normalize_signal = MagicMock(side_effect=lambda x: SignalNorm(
        sid=x["sid"], symbol=x["symbol"], direction="LONG", entry_price=1.0, sl=0.9, tp_levels=[1.1], 
        source="Test", tf="1m", entry_ts_ms=1000, strategy="Test", lot=1.0
    ))
    mon._get_symbol_lock = MagicMock(return_value=MagicMock(__enter__=MagicMock(), __exit__=MagicMock()))
    mon._sid_claim = MagicMock(return_value=True)
    mon._sid_finalize = MagicMock()
    mon.repo.persist_signal = MagicMock()
    mon.repo.save_open = MagicMock()
    mon.repo.append_event = MagicMock()
    
    # Process 200 signals
    import logging
    caplog.set_level(logging.INFO)
    
    for i in range(1, 201):
        mon.on_signal({"sid": f"sig-{i}", "symbol": "BTCUSDT"})
    
    # Filter for OPEN messages
    open_logs = [record.message for record in caplog.records if "OPEN" in record.message]
    
    # Expected log calls: 1st, 100th, 200th
    assert len(open_logs) == 3
    
    # Verify the messages (check for the counter suffix)
    assert "[#1]" in open_logs[0]
    assert "[#100]" in open_logs[1]
    assert "[#200]" in open_logs[2]
