from __future__ import annotations
import pytest
from unittest.mock import MagicMock, patch
import logging

def test_symbol_runtime_sampler_initialization():
    config = {"delta_window": 120, "tick_buffer": 500}
    
    with patch("services.crypto_orderflow_service.PressureTracker"), \
         patch("services.crypto_orderflow_service.BurstCandidateSelector"), \
         patch("services.crypto_orderflow_service.RollingRobustZ"), \
         patch("services.crypto_orderflow_service.EffQuoteCalibrator"):
        
        from services.crypto_orderflow_service import SymbolRuntime
        runtime = SymbolRuntime(symbol="BTCUSDT", config=config)
        
        assert hasattr(runtime, "signal_emit_log_sampler")
        assert runtime.signal_emit_log_sampler.sample_rate == 1 # Updated implementation: 1 by default
        assert runtime.delta_log_sampler.sample_rate == 20 # Updated implementation: 20 by default (1/0.05)
        assert runtime.weak_signal_log_sampler.sample_rate == 20 # Updated implementation: 20 by default (1/0.05)
        assert hasattr(runtime, "loop_log_sampler")
        assert runtime.loop_log_sampler.sample_rate == 10000

def test_log_throttling_logic(caplog):
    from handlers.crypto_orderflow.utils.log_sampler import LogSampler
    
    weak_sampler = LogSampler(sample_rate=50)
    emit_sampler = LogSampler(sample_rate=10000)
    
    logger = logging.getLogger("test_logger")
    caplog.set_level(logging.INFO)
    
    # Simulate processing with "delta_spike" (using emit_sampler)
    for i in range(1, 25001):
        primary_reason = "delta_spike"
        if primary_reason == "weak_progress":
            if weak_sampler.should_log("weak_progress"):
                logger.info("emit signal %s", primary_reason)
        else:
            if emit_sampler.should_log(primary_reason):
                logger.info("emit signal %s", primary_reason)
                
    logs = [record.message for record in caplog.records if "emit signal" in record.message]
    assert len(logs) == 3 # 1, 10001, 20001
    
    caplog.clear()
    
    # Simulate processing with "weak_progress" (using weak_sampler)
    for i in range(1, 151):
        primary_reason = "weak_progress"
        if primary_reason == "weak_progress":
            if weak_sampler.should_log("weak_progress"):
                logger.info("emit signal %s", primary_reason)
        else:
            if emit_sampler.should_log(primary_reason):
                logger.info("emit signal %s", primary_reason)
                
    logs = [record.message for record in caplog.records if "emit signal" in record.message]
    assert len(logs) == 3 # 1, 51, 101
