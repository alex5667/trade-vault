# -*- coding: utf-8 -*-
from __future__ import annotations
"""
Regression: Calibrator Redis Errors Isolation (Before Canary 1.5)

Tests that calibrator modes fail safe and do not crash on Redis connection errors,
Timeouts, or malformed data.
"""

import json
import pytest
from unittest.mock import MagicMock, patch

from services.strong_gate_calibrator_service import (
    _load_state,
    _save_state,
    _apply_dynamic_cfg,
    _send_telegram,
    _discover_active_symbols,
    run_strong_gate_calibration,
)
from core.strong_gate_calibrator import StrongGateCalibResult

class ExceptionRaisingRedis:
    def get(self, *args, **kwargs):
        raise ConnectionError("Redis is down")
        
    def set(self, *args, **kwargs):
        raise ConnectionError("Redis is down")
        
    def hset(self, *args, **kwargs):
        raise ConnectionError("Redis is down")
        
    def xadd(self, *args, **kwargs):
        raise ConnectionError("Redis is down")
        
    def keys(self, *args, **kwargs):
        raise ConnectionError("Redis is down")
        
    def pipeline(self):
        pipe = MagicMock()
        pipe.execute.side_effect = ConnectionError("Pipeline failed")
        return pipe

def test_load_state_redis_error_returns_empty() -> None:
    r = ExceptionRaisingRedis()
    state = _load_state(r)
    assert state == {}

def test_save_state_redis_error_handled() -> None:
    r = ExceptionRaisingRedis()
    res = StrongGateCalibResult()
    # Should not raise
    _save_state(r, res, "run_id_123")
    
def test_apply_dynamic_cfg_redis_error_handled() -> None:
    r = ExceptionRaisingRedis()
    res = StrongGateCalibResult()
    # Should not raise
    _apply_dynamic_cfg(r, res, ["BTCUSDT"])

def test_send_telegram_redis_error_handled() -> None:
    r = ExceptionRaisingRedis()
    # Should not raise
    _send_telegram(r, "test")

def test_discover_symbols_redis_error_handled() -> None:
    r = ExceptionRaisingRedis()
    # Should return empty list and not raise
    syms = _discover_active_symbols(r)
    assert syms == []

@patch("services.strong_gate_calibrator_service.load_shadow_veto_outcomes")
@patch("services.strong_gate_calibrator_service.redis_lib")
def test_run_strong_gate_calibration_redis_error(mock_redis_lib, mock_load) -> None:
    mock_r = ExceptionRaisingRedis()
    # Configure mock_redis_lib to return our broken redis
    mock_redis_lib.Redis.from_url.return_value = mock_r
    
    mock_load.return_value = []
    
    # Should complete without crashing and return a result
    res = run_strong_gate_calibration(
        dsn="fake",
        redis_url="fake",
        window_hours=24,
        send_telegram=True,
    )
    assert res is not None
    # Defaults should apply since it couldn't load state
    assert res.effective_mode == "shadow"
