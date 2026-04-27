import asyncio
import numpy as np
from unittest.mock import AsyncMock, patch
import pytest

from tools.nightly_slippage_calibrator_v1 import run

@pytest.fixture
def mock_env(monkeypatch):
    monkeypatch.setenv("CALIB_SPREAD_MULT", "0.5")
    monkeypatch.setenv("CALIB_SIZE_REF_USD", "10000.0")
    monkeypatch.setenv("CALIB_SIZE_POWER", "1.0")
    monkeypatch.setenv("CALIB_QUANTILE", "0.80")
    monkeypatch.setenv("CALIB_C_MIN", "1.0")
    monkeypatch.setenv("CALIB_C_MAX", "50.0")
    monkeypatch.setenv("CALIB_X_MIN", "1e-6")
    monkeypatch.setenv("CALIB_EMA_ALPHA", "0.2")

@pytest.mark.asyncio
async def test_run_calibration_success(mock_env):
    mock_redis = AsyncMock()
    mock_conn = AsyncMock()
    
    # 11 rows for 'BTCUSDT', 'NORMAL'
    # spread_bps=2.0 -> spread_part = 1.0
    # worse_slip = 5.0 -> impact_part = 4.0
    # proxy = 10.0, size=10000 -> x = 10.0
    # ratio = 4.0 / 10.0 = 0.4
    rows = []
    for i in range(11):
        rows.append({
            'sym': 'BTCUSDT',
            'exec_regime_bucket': 'NORMAL',
            'spread_bps': 2.0,
            'impact_proxy': 10.0 + i, # varying x
            'size_usd': 10000.0,
            'realized_slip_worse_bps': 1.0 + (10.0 + i) * 0.4 # ensure ratio=0.4
        })
        
    mock_conn.fetch.return_value = rows
    mock_redis.get.return_value = "0.5" # old value
    
    with patch("tools.nightly_slippage_calibrator_v1.redis.Redis.from_url", return_value=mock_redis), \
         patch("tools.nightly_slippage_calibrator_v1.asyncpg.connect", new_callable=AsyncMock, return_value=mock_conn):
        
        result = await run_calibration()
        
        assert result is True
        mock_conn.fetch.assert_called_once()
        
        # Checking what it setted in Redis
        # fit = approx 0.4
        # old = 0.5, new = 0.8 * 0.5 + 0.2 * 0.4 = 0.48
        # Wait, since C_MIN=1.0, and fit=0.4, it should be clamped to 1.0.
        # old = 0.5 (from redis mock), new = 0.8 * 0.5 + 0.2 * 1.0 = 0.4 + 0.2 = 0.60
        
        mock_redis.set.assert_called_once_with("cfg:slippage_decomp_impact_coeff_bps:BTCUSDT:NORMAL", "0.6")

@pytest.mark.asyncio
async def test_run_calibration_insufficient_samples(mock_env):
    mock_redis = AsyncMock()
    mock_conn = AsyncMock()
    
    rows = []
    for i in range(5): # less than 10
        rows.append({
            'sym': 'BTCUSDT',
            'exec_regime_bucket': 'NORMAL',
            'spread_bps': 2.0,
            'impact_proxy': 10.0,
            'size_usd': 10000.0,
            'realized_slip_worse_bps': 5.0
        })
        
    mock_conn.fetch.return_value = rows
    
    with patch("tools.nightly_slippage_calibrator_v1.redis.Redis.from_url", return_value=mock_redis), \
         patch("tools.nightly_slippage_calibrator_v1.asyncpg.connect", new_callable=AsyncMock, return_value=mock_conn):
        
        result = await run_calibration()
        assert result is True
        mock_redis.set.assert_not_called()
