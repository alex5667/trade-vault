"""
Failure drill tests for TrailShadowSimulator.

Ensures that the nightly/hourly reporter pipelines do not crash when Redis
is unavailable or returns malformed data during shadow calibration reading 
or shadow results writing.
"""
from unittest.mock import Mock

import pytest

from services.trail_shadow_simulator import ShadowSimConfig, TrailShadowSimulator


@pytest.fixture
def mock_trades():
    from services.trail_shadow_simulator import _TradeForSim
    return {
        "BTCUSDT:trend": [
            _TradeForSim(
                symbol="BTCUSDT", regime="trend", pnl_net=10.0, one_r_money=20.0,
                mfe_pnl=30.0, giveback=0.0, entry_price=50000.0, notional=1000.0,
                trailing_started=False
            )
        ] * 10  # 10 identical trades to bypass minimum trade count
    }

def test_shadow_simulator_redis_hgetall_failure(mock_trades):
    # Setup mock Redis to raise an exception on hgetall
    mock_redis = Mock()
    mock_redis.hgetall.side_effect = ConnectionError("Redis cluster unavailable")

    cfg = ShadowSimConfig(
        enabled=True,
        atr_fallback_bps=50,
        key_prefix="trail:shadow",
        calib_prefix="trail:calib",
        ttl_sec=3600
    )

    # Instantiate the simulator with the mock Redis
    simulator = TrailShadowSimulator(redis_client=mock_redis, cfg=cfg)

    # Run the simulator. It should catch the exception, log it, and return empty results quietly
    results = simulator.run(mock_trades)

    assert len(results) == 0
    # hgetall should have been called exactly once before failing locally for the single bucket
    mock_redis.hgetall.assert_called_once_with("trail:calib:BTCUSDT:trend")

def test_shadow_simulator_redis_pipeline_failure(mock_trades):
    # Setup mock Redis where hgetall works, but the transaction pipeline fails
    mock_redis = Mock()
    mock_redis.hgetall.return_value = {
        "callback_atr_mult": "1.5",
        "activate_offset_bps": "5.0",
        "min_profit_lock_r": "0.1"
    }

    mock_pipe = Mock()
    mock_pipe.execute.side_effect = ConnectionError("Redis write timeout")
    mock_redis.pipeline.return_value = mock_pipe

    cfg = ShadowSimConfig(
        enabled=True,
        atr_fallback_bps=50,
        key_prefix="trail:shadow",
        calib_prefix="trail:calib",
        ttl_sec=3600
    )

    simulator = TrailShadowSimulator(redis_client=mock_redis, cfg=cfg)

    results = simulator.run(mock_trades)

    # The result computation succeeds and returns elements...
    assert len(results) == 1
    assert results[0].symbol == "BTCUSDT"

    # ...but the exception on write shouldn't crash the loop
    mock_pipe.execute.assert_called_once()
