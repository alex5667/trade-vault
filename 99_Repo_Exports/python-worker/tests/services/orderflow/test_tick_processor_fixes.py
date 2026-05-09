import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.orderflow.service_config import TickCfg
from services.orderflow.tick_processor import TickProcessor


def test_unknown_side_default_is_ignore_delta():
    cfg = TickCfg()
    assert cfg.unknown_side_policy == "ignore_delta"

@pytest.mark.asyncio
async def test_dq_quarantine_tick_serialized_synchronously():
    policy = MagicMock()
    strategy = MagicMock()
    gate = AsyncMock()
    gate.allows.return_value = True
    flusher = MagicMock()
    main = AsyncMock()
    ticks = AsyncMock()

    tp = TickProcessor(
        tick_dq_policy=policy,
        strategy_fn=lambda: strategy,
        gate=gate,
        flusher=flusher,
        health_metrics=None,
        main_redis=main,
        ticks_redis=ticks,
        drop_on_lag=False,
        max_lag_ms=60000,
        max_ts_skew_ms=5000,
        unknown_side_policy="ignore_delta",
        unknown_side_quarantine_stream="stream:tick_dq:quarantine",
        unknown_side_quarantine_sample=1.0,
        unknown_side_quarantine_maxlen=1000,
        exec_quarantine_enable=True,
        quarantine_stream="stream:tick_dq:quarantine",
        lag_trackers={},
        lag_export_counters={}
    )

    original_tick = {"event_ts_ms": 100, "price": 50000}
    # To prove it's serialized synchronously, we modify original_tick right after the call
    tp._xadd_dq_quarantine(original_tick, "test_reason")
    original_tick["price"] = 99999

    # Give the event loop a beat to execute the background task inside _xadd_dq_quarantine
    await asyncio.sleep(0.01)

    main.xadd.assert_called_once()
    payload = main.xadd.call_args[0][1]

    assert payload["reason"] == "test_reason"
    assert "data" in payload
    # The data string should have the price from BEFORE the mutation
    assert '"price": 50000' in payload["data"]
