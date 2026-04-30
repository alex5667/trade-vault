import json
import pytest
from unittest.mock import AsyncMock

from services.orderflow.sentiment_context import aread_sentiment_context

@pytest.mark.asyncio
async def test_aread_sentiment_context_success():
    redis_mock = AsyncMock()
    mock_data = {
        "schema_version": 1
        "provider": "alternative_me"
        "ts_ms": 1760000000000
        "ingest_ts_ms": 1760000001000
        "fear_greed_value": 21
        "fear_greed_delta_1d": -2
        "fear_greed_delta_7d": 5
        "sentiment_regime": "extreme_fear"
        "sentiment_risk_multiplier": 0.7
        "value_classification": "Extreme Fear"
        "time_until_update_sec": 68499
        "quality_status": "OK"
    }
    redis_mock.get.return_value = json.dumps(mock_data).encode("utf-8")

    res = await aread_sentiment_context(redis_mock)
    assert res is not None
    assert res.fear_greed_value == 21
    assert res.sentiment_regime == "extreme_fear"
    assert res.sentiment_risk_multiplier == 0.7
    assert res.provider == "alternative_me"

@pytest.mark.asyncio
async def test_aread_sentiment_context_none():
    redis_mock = AsyncMock()
    redis_mock.get.return_value = None

    res = await aread_sentiment_context(redis_mock)
    assert res is None

@pytest.mark.asyncio
async def test_aread_sentiment_context_invalid_json():
    redis_mock = AsyncMock()
    redis_mock.get.return_value = b"{invalid"

    res = await aread_sentiment_context(redis_mock)
    assert res is None
