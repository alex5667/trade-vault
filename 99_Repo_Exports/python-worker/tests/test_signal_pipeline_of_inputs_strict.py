from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.orderflow.signal_pipeline import SignalPipeline
from core.redis_keys import RedisStreams as RS


def _make_pipeline(strict: bool) -> SignalPipeline:
    publisher = MagicMock()
    publisher.xadd_json = AsyncMock(side_effect=RuntimeError("redis xadd failed"))
    publisher.r = MagicMock()
    atr_cache = MagicMock()
    atr_cache.get.return_value = 100.0

    with patch.dict(
        "os.environ",
        {
            "OF_INPUTS_PUBLISH_STRICT": "1" if strict else "0",
            "OF_INPUTS_STREAM": RS.OF_INPUTS,
            "OF_INPUTS_STREAM_MAXLEN": "5000",
        },
        clear=False,
    ):
        return SignalPipeline(publisher=publisher, atr_cache=atr_cache)


@pytest.mark.asyncio
async def test_publish_of_inputs_logs_metric_and_swallow_when_not_strict():
    pipeline = _make_pipeline(strict=False)

    with patch("services.orderflow.signal_pipeline.of_inputs_publish_error_total") as metric:
        await pipeline._publish_of_inputs(
            publisher=pipeline.publisher,
            enriched_signal={"sid": "s1"},
            symbol="BTCUSDT",
            path="direct",
        )

    metric.labels.assert_called_once_with(
        symbol="BTCUSDT",
        stream=RS.OF_INPUTS,
        path="direct",
    )


@pytest.mark.asyncio
async def test_publish_of_inputs_raises_in_strict_mode():
    pipeline = _make_pipeline(strict=True)

    with patch("services.orderflow.signal_pipeline.of_inputs_publish_error_total") as metric:
        with pytest.raises(RuntimeError, match="redis xadd failed"):
            await pipeline._publish_of_inputs(
                publisher=pipeline.publisher,
                enriched_signal={"sid": "s1"},
                symbol="BTCUSDT",
                path="outbox",
            )

    metric.labels.assert_called_once_with(
        symbol="BTCUSDT",
        stream=RS.OF_INPUTS,
        path="outbox",
    )
