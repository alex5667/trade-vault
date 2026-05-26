from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.dyn_cfg_keys import DynCfgKeys as DK
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


@pytest.mark.asyncio
async def test_publish_of_inputs_mirrors_runtime_volatility_features():
    publisher = MagicMock()
    publisher.xadd_json = AsyncMock()
    publisher.r = MagicMock()
    atr_cache = MagicMock()
    atr_cache.get.return_value = 100.0

    with patch.dict(
        "os.environ",
        {
            "OF_INPUTS_PUBLISH_STRICT": "0",
            "OF_INPUTS_STREAM": RS.OF_INPUTS,
            "OF_INPUTS_STREAM_MAXLEN": "5000",
        },
        clear=False,
    ):
        pipeline = SignalPipeline(publisher=publisher, atr_cache=atr_cache)

    sync_redis = MagicMock()
    sync_redis.mget.return_value = [None] * 11
    sync_redis.hgetall.return_value = {}
    pipeline._sync_redis_client = sync_redis

    runtime = SimpleNamespace(
        dynamic_cfg={
            DK.VOL_FAST_BPS: 42.0,
            DK.VOL_SLOW_BPS: 38.0,
            DK.VOL_RATIO: 1.105,
            DK.VOL_RATIO_Z: 0.55,
            DK.VOL_REGIME_LABEL: "shock",
        },
        v13_tracker=SimpleNamespace(
            snapshot=lambda: {
                "garman_klass_vol": 0.012,
                "parkinson_vol": 0.013,
                "yang_zhang_vol": 0.014,
                "vol_of_vol": 0.33,
            }
        ),
        last_regime="trending_bear",
    )

    enriched_signal = {
        "sid": "s-vol-1",
        "indicators": {
            "obi_avg": 0.2,
            "pressure_per_min_ema": 1.5,
        },
    }

    await pipeline._publish_of_inputs(
        publisher=publisher,
        enriched_signal=enriched_signal,
        symbol="BTCUSDT",
        path="direct",
        runtime=runtime,
    )

    payload = publisher.xadd_json.await_args.kwargs["payload"]
    inds = payload["indicators"]

    assert inds["vol_fast_bps"] == 42.0
    assert inds["vol_slow_bps"] == 38.0
    assert inds["vol_ratio"] == 1.105
    assert inds["vol_ratio_z"] == 0.55
    assert inds["vol_regime_label"] == "shock"
    assert inds["vol_regime_code"] == 1.0
    assert inds["garman_klass_vol"] == 0.012
    assert inds["parkinson_vol"] == 0.013
    assert inds["yang_zhang_vol"] == 0.014
    assert inds["vol_of_vol"] == 0.33
