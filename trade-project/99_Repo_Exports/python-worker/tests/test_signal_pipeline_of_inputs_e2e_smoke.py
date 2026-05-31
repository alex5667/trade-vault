from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import fakeredis
import pytest

from core.dyn_cfg_keys import DynCfgKeys as DK
from core.redis_keys import RedisStreams as RS
from services.async_signal_publisher import AsyncSignalPublisher
from services.orderflow.signal_pipeline import SignalPipeline
import tools.export_of_inputs_ndjson_v2 as ex

from tests.test_signals_of_inputs_golden import (
    FIXTURE as GOLDEN_FIXTURE,
    REQUIRED_TOP_FIELDS,
    V13_COVERAGE_FLOOR,
    V14_COVERAGE_FLOOR,
    V15_COVERAGE_FLOOR,
)


class _AsyncRedisAdapter:
    """Async wrapper over a sync fakeredis client for publisher smoke tests."""

    def __init__(self, sync_client):
        self._sync = sync_client

    async def xadd(self, stream: str, fields: dict, maxlen: int = 0, approximate: bool = True):
        return self._sync.xadd(stream, fields, maxlen=maxlen or None, approximate=approximate)


def _make_pipeline_and_runtime():
    sync_r = fakeredis.FakeRedis(decode_responses=False)
    async_r = _AsyncRedisAdapter(sync_r)
    publisher = AsyncSignalPublisher(
        redis_client=async_r,
        source="test_of_inputs_e2e",
        metrics_prefix="test",
        logger=None,
    )

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
    return sync_r, pipeline, runtime


def _export_rows(monkeypatch, sync_r, out: Path, state: Path):
    class _RedisMod:
        class Redis:
            @staticmethod
            def from_url(*_a, **_k):
                return sync_r

    monkeypatch.setitem(ex.sys.modules, "redis", _RedisMod())
    return ex.export_of_inputs(
        redis_url="redis://fake",
        stream=RS.OF_INPUTS,
        field="payload",
        out_path=out,
        state_file=state,
        resume=True,
        start_id="0-0",
        end_id="+",
        batch=100,
        max_records=10,
        validate=True,
        quiet=True,
    )


@pytest.mark.asyncio
async def test_publish_of_inputs_to_stream_and_export_ndjson_smoke(tmp_path: Path, monkeypatch):
    sync_r, pipeline, runtime = _make_pipeline_and_runtime()

    enriched_signal = {
        "sid": "smoke-vol-1",
        "signal_id": "smoke-vol-1",
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "ts_ms": 1700000000000,
        "schema_version": "v1",
        "price": 100.0,
        "indicators": {
            "obi_avg": 0.2,
            "pressure_per_min_ema": 1.5,
        },
    }

    await pipeline._publish_of_inputs(
        publisher=pipeline.publisher,
        enriched_signal=enriched_signal,
        symbol="BTCUSDT",
        path="direct",
        runtime=runtime,
    )

    out = tmp_path / "of_inputs.ndjson"
    state = tmp_path / "of_inputs.state"
    st = _export_rows(monkeypatch, sync_r, out, state)

    assert st.written == 1
    row = json.loads(out.read_text(encoding="utf-8").strip())
    inds = row["indicators"]

    assert row["symbol"] == "BTCUSDT"
    assert row["sid"] == "smoke-vol-1"
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


@pytest.mark.asyncio
async def test_fresh_generated_payload_keeps_golden_coverage_floors(tmp_path: Path, monkeypatch):
    sync_r, pipeline, runtime = _make_pipeline_and_runtime()
    fixture = json.loads(GOLDEN_FIXTURE.read_text(encoding="utf-8"))
    enriched_signal = deepcopy(fixture["payload"])
    enriched_signal["sid"] = "fresh-golden-coverage-1"
    enriched_signal["signal_id"] = "fresh-golden-coverage-1"
    enriched_signal["ts_ms"] = 1700000001234
    enriched_signal["symbol"] = enriched_signal.get("symbol") or "BTCUSDT"
    enriched_signal["price"] = float(enriched_signal.get("price") or 100.0)
    enriched_signal["indicators"] = deepcopy(enriched_signal.get("indicators") or {})
    for k in (
        "vol_fast_bps",
        "vol_slow_bps",
        "vol_ratio",
        "vol_ratio_z",
        "vol_regime_label",
        "vol_regime_code",
        "garman_klass_vol",
        "parkinson_vol",
        "yang_zhang_vol",
        "vol_of_vol",
    ):
        enriched_signal["indicators"].pop(k, None)

    await pipeline._publish_of_inputs(
        publisher=pipeline.publisher,
        enriched_signal=enriched_signal,
        symbol=str(enriched_signal["symbol"]),
        path="direct",
        runtime=runtime,
    )

    out = tmp_path / "fresh_golden.ndjson"
    state = tmp_path / "fresh_golden.state"
    st = _export_rows(monkeypatch, sync_r, out, state)
    assert st.written == 1

    row = json.loads(out.read_text(encoding="utf-8").strip())
    inds = row["indicators"]
    missing = [k for k in REQUIRED_TOP_FIELDS if k not in row]
    assert not missing, f"payload missing required fields: {missing}"

    from core.ml_feature_schema_v13_of import V13_OF_NUMERIC_KEYS
    from core.ml_feature_schema_v14_of import V14_OF_NUMERIC_KEYS
    from core.ml_feature_schema_v15_of import V15_OF_NUMERIC_KEYS

    cov_v13 = len(set(V13_OF_NUMERIC_KEYS) & set(inds)) / len(V13_OF_NUMERIC_KEYS)
    cov_v14 = len(set(V14_OF_NUMERIC_KEYS) & set(inds)) / len(V14_OF_NUMERIC_KEYS)
    cov_v15 = len(set(V15_OF_NUMERIC_KEYS) & set(inds)) / len(V15_OF_NUMERIC_KEYS)

    assert cov_v13 >= V13_COVERAGE_FLOOR, f"v13_of coverage {cov_v13:.1%} < floor {V13_COVERAGE_FLOOR:.0%}"
    assert cov_v14 >= V14_COVERAGE_FLOOR, f"v14_of coverage {cov_v14:.1%} < floor {V14_COVERAGE_FLOOR:.0%}"
    assert cov_v15 >= V15_COVERAGE_FLOOR, f"v15_of coverage {cov_v15:.1%} < floor {V15_COVERAGE_FLOOR:.0%}"

    assert inds["vol_fast_bps"] == 42.0
    assert inds["vol_slow_bps"] == 38.0
    assert inds["vol_ratio"] == 1.105
    assert inds["vol_ratio_z"] == 0.55
    assert inds["vol_regime_label"] == "shock"
    assert inds["vol_of_vol"] == 0.33
