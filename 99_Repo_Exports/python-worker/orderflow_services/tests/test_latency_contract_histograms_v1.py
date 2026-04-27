from __future__ import annotations

import asyncio

from orderflow_services.latency_contract_exporter_v1 import (
    _HISTOGRAM_OBSERVED_TOKENS,
    _extract_event_lag_ms,
    _observe_histograms_if_fresh,
    _scrape_once,
    latency_contract_event_lag_latest_ms,
    latency_contract_event_lag_ms,
    latency_contract_stage_ms,
)


class FakeRedisAsync:
    def __init__(self, rows: dict[str, dict[str, str]]) -> None:
        self.rows = rows

    async def keys(self, pattern: str):
        prefix = pattern.rstrip('*')
        return [k for k in self.rows if k.startswith(prefix)]

    async def hgetall(self, key: str):
        return dict(self.rows.get(key, {}))


def test_extract_event_lag_ms_handles_missing_or_reversed() -> None:
    assert _extract_event_lag_ms({}) == 0
    assert _extract_event_lag_ms({'ts_event_ms': '10', 'last_ts_ms': '5'}) == 0
    assert _extract_event_lag_ms({'ts_event_ms': '10', 'last_ts_ms': '25'}) == 15


def test_observe_histograms_if_fresh_dedupes_same_token() -> None:
    obs_key = 'unit:test:latency:btc'
    _HISTOGRAM_OBSERVED_TOKENS.pop(obs_key, None)
    row = {'last_ts_ms': '1000', 'last_duration_ms': '42', 'ts_event_ms': '960'}
    before_stage = latency_contract_stage_ms.labels(service='svc_ut', stage='feature_to_emit', symbol='BTCUSDT_UH1')._sum.get()
    before_lag_sum = latency_contract_event_lag_ms.labels(service='svc_ut', stage='feature_to_emit', symbol='BTCUSDT_UH1')._sum.get()

    _observe_histograms_if_fresh(obs_key, 'svc_ut', 'feature_to_emit', 'BTCUSDT_UH1', row)
    _observe_histograms_if_fresh(obs_key, 'svc_ut', 'feature_to_emit', 'BTCUSDT_UH1', row)

    assert latency_contract_stage_ms.labels(service='svc_ut', stage='feature_to_emit', symbol='BTCUSDT_UH1')._sum.get() == before_stage + 42.0
    assert latency_contract_event_lag_ms.labels(service='svc_ut', stage='feature_to_emit', symbol='BTCUSDT_UH1')._sum.get() == before_lag_sum + 40.0


def test_scrape_once_updates_event_lag_latest_and_histograms() -> None:
    key = 'metrics:latency_contract:last:python_worker:feature_to_emit:ETHUSDT_UH2'
    _HISTOGRAM_OBSERVED_TOKENS.pop(key, None)
    redis = FakeRedisAsync({
        key: {
            'last_duration_ms': '55',
            'last_ts_ms': '5000',
            'ts_event_ms': '4925',
        }
    })
    asyncio.run(_scrape_once(redis, 'metrics:latency_contract:last', stale_s=60, budgets={'feature_to_emit': 100}))

    assert latency_contract_event_lag_latest_ms.labels(service='python_worker', stage='feature_to_emit', symbol='ETHUSDT_UH2')._value.get() == 75.0
