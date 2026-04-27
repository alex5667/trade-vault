"""Tests for latency_contract_slo_gate_v1.evaluate_once()."""
from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from orderflow_services.latency_contract_slo_gate_v1 import evaluate_once, Cfg


class FakeRedis:
    def __init__(self, data):
        self.data = data

    def hgetall(self, key):
        return dict(self.data.get(key, {}))


def _cfg(symbols=('BTCUSDT',), stale_s=999999999):
    return Cfg(
        redis_url='redis://x',
        key_prefix='metrics:latency_contract:last',
        summary_key='metrics:latency_contract:slo:last',
        interval_s=10.0,
        stale_s=stale_s,
        symbols=symbols,
    )


def test_slo_gate_detects_missing_external_stage():
    """Only Python stages present — Go and NestJS stages missing → gate_ok=0."""
    data = {
        'metrics:latency_contract:last:python_worker:redis_to_feature:BTCUSDT': {
            'last_ts_ms': '9999999999999', 'last_duration_ms': '10',
        },
        'metrics:latency_contract:last:python_worker:feature_to_emit:BTCUSDT': {
            'last_ts_ms': '9999999999999', 'last_duration_ms': '10',
        },
    }
    m = evaluate_once(FakeRedis(data), _cfg())
    assert int(m['missing_total']) >= 3  # go_ingest + 2 nest_gateway stages
    assert int(m['gate_ok']) == 0


def test_slo_gate_ok_when_all_required_present():
    """All 5 required stages present and fresh → gate_ok=1."""
    data = {}
    for service, stage in (
        ('go_ingest', 'ingest_to_redis'),
        ('python_worker', 'redis_to_feature'),
        ('python_worker', 'feature_to_emit'),
        ('nest_gateway', 'emit_to_ws'),
        ('nest_gateway', 'end_to_end_event'),
    ):
        data[f'metrics:latency_contract:last:{service}:{stage}:BTCUSDT'] = {
            'last_ts_ms': '9999999999999',
            'last_duration_ms': '10',
        }
    m = evaluate_once(FakeRedis(data), _cfg())
    assert int(m['missing_total']) == 0
    assert int(m['stale_total']) == 0
    assert int(m['gate_ok']) == 1


def test_slo_gate_detects_stale():
    """Present but stale hash → stale_total > 0, gate_ok=0."""
    data = {}
    for service, stage in (
        ('go_ingest', 'ingest_to_redis'),
        ('python_worker', 'redis_to_feature'),
        ('python_worker', 'feature_to_emit'),
        ('nest_gateway', 'emit_to_ws'),
        ('nest_gateway', 'end_to_end_event'),
    ):
        data[f'metrics:latency_contract:last:{service}:{stage}:BTCUSDT'] = {
            'last_ts_ms': '1',  # epoch 1ms = very old
            'last_duration_ms': '10',
        }
    m = evaluate_once(FakeRedis(data), _cfg(stale_s=5))
    assert int(m['stale_total']) > 0
    assert int(m['gate_ok']) == 0


def test_slo_gate_required_total():
    """5 stages × 1 symbol = 5 required."""
    m = evaluate_once(FakeRedis({}), _cfg())
    assert int(m['required_total']) == 5
