from __future__ import annotations
"""Tests for P4.1 latency_semconv additions: required_stage_owners and build_external_state_mapping."""

import sys
import os
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from services.observability.latency_semconv import required_stage_owners, build_external_state_mapping


def test_required_stage_owner_matrix_contains_external_services():
    pairs = set(required_stage_owners())
    assert ('go_ingest', 'ingest_to_redis') in pairs
    assert ('nest_gateway', 'emit_to_ws') in pairs
    assert ('nest_gateway', 'end_to_end_event') in pairs


def test_required_stage_owner_matrix_contains_python_stages():
    pairs = set(required_stage_owners())
    assert ('python_worker', 'redis_to_feature') in pairs
    assert ('python_worker', 'feature_to_emit') in pairs


def test_required_stage_owner_matrix_has_five_entries():
    assert len(required_stage_owners()) == 5


def test_build_external_state_mapping_includes_external_ts_fields():
    payload = {
        'ts_event_ms': 1000,
        'ts_ingest_source_ms': 1100,
        'ts_redis_xadd_ms': 1200,
        'ts_emit_ms': 1300,
        'ts_ws_emit_ms': 1500,
    }
    m = build_external_state_mapping(
        service='nest_gateway',
        stage='end_to_end_event',
        symbol='BTCUSDT',
        duration_ms=500,
        payload=payload,
        instance_id='n1',
        source='test',
    )
    assert m['service'] == 'nest_gateway'
    assert m['stage'] == 'end_to_end_event'
    assert m['symbol'] == 'BTCUSDT'
    assert m['ts_ws_emit_ms'] == '1500'
    assert m['ts_ingest_source_ms'] == '1100'
    assert m['ts_redis_xadd_ms'] == '1200'
    assert m['instance_id'] == 'n1'
    assert m['source'] == 'test'
    assert m['last_duration_ms'] == '500'


def test_build_external_state_mapping_schema_version():
    m = build_external_state_mapping(
        service='go_ingest', stage='ingest_to_redis', symbol='ethusdt',
        duration_ms=20, payload={},
    )
    assert m['schema_version'] == '1'
    assert m['symbol'] == 'ETHUSDT'  # normalised to upper
