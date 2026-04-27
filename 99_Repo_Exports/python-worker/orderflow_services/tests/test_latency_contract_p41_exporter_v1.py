"""Tests for P4.1 additions to latency_contract_exporter_v1:
  - _parse_key roundtrips for NestJS stage names
  - P4.1 alerts YAML is valid
"""
from __future__ import annotations

import os
import sys
import yaml

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from orderflow_services.latency_contract_exporter_v1 import _parse_key

PREFIX = 'metrics:latency_contract:last'


def test_parse_key_roundtrips_nest_gateway_end_to_end():
    service, stage, symbol = _parse_key(
        f'{PREFIX}:nest_gateway:end_to_end_event:BTCUSDT', PREFIX
    )
    assert service == 'nest_gateway'
    assert stage == 'end_to_end_event'
    assert symbol == 'BTCUSDT'


def test_parse_key_roundtrips_go_ingest():
    service, stage, symbol = _parse_key(
        f'{PREFIX}:go_ingest:ingest_to_redis:ETHUSDT', PREFIX
    )
    assert service == 'go_ingest'
    assert stage == 'ingest_to_redis'
    assert symbol == 'ETHUSDT'


def test_p41_alerts_yaml_valid():
    path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', 'prometheus_alerts_latency_contract_p41_v1.yml')
    )
    with open(path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    assert 'groups' in data and data['groups']
    rules = data['groups'][0]['rules']
    alert_names = {r['alert'] for r in rules}
    assert 'OF_LatencyContract_MissingExternalStage_Crit' in alert_names
    assert 'OF_LatencyContract_SLOGateOpen_Crit' in alert_names
