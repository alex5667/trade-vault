"""Tests that external example adapters (Go/TypeScript) exist and contain expected identifiers."""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'integrations'))


def test_external_examples_exist():
    go_path = os.path.join(BASE, 'go_ingest_latency_writer_v1.go')
    ts_path = os.path.join(BASE, 'nest_ws_latency_writer_v1.ts')
    readme_path = os.path.join(BASE, 'README_latency_contract_p41.md')

    assert os.path.isfile(go_path), f"Missing: {go_path}"
    assert os.path.isfile(ts_path), f"Missing: {ts_path}"
    assert os.path.isfile(readme_path), f"Missing: {readme_path}"

    with open(go_path, 'r', encoding='utf-8') as f:
        go = f.read()
    with open(ts_path, 'r', encoding='utf-8') as f:
        ts = f.read()

    assert 'go_ingest' in go
    assert 'ingest_to_redis' in go
    assert 'writeIngestToRedis' in go

    assert 'nest_gateway' in ts
    assert 'emit_to_ws' in ts
    assert 'end_to_end_event' in ts
    assert 'writeNestGatewayLatency' in ts
