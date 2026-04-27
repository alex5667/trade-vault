"""P4.8 test: verify trade_execution_p48_unified.json dashboard exists and is valid JSON."""
from pathlib import Path
import json


def test_unified_dashboard_exists():
    """The unified Grafana dashboard JSON must be present."""
    p = (
        Path(__file__).resolve().parents[2]
        / 'monitoring'
        / 'grafana'
        / 'dashboards'
        / 'trade_execution_p48_unified.json'
    )
    assert p.exists(), f'Unified dashboard not found: {p}'


def test_unified_dashboard_is_valid_json():
    """The unified dashboard must parse as valid JSON with expected uid."""
    p = (
        Path(__file__).resolve().parents[2]
        / 'monitoring'
        / 'grafana'
        / 'dashboards'
        / 'trade_execution_p48_unified.json'
    )
    data = json.loads(p.read_text(encoding='utf-8'))
    assert data.get('uid') == 'trade-execution-p48-unified', \
        "Dashboard uid must be 'trade-execution-p48-unified'"
    assert 'panels' in data, 'Dashboard must have panels'
    assert len(data['panels']) > 0, 'Dashboard must have at least 1 panel'
