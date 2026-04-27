"""P4.6: Verify that the Grafana risk quality dashboard now has annotations."""
import json
from pathlib import Path


def _root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_risk_dashboard_has_annotations_block() -> None:
    """Dashboard JSON must contain an annotations.list block."""
    src = (
        _root()
        / 'monitoring'
        / 'grafana'
        / 'dashboards'
        / 'trade_execution_p45_risk_quality.json'
    ).read_text(encoding='utf-8')
    data = json.loads(src)
    assert 'annotations' in data, "Dashboard missing top-level 'annotations' key"
    assert 'list' in data['annotations'], "Dashboard 'annotations' missing 'list'"


def test_risk_dashboard_has_two_or_more_annotations() -> None:
    """Dashboard must have at least two annotation entries (audit + summary refresh)."""
    src = (
        _root()
        / 'monitoring'
        / 'grafana'
        / 'dashboards'
        / 'trade_execution_p45_risk_quality.json'
    ).read_text(encoding='utf-8')
    data = json.loads(src)
    ann_list = data.get('annotations', {}).get('list', [])
    assert len(ann_list) >= 2, (
        f"Expected >=2 annotations in dashboard, found {len(ann_list)}"
    )
