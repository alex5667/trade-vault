from __future__ import annotations

import json
import py_compile
from pathlib import Path


def test_feature_drift_batch_exporter_compiles() -> None:
    p = Path(__file__).resolve().parents[1] / 'feature_drift_batch_exporter_v1.py'
    py_compile.compile(str(p), doraise=True)


def test_feature_drift_batch_dashboard_exists_and_parses() -> None:
    p = Path(__file__).resolve().parents[1] / 'grafana' / 'feature_drift_batch_v1.json'
    obj = json.loads(p.read_text(encoding='utf-8'))
    assert obj['title'] == 'Feature Drift Batch (v1)'
    assert isinstance(obj.get('panels'), list) and obj['panels']
