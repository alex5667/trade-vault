from __future__ import annotations

import os
import yaml
import pytest


def _alerts_path(tick_flow_full: bool = False) -> str:
    base = os.path.dirname(__file__)
    root = os.path.abspath(os.path.join(base, '..'))
    if tick_flow_full:
        # tick_flow_full is at the repo root, which is 3 levels up from this tests/ dir:
        # tests/ -> orderflow_services/ -> python-worker/ -> repo_root/
        tf_root = os.path.abspath(os.path.join(base, '..', '..', '..', 'tick_flow_full', 'orderflow_services'))
        return os.path.join(tf_root, 'prometheus_alerts_feature_drift_batch_v1.yml')
    return os.path.join(root, 'prometheus_alerts_feature_drift_batch_v1.yml')


@pytest.mark.parametrize('tick_flow_full', [False, True])
def test_feature_drift_batch_alerts_yaml_parses(tick_flow_full: bool) -> None:
    path = _alerts_path(tick_flow_full)
    with open(path, 'r', encoding='utf-8') as f:
        doc = yaml.safe_load(f)
    assert 'groups' in doc and doc['groups']
    rules = doc['groups'][0].get('rules', [])
    names = {r.get('alert') for r in rules}
    assert 'FeatureDriftBatchCriticalFeatures' in names
    assert 'FeatureDriftBatchExporterDown' in names
