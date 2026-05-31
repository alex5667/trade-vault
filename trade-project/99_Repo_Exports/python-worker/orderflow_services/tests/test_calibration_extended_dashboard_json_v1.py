from __future__ import annotations

import json
import os

import pytest


def _path(tick_flow_full: bool = False) -> str:
    base = os.path.dirname(__file__)
    if tick_flow_full:
        return os.path.abspath(os.path.join(base, '..', '..', 'tick_flow_full', 'orderflow_services', 'grafana', 'calibration_extended_v1.json'))
    return os.path.abspath(os.path.join(base, '..', 'grafana', 'calibration_extended_v1.json'))


@pytest.mark.parametrize('tick_flow_full', [False, True])
def test_dashboard_json_has_expected_queries(tick_flow_full: bool):
    with open(_path(tick_flow_full)) as fh:
        doc = json.load(fh)
    assert doc['title'] == 'Calibration Extended (v1)'
    joined = '\n'.join(t['expr'] for p in doc.get('panels', []) for t in p.get('targets', []))
    assert 'conf_cal_extended_metric' in joined
    assert 'mce_cal' in joined
    assert 'sharpness_mean' in joined
