from __future__ import annotations

import os

import pytest
import yaml


def _path(tick_flow_full: bool = False) -> str:
    base = os.path.dirname(__file__)
    if tick_flow_full:
        return os.path.abspath(os.path.join(base, '..', '..', 'tick_flow_full', 'orderflow_services', 'prometheus_alerts_calibration_extended_v1.yml'))
    return os.path.abspath(os.path.join(base, '..', 'prometheus_alerts_calibration_extended_v1.yml'))


@pytest.mark.parametrize('tick_flow_full', [False, True])
def test_alerts_yaml_parses(tick_flow_full: bool):
    with open(_path(tick_flow_full)) as fh:
        doc = yaml.safe_load(fh)
    names = {r.get('alert') for r in doc['groups'][0]['rules']}
    assert 'OF_CalibrationExtended_MCEHigh_Crit' in names
    assert 'OF_CalibrationExtended_SharpnessGrey_Warn' in names
