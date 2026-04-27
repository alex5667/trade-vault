from __future__ import annotations

import pytest
from orderflow_services.strategy_research_stats_exporter_v1 import _reason_kind


def test_reason_kind_ok():
    assert _reason_kind('ok') == 'ok'
    assert _reason_kind('') == 'ok'
    assert _reason_kind('   ') == 'ok'


def test_reason_kind_known():
    assert _reason_kind('psr_low') == 'psr_low'
    assert _reason_kind('dsr_low') == 'dsr_low'
    assert _reason_kind('pbo_high') == 'pbo_high'
    assert _reason_kind('metric_low') == 'metric_low'
    assert _reason_kind('report_stale') == 'report_stale'
    assert _reason_kind('state_missing') == 'state_missing'
    assert _reason_kind('invalid') == 'invalid'


def test_reason_kind_partial_match():
    assert _reason_kind('psr_low,dsr_low') == 'psr_low'
    assert _reason_kind('SOMETHING_pso_pbo_high_xyz') == 'pbo_high'


def test_reason_kind_unknown():
    assert _reason_kind('unexplained_condition') == 'other'
    assert _reason_kind('random_string') == 'other'
