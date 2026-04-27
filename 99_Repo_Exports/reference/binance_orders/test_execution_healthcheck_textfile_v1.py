from __future__ import annotations

"""Unit tests for the Prometheus textfile renderer in execution_healthcheck.py."""

import importlib.util
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent.parent / 'scripts' / 'execution_healthcheck.py'
SPEC = importlib.util.spec_from_file_location('execution_healthcheck', SCRIPT)
mod = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = mod
SPEC.loader.exec_module(mod)


def _sample_report(status='warning'):
    return {
        'checked_at_ms': 123456789,
        'overall_status': status,
        'consistency': {
            'redis_state_count': 5,
            'stream_sid_count': 4,
            'sql_order_count': 3,
            'mismatches_total': 4,
            'critical_mismatches': 1,
            'warning_mismatches': 3,
        },
        'user_stream': {
            'age_ms': 800,
            'keys_checked': 6,
            'is_stale': False,
        },
    }


def test_render_prometheus_textfile_contains_key_metrics():
    """All expected metric names and values must appear in the textfile output."""
    report = _sample_report('warning')
    text = mod.render_prometheus_textfile(report)
    # status code: warning=1
    assert 'trade_execution_health_status_code 1' in text
    assert 'trade_execution_consistency_critical_mismatches 1' in text
    assert 'trade_execution_consistency_warning_mismatches 3' in text
    assert 'trade_execution_user_stream_age_ms 800' in text
    assert 'trade_execution_user_stream_keys_checked 6' in text
    assert 'trade_execution_user_stream_stale 0' in text


def test_render_prometheus_textfile_ok_status_code():
    """ok status → numeric code 0."""
    text = mod.render_prometheus_textfile(_sample_report('ok'))
    assert 'trade_execution_health_status_code 0' in text


def test_render_prometheus_textfile_critical_status_code():
    """critical status → numeric code 2."""
    text = mod.render_prometheus_textfile(_sample_report('critical'))
    assert 'trade_execution_health_status_code 2' in text


def test_render_prometheus_textfile_unknown_status_code():
    """Unknown status → numeric code 3."""
    text = mod.render_prometheus_textfile({'overall_status': 'unknown'})
    assert 'trade_execution_health_status_code 3' in text


def test_render_prometheus_textfile_ends_with_newline():
    """Output must end with a trailing newline (required by Prometheus textfile format)."""
    text = mod.render_prometheus_textfile(_sample_report())
    assert text.endswith('\n')


def test_write_atomic_creates_file(tmp_path):
    """_write_atomic must produce the target file atomically (no .tmp remnant)."""
    target = tmp_path / 'test.prom'
    mod._write_atomic(target, 'hello\n')
    assert target.read_text() == 'hello\n'
    # The .tmp sibling should be cleaned up by rename
    assert not (tmp_path / 'test.prom.tmp').exists()
