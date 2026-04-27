from pathlib import Path

from orderflow_services.ofc_contextual_runtime_summary_writer_v1 import build_summary, write_summary_once


def test_build_summary_derives_runtime_fields():
    state = {
        'ts_ms': 1000,
        'child_pid': 4242,
        'child_start_ts_ms': 500,
        'restart_count': 3,
        'last_restart_reason_kind': 'overlay_changed',
        'cooldown_until_ts_ms': 1400,
        'defer_active': 1,
        'defer_reason': 'cooldown',
        'defer_until_ts_ms': 1300,
        'overlay_dirty': 1,
        'rollback_exists': 0,
        'active_overlay_fingerprint': 'abc',
        'desired_overlay_fingerprint': 'def',
    }
    summary = build_summary(state, now_ms=1200)
    assert summary['child_pid'] == 4242
    assert summary['child_uptime_seconds'] == 0.7
    assert summary['restart_count'] == 3
    assert summary['cooldown_remaining_seconds'] == 0.2
    assert summary['defer_remaining_seconds'] == 0.1
    assert summary['active_overlay_fingerprint'] == 'abc'


def test_write_summary_once_emits_textfile(tmp_path: Path):
    state_path = tmp_path / 'state.json'
    textfile = tmp_path / 'collector' / 'runtime.prom'
    state_path.write_text("""{
  "ts_ms": 1000,
  "child_pid": 123,
  "child_start_ts_ms": 900,
  "restart_count": 1,
  "last_restart_reason_kind": "initial",
  "active_overlay_fingerprint": "fp"
}
""", encoding='utf-8')
    summary = write_summary_once(
        state_path=str(state_path),
        redis_url='',
        summary_key='',
        textfile_path=str(textfile),
        now_ms=1100,
    )
    assert summary['child_pid'] == 123
    data = textfile.read_text(encoding='utf-8')
    assert 'ofc_ctx_runtime_summary_up 1' in data
    assert 'active_overlay_fingerprint="fp"' in data
