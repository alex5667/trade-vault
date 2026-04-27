from pathlib import Path

from orderflow_services.ofc_contextual_runtime_health_exporter_v1 import export_once


def test_export_once_reads_runtime_state_and_exposes_fields(tmp_path: Path):
    state = tmp_path / "state.json"
    state.write_text(
        """{
  "ts_ms": 9999999999999,
  "child_pid": 4242,
  "child_start_ts_ms": 9999999999000,
  "restart_count": 7,
  "last_child_exit_code": 143,
  "rollback_exists": 1,
  "overlay_dirty": 1,
  "defer_active": 1,
  "cooldown_until_ts_ms": 9999999999999,
  "defer_until_ts_ms": 9999999999999,
  "active_overlay_fingerprint": "abc123",
  "last_restart_reason_kind": "overlay_changed",
  "defer_reason": "cooldown"
}
""",
        encoding="utf-8",
    )
    data = export_once(str(state))
    assert data["child_pid"] == 4242
    assert data["restart_count"] == 7
    assert data["active_overlay_fingerprint"] == "abc123"
    assert data["last_restart_reason_kind"] == "overlay_changed"
