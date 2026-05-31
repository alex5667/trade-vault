from __future__ import annotations

from services.orderflow.exec_health_freeze_control import parse_exec_health_freeze_control


def test_parse_for_exporter_manual_override_source() -> None:
    """Exporter-relevant: parse state where source=manual_override_thaw."""
    st = parse_exec_health_freeze_control(
        {
            'effective_freeze_active': '0',
            'control_source': 'manual_override_thaw',
            'manual_ack_required': '0',
            'manual_override_active': '1',
            'manual_override_action': 'thaw',
            'manual_ack_ts_ms': '1000',
            'updated_ts_ms': '1000',
        },
        now_ms=2_000,
    )
    assert st.control_source == 'manual_override_thaw'
    assert st.manual_override_active is True
    assert st.effective_freeze_active is False


def test_parse_for_exporter_autoguard_source() -> None:
    """Exporter-relevant: parse state where source=autoguard."""
    st = parse_exec_health_freeze_control(
        {
            'effective_freeze_active': '1',
            'control_source': 'autoguard',
            'manual_ack_required': '1',
            'last_trigger_ts_ms': '500',
            'updated_ts_ms': '500',
            'trigger_total': '3',
            'thaw_total': '1',
        },
        now_ms=2_000,
    )
    assert st.effective_freeze_active is True
    assert st.control_source == 'autoguard'
    assert st.manual_ack_required is True
    assert st.raw_payload['trigger_total'] == '3'
