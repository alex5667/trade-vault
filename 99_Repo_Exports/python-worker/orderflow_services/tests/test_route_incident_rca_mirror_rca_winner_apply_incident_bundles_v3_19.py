from orderflow_services.route_incident_rca_mirror_rca_winner_apply_incident_bundle_builder_v3_19 import (
    find_unprocessed_triggers,
)


def test_find_unprocessed_triggers_apply():
    last_times = {"applies": 1000, "rollbacks": 1000, "escalations": 1000}
    current_data = {
        "applies": [
            {"action": "APPLY_PRIMARY_ARM_SHADOW", "ts_ms": "1001"},
            {"action": "APPLY_PRIMARY_ARM_SHADOW", "ts_ms": "999"} # old
        ]
    }

    triggers = find_unprocessed_triggers(last_times, current_data)
    assert len(triggers) == 1
    assert triggers[0][0] == "apply"
    assert triggers[0][1] == "warning"

def test_find_unprocessed_triggers_rollback():
    last_times = {"applies": 1000, "rollbacks": 1000, "escalations": 1000}
    current_data = {
        "rollbacks": [
            {"reason": "LOW_PRIMARY_MATCH_RATE", "ts_ms": "1005"}
        ]
    }

    triggers = find_unprocessed_triggers(last_times, current_data)
    assert len(triggers) == 1
    assert triggers[0][0] == "rollback"
    assert triggers[0][1] == "critical"

def test_find_unprocessed_triggers_escalation_critical():
    last_times = {"applies": 1000, "rollbacks": 1000, "escalations": 1000}
    current_data = {
        "escalations": [
            {"severity": "critical", "ts_ms": "1010"}
        ]
    }

    triggers = find_unprocessed_triggers(last_times, current_data)
    assert len(triggers) == 1
    assert triggers[0][0] == "escalation"
    assert triggers[0][1] == "critical"

def test_find_unprocessed_triggers_escalation_info_ignored():
    last_times = {"applies": 1000, "rollbacks": 1000, "escalations": 1000}
    current_data = {
        "escalations": [
            {"severity": "info", "ts_ms": "1010"}
        ]
    }
    # Info is not in ONLY_SEVERITY
    triggers = find_unprocessed_triggers(last_times, current_data)
    assert len(triggers) == 0
