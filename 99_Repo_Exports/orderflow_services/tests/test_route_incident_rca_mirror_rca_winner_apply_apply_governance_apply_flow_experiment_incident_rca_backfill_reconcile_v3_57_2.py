from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_backfill_replayer_v3_57_2 import SPECS


def test_specs_have_required_aliases():
    assert set(SPECS.keys()) == {"slo", "retry", "escalation"}


def test_slo_upsert_uses_ts_ms_conflict():
    assert "ON CONFLICT (ts_ms)" in SPECS["slo"]["upsert_sql"]


def test_retry_upsert_uses_natural_key_conflict():
    assert "ON CONFLICT (ts_ms, rollback_mode, failed_target_mode, reason_code)" in SPECS["retry"]["upsert_sql"]


def test_escalation_upsert_uses_natural_key_conflict():
    assert "ON CONFLICT (ts_ms, rollback_mode, failed_target_mode, reason_code)" in SPECS["escalation"]["upsert_sql"]
