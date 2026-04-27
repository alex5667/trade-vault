import pytest
from unittest.mock import patch, MagicMock

from services.atr_replay_certification_service import ATRReplayCertificationService


def test_classify_datasets_for_change():
    assert ATRReplayCertificationService.classify_datasets_for_change("LOW_RISK_CONFIG") == ["SMOKE_GOLDEN"]
    assert ATRReplayCertificationService.classify_datasets_for_change("MEDIUM_POLICY") == ["SMOKE_GOLDEN", "CANARY_GOLDEN", "RUNTIME_GOLDEN"]
    assert ATRReplayCertificationService.classify_datasets_for_change("CRITICAL_RUNTIME_GATING") == ["RUNTIME_GOLDEN", "CANARY_GOLDEN", "RELEASE_WINDOW_GOLDEN"]


@patch("services.atr_replay_certification_service.get_db_connection")
def test_select_required_datasets(mock_get_db):
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_cur.fetchall.return_value = [
        {"dataset_id": "ds_1", "dataset_class": "SMOKE_GOLDEN", "status": "ACTIVE"}
    ]

    res = ATRReplayCertificationService.select_required_datasets("LOW_RISK_CONFIG")
    assert len(res) == 1
    assert res[0]["dataset_class"] == "SMOKE_GOLDEN"


def test_decide_replay_cert_status():
    checks_pass = [
        {"check_name": "S1", "status": "passed", "severity": "critical"}
    ]
    status, msg = ATRReplayCertificationService.decide_replay_cert_status(checks_pass)
    assert status == "passed"

    checks_fail_crit = [
        {"check_name": "S1", "status": "passed", "severity": "critical"},
        {"check_name": "S2", "status": "failed", "severity": "critical"}
    ]
    status, msg = ATRReplayCertificationService.decide_replay_cert_status(checks_fail_crit)
    assert status == "failed"

    checks_fail_warn = [
        {"check_name": "S1", "status": "passed", "severity": "critical"},
        {"check_name": "S2", "status": "failed", "severity": "warn"}
    ]
    status, msg = ATRReplayCertificationService.decide_replay_cert_status(checks_fail_warn)
    assert status == "passed_with_warnings"


def test_compare_replay_outputs_missing_refs_fail_closed():
    checks = ATRReplayCertificationService.compare_replay_outputs("/tmp/no-baseline.jsonl", "/tmp/no-candidate.jsonl")
    assert checks[0]["check_name"] == "S0_replay_artifacts_available"
    assert checks[0]["status"] == "failed"
    assert checks[0]["severity"] == "critical"


def test_compare_replay_outputs_jsonl_passes_for_identical_outputs(tmp_path):
    baseline = tmp_path / "baseline.jsonl"
    candidate = tmp_path / "candidate.jsonl"
    payload = '{"signal_id":"sid-1","symbol":"BTCUSDT","allow":true,"clip":false,"protective_state":"armed"}\n'
    baseline.write_text(payload, encoding="utf-8")
    candidate.write_text(payload, encoding="utf-8")

    checks = ATRReplayCertificationService.compare_replay_outputs(str(baseline), str(candidate))
    assert {c["check_name"]: c["status"] for c in checks} == {
        "S0_record_count": "passed",
        "S1_signal_id_stability": "passed",
        "S2_allow_clip_deny": "passed",
        "S7_protective_lifecycle": "passed",
    }


def test_compare_replay_outputs_detects_decision_mismatch(tmp_path):
    baseline = tmp_path / "baseline.jsonl"
    candidate = tmp_path / "candidate.jsonl"
    baseline.write_text('{"signal_id":"sid-1","allow":true,"clip":false}\n', encoding="utf-8")
    candidate.write_text('{"signal_id":"sid-1","allow":false,"clip":false}\n', encoding="utf-8")

    checks = ATRReplayCertificationService.compare_replay_outputs(str(baseline), str(candidate))
    by_name = {c["check_name"]: c for c in checks}
    assert by_name["S2_allow_clip_deny"]["status"] == "failed"


@patch("services.atr_replay_certification_service.get_db_connection")
def test_run_replay_certification_enabled(mock_get_db):
    # Setup mock
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # Mock select_required_datasets through its inner fetchall
    mock_cur.fetchall.return_value = [
        {"dataset_id": "ds_1", "dataset_class": "SMOKE_GOLDEN", "status": "ACTIVE"}
    ]

    # Enabled
    with patch("services.atr_replay_certification_service.ATR_REPLAY_CERT_ENABLE", True):
        run_ids = ATRReplayCertificationService.run_replay_certification("change1", "LOW_RISK_CONFIG", "ref1", "ref2")
        assert len(run_ids) == 1


@patch("services.atr_replay_certification_service.get_db_connection")
def test_run_replay_certification_defects(mock_get_db):
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_cur.fetchall.return_value = [
        {"dataset_id": "ds_1", "dataset_class": "SMOKE_GOLDEN", "status": "ACTIVE"}
    ]

    with patch("services.atr_replay_certification_service.ATR_REPLAY_CERT_ENABLE", True):
        # defect triggers fail
        run_ids = ATRReplayCertificationService.run_replay_certification("change1", "LOW_RISK_CONFIG", "ref1", "ref2", defect="signal_id_mismatch")
        assert len(run_ids) == 1
        # It should run insert checks including the 'failed' status check, but logic is buried in execute calls


@patch("services.atr_replay_certification_service.get_db_connection")
def test_get_cert_status_for_change(mock_get_db):
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # If no runs found
    mock_cur.fetchall.return_value = []
    assert ATRReplayCertificationService.get_cert_status_for_change("change1", "LOW_RISK_CONFIG") == "missing"

    # If all runs passed
    mock_cur.fetchall.return_value = [
        {"status": "passed", "dataset_class": "SMOKE_GOLDEN"}
    ]
    # AND there's an active dataset (fetchone returns True)
    mock_cur.fetchone.return_value = [True]
    assert ATRReplayCertificationService.get_cert_status_for_change("change1", "LOW_RISK_CONFIG") == "passed"

    # If a run failed
    mock_cur.fetchall.return_value = [
        {"status": "failed", "dataset_class": "SMOKE_GOLDEN"}
    ]
    mock_cur.fetchone.return_value = [True]
    assert ATRReplayCertificationService.get_cert_status_for_change("change1", "LOW_RISK_CONFIG") == "failed"
