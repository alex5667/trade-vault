"""P4.13 Grafana dashboard JSON presence tests."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_dashboard_json_contains_p413_panels() -> None:
    data = json.loads((ROOT / 'orderflow_services/grafana/latency_contract_p413_v1.json').read_text(encoding='utf-8'))
    blob = json.dumps(data, sort_keys=True)
    assert 'latency_contract_deploy_lint_silence_approval_binding_match' in blob
    assert 'latency_contract_deploy_lint_silence_approval_details_fingerprint_match' in blob
    assert 'latency_contract_deploy_lint_silence_approval_binding_schema_version' in blob
    assert 'latency_contract_deploy_lint_summary_dual_control_semantic_binding_mismatch_total' in blob
