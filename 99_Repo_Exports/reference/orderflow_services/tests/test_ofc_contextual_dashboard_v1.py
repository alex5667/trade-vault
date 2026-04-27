import json
from pathlib import Path


def test_ofc_contextual_rollout_dashboard_parses_and_has_expected_title():
    path = Path("orderflow_services/grafana/ofc_contextual_rollout_v1.json")
    doc = json.loads(path.read_text())
    assert doc.get("title") == "OFC Contextual Rollout + Runtime Reloader (v2)"
    assert len(doc.get("panels", [])) >= 4


def test_ofc_contextual_rollout_dashboard_queries_cover_key_metrics():
    path = Path("orderflow_services/grafana/ofc_contextual_rollout_v1.json")
    doc = json.loads(path.read_text())
    exprs = []
    for panel in doc.get("panels", []):
        for target in panel.get("targets", []) or []:
            expr = target.get("expr") if isinstance(target, dict) else None
            if expr:
                exprs.append(expr)
    joined = "\n".join(exprs)
    assert "ofc_ctx_rollout_shadow_disagree_rate" in joined
    assert "ofc_ctx_rollout_bundle_age_seconds" in joined
    assert "ofc_ctx_rollout_current_mode" in joined
