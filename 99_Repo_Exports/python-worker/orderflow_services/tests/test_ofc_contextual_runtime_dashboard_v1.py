import json
from pathlib import Path


def test_runtime_rollout_dashboard_contains_runtime_panels():
    data = json.loads(Path("orderflow_services/grafana/ofc_contextual_rollout_v1.json").read_text(encoding="utf-8"))
    titles = {(p.get("title", "")) for p in data.get("panels", [])}
    assert "Runtime reloader pid / restart count / state age" in titles
    assert "Runtime child uptime / cooldown / defer" in titles
    assert "Active overlay fingerprint / last restart reason / defer reason" in titles


def test_runtime_rollout_dashboard_references_runtime_metrics():
    txt = Path("orderflow_services/grafana/ofc_contextual_rollout_v1.json").read_text(encoding="utf-8")
    assert "ofc_ctx_runtime_reloader_child_pid" in txt
    assert "ofc_ctx_runtime_reloader_overlay_dirty" in txt
    assert "ofc_ctx_runtime_reloader_info" in txt
