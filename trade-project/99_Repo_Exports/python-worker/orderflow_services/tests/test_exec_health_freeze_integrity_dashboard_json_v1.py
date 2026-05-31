from __future__ import annotations

"""P8 tests: Grafana dashboard JSON structure."""

import json


def test_dashboard_json_loads_and_has_key_panels() -> None:
    """Dashboard JSON must be valid and include violation and ack state panels."""
    with open("orderflow_services/grafana/exec_health_freeze_integrity_v1.json") as fh:
        d = json.load(fh)
    assert "ExecHealth" in d.get("title", "")
    panel_titles = {p.get("title", "") for p in d.get("panels", [])}
    assert any("Violations" in t or "violation" in t.lower() for t in panel_titles)
    assert any("Ack" in t or "ack" in t.lower() for t in panel_titles)
    # At least one panel uses the tamper metric
    all_exprs = " ".join(t["expr"] for p in d.get("panels", []) for t in p.get("targets", []))
    assert "exec_health_freeze_integrity_violation" in all_exprs
