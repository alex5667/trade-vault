from __future__ import annotations

import json
import os

import pytest

_THIS_DIR = os.path.dirname(__file__)
_DASHBOARD_JSON = os.path.abspath(os.path.join(_THIS_DIR, "../../..", "orderflow_services", "grafana", "derivatives_context_v1.json"))


@pytest.mark.parametrize("tick_flow_full", [False, True])
def test_derivatives_context_dashboard_json_parses(tick_flow_full: bool):
    path = _DASHBOARD_JSON
    assert os.path.isfile(path), f"Dashboard JSON not found: {path}"
    with open(path) as fh:
        doc = json.load(fh)
    assert doc.get("title") == "Derivatives Context (v1)"
    exprs = "\n".join(t.get("expr", "") for p in doc.get("panels", []) for t in p.get("targets", []))
    assert "deriv_ctx_exporter_funding_rate_z" in exprs
    assert "deriv_ctx_exporter_basis_bps" in exprs
