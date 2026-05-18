import os

import pytest
import yaml

# The alerts YAML lives in python-worker/orderflow_services/ (canonical location).
# This test file is at python-worker/tests/orderflow_services/tests/, so go up 3 levels.
_THIS_DIR = os.path.dirname(__file__)
_ALERTS_YAML = os.path.abspath(os.path.join(_THIS_DIR, "../../..", "orderflow_services", "prometheus_alerts_derivatives_context_v1.yml"))


@pytest.mark.parametrize("tick_flow_full", [False, True])
def test_derivatives_context_alerts_yaml_parses(tick_flow_full: bool):
    path = _ALERTS_YAML
    assert os.path.isfile(path), f"Alert YAML not found: {path}"
    with open(path) as fh:
        doc = yaml.safe_load(fh)
    assert "groups" in doc and doc["groups"]
    names = {r.get("alert") for r in doc["groups"][0].get("rules", [])}
    assert "OF_DerivativesContext_SnapshotStale_Warn" in names
    assert "OF_DerivativesContext_FundingExtreme_Crit" in names
