import os
import yaml
import pytest


# Both parametrize variants resolve to the same real path (the file lives in
# tick_flow_full/orderflow_services alongside this test directory).
_THIS_DIR = os.path.dirname(__file__)
_ALERTS_YAML = os.path.abspath(os.path.join(_THIS_DIR, "..", "prometheus_alerts_derivatives_context_v1.yml"))


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
