import pytest

from ml_analysis.tools.edge_stack_train_bundle_utils_p59 import read_monitoring_smoke_gate, SmokeGate


def test_smoke_gate_missing_fail_closed():
    # Redis URL is invalid -> should fail-closed by default
    g = read_monitoring_smoke_gate("redis://127.0.0.1:6399/0", fail_mode="fail_closed")
    assert isinstance(g, SmokeGate)
    assert g.ok is False


def test_smoke_gate_missing_fail_open():
    g = read_monitoring_smoke_gate("redis://127.0.0.1:6399/0", fail_mode="fail_open")
    assert g.ok is True
