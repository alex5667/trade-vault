import inspect

import services.orderflow_strategy as orderflow_strategy
from services.orderflow.metrics import evidence_used_total


def test_stage1_p0_evidence_used_total_labelnames_contract() -> None:
    # P0 contract: dashboards and alerts rely on labelname 'key' (not 'feature')
    assert hasattr(evidence_used_total, "_labelnames")
    assert "key" in evidence_used_total._labelnames
    assert "feature" not in evidence_used_total._labelnames


def test_stage1_p0_no_feature_kw_in_evidence_used_total_labels_calls() -> None:
    # Guardrail: calling .labels(feature=...) on a metric that uses labelname 'key'
    # silently breaks evidence telemetry (and downstream ok_rate).
    src = inspect.getsource(orderflow_strategy)
    for line in src.splitlines():
        if "evidence_used_total.labels" in line:
            assert "feature=" not in line


def test_stage1_p0_record_evidence_used_call_order_contract() -> None:
    # Guardrail: prevent swapped args / double-binding of 'session' param.
    src = inspect.getsource(orderflow_strategy)
    assert "record_evidence_used(runtime.symbol, k, sess_name)" not in src
    assert "record_evidence_used(runtime.symbol, ckqr, session=sess_name)" not in src
