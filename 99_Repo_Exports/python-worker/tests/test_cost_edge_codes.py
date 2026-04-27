from common.cost_edge_codes import cost_edge_reason_codes


def test_cost_edge_reason_codes_default(monkeypatch):
    monkeypatch.delenv("EDGE_DUAL_EMIT_LEGACY_THIN_COST", raising=False)
    assert cost_edge_reason_codes("VETO_EDGE_COST") == ["VETO_EDGE_COST"]


def test_cost_edge_reason_codes_dual_emit(monkeypatch):
    monkeypatch.setenv("EDGE_DUAL_EMIT_LEGACY_THIN_COST", "1")
    assert cost_edge_reason_codes("VETO_EDGE_COST") == ["VETO_EDGE_COST", "VETO_EDGE_THIN_COST"]
