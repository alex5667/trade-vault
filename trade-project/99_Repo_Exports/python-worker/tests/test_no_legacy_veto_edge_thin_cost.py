from common.cost_edge_codes import cost_edge_reason_codes


def test_no_legacy_veto_edge_thin_cost_by_default(monkeypatch):
    """
    Default behavior MUST NOT emit legacy code.
    Keeps analytics stable after P0.1.1 cleanup.
    """
    monkeypatch.delenv("EDGE_DUAL_EMIT_LEGACY_THIN_COST", raising=False)
    codes = cost_edge_reason_codes("VETO_EDGE_COST")
    assert "VETO_EDGE_THIN_COST" not in codes


def test_can_dual_emit_legacy_by_env(monkeypatch):
    """
    Migration switch: enable ONLY during dashboard transition.
    """
    monkeypatch.setenv("EDGE_DUAL_EMIT_LEGACY_THIN_COST", "1")
    codes = cost_edge_reason_codes("VETO_EDGE_COST")
    assert codes == ["VETO_EDGE_COST", "VETO_EDGE_THIN_COST"]
