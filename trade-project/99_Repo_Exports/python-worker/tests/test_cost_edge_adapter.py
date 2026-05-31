from types import SimpleNamespace

from common.cost_edge_adapter import attach_cost_edge_veto_fields, decision_to_legacy_tuple


class _Dec:
    def __init__(self, apply=True, veto=False):
        self.apply = apply
        self.veto = veto
        self.reason_code = "VETO_EDGE_COST"
        self.expected_move_bps = 12.3
        self.threshold_bps = 20.0
        self.fees_bps = 8.0
        self.slippage_bps = 5.0
        self.k = 2.0
        self.mode = "tp1"
        self.notes = "x"


def test_decision_to_legacy_tuple_ok_when_not_applied():
    ok, d = decision_to_legacy_tuple(_Dec(apply=False, veto=True))
    assert ok is True
    assert "threshold_bps" in d


def test_decision_to_legacy_tuple_veto_when_applied():
    ok, d = decision_to_legacy_tuple(_Dec(apply=True, veto=True))
    assert ok is False
    assert d["threshold_bps"] == 20.0


def test_attach_cost_edge_veto_fields_is_best_effort():
    ctx = SimpleNamespace()
    dec = _Dec(apply=True, veto=True)
    attach_cost_edge_veto_fields(ctx, dec)
    assert ctx.veto_reason_code == "VETO_EDGE_COST"
    assert ctx.veto_threshold_bps == 20.0
