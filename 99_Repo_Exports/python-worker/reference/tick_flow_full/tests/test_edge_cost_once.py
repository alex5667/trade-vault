from services.candidate_emit_pipeline_v2 import CandidateFrame, GateRunner

class DummyCtx: pass
class DummyCand:
    kind="breakout"
    side="LONG"
    raw_score=1.0
    reasons=[]
    signal_id="sid-1"

class DummyHandler:
    def __init__(self):
        self.calls = 0
    def _legacy_gate_cost_edge(self, *, frame, pre):
        self.calls += 1
        return True, ""

def test_edge_cost_once_calls_handler_once():
    h = DummyHandler()
    ctx = DummyCtx()
    cand = DummyCand()
    f = CandidateFrame(
        handler=h, ctx=ctx, cand=cand,
        kind_str="breakout", kind_key="breakout",
        side_int=1, ctx_symbol="BTCUSDT", ctx_ts=1, ctx_price=1.0
    )
    gates = GateRunner()

    ok1, rc1 = gates.edge_cost_once(f)
    ok2, rc2 = gates.edge_cost_once(f)

    assert ok1 is True and rc1 == ""
    assert ok2 is True and rc2 == ""
    assert h.calls == 1