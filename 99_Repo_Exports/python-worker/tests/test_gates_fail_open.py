import pytest
from types import SimpleNamespace
from handlers.crypto_orderflow.components.gates import CryptoSignalGates
from handlers.crypto_orderflow.utils.pre_publish_gates import GateDecision

class RaiserGate:
    def evaluate(self, *args, **kwargs):
        raise RuntimeError("simulated gate failure")

def test_gates_fail_open():
    gates = CryptoSignalGates(
        entry_policy=RaiserGate(),  # type: ignore
        cost_gate=RaiserGate(),     # type: ignore
        consistency_gate=RaiserGate(),
        regime_liquidity_gate=RaiserGate(),
        smt_gate=RaiserGate(),
    )
    
    ctx = SimpleNamespace(data_quality_flags=[])
    
    # 1. check_quality
    qa_res = gates.check_quality(ctx, kind="custom")
    assert qa_res.veto is False
    assert qa_res.reason == "FAIL_OPEN_QUALITY"
    assert "quality_error" in ctx.data_quality_flags
    
    # 2. check_smt
    smt_res = gates.check_smt(ctx, kind="custom", side=1)
    assert smt_res.veto is False
    assert smt_res.reason == "FAIL_OPEN_SMT"
    assert "smt_error" in ctx.data_quality_flags

    # 3. edge_cost_cached
    edge_res = gates.edge_cost_cached(ctx=ctx, kind="custom", symbol="BTC", side=1)
    assert edge_res.veto is False
    assert edge_res.reason_code == "FAIL_OPEN_EDGE_COST"
    assert "edge_cost_error" in ctx.data_quality_flags

    # 4. consistency_once
    cons_res = gates.consistency_once(ctx=ctx, symbol="BTC", kind="custom", side="LONG")
    assert cons_res.veto is False
    assert cons_res.reason_code == "FAIL_OPEN_CONSISTENCY"
    assert "consistency_error" in ctx.data_quality_flags

    # 5. check_entry_policy
    entry_res = gates.check_entry_policy(ctx, payload={"kind": "custom"})
    assert isinstance(entry_res, GateDecision)
    assert entry_res.veto is False
    assert entry_res.reason_code == "ERROR"
    assert entry_res.apply is True
