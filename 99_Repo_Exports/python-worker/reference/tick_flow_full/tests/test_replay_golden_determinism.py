import os
from types import SimpleNamespace

import pytest

# NOTE: these imports assume tests are executed with python-worker/ on PYTHONPATH.
from common.payload_fingerprint import fingerprint_tradeable_payload
from handlers.crypto_orderflow.pipeline import candidate_emit_pipeline_v2 as mod
from handlers.crypto_orderflow.pipeline.candidate_emit_pipeline_v2 import (
    CandidateFrame
    GateRunner
    PayloadBuilder
)


def create_minimal_frame(symbol="BTCUSDT", price=43210.5, side=1):
    from handlers.crypto_orderflow.pipeline.candidate_emit_pipeline_v2 import CandidateFrame
    ctx = SimpleNamespace(
        symbol=symbol
        entry_price=price
        entry_ts_ms=1700000000000
        ts_ms=1700000000123
        tp1_price=price * 1.01
        sl_price=price * 0.99
    )
    cand = SimpleNamespace(signal_id="")
    f = CandidateFrame(
        handler=None
        ctx=ctx
        cand=cand
        kind_str="breakout"
        kind_key="breakout"
        side_int=side
        ctx_symbol=symbol
        ctx_ts=1700000000123
        ctx_price=price
    )
    # emulate pipeline preconditions
    f.memo["levels_ensured"] = True
    f.memo["trade_levels_attached"] = True
    return f


class DummyEdgeCostGate:
    def __init__(self): self.calls = 0
    def evaluate(self, *, ctx, kind, symbol):
        self.calls += 1
        return SimpleNamespace(apply=True, veto=False, reason_code="OK", diagnostics={})


def _mk_frame() -> CandidateFrame:
    # Minimal context shape for the pieces tested here.
    ctx = SimpleNamespace(
        symbol="BTCUSDT"
        entry_price=43210.5
        entry_ts_ms=1700000000000
        ts_ms=1700000000123
    )

    cand = SimpleNamespace(signal_id="")

    return CandidateFrame(
        ctx=ctx
        cand=cand
        kind_str="absorption"
        kind_key="absorption"
        side_int=1,  # 1 for buy/long, -1 for sell/short
        ctx_symbol="BTCUSDT"
        ctx_ts=1700000000123
        ctx_price=43210.5
        handler=None
    )


def test_replay_stable_signal_id_payload_builder_is_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    """Golden/Replay: same input -> same sid + decision fields.

    We freeze time to avoid nondeterminism from created_ts_ms.
    """

    monkeypatch.setenv("REPLAY_STABLE_SIGNAL_ID", "1")
    monkeypatch.setattr(mod.time, "time", lambda: 1234567.89)

    f = _mk_frame()

    res = SimpleNamespace(
        veto=False
        reason_code="OK"
        decision_code="OK"
        decision_u16=100
        gate_reasons=["levels:OK", "edge_cost:OK"]
        reasons={"levels": "OK", "edge_cost": "OK"}
    )

    b = PayloadBuilder()
    p1 = b.build(f, raw_score=0.5, final_score=0.75, confidence=0.9, conf_factor=1.0, res=res)
    p2 = b.build(f, raw_score=0.5, final_score=0.75, confidence=0.9, conf_factor=1.0, res=res)

    assert p1 == p2

    # sid is derived only from the stable subset of fields (no timestamps, no randomness).
    base = {k: v for k, v in p1.items() if k not in ("sid", "signal_id")}
    sha1, _ = fingerprint_tradeable_payload(base)
    assert p1["sid"] == f"s_{sha1[:24]}"

    assert p1["decision_code"] == "OK"
    assert p1["decision_u16"] == 100
    assert p1["reasons"]["edge_cost"] == "OK"


def test_gate_order_levels_attached_before_edge_cost(monkeypatch: pytest.MonkeyPatch) -> None:
    """Golden: enforce the ordering contract.

    Requirement: trade levels must be attached to ctx BEFORE EV/Cost (edge-cost) gate runs.
    """

    events = []

    def _trace_gate(ctx, *, stage, name, passed, veto, reason_code, duration_ms, metrics=None):
        events.append((stage, name))

    def _ensure_levels(handler, *, ctx):
        setattr(ctx, "_levels_ensured", True)

    def _attach_levels(ctx):
        assert getattr(ctx, "_levels_ensured", False) is True
        setattr(ctx, "_levels_attached", True)

    class _H:
        def __init__(self):
            self._last_call = None

        def _legacy_gate_cost_edge(self, *, ctx, kind, side_str, cfg=None):
            # If this assertion fails, levels were NOT attached prior to EV/Cost.
            assert getattr(ctx, "_levels_attached", False) is True
            self._last_call = (kind, side_str)
            return True, "OK"

    monkeypatch.setattr(mod, "trace_gate", _trace_gate)
    monkeypatch.setattr(mod, "ensure_levels", _ensure_levels)
    monkeypatch.setattr(mod, "attach_trade_levels_to_ctx", _attach_levels)

    f = _mk_frame()
    h = _H()
    gm = GateRunner(h)

    ok, rc = gm.edge_cost_once(f, cfg={})

    assert ok is True
    assert rc == "OK"

    # Ordering: levels attach happens before edge-cost gate event.
    assert ("levels", "trade_levels_attached") in events
    assert ("gates", "edge_cost_gate") in events

    assert events.index(("levels", "trade_levels_attached")) < events.index(("gates", "edge_cost_gate"))


def test_replay_stable_signal_id_and_edge_cost_determinism(monkeypatch):
    os.environ["REPLAY_STABLE_SIGNAL_ID"] = "1"
    from handlers.crypto_orderflow.pipeline.candidate_emit_pipeline_v2 import GateRunner

    gate = DummyEdgeCostGate()
    handler = SimpleNamespace(_edge_cost_gate=gate)

    f1 = create_minimal_frame()
    # Create a new frame with the handler
    from handlers.crypto_orderflow.pipeline.candidate_emit_pipeline_v2 import CandidateFrame
    f1 = CandidateFrame(
        handler=handler
        ctx=f1.ctx
        cand=f1.cand
        kind_str=f1.kind_str
        kind_key=f1.kind_key
        side_int=f1.side_int
        ctx_symbol=f1.ctx_symbol
        ctx_ts=f1.ctx_ts
        ctx_price=f1.ctx_price
        memo=f1.memo
    )

    runner = GateRunner()

    ok1, rc1 = runner.edge_cost_once(f1)
    sid1 = f1.cand.signal_id

    # Same input → same sid, and gate memoization means evaluate not called twice
    f2 = create_minimal_frame()
    f2 = CandidateFrame(
        handler=handler
        ctx=f2.ctx
        cand=f2.cand
        kind_str=f2.kind_str
        kind_key=f2.kind_key
        side_int=f2.side_int
        ctx_symbol=f2.ctx_symbol
        ctx_ts=f2.ctx_ts
        ctx_price=f2.ctx_price
        memo=f2.memo
    )
    ok2, rc2 = runner.edge_cost_once(f2)
    sid2 = f2.cand.signal_id

    assert sid1 == sid2
    assert (ok1, rc1) == (ok2, rc2)
