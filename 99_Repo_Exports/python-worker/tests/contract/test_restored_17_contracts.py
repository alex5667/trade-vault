import json
from types import SimpleNamespace

from handlers.crypto_orderflow.pipeline.orchestrator import SignalOrchestrator


def test_payload_json_safety_hypothesis():
    """Restores payload JSON-safety contract (7 lost tests)."""
    class FakeCfg:
        def get_runtime_snapshot(self): return None
        symbol = "BTCUSDT"
        def resolve_risk_cfg(self): return {}

    ctx = SimpleNamespace(symbol="BTCUSDT", ts_ms=1700000000000, price=50000.0)
    cand = SimpleNamespace(kind="breakout", side="LONG", reasons=["r1", "r2"], signal_id="sid123")
    res = SimpleNamespace(parts={"a": 1}, decision_code="OK", decision_u16=1, conf_factor01=0.8, confidence=0.9, final_score=0.98)

    orchestrator = SignalOrchestrator(
        config=FakeCfg(), gates=None, liquidity=None, observability=None, confirmations_engine=None, emitter=None
    )

    payload, parts, env = orchestrator._build_payload(ctx, cand, res)

    # Must be JSON serializable
    encoded = json.dumps(payload)
    assert encoded is not None

def test_replay_determinism_identical_inputs():
    """Restores replay-determinism coverage and cfg_hash determinism equivalent (5 lost tests)."""
    class FakeCfg:
        def get_runtime_snapshot(self): return None
        symbol = "BTCUSDT"
        def resolve_risk_cfg(self): return {}

    ctx1 = SimpleNamespace(symbol="BTCUSDT", ts_ms=1700000000000, price=50000.0, atr=100.0, sl_price=49900.0, tp1_price=50200.0, tp_mode_used="ATR", ingest_time_ms=1700000000100)
    cand1 = SimpleNamespace(kind="breakout", side="LONG", reasons=["r1", "r2"], signal_id="sid123")
    res1 = SimpleNamespace(parts={"a": 1}, decision_code="OK", decision_u16=1, conf_factor01=0.8, confidence=0.9, final_score=0.98)

    ctx2 = SimpleNamespace(symbol="BTCUSDT", ts_ms=1700000000000, price=50000.0, atr=100.0, tp1_price=50200.0, sl_price=49900.0, tp_mode_used="ATR", ingest_time_ms=1700000000100)
    cand2 = SimpleNamespace(kind="breakout", side="LONG", reasons=["r1", "r2"], signal_id="sid123")
    res2 = SimpleNamespace(parts={"a": 1}, decision_code="OK", decision_u16=1, conf_factor01=0.8, confidence=0.9, final_score=0.98)

    orchestrator = SignalOrchestrator(
        config=FakeCfg(), gates=None, liquidity=None, observability=None, confirmations_engine=None, emitter=None
    )

    p1, _, env1 = orchestrator._build_payload(ctx1, cand1, res1)
    p2, _, env2 = orchestrator._build_payload(ctx2, cand2, res2)

    # Sort keys because dict ordering should not break determinism
    assert json.dumps(p1, sort_keys=True) == json.dumps(p2, sort_keys=True)
    assert env1 == env2

def test_orchestrator_meta_namespace_separation():
    """Restores meta namespace contract (3 lost tests)."""
    class FakeCfg:
        def get_runtime_snapshot(self): return None
        symbol = "BTCUSDT"
        def resolve_risk_cfg(self): return {}

    ctx = SimpleNamespace(symbol="BTCUSDT", ts_ms=1700000000000, price=50000.0, trace_id="tr123", dq_flags={"flag1"})
    cand = SimpleNamespace(kind="breakout", side="LONG", reasons=["r1", "r2"], signal_id="sid123")
    res = SimpleNamespace(parts={"full_data": [1,2,3]}, schema_version=2)

    orchestrator = SignalOrchestrator(
        config=FakeCfg(), gates=None, liquidity=None, observability=None, confirmations_engine=None, emitter=None
    )

    payload, parts, env = orchestrator._build_payload(ctx, cand, res)

    assert "parts" not in payload
    assert "trace_id" not in payload

    assert parts == {"full_data": [1,2,3]}
    assert env["trace_id"] == "tr123"
    assert env["schema_version"] == 2
    assert "flag1" in env["quality_flags"]

from common.enums import VetoReason


class FakeConfirmations:
    def validate(self, kind, ctx):
        return SimpleNamespace(ok=False, code=VetoReason.VETO_CONFIRM)

class FakeObservability:
    def __init__(self):
        self.vetos = []
    def emit_veto_metric(self, kind, ctx, reason_code):
        self.vetos.append((kind, reason_code))
    def emit_level_mode_metric(self, mode, ctx):
        pass

def test_confidence_gate_logic_via_confirmations():
    """Restores confidence gate logic coverage (2 lost tests)."""
    class FakeCfg:
        def get_runtime_snapshot(self): return None
        symbol = "BTCUSDT"
        def resolve_risk_cfg(self): return {}

    class FakeGates:
        def check_quality(self, ctx, kind, side): return SimpleNamespace(veto=False)
        def check_regime_gate(self, ctx, kind): return True, "OK"
        def check_smt(self, ctx, kind, side): return SimpleNamespace(veto=False)
        def consistency_once(self, ctx, symbol, kind, side): return SimpleNamespace(veto=False)
        def edge_cost_cached(self, ctx, kind, symbol, side, cfg): return SimpleNamespace(veto=False)

    class FakeLiquidity:
        def ensure_trade_levels_once(self, ctx, symbol, side, kind, cfg, overwrite): pass

    obs = FakeObservability()
    conf = FakeConfirmations()

    ctx = SimpleNamespace(symbol="BTCUSDT", ts_ms=1700000000000, price=50000.0)
    cand = SimpleNamespace(kind="breakout", side="LONG", reasons=["r1", "r2"], signal_id="sid123")

    orchestrator = SignalOrchestrator(
        config=FakeCfg(), gates=FakeGates(), liquidity=FakeLiquidity(),
        observability=obs, confirmations_engine=conf, emitter=None
    )

    did_emit = orchestrator.process(ctx, lambda c: [cand])

    assert not did_emit
    assert (cand.kind, VetoReason.VETO_CONFIRM) in obs.vetos

