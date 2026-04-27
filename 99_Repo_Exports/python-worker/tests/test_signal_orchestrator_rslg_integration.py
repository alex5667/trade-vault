import pytest
from types import SimpleNamespace

from handlers.crypto_orderflow.components.gates import CryptoSignalGates
from handlers.crypto_orderflow.pipeline.orchestrator import SignalOrchestrator
from handlers.crypto_orderflow.utils.quality_gates import RegimeSessionLiquidityGate
from handlers.crypto_orderflow.config.handler_config import CryptoOrderFlowConfigManager
from handlers.crypto_orderflow.components.liquidity import CryptoLiquidity
from handlers.crypto_orderflow.components.observability import CryptoObservability

class DummyConfigManager:
    def __init__(self, symbol="BTCUSDT"):
        self.symbol = symbol
    def get_runtime_snapshot(self):
        return None
    def resolve_risk_cfg(self):
        return {}

class DummyConfirmations:
    def validate(self, **kwargs):
        res = SimpleNamespace(
            ok=True, final_score=1.0, confidence=0.9, parts={},
            meta_schema_version=1, schema_version=1,
        )
        return res

class DummyEmitter:
    def __init__(self):
        self.emitted = []
    def emit(
        self, *,
        signal_id="", kind="", symbol="", side=None,
        ts_event_ms=0, ingest_time_ms=0, trace_id=None,
        quality_flags=None, source="python-worker",
        meta_schema_version=1, raw_score=0.0, final_score=0.0,
        confidence_pct=0.0, payload=None,
    ):
        self.emitted.append(payload or {})
        return True

def test_orchestrator_rslg_integration_veto(monkeypatch):
    monkeypatch.setenv("QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("QUALITY_SPREAD_MAX_BPS_DEFAULT", "5.0")
    
    # 1. Initialize RSLG Gate
    rslg = RegimeSessionLiquidityGate.from_env()
    
    # 2. Wraps in CryptoSignalGates
    gates = CryptoSignalGates(
        entry_policy=None,
        cost_gate=None,
        consistency_gate=None,
        regime_liquidity_gate=rslg,
        smt_gate=None,
    )
    
    # 3. Setup Orchestrator
    cfg = DummyConfigManager()
    liquidity = CryptoLiquidity()
    obs = CryptoObservability(None, None)
    conf = DummyConfirmations()
    emitter = DummyEmitter()
    
    orchestrator = SignalOrchestrator(
        config=cfg,
        gates=gates,
        liquidity=liquidity,
        observability=obs,
        confirmations_engine=conf,
        emitter=emitter
    )
    
    import time
    _ts = int(time.time() * 1000)
    # Context with wide spread (10.0 > 5.0) -> SHOULD BE VETOED
    ctx_wide = SimpleNamespace(
        symbol="BTCUSDT", ts=_ts, ts_ms=_ts, price=100.0, sizing_ok=True,
        qty=0.001, venue="binance", timeframe="1m", atr=5.0,
        sl_price=99.0, tp1_price=102.0, tp_mode_used="RR",
        risk_usd_target=5.0, risk_usd=4.9, trail_profile="",
        trailing_min_lock_r=0.0, risk_cfg={}, redis=None,
        of=SimpleNamespace(spread_bps=10.0)
    )
    cand1 = SimpleNamespace(kind="breakout", side=1, raw_score=2.0)
    
    def detect_fn_wide(ctx):
        return [cand1]
        
    any_sent = orchestrator.process(ctx_wide, detect_fn_wide)
    assert not any_sent
    assert len(emitter.emitted) == 0

def test_orchestrator_rslg_integration_pass(monkeypatch):
    monkeypatch.setenv("QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("QUALITY_SPREAD_MAX_BPS_DEFAULT", "10.0")
    
    rslg = RegimeSessionLiquidityGate.from_env()
    gates = CryptoSignalGates(
        entry_policy=None,
        cost_gate=None,
        consistency_gate=None,
        regime_liquidity_gate=rslg,
        smt_gate=None,
    )
    
    cfg = DummyConfigManager()
    liquidity = CryptoLiquidity()
    obs = CryptoObservability(None, None)
    conf = DummyConfirmations()
    emitter = DummyEmitter()
    
    orchestrator = SignalOrchestrator(
        config=cfg,
        gates=gates,
        liquidity=liquidity,
        observability=obs,
        confirmations_engine=conf,
        emitter=emitter
    )
    
    import time
    _ts = int(time.time() * 1000)
    # Context with narrow spread (5.0 < 10.0) -> SHOULD PASS
    ctx_narrow = SimpleNamespace(
        symbol="BTCUSDT", ts=_ts, ts_ms=_ts, price=100.0, sizing_ok=True,
        qty=0.001, venue="binance", timeframe="1m", atr=5.0,
        sl_price=99.0, tp1_price=102.0, tp_mode_used="RR",
        risk_usd_target=5.0, risk_usd=4.9, trail_profile="",
        trailing_min_lock_r=0.0, risk_cfg={}, redis=None,
        of=SimpleNamespace(spread_bps=5.0)
    )
    cand2 = SimpleNamespace(kind="breakout", side=1, raw_score=2.0)
    
    def detect_fn_narrow(ctx):
        return [cand2]
        
    any_sent = orchestrator.process(ctx_narrow, detect_fn_narrow)
    assert any_sent
    assert len(emitter.emitted) == 1
    assert emitter.emitted[0]["kind"] == "breakout"
