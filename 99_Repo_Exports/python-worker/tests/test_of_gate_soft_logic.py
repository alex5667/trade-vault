
import pytest
from core.of_confirm_engine import OFConfirmEngine
from unittest.mock import MagicMock

class MockRuntime:
    def __init__(self):
        self.config = {}
        self.dynamic_cfg = {}
        self.last_regime = "bull_trend"
        self.last_wp = type("WP", (), {"weak_any": True})() 
        self.last_obi_event = None
        self.last_iceberg_event = None
        self.last_ofi_event = None
        self.last_sweep = None
        self.last_reclaim = None
        self.last_fp_edge = None
        self.last_bar = None
        self.pressure = type("Press", (), {"is_pressure_hi": lambda *a: False})()
        
def test_soft_fail_logic_default_threshold_fix(monkeypatch):
    monkeypatch.setattr("core.of_confirm_engine.compute_obi_flags", lambda **k: (True, True, 10.0, 1.0))
    monkeypatch.setattr("core.of_confirm_engine.compute_reclaim_recent", lambda **k: (True, 5))
    monkeypatch.setattr("core.of_confirm_engine.compute_absorption_flags", lambda **k: (True, 100.0))
    
    engine = OFConfirmEngine()
    cfg = {}
    runtime = MockRuntime()
    indicators = {
        "spread_bps": 5.0, # Explicit low spread to pass exec risk check
        "expected_slippage_bps": 2.0,
    }
    
    ofc, dec = engine.build(
        symbol="TEST",
        tf="1s",
        direction="LONG",
        tick_ts_ms=1000,
        price=100.0,
        delta_z=5.0, 
        runtime=runtime,
        cfg=cfg,
        indicators=indicators,
        absorption={"ok": 1}
    )
    
    print(f"DEBUG: scenario={ofc.scenario} ok={ofc.ok} have={ofc.have} need={ofc.need} score={ofc.score} ok_soft={ofc.evidence.get('ok_soft')}")
    print(f"DEBUG CONTRIB: {ofc.contrib}")
    
    assert ofc.scenario == "continuation"
    
    if ofc.have == ofc.need - 1:
        assert ofc.ok == 0
        assert ofc.evidence["ok_soft"] == 1 
    elif ofc.have >= ofc.need:
        assert False, "Too many legs! We wanted a near-miss."
    else:
        assert False, f"Not enough legs for near-miss. Have={ofc.have} Need={ofc.need}"

def test_missing_legs_diagnostic():
    engine = OFConfirmEngine()
    cfg = {} 
    indicators = {}
    runtime = MockRuntime()
    runtime.last_sweep = None
    runtime.last_regime = "bull_trend" 
    
    ofc, dec = engine.build(
        symbol="TEST",
        tf="1s",
        direction="LONG", 
        tick_ts_ms=1000,
        price=100.0,
        delta_z=1.0, 
        runtime=runtime,
        cfg=cfg,
        indicators=indicators
    )
    
    print(f"DEBUG: scenario={ofc.scenario} missing_legs={ofc.evidence.get('missing_legs')}")
    
    assert ofc.scenario == "continuation"
    assert len(ofc.evidence["missing_legs"]) > 0
    assert "obi_stable" in ofc.evidence["missing_legs"]
