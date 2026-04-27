import types
import core.of_confirm_engine as ofe

class _PressureStub:
    def is_pressure_hi(self, *_args, **_kwargs):
        return False

def test_exec_risk_penalty_reduces_score(monkeypatch):
    """
    Verify that A3 execution risk penalty is applied and reduces the final score.
    """
    # Mock gate/dist/veto
    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: type("G", (), {"check": lambda s, **k: type("R", (), {"allow": True, "reason": "ok", "meta": {}})()}))
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)

    runtime = types.SimpleNamespace(
        last_obi_event=None,
        last_iceberg_event=None,
        last_ofi_event=None,
        last_fp_edge=None,
        last_sweep=None,
        last_reclaim=None,
        last_wp=types.SimpleNamespace(weak_any=False),
        last_bar=None,
        dynamic_cfg={},
        pressure=_PressureStub(),
        book_churn_hi=0,
        last_regime="na",
        last_div=None,
    )
    
    # 1. Base case: Low execution risk -> penalty ~ 0
    # spread=1.0, slip=1.0 => exec_risk=2.0. Ref=10.0. Norm=0.2. w=0.18. Penalty=0.036
    ind_low = {"book_health_ok": 1, "data_health": 1.0, "spread_bps": 1.0, "expected_slippage_bps": 1.0}
    
    eng = ofe.OFConfirmEngine()
    ofc_low, _ = eng.build(
        symbol="TEST", tf="1m", direction="LONG", tick_ts_ms=1000, price=100.0, delta_z=5.0, # High Z -> high base score
        runtime=runtime, cfg={"w_z": 1.0}, indicators=ind_low
    )
    
    # 2. High risk case: High execution risk -> penalty significant
    # spread=8.0, slip=4.0 => exec_risk=12.0. Ref=10.0. Norm=1.0 (clamped). w=0.18. Penalty=0.18
    ind_high = {"book_health_ok": 1, "data_health": 1.0, "spread_bps": 8.0, "expected_slippage_bps": 4.0}
    
    ofc_high, _ = eng.build(
        symbol="TEST", tf="1m", direction="LONG", tick_ts_ms=1000, price=100.0, delta_z=5.0,
        runtime=runtime, cfg={"w_z": 1.0}, indicators=ind_high
    )
    
    assert ofc_low.score > ofc_high.score
    assert ofc_high.evidence["exec_risk_penalty"] > 0.1
    assert ofc_high.evidence["exec_risk_norm"] == 1.0

def test_exec_risk_lowliq_regime_stricter(monkeypatch):
    """
    Verify that 'illiquid' regime lowers the reference bps, increasing penalty for same risk.
    """
    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: type("G", (), {"check": lambda s, **k: type("R", (), {"allow": True, "reason": "ok", "meta": {}})()}))
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)
    
    runtime = types.SimpleNamespace(
        last_obi_event=None, last_iceberg_event=None, last_ofi_event=None, last_fp_edge=None, last_sweep=None, last_reclaim=None, last_wp=None, last_bar=None, dynamic_cfg={}, pressure=_PressureStub(), book_churn_hi=0, last_regime="na", last_div=None
    )
    
    # Same risk: 9.0 bps
    # Normal ref=10.0 => norm=0.9
    # Lowliq ref=8.0 => norm=1.0 (clamped). Actually 1.125 clamped to 1.0
    
    ind_normal = {"book_health_ok": 1, "spread_bps": 9.0, "liq_regime": "normal"}
    ind_lowliq = {"book_health_ok": 1, "spread_bps": 9.0, "liq_regime": "illiquid"}
    
    eng = ofe.OFConfirmEngine()
    ofc_norm, _ = eng.build(symbol="T", tf="1m", direction="L", tick_ts_ms=1, price=10, delta_z=5, runtime=runtime, cfg={}, indicators=ind_normal)
    ofc_lowliq, _ = eng.build(symbol="T", tf="1m", direction="L", tick_ts_ms=1, price=10, delta_z=5, runtime=runtime, cfg={}, indicators=ind_lowliq)
    
    # Check penalty difference
    # Normal: 9.0 / 10.0 = 0.9. Penalty = 0.9 * 0.18 = 0.162
    # Lowliq: 9.0 / 8.0 = 1.125 -> 1.0. Penalty = 1.0 * 0.18 = 0.18
    
    assert ofc_lowliq.evidence["exec_risk_norm"] > ofc_norm.evidence["exec_risk_norm"]
    assert ofc_lowliq.score < ofc_norm.score
