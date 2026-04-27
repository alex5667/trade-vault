"""
Test that ML gate always receives scenario_v4 instead of legacy reversal/continuation.

This ensures ML v10.4 util_mh gets correct bucket selection and util_floor_by_bucket.
"""
import types
import core.of_confirm_engine as ofe


class _PressureStub:
    def is_pressure_hi(self, *_args, **_kwargs):
        return False


class _MLGateStub:
    """Mock ML gate that captures scenario parameter for verification"""
    def __init__(self):
        self.last_scenario = None
        self.last_indicators = None
        self.call_count = 0

    def check(self, **kwargs):
        self.call_count += 1
        self.last_scenario = kwargs.get("scenario", "")
        self.last_indicators = kwargs.get("indicators", {})
        # Return a mock decision
        return types.SimpleNamespace(
            mode="SHADOW",
            kind="util_mh",
            allow=True,
            p_edge=0.65,
            p_min=0.55,
            score=0.65,
            floor=0.55,
            best_h_ms=3600000,
            to_dict=lambda: {
                "mode": "SHADOW",
                "kind": "util_mh",
                "allow": True,
                "p_edge": 0.65,
                "p_min": 0.55,
            }
        )

    @classmethod
    def from_env(cls):
        return cls()


def test_ml_gate_receives_scenario_v4_from_indicators(monkeypatch):
    """Test that ML gate uses scenario_v4 from indicators when dec.scenario is legacy"""
    ml_gate_stub = _MLGateStub()
    
    # Mock ML gate
    monkeypatch.setattr(ofe, "MLConfirmGate", _MLGateStub)
    
    # Mock cancellation gate
    class _CancelGateStub:
        def check(self, **_kwargs):
            return types.SimpleNamespace(allow=True, reason="ok", meta={"ready": 0, "veto_kind": "none"})
    
    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: _CancelGateStub())
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)
    
    class _ClassifyV4Stub:
        def __init__(self, id_val="range_meanrev"):
            self.id = id_val
            self.reason = "mock"
    monkeypatch.setattr(ofe, "classify_v4", lambda *a, **k: _ClassifyV4Stub("range_meanrev"))

    runtime = types.SimpleNamespace(
        last_obi_event=None,
        last_iceberg_event=None,
        last_ofi_event=None,
        last_fp_edge=None,
        last_sweep=None,
        last_reclaim=None,
        last_wp=types.SimpleNamespace(weak_any=False),
        last_bar=None,
        dynamic_cfg={"scenario_v4_enable": 1},
        pressure=_PressureStub(),
        book_churn_hi=0,
        last_regime="na",
        last_div=None,
        liq_regime="na",
        liq_score=0.5,
    )
    
    # Provide scenario_v4 in indicators (simulating strategy/engine setting it)
    indicators = {
        "book_health_ok": 1,
        "data_health": 1.0,
        "spread_bps": 2.0,
        "expected_slippage_bps": 1.0,
        "scenario_v4": "range_meanrev",  # V4 scenario provided in indicators
    }

    eng = ofe.OFConfirmEngine()
    ofc, dec = eng.build(
        symbol="TEST",
        tf="1s",
        direction="LONG",
        tick_ts_ms=2_000,
        price=1.0,
        delta_z=2.0,
        runtime=runtime,
        cfg={"scenario_v4_enable": 1},
        indicators=indicators,
        absorption={"side": "LONG", "volume": 10.0},
    )
    
    assert ofc is not None
    # Verify ML gate was called
    assert eng._ml_gate.call_count > 0
    # Verify ML gate received scenario_v4 instead of legacy scenario
    assert eng._ml_gate.last_scenario == "range_meanrev"
    # Verify scenario_v4 is in indicators passed to ML gate
    assert eng._ml_gate.last_indicators.get("scenario_v4") == "range_meanrev"


def test_ml_gate_receives_scenario_v4_from_computed(monkeypatch):
    """Test that ML gate uses computed scenario_v4 when dec.scenario is legacy and indicators don't have it"""
    ml_gate_stub = _MLGateStub()
    
    # Mock ML gate
    monkeypatch.setattr(ofe, "MLConfirmGate", _MLGateStub)
    
    # Mock cancellation gate
    class _CancelGateStub:
        def check(self, **_kwargs):
            return types.SimpleNamespace(allow=True, reason="ok", meta={"ready": 0, "veto_kind": "none"})
    
    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: _CancelGateStub())
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)
    
    class _ClassifyV4Stub:
        def __init__(self, id_val="range_meanrev"):
            self.id = id_val
            self.reason = "mock"
    monkeypatch.setattr(ofe, "classify_v4", lambda *a, **k: _ClassifyV4Stub("range_meanrev"))

    runtime = types.SimpleNamespace(
        last_obi_event=None,
        last_iceberg_event=None,
        last_ofi_event=None,
        last_fp_edge=None,
        last_sweep=None,
        last_reclaim=None,
        last_wp=types.SimpleNamespace(weak_any=False),
        last_bar=None,
        dynamic_cfg={"scenario_v4_enable": 1},
        pressure=_PressureStub(),
        book_churn_hi=0,
        last_regime="na",
        last_div=None,
        liq_regime="na",
        liq_score=0.5,
    )
    
    # No scenario_v4 in indicators - should use computed one
    indicators = {
        "book_health_ok": 1,
        "data_health": 1.0,
        "spread_bps": 2.0,
        "expected_slippage_bps": 1.0,
    }

    eng = ofe.OFConfirmEngine()
    ofc, dec = eng.build(
        symbol="TEST",
        tf="1s",
        direction="LONG",
        tick_ts_ms=2_000,
        price=1.0,
        delta_z=2.0,
        runtime=runtime,
        cfg={"scenario_v4_enable": 1},
        indicators=indicators,
        absorption={"side": "LONG", "volume": 10.0},
    )
    
    assert ofc is not None
    # Verify ML gate was called
    assert eng._ml_gate.call_count > 0
    # Verify ML gate received computed scenario_v4 (range_meanrev when no sweep/trend)
    # Should not be legacy "reversal" or "continuation"
    assert eng._ml_gate.last_scenario not in ("reversal", "continuation", "none")
    # Verify scenario_v4 is in indicators passed to ML gate
    assert "scenario_v4" in eng._ml_gate.last_indicators
    assert eng._ml_gate.last_indicators.get("scenario_v4") not in ("reversal", "continuation", "none")


def test_ml_gate_preserves_v4_scenario_when_already_v4(monkeypatch):
    """Test that ML gate preserves scenario_v4 when dec.scenario is already v4"""
    ml_gate_stub = _MLGateStub()
    
    # Mock ML gate
    monkeypatch.setattr(ofe, "MLConfirmGate", _MLGateStub)
    
    # Mock cancellation gate
    class _CancelGateStub:
        def check(self, **_kwargs):
            return types.SimpleNamespace(allow=True, reason="ok", meta={"ready": 0, "veto_kind": "none"})
    
    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: _CancelGateStub())
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)
    
    class _ClassifyV4Stub:
        def __init__(self, id_val="vol_shock_news_proxy"):
            self.id = id_val
            self.reason = "mock"
    monkeypatch.setattr(ofe, "classify_v4", lambda *a, **k: _ClassifyV4Stub("vol_shock_news_proxy"))

    runtime = types.SimpleNamespace(
        last_obi_event=None,
        last_iceberg_event=None,
        last_ofi_event=None,
        last_fp_edge=None,
        last_sweep=None,
        last_reclaim=None,
        last_wp=types.SimpleNamespace(weak_any=False),
        last_bar=None,
        dynamic_cfg={"scenario_v4_enable": 1},
        pressure=_PressureStub(),
        book_churn_hi=0,
        last_regime="na",
        last_div=None,
        liq_regime="na",
        liq_score=0.5,
    )
    
    # Provide vol_shock_news_proxy in indicators
    indicators = {
        "book_health_ok": 1,
        "data_health": 1.0,
        "spread_bps": 2.0,
        "expected_slippage_bps": 1.0,
        "scenario_v4": "vol_shock_news_proxy",  # V4 scenario
    }

    eng = ofe.OFConfirmEngine()
    ofc, dec = eng.build(
        symbol="TEST",
        tf="1s",
        direction="LONG",
        tick_ts_ms=2_000,
        price=1.0,
        delta_z=2.0,
        runtime=runtime,
        cfg={"scenario_v4_enable": 1},
        indicators=indicators,
        absorption={"side": "LONG", "volume": 10.0},
    )
    
    assert ofc is not None
    # Verify ML gate was called
    assert eng._ml_gate.call_count > 0
    # Verify ML gate received vol_shock_news_proxy
    assert eng._ml_gate.last_scenario == "vol_shock_news_proxy"
    # Verify scenario_v4 is in indicators passed to ML gate
    assert eng._ml_gate.last_indicators.get("scenario_v4") == "vol_shock_news_proxy"

