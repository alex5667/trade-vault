import types
import pytest
from unittest.mock import MagicMock
import core.of_confirm_engine as ofe
import core.compat_utils

def test_wiring_passes_native_legs(monkeypatch):
    """
    Ensures that OFConfirmEngine passes ofi_leg and fp_edge_absorb natively 
    to eval_reversal/eval_continuation without OR-ing them into obi_stable/abs_lvl_ok.
    """
    
    # Mock eval_reversal to capture arguments
    captured = {}
    def mock_eval_reversal(**kwargs):
        captured.update(kwargs)
        return types.SimpleNamespace(ok=True, have=3, need=2, scenario="reversal", reason="ok", gate_bits=0)

    # Mock CancellationSpikeGate
    class _CancelGateStub:
        def check(self, **_kwargs):
            return types.SimpleNamespace(allow=True, reason="ok", meta={})

    monkeypatch.setattr(ofe, "eval_reversal", mock_eval_reversal)
    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: _CancelGateStub())
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)
    
    # Mock _filter_kwargs_for_callable in compat_utils (where it is defined)
    # The engine imports it inside the method, so we might need to patch it where it is imported FROM
    # BUT, the engine does `from core.compat_utils import _filter_kwargs_for_callable` inside the method.
    # So patching `core.compat_utils._filter_kwargs_for_callable` should work.
    monkeypatch.setattr(core.compat_utils, "_filter_kwargs_for_callable", lambda func, **kwargs: kwargs)

    # Setup Runtime with OFI=True, FP=True, OBI=False, AbsLvl=False
    rt = types.SimpleNamespace(
        last_obi_event={"ts_ms": 1000, "direction": "LONG", "obi": 0.0, "stable_secs": 0.0, "stable": 0},
        last_iceberg_event=None,
        last_ofi_event={"ts_ms": 1000, "direction": "LONG", "ofi": 1.0, "stable_secs": 2.0, "stable": 1}, # OFI OK
        last_fp_edge=types.SimpleNamespace(ts_ms=1000, p90=1.0, value=2.0, bias="LONG", range_expansion=0), # FP OK
        last_sweep=types.SimpleNamespace(ts_ms=1000, kind="EQH_SWEEP", direction_bias="SHORT"),
        last_reclaim=types.SimpleNamespace(ts_ms=1000, direction="LONG"),
        last_wp=types.SimpleNamespace(weak_any=True),
        last_bar=None, # no abs_lvl
        dynamic_cfg={},
        pressure=MagicMock(is_pressure_hi=lambda *a: False),
        book_churn_hi=0,
        last_regime="na",
        last_div=None,
    )
    
    ind = {"book_health_ok": 1, "data_health": 1.0}
    cfg = {"strong_need_reversal": 2} # A+B+C
    
    eng = ofe.OFConfirmEngine()
    eng.build(
        symbol="T", tf="1m", direction="LONG", tick_ts_ms=2000, price=100, delta_z=3.0, 
        runtime=rt, cfg=cfg, indicators=ind, absorption=None
    )
    
    # Assertions
    # 1. obi_stable should be False (not OR-ed with OFI)
    assert captured["obi_stable"] is False, "obi_stable should be False (native)"
    
    # 2. ofi_leg should be True
    assert captured["ofi_leg"] is True, "ofi_leg should be passed as True"
    
    # 3. abs_lvl_ok should be False (not OR-ed with FP)
    assert captured["abs_lvl_ok"] is False, "abs_lvl_ok should be False (native)"
    
    # 4. fp_edge_absorb should be True
    assert captured["fp_edge_absorb"] is True, "fp_edge_absorb should be passed as True"
