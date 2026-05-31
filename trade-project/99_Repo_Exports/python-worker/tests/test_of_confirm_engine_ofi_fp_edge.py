import types

import core.of_confirm_engine as ofe


class _PressureStub:
    def is_pressure_hi(self, *_args, **_kwargs):
        return False


def test_ofi_substitutes_obi_stable_in_eval_reversal(monkeypatch):
    """
    C1: OFI is treated as alternative to OBI for the microstructure leg.
    We validate it by checking the value passed into eval_reversal(obi_stable=...).
    """
    captured = {}

    def fake_eval_reversal(**kwargs):
        captured["obi_stable"] = kwargs.get("obi_stable")
        captured["abs_lvl_ok"] = kwargs.get("abs_lvl_ok")
        # return minimal decision-like object
        return types.SimpleNamespace(ok=True, have=2, need=2, reason="ok", scenario="reversal", gate_bits=0)

    class _CancelGateStub:
        def check(self, **_kwargs):
            return types.SimpleNamespace(allow=True, reason="ok", meta={"ready": 1})

    monkeypatch.setattr(ofe, "eval_reversal", fake_eval_reversal)
    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: _CancelGateStub())
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)

    runtime = types.SimpleNamespace(
        last_obi_event=None,
        last_iceberg_event=None,
        last_ofi_event={"ts_ms": 1000, "direction": "LONG", "ofi": 1.0, "ofi_z": 2.0, "stable_secs": 2.0, "stability_score": 1.0},
        last_fp_edge=types.SimpleNamespace(ts_ms=1200, p90=10.0, value=20.0, bias="LONG", range_expansion=0),
        last_sweep=types.SimpleNamespace(ts_ms=1500, kind="EQH_SWEEP", direction_bias="SHORT"),
        last_reclaim=None,
        last_wp=types.SimpleNamespace(weak_any=False),
        last_bar=None,
        dynamic_cfg={},
        pressure=_PressureStub(),
        book_churn_hi=0,
        last_regime="na",
        last_div=None,
    )

    indicators = {"book_health_ok": 1, "data_health": 1.0}
    eng = ofe.OFConfirmEngine()
    ofc, _dec = eng.build(
        symbol="TEST",
        tf="1m",
        direction="LONG",
        tick_ts_ms=2000,
        price=1.0,
        delta_z=2.0,
        runtime=runtime,
        cfg={"fp_edge_min_strength": 1.0, "fp_edge_valid_ms": 30000},
        indicators=indicators,
        absorption=None,
    )

    assert captured["obi_stable"] is True, "OFI stable should substitute OBI stable"
    assert captured["abs_lvl_ok"] is True, "FP edge absorb should be allowed to satisfy abs_lvl_ok input"
    assert ofc is not None
    assert ofc.evidence.get("ofi_stable") == 1
    assert ofc.evidence.get("fp_edge_absorb") == 1

