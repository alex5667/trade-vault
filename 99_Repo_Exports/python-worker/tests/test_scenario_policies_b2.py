import types

import core.of_confirm_engine as ofe


class _PressureStub:
    def is_pressure_hi(self, *_args, **_kwargs):
        return True # Always pressure HI

def _make_runtime(scenario_id="vol_shock_news_proxy"):
    class _Stub: pass
    rt = types.SimpleNamespace(
        last_obi_event={"ts_ms": 1000, "direction": "LONG", "obi": 1.0, "stable_secs": 2.0}, # obi_stable=1
        last_iceberg_event={"ts_ms": 1000, "price": 100.0, "refresh": 5, "duration": 2.0, "side": "bid"}, # iceberg_strict=1
        last_ofi_event={"ts_ms": 1000, "direction": "LONG", "ofi": 1.0, "stable_secs": 2.0, "ofi_z": 3.0, "stability_score": 1.0}, # ofi_stable=1
        last_fp_edge=types.SimpleNamespace(ts_ms=1000, p90=1.0, value=2.0, bias="LONG", range_expansion=0), # fp_edge_ok=1
        last_sweep=None,
        last_reclaim={"ts_ms": 1000, "direction": "LONG"}, # reclaim=1
        last_wp=types.SimpleNamespace(weak_any=False),
        last_bar=None,
        dynamic_cfg={"scenario_v4_enable": 1},
        pressure=_PressureStub(),
        book_churn_hi=1, # churn_hi=1 => vol_shock trigger
        last_regime="na",
        last_div=None,
    )
    return rt

def test_vol_shock_fail_closed(monkeypatch):
    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: type("G", (), {"check": lambda s, **k: type("R", (), {"allow": True, "reason": "ok", "meta": {}})()}))
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)

    rt = _make_runtime()
    # Indicators perfect
    ind = {"book_health_ok": 1, "data_health": 1.0, "spread_bps": 1.0, "expected_slippage_bps": 1.0}
    # But fail_closed=1
    cfg = {"scenario_v4_enable": 1, "vol_shock_fail_closed": 1, "w_z": 1.0}

    eng = ofe.OFConfirmEngine()
    # Trigger vol_shock via pressure+churn in runtime
    ofc, _ = eng.build(symbol="T", tf="1m", direction="LONG", tick_ts_ms=2000, price=100, delta_z=5.0, runtime=rt, cfg=cfg, indicators=ind, absorption={"side": "LONG", "volume": 10})

    assert ofc.scenario == "vol_shock_news_proxy"
    # Reason might include (have/need) suffix, so check containment
    assert "vol_shock_fail_closed" in ofc.reason
    assert ofc.ok == 0

def test_vol_shock_exec_risk_cap_hit(monkeypatch):
    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: type("G", (), {"check": lambda s, **k: type("R", (), {"allow": True, "reason": "ok", "meta": {}})()}))
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)

    rt = _make_runtime()
    # Indicators BAD: spread+slip > 20bps
    ind = {"book_health_ok": 1, "data_health": 1.0, "spread_bps": 15.0, "expected_slippage_bps": 6.0} # 21bps
    cfg = {"scenario_v4_enable": 1, "vol_shock_fail_closed": 0, "vol_shock_exec_risk_max_bps": 20.0}

    eng = ofe.OFConfirmEngine()
    ofc, _ = eng.build(symbol="T", tf="1m", direction="LONG", tick_ts_ms=2000, price=100, delta_z=5.0, runtime=rt, cfg=cfg, indicators=ind, absorption={"side": "LONG", "volume": 10})

    assert ofc.scenario == "vol_shock_news_proxy"
    assert "exec_risk_cap" in ofc.reason
    assert ofc.ok == 0
    assert ofc.evidence["policy_vol_shock_exec_risk_cap_hit"] == 1

def test_saw_chop_hard_evidence_strict(monkeypatch):
    # Setup saw/chop via cancel_meta
    class _CancelGateStub:
        def check(self, **_kwargs):
            # ready=1, veto_kind='pull_without_aggr' => triggers saw_chop classifier
            return type("R", (), {"allow": True, "reason": "ok", "meta": {"ready": 1, "veto_kind": "pull_without_aggr"}})()

    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: _CancelGateStub())
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)

    rt = _make_runtime()
    rt.book_churn_hi = 0 # disable vol_shock

    # 1. Missing hard evidence (e.g. no iceberg)
    rt.last_iceberg_event = None # iceberg=0
    ind = {"book_health_ok": 1, "data_health": 1.0, "spread_bps": 1.0}
    cfg = {"scenario_v4_enable": 1, "of_score_min_saw_chop": 0.5} # Lower score threshold to pass test (defaults to 0.75 which might veto 0.68)

    eng = ofe.OFConfirmEngine()
    ofc, _ = eng.build(symbol="T", tf="1m", direction="LONG", tick_ts_ms=2000, price=100, delta_z=5.0, runtime=rt, cfg=cfg, indicators=ind, absorption={"side": "LONG", "volume": 10})

    assert ofc.scenario == "saw_chop_spoof_proxy"
    assert "saw_chop_missing_hard_evidence" in ofc.reason
    assert ofc.ok == 0

    # 2. Perfect evidence
    rt.last_iceberg_event = {"ts_ms": 1000, "price": 100.0, "refresh": 5, "duration": 2.0, "side": "bid"} # restore
    ofc_ok, _ = eng.build(symbol="T", tf="1m", direction="LONG", tick_ts_ms=2000, price=100, delta_z=5.0, runtime=rt, cfg=cfg, indicators=ind, absorption={"side": "LONG", "volume": 10})

    assert ofc_ok.scenario == "saw_chop_spoof_proxy"
    assert "strict" in ofc_ok.reason
    assert ofc_ok.ok == 1
