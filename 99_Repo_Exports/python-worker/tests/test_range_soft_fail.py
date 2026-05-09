import types

import core.of_confirm_engine as ofe


class _PressureStub:
    def is_pressure_hi(self, *_args, **_kwargs):
        return False # No pressure => no vol shock

def _make_runtime():
    rt = types.SimpleNamespace(
        last_obi_event={"ts_ms": 1000, "direction": "LONG", "obi": 1.0, "stable_secs": 2.0}, # obi_stable=1
        last_iceberg_event=None,
        last_ofi_event=None,
        last_fp_edge=None,
        last_sweep=None, # No sweep
        last_reclaim=None,
        last_wp=types.SimpleNamespace(weak_any=False),
        last_bar=None,
        dynamic_cfg={"scenario_v4_enable": 1, "range_meanrev_enable": 1},
        pressure=_PressureStub(),
        book_churn_hi=0,
        last_regime="na",
        last_div=None,
    )
    return rt

def test_range_soft_fail_triggered(monkeypatch):
    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: type("G", (), {"check": lambda s, **k: type("R", (), {"allow": True, "reason": "ok", "meta": {}})()}))
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)

    rt = _make_runtime()

    # We want scenario_base="none" -> scenario_v4="range_meanrev"
    # legs:
    # 1. micro: obi_stable=1 (from runtime) -> 1 leg
    # 2. abs: need absorption. Let's provide it via kwargs/indicators
    # 3. edge: iceberg/fp_edge=0.

    # have = 2 (micro + abs). need = 3. have == need-1.

    ind = {"book_health_ok": 1, "data_health": 1.0, "spread_bps": 1.0, "expected_slippage_bps": 1.0} # Low risk => exec_risk_norm low
    cfg = {
        "scenario_v4_enable": 1,
        "range_meanrev_enable": 1,
        "strong_need_range": 3,
        "range_soft_score_min": 0.5, # ensure score passes
        "range_soft_exec_risk_norm_max": 0.6,
        "w_obi": 0.5, # ensure score is high enough
        "w_abs": 0.5,
        "range_abs_min_vol": 5.0
    }

    eng = ofe.OFConfirmEngine()
    ofc, _ = eng.build(
        symbol="T", tf="1m", direction="LONG", tick_ts_ms=2000, price=100, delta_z=0.0,
        runtime=rt, cfg=cfg, indicators=ind,
        absorption={"side": "LONG", "volume": 10.0} # abs_leg=1
    )

    assert ofc.scenario == "range_meanrev"
    assert ofc.ok == 0 # Hard fail
    assert ofc.evidence["ok_soft"] == 1 # Soft pass
    assert ofc.evidence["soft_reason"] == "range_soft_fail"
    assert ofc.evidence["exec_risk_norm"] < 0.6

def test_range_hard_pass(monkeypatch):
    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: type("G", (), {"check": lambda s, **k: type("R", (), {"allow": True, "reason": "ok", "meta": {}})()}))
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)

    rt = _make_runtime()
    # Add iceberg => edge_leg=1
    rt.last_iceberg_event = {"ts_ms": 2000, "price": 100.0, "refresh": 5, "duration": 2.0, "side": "bid"}

    ind = {"book_health_ok": 1, "data_health": 1.0, "spread_bps": 1.0, "expected_slippage_bps": 1.0}
    cfg = {
        "scenario_v4_enable": 1,
        "range_meanrev_enable": 1,
        "strong_need_range": 3,
        "w_obi": 0.3, "w_ice": 0.3, "w_abs": 0.3, # score ~ 0.9
        "w_z": 0.0, # disable Z to not dilute
        "of_score_min_range": 0.5 # lower score req just in case
    }

    eng = ofe.OFConfirmEngine()
    ofc, _ = eng.build(
        symbol="T", tf="1m", direction="LONG", tick_ts_ms=2000, price=100, delta_z=0.0,
        runtime=rt, cfg=cfg, indicators=ind,
        absorption={"side": "LONG", "volume": 10.0} # abs_leg=1
    )

    # have = 3 (micro + edge + abs). need = 3.
    assert ofc.scenario == "range_meanrev"
    assert ofc.ok == 1
    assert ofc.evidence["ok_soft"] == 0
