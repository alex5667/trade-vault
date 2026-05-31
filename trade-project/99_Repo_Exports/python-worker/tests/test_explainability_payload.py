import types
from unittest.mock import MagicMock

import core.of_confirm_engine as ofe


class _CancelGateStub:
    def __init__(self, allow=True, reason="ok"):
        self._allow = allow
        self._reason = reason

    def check(self, **_kwargs):
        return types.SimpleNamespace(allow=self._allow, reason=self._reason, meta={"ready": 1})

def test_explainability_fields_present(monkeypatch):
    """
    Verifies that legs_detail, score_breakdown, missing_legs, need_reason 
    are present in the evidence payload.
    """
    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: _CancelGateStub())
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "eval_reversal", lambda **k: types.SimpleNamespace(ok=True, have=3, need=2, scenario="reversal", reason="ok", gate_bits=0, need_reason="mock_reason"))

    # Mock compute_strong_need_same_tick to ensure need_reason is "escalated"
    mock_nd = types.SimpleNamespace(need_rev=2, need_cont=2, reason="escalated")
    monkeypatch.setattr(ofe, "compute_strong_need_same_tick", lambda **k: mock_nd)

    rt = types.SimpleNamespace(
        last_obi_event=None,
        last_iceberg_event=None,
        last_ofi_event={"ts_ms": 1000, "direction": "LONG", "ofi": 1.0, "stable_secs": 2.0},
        last_fp_edge=None,
        last_sweep=types.SimpleNamespace(ts_ms=1000, kind="EQH_SWEEP", direction_bias="SHORT"),
        last_reclaim=None,
        last_wp=types.SimpleNamespace(weak_any=False),
        last_bar=None,
        dynamic_cfg={},
        pressure=MagicMock(is_pressure_hi=lambda *a: False),
        book_churn_hi=0,
        last_regime="na",
        last_div=None,
    )

    eng = ofe.OFConfirmEngine()
    ofc, dec = eng.build(
        symbol="T", tf="1m", direction="LONG", tick_ts_ms=2000, price=100, delta_z=3.0,
        runtime=rt, cfg={"scenario_v4_enable": 1}, indicators={}, absorption=None
    )

    assert "legs_detail" in ofc.evidence
    ld = ofc.evidence["legs_detail"]
    assert isinstance(ld, list)
    # Check OFI leg detail
    ofi_det = next((x for x in ld if x["name"] == "ofi_leg"), None)
    assert ofi_det is not None
    assert ofi_det["pass"] == 1
    assert "why" in ofi_det

    assert "score_breakdown" in ofc.evidence
    sb = ofc.evidence["score_breakdown"]
    assert "base_score" in sb
    assert "score_final" in sb
    assert "exec_risk_penalty" in sb

    assert "missing_legs" in ofc.evidence
    assert isinstance(ofc.evidence["missing_legs"], list)

    assert "need_reason" in ofc.evidence
    assert ofc.evidence["need_reason"] == "escalated"

def test_cancel_spike_penalty_in_breakdown(monkeypatch):
    """
    Verifies that cancel_spike_penalty=1.0 when cancel gate vetoes,
    but it does not affect the score calculation itself (it is a hard veto).
    """
    # Force cancel gate to veto
    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: _CancelGateStub(allow=False, reason="spike"))
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "eval_reversal", lambda **k: types.SimpleNamespace(ok=True, have=3, need=2, scenario="reversal", reason="ok", gate_bits=0))
    monkeypatch.setattr(ofe, "compute_strong_need_same_tick", lambda **k: types.SimpleNamespace(need_rev=2, need_cont=2, reason="BASE"))

    rt = types.SimpleNamespace(
        last_obi_event=None,
        last_iceberg_event=None,
        last_ofi_event=None,
        last_fp_edge=None,
        last_sweep=types.SimpleNamespace(ts_ms=1000, kind="EQH_SWEEP", direction_bias="SHORT"),
        last_reclaim=None,
        last_wp=types.SimpleNamespace(weak_any=False),
        last_bar=None,
        dynamic_cfg={},
        pressure=MagicMock(is_pressure_hi=lambda *a: False),
        book_churn_hi=0,
        last_regime="na",
        last_div=None,
    )

    # Set score_min to 0 to ensure ok_pre_gate=1 despite low score (since we only care about gate logic here)
    eng = ofe.OFConfirmEngine()
    ofc, dec = eng.build(
        symbol="T", tf="1m", direction="LONG", tick_ts_ms=2000, price=100, delta_z=3.0,
        runtime=rt, cfg={"of_score_min": 0.0}, indicators={}, absorption=None
    )

    # Gate vetoed -> ok should be 0
    assert ofc.ok == 0

    # Check score breakdown
    sb = ofc.evidence["score_breakdown"]
    assert sb["cancel_spike_penalty"] == 1.0
