"""
Tests for OFConfirmEngine legs/missing_legs fixes and trend_dir fallback.

This test verifies:
1. leg_a/leg_b/leg_c are correctly extracted from StrongGateDecision and added to legs dict
2. missing_legs for continuation/reversal uses leg_a/leg_b/leg_c instead of raw evidence flags
3. trend_dir fallback chain works correctly (hidden_div -> indicators -> regime -> direction)
4. trend_dir_source is correctly set in indicators and evidence
5. absorption and ofi_stable are added to legs dict
"""
import types
from unittest.mock import MagicMock

import core.compat_utils
import core.of_confirm_engine as ofe


def test_legs_contain_leg_a_b_c_from_decision(monkeypatch):
    """Verify that leg_a/leg_b/leg_c are extracted from StrongGateDecision and added to legs."""

    # Mock eval_reversal to return a decision with a=1, b=1, c=0
    def mock_eval_reversal(**kwargs):
        dec = types.SimpleNamespace(
            ok=True, have=2, need=2, scenario="reversal", reason="ok",
            gate_bits=3,  # bits 0 and 1 set (A and B)
            a=1, b=1, c=0
        )
        return dec

    class _CancelGateStub:
        def check(self, **_kwargs):
            return types.SimpleNamespace(allow=True, reason="ok", meta={})

    monkeypatch.setattr(ofe, "eval_reversal", mock_eval_reversal)
    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: _CancelGateStub())
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)
    monkeypatch.setattr(core.compat_utils, "_filter_kwargs_for_callable", lambda func, **kwargs: kwargs)

    rt = types.SimpleNamespace(
        last_obi_event={"ts_ms": 1000, "direction": "LONG", "obi": 0.5, "stable_secs": 2.0, "stable": 1},
        last_iceberg_event=None,
        last_ofi_event={"ts_ms": 1000, "direction": "LONG", "ofi": 1.0, "stable_secs": 2.0, "stable": 1},
        last_fp_edge=None,
        last_sweep=types.SimpleNamespace(ts_ms=1000, kind="EQH_SWEEP", direction_bias="SHORT"),
        last_reclaim=types.SimpleNamespace(ts_ms=1000, direction="LONG"),
        last_wp=types.SimpleNamespace(weak_any=True),
        last_bar=None,
        dynamic_cfg={},
        pressure=MagicMock(is_pressure_hi=lambda *a: False),
        book_churn_hi=0,
        last_regime="na",
        last_div=None,
    )

    ind = {"book_health_ok": 1, "data_health": 1.0}
    cfg = {"strong_need_reversal": 2}

    eng = ofe.OFConfirmEngine()
    ofc, _ = eng.build(
        symbol="T", tf="1m", direction="LONG", tick_ts_ms=2000, price=100, delta_z=3.0,
        runtime=rt, cfg=cfg, indicators=ind, absorption=None
    )

    assert ofc is not None
    ev = ofc.evidence
    legs = ev.get("legs", {})

    # Verify leg_a/leg_b/leg_c are in legs
    assert "leg_a" in legs, "leg_a should be in legs dict"
    assert "leg_b" in legs, "leg_b should be in legs dict"
    assert "leg_c" in legs, "leg_c should be in legs dict"

    # Verify values match decision
    assert legs["leg_a"] == 1, "leg_a should be 1"
    assert legs["leg_b"] == 1, "leg_b should be 1"
    assert legs["leg_c"] == 0, "leg_c should be 0"

    # Verify absorption and ofi_stable are present
    assert "absorption" in legs, "absorption should be in legs dict"
    assert "ofi_stable" in legs, "ofi_stable should be in legs dict"


def test_missing_legs_uses_leg_a_b_c_for_base_scenarios(monkeypatch):
    """Verify that missing_legs for continuation/reversal uses leg_a/leg_b/leg_c."""

    # Mock eval_continuation to return a decision with a=1, b=0, c=0 (missing b and c)
    def mock_eval_continuation(**kwargs):
        dec = types.SimpleNamespace(
            ok=False, have=1, need=2, scenario="continuation", reason="need_failed",
            gate_bits=1,  # only bit 0 set (A)
            a=1, b=0, c=0
        )
        return dec

    class _CancelGateStub:
        def check(self, **_kwargs):
            return types.SimpleNamespace(allow=True, reason="ok", meta={})

    monkeypatch.setattr(ofe, "eval_continuation", mock_eval_continuation)
    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: _CancelGateStub())
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)
    monkeypatch.setattr(core.compat_utils, "_filter_kwargs_for_callable", lambda func, **kwargs: kwargs)

    rt = types.SimpleNamespace(
        last_obi_event={"ts_ms": 1000, "direction": "LONG", "obi": 0.5, "stable_secs": 2.0, "stable": 1},
        last_iceberg_event=None,
        last_ofi_event={"ts_ms": 1000, "direction": "LONG", "ofi": 1.0, "stable_secs": 2.0, "stable": 1},
        last_fp_edge=None,
        last_sweep=None,
        last_reclaim=None,
        last_wp=types.SimpleNamespace(weak_any=False),
        last_bar=None,
        dynamic_cfg={},
        pressure=MagicMock(is_pressure_hi=lambda *a: False),
        book_churn_hi=0,
        last_regime="bull_trend",
        last_div=types.SimpleNamespace(kind="bullish_hidden", ts_ms=1000),
        cont_ctx_ts_ms=1000,
    )

    ind = {"book_health_ok": 1, "data_health": 1.0}
    cfg = {"strong_need_continuation": 2}

    eng = ofe.OFConfirmEngine()
    ofc, _ = eng.build(
        symbol="T", tf="1m", direction="LONG", tick_ts_ms=2000, price=100, delta_z=1.0,
        runtime=rt, cfg=cfg, indicators=ind, absorption=None
    )

    assert ofc is not None
    ev = ofc.evidence
    missing = ev.get("missing_legs", [])

    # For continuation, missing_legs should use semantic names corresponding to a, b, c.
    # Since a=1 (hidden_ctx_recent), b=0 (obi_stable), c=0 (cont_ctx_recent), missing should be ["obi_stable", "cont_ctx_recent"]
    assert "obi_stable" in missing, "obi_stable should be in missing_legs"
    assert "cont_ctx_recent" in missing, "cont_ctx_recent should be in missing_legs"
    assert "hidden_ctx_recent" not in missing, "hidden_ctx_recent should NOT be in missing_legs (it's present)"


def test_trend_dir_fallback_chain(monkeypatch):
    """Verify trend_dir fallback chain: hidden_div -> indicators -> regime -> direction."""

    class _CancelGateStub:
        def check(self, **_kwargs):
            return types.SimpleNamespace(allow=True, reason="ok", meta={})

    def mock_eval_continuation(**kwargs):
        trend_dir = kwargs.get("trend_dir")
        if trend_dir is None:
            return types.SimpleNamespace(ok=False, have=0, need=2, scenario="continuation", reason="no_trend_dir", gate_bits=0, a=0, b=0, c=0)
        return types.SimpleNamespace(ok=True, have=2, need=2, scenario="continuation", reason="ok", gate_bits=3, a=1, b=1, c=0)

    monkeypatch.setattr(ofe, "eval_continuation", mock_eval_continuation)
    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: _CancelGateStub())
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)
    monkeypatch.setattr(core.compat_utils, "_filter_kwargs_for_callable", lambda func, **kwargs: kwargs)

    # Test 1: hidden_div should be used first
    rt1 = types.SimpleNamespace(
        last_obi_event={"ts_ms": 1000, "direction": "LONG", "obi": 0.5, "stable_secs": 2.0, "stable": 1},
        last_iceberg_event=None,
        last_ofi_event={"ts_ms": 1000, "direction": "LONG", "ofi": 1.0, "stable_secs": 2.0, "stable": 1},
        last_fp_edge=None,
        last_sweep=None,
        last_reclaim=None,
        last_wp=types.SimpleNamespace(weak_any=False),
        last_bar=None,
        dynamic_cfg={},
        pressure=MagicMock(is_pressure_hi=lambda *a: False),
        book_churn_hi=0,
        last_regime="na",
        last_div=types.SimpleNamespace(kind="bullish_hidden", ts_ms=1000),
        cont_ctx_ts_ms=1000,
    )

    ind1 = {"book_health_ok": 1, "data_health": 1.0}
    cfg1 = {"strong_need_continuation": 2}

    eng = ofe.OFConfirmEngine()
    ofc1, _ = eng.build(
        symbol="T", tf="1m", direction="LONG", tick_ts_ms=2000, price=100, delta_z=1.0,
        runtime=rt1, cfg=cfg1, indicators=ind1, absorption=None
    )

    assert ofc1 is not None
    assert ind1.get("trend_dir_source") == "hidden_div", "trend_dir_source should be 'hidden_div' when hidden div is present"

    # Test 2: regime fallback when no hidden_div
    rt2 = types.SimpleNamespace(
        last_obi_event={"ts_ms": 1000, "direction": "LONG", "obi": 0.5, "stable_secs": 2.0, "stable": 1},
        last_iceberg_event=None,
        last_ofi_event={"ts_ms": 1000, "direction": "LONG", "ofi": 1.0, "stable_secs": 2.0, "stable": 1},
        last_fp_edge=None,
        last_sweep=None,
        last_reclaim=None,
        last_wp=types.SimpleNamespace(weak_any=False),
        last_bar=None,
        dynamic_cfg={},
        pressure=MagicMock(is_pressure_hi=lambda *a: False),
        book_churn_hi=0,
        last_regime="bull_trend",
        last_div=None,  # No hidden div
        cont_ctx_ts_ms=1000,
    )

    ind2 = {"book_health_ok": 1, "data_health": 1.0}
    cfg2 = {"strong_need_continuation": 2}

    ofc2, _ = eng.build(
        symbol="T", tf="1m", direction="LONG", tick_ts_ms=2000, price=100, delta_z=1.0,
        runtime=rt2, cfg=cfg2, indicators=ind2, absorption=None
    )

    assert ofc2 is not None
    assert ind2.get("trend_dir_source") == "regime", "trend_dir_source should be 'regime' when using regime fallback"

    # Test 3: direction fallback when OF_TREND_DIR_FALLBACK_TO_DIRECTION is enabled
    with monkeypatch.context() as m:
        m.setenv("OF_TREND_DIR_FALLBACK_TO_DIRECTION", "1")
        rt3 = types.SimpleNamespace(
            last_obi_event={"ts_ms": 1000, "direction": "LONG", "obi": 0.5, "stable_secs": 2.0, "stable": 1},
            last_iceberg_event=None,
            last_ofi_event={"ts_ms": 1000, "direction": "LONG", "ofi": 1.0, "stable_secs": 2.0, "stable": 1},
            last_fp_edge=None,
            last_sweep=None,
            last_reclaim=None,
            last_wp=types.SimpleNamespace(weak_any=False),
            last_bar=None,
            dynamic_cfg={},
            pressure=MagicMock(is_pressure_hi=lambda *a: False),
            book_churn_hi=0,
            last_regime="na",  # No regime hint
            last_div=None,  # No hidden div
            cont_ctx_ts_ms=1000,
        )

        ind3 = {"book_health_ok": 1, "data_health": 1.0}
        cfg3 = {"strong_need_continuation": 2}

        ofc3, _ = eng.build(
            symbol="T", tf="1m", direction="LONG", tick_ts_ms=2000, price=100, delta_z=1.0,
            runtime=rt3, cfg=cfg3, indicators=ind3, absorption=None
        )

        assert ofc3 is not None
        assert ind3.get("trend_dir_source") == "direction", "trend_dir_source should be 'direction' when using direction fallback"


def test_trend_dir_source_in_evidence(monkeypatch):
    """Verify that trend_dir_source is included in evidence."""

    def mock_eval_continuation(**kwargs):
        trend_dir = kwargs.get("trend_dir")
        if trend_dir is None:
            return types.SimpleNamespace(ok=False, have=0, need=2, scenario="continuation", reason="no_trend_dir", gate_bits=0, a=0, b=0, c=0)
        return types.SimpleNamespace(ok=True, have=2, need=2, scenario="continuation", reason="ok", gate_bits=3, a=1, b=1, c=0)

    class _CancelGateStub:
        def check(self, **_kwargs):
            return types.SimpleNamespace(allow=True, reason="ok", meta={})

    monkeypatch.setattr(ofe, "eval_continuation", mock_eval_continuation)
    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: _CancelGateStub())
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)
    monkeypatch.setattr(core.compat_utils, "_filter_kwargs_for_callable", lambda func, **kwargs: kwargs)

    rt = types.SimpleNamespace(
        last_obi_event={"ts_ms": 1000, "direction": "LONG", "obi": 0.5, "stable_secs": 2.0, "stable": 1},
        last_iceberg_event=None,
        last_ofi_event={"ts_ms": 1000, "direction": "LONG", "ofi": 1.0, "stable_secs": 2.0, "stable": 1},
        last_fp_edge=None,
        last_sweep=None,
        last_reclaim=None,
        last_wp=types.SimpleNamespace(weak_any=False),
        last_bar=None,
        dynamic_cfg={},
        pressure=MagicMock(is_pressure_hi=lambda *a: False),
        book_churn_hi=0,
        last_regime="bull_trend",
        last_div=None,
        cont_ctx_ts_ms=1000,
    )

    ind = {"book_health_ok": 1, "data_health": 1.0}
    cfg = {"strong_need_continuation": 2}

    eng = ofe.OFConfirmEngine()
    ofc, _ = eng.build(
        symbol="T", tf="1m", direction="LONG", tick_ts_ms=2000, price=100, delta_z=1.0,
        runtime=rt, cfg=cfg, indicators=ind, absorption=None
    )

    assert ofc is not None
    ev = ofc.evidence

    # Verify trend_dir_source is in evidence
    assert "trend_dir_source" in ev, "trend_dir_source should be in evidence"
    assert ev["trend_dir_source"] == "regime", "trend_dir_source should be 'regime'"

