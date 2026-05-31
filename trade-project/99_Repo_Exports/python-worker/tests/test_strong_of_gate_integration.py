import types
from unittest.mock import MagicMock

import pytest

import core.compat_utils
import core.of_confirm_engine as ofe
import contextlib


@pytest.fixture
def mock_engine_deps(monkeypatch):
    class _CancelGateStub:
        def check(self, **_kwargs):
            return types.SimpleNamespace(allow=True, reason="ok", meta={})

    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: _CancelGateStub())
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)

def test_engine_reversal_scenario_triggers_eval(mock_engine_deps, monkeypatch):
    """
    Test that OFConfirmEngine properly triggers eval_reversal when sweep_recent is True.
    """
    captured = {}
    def mock_eval_reversal(**kwargs):
        captured.update(kwargs)
        return types.SimpleNamespace(ok=True, have=2, need=2, scenario="reversal", reason="ok", gate_bits=0)

    monkeypatch.setattr(ofe, "eval_reversal", mock_eval_reversal)
    monkeypatch.setattr(core.compat_utils, "_filter_kwargs_for_callable", lambda func, **kwargs: kwargs)

    rt = types.SimpleNamespace(
        last_obi_event={"ts_ms": 1000, "direction": "LONG", "obi": 0.0, "stable_secs": 0.0, "stable": 0},
        last_iceberg_event=None,
        last_ofi_event=None,
        last_fp_edge=None,
        last_sweep=types.SimpleNamespace(ts_ms=1000, kind="EQH_SWEEP", direction_bias="SHORT"), # Triggers Reversal
        last_reclaim=types.SimpleNamespace(ts_ms=1000, direction="LONG"), # Triggers Reclaim
        last_wp=types.SimpleNamespace(weak_any=True), # Triggers WP
        last_bar=None,
        dynamic_cfg={},
        pressure=MagicMock(is_pressure_hi=lambda *a: False),
        book_churn_hi=0,
        last_regime="na",
        last_div=None,
    )

    ind = {"book_health_ok": 1, "data_health": 1.0, "now_ts_ms": 1100}
    cfg = {"strong_need_reversal": 2}

    eng = ofe.OFConfirmEngine()

    # We catch any exception from other parts of build(). We only care that eval_reversal was called with the correct args.
    with contextlib.suppress(Exception):
        eng.build(
            symbol="T", tf="1m", direction="LONG", tick_ts_ms=1100, price=100, delta_z=3.0,
            runtime=rt, cfg=cfg, indicators=ind, absorption=None
        )

    assert len(captured) > 0, "eval_reversal was not called"
    assert captured["weak_progress"] is True
    assert captured["sweep_recent"] is True
    assert captured["reclaim_recent"] is True
    assert captured["delta_z"] == 3.0

def test_engine_continuation_scenario_triggers_eval(mock_engine_deps, monkeypatch):
    """
    Test that OFConfirmEngine properly triggers eval_continuation when sweep is absent and trend exists.
    """
    captured = {}
    def mock_eval_continuation(**kwargs):
        captured.update(kwargs)
        return types.SimpleNamespace(ok=True, have=2, need=2, scenario="continuation", reason="ok", gate_bits=0)

    monkeypatch.setattr(ofe, "eval_continuation", mock_eval_continuation)
    monkeypatch.setattr(core.compat_utils, "_filter_kwargs_for_callable", lambda func, **kwargs: kwargs)

    # Trend dir requires last_div or regime
    rt = types.SimpleNamespace(
        last_obi_event={"ts_ms": 1000, "direction": "LONG", "obi": 0.0, "stable_secs": 0.0, "stable": 0},
        last_iceberg_event=None,
        last_ofi_event=None,
        last_fp_edge=None,
        last_sweep=None, # No sweep -> Continuation
        last_reclaim=None,
        last_wp=types.SimpleNamespace(weak_any=True),
        last_bar=None,
        dynamic_cfg={},
        pressure=MagicMock(is_pressure_hi=lambda *a: False),
        book_churn_hi=0,
        last_regime="bullish_hidden", # trend_dir=LONG
        last_div=types.SimpleNamespace(ts_ms=1050, kind="bullish_hidden"),
        cont_ctx_ts_ms=1050,
    )

    ind = {"book_health_ok": 1, "data_health": 1.0, "now_ts_ms": 1100}
    cfg = {"strong_need_continuation": 2, "hidden_ctx_valid_ms": 1000, "cont_ctx_valid_ms": 1000}

    eng = ofe.OFConfirmEngine()

    with contextlib.suppress(Exception):
        eng.build(
            symbol="T", tf="1m", direction="LONG", tick_ts_ms=1100, price=100, delta_z=3.0,
            runtime=rt, cfg=cfg, indicators=ind, absorption=None
        )

    assert len(captured) > 0, "eval_continuation was not called"
    assert captured["trend_dir"] == "LONG"
    assert captured["hidden_ctx_recent"] is True
    assert captured["cont_ctx_recent"] is True
    assert captured["obi_stable"] is False
