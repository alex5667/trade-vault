import time
import pytest
from types import SimpleNamespace

from handlers.crypto_orderflow.utils.quality_gates import SignalConsistencyGate

def _of(**kwargs):
    of = SimpleNamespace()
    for k, v in kwargs.items():
        setattr(of, k, v)
    return of

def test_absorption_integration_pipeline(monkeypatch):
    """
    Test that the SignalConsistencyGate correctly processes a mocked ctx
    for the 'absorption' kind and integrates multiple thresholds.
    """
    monkeypatch.setenv("CONSISTENCY_GATE_ENABLED", "1")
    monkeypatch.setenv("CONSISTENCY_APPLY_KINDS", "absorption")
    
    # Let's set the specific ENV variables to match defaults from the docs
    monkeypatch.setenv("CONS_ABSORPTION_MIN_Z", "2.0")
    monkeypatch.setenv("CONS_ABSORPTION_REQUIRE_WEAK_PROGRESS", "1")
    monkeypatch.setenv("CONS_ABSORPTION_REQUIRE_TOUCH_FRESH", "1")
    monkeypatch.setenv("CONS_ABSORPTION_TOUCH_TAG_REQUIRED", "refill")
    monkeypatch.setenv("CONS_ABSORPTION_MIN_TOUCH_RHO", "0.10")
    monkeypatch.setenv("CONS_ABSORPTION_MIN_TOUCH_TRADED_W", "0.0")

    gate = SignalConsistencyGate.from_env()

    # 1. Happy path: LONG absorption
    # LONG targets bid side for refill
    ctx_happy = SimpleNamespace(
        of=_of(z_delta=2.5, weak_progress=True),
        touch_is_stale=False,
        touch_bid_tag="refill",
        touch_bid_rho=0.25,
        touch_bid_traded_w=5.0,
    )
    decision = gate.evaluate(ctx=ctx_happy, symbol="BTCUSDT", kind="absorption", side="LONG")
    assert decision.veto is False, f"Expected OK, got {decision.reason_code}"

    # 2. Veto path: weak progress is False
    ctx_no_wp = SimpleNamespace(
        of=_of(z_delta=2.5, weak_progress=False),
        touch_is_stale=False,
        touch_bid_tag="refill",
        touch_bid_rho=0.25,
        touch_bid_traded_w=5.0,
    )
    decision = gate.evaluate(ctx=ctx_no_wp, symbol="BTCUSDT", kind="absorption", side="LONG")
    assert decision.veto is True
    assert decision.reason_code == "VETO_ABSORPTION_NO_WEAK_PROGRESS"

    # 3. Veto path: touch is stale
    ctx_stale = SimpleNamespace(
        of=_of(z_delta=3.0, weak_progress=True),
        touch_is_stale=True,
        touch_bid_tag="refill",
        touch_bid_rho=0.25,
        touch_bid_traded_w=5.0,
    )
    decision = gate.evaluate(ctx=ctx_stale, symbol="XRPUSDT", kind="absorption", side="LONG")
    assert decision.veto is True
    assert decision.reason_code == "VETO_ABSORPTION_TOUCH_STALE"

    # 4. Veto path: wrong tag (depletion instead of refill)
    ctx_wrong_tag = SimpleNamespace(
        of=_of(z_delta=2.1, weak_progress=True),
        touch_is_stale=False,
        touch_ask_tag="depletion", # SHORT side hit is ask
        touch_ask_rho=0.15,
        touch_ask_traded_w=2.0,
    )
    decision = gate.evaluate(ctx=ctx_wrong_tag, symbol="ETHUSDT", kind="absorption", side="SHORT")
    assert decision.veto is True
    assert decision.reason_code == "VETO_ABSORPTION_TOUCH_TAG_MISMATCH"

    # 5. Veto path: low z-score with symbol override
    monkeypatch.setenv("ETH_DELTA_Z_THRESHOLD", "3.0")
    ctx_low_z = SimpleNamespace(
        of=_of(z_delta=2.5, weak_progress=True),
        touch_is_stale=False,
        touch_ask_tag="refill",
        touch_ask_rho=0.15,
        touch_ask_traded_w=2.0,
    )
    decision = gate.evaluate(ctx=ctx_low_z, symbol="ETHUSDT", kind="absorption", side="SHORT")
    assert decision.veto is True
    assert decision.reason_code == "VETO_ABSORPTION_Z_TOO_LOW"
