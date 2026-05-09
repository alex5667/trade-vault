from dataclasses import dataclass

from handlers.confirmations.engine import ConfirmationsEngine
from signal_scoring.reason_codes import ReasonCode


@dataclass
class Ctx:
    symbol: str = "BTCUSDT"
    spread_bps: float = 0.0


def _mk_engine(monkeypatch):
    # Ensure deterministic thresholds for tests.
    monkeypatch.setenv("MIN_CONF_FACTOR01", "0.50")
    monkeypatch.setenv("SPREAD_VETO_BPS", "25")
    return ConfirmationsEngine()


def test_breakout_l2_missing_has_structured_reason_code(monkeypatch):
    eng = _mk_engine(monkeypatch)
    ctx = Ctx(spread_bps=0.0)

    res = eng.validate(
        kind="breakout",
        ctx=ctx,
        l2=None,
        l3=None,
        level_price=100.0,
    )
    assert res.veto is True
    assert res.reason_code == ReasonCode.VETO_L2_MISSING.value


def test_breakout_l2_stale_has_structured_reason_code(monkeypatch):
    eng = _mk_engine(monkeypatch)
    ctx = Ctx(spread_bps=0.0)

    # Mock stale l2
    l2 = type("MockL2", (), {"ts_ms": 0})()
    eng._now_ms = lambda: 2000  # Make it stale

    res = eng.validate(
        kind="breakout",
        ctx=ctx,
        l2=l2,
        l3=None,
        level_price=100.0,
    )
    assert res.veto is True
    assert res.reason_code == ReasonCode.VETO_L2_STALE.value


def test_conf_below_min_has_structured_reason_code(monkeypatch):
    eng = _mk_engine(monkeypatch)
    ctx = Ctx(spread_bps=0.0)

    res = eng.validate(
        kind="extreme",
        ctx=ctx,
        l2={"ok": 1},
        l3=None,
        level_price=None,
    )
    assert res.veto is True
    assert res.reason_code == ReasonCode.VETO_CONF_BELOW_MIN.value


def test_spread_wide_has_structured_reason_code(monkeypatch):
    eng = _mk_engine(monkeypatch)
    ctx = Ctx(spread_bps=50.0)  # >= SPREAD_VETO_BPS=25

    res = eng.validate(
        kind="breakout",
        ctx=ctx,
        l2={"ok": 1},
        l3=None,
        level_price=100.0,
    )
    assert res.veto is True
    assert res.reason_code == ReasonCode.VETO_SPREAD_WIDE.value
    assert "spread_bps" in res.parts
