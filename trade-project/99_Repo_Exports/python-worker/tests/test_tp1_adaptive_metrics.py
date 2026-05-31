"""Tests for core/tp1_adaptive_metrics.emit_decision — envelope + fail-open."""

from __future__ import annotations

from typing import Any

import pytest

from core.adaptive_tp1_policy import AdaptiveTP1Decision
from core.tp1_adaptive_metrics import (
    _flatten_for_xadd,
    build_envelope,
    emit_decision,
    reset_redis_for_tests,
)


def _mk_decision(
    *, reason: str = "tp1_adaptive_shadow", apply: bool = False,
    tp1_rr: float | None = 0.80, p_hit: float | None = 0.9,
) -> AdaptiveTP1Decision:
    return AdaptiveTP1Decision(
        enabled=True, apply=apply, mode="shadow", reason=reason,
        tp1_dist=80.0 if tp1_rr else None, tp1_rr=tp1_rr,
        p_hit=p_hit, p_hit_baseline=0.40,
        ev_baseline_r=-0.5, ev_adaptive_r=0.1, ev_delta_r=0.6,
        cost_r=0.08, samples=350, baseline_rr=1.15,
        grid_evaluated=(0.80, 1.15),
    )


class _FakeRedis:
    def __init__(self) -> None:
        self.last: tuple[str, dict[str, Any]] | None = None
        self.kwargs: dict[str, Any] = {}

    def xadd(self, stream: str, fields: dict[str, Any], **kwargs: Any) -> str:
        self.last = (stream, dict(fields))
        self.kwargs = kwargs
        return "1234-0"


@pytest.fixture(autouse=True)
def _reset(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_redis_for_tests()
    for k in ("TP1_ADAPTIVE_EMIT_ENABLED", "TP1_ADAPTIVE_EMIT_REDIS_URL",
              "TP1_ADAPTIVE_SHADOW_MAXLEN", "TAKER_FEE_BPS"):
        monkeypatch.delenv(k, raising=False)
    yield
    reset_redis_for_tests()


# ---------------------------------------------------------------------------
# build_envelope
# ---------------------------------------------------------------------------


def test_build_envelope_full_fields() -> None:
    env = build_envelope(
        decision=_mk_decision(),
        symbol="BTCUSDT", kind="of", side="LONG", regime="range",
        entry_price=10000.0, sl_price=9900.0,
        baseline_tp1_price=10115.0, baseline_tp1_rr=1.15,
        adaptive_tp1_price=10080.0,
        spread_bps=2.0, slippage_bps=1.0, fee_bps=4.0,
        ts_ms=1700000000000, sid="of:BTCUSDT:42",
    )
    assert env["ts_ms"] == 1700000000000
    assert env["sid"] == "of:BTCUSDT:42"
    assert env["symbol"] == "BTCUSDT"
    assert env["baseline_tp1_rr"] == pytest.approx(1.15)
    assert env["adaptive_tp1_rr"] == pytest.approx(0.80)
    assert env["p_hit_baseline"] == pytest.approx(0.40)
    assert env["p_hit_adaptive"] == pytest.approx(0.9)
    assert env["ev_delta_r"] == pytest.approx(0.6)
    assert env["reason_code"] == "tp1_adaptive_shadow"
    assert env["mode"] == "shadow"


def test_build_envelope_synthesises_sid_when_missing() -> None:
    env = build_envelope(
        decision=_mk_decision(),
        symbol="ETHUSDT", kind="of", side="SHORT", regime="trending",
        entry_price=2000.0, sl_price=2010.0,
        baseline_tp1_price=1985.0, baseline_tp1_rr=1.15,
        adaptive_tp1_price=None,
        spread_bps=0.0, slippage_bps=0.0, fee_bps=4.0,
        ts_ms=1700000000999,
    )
    assert env["sid"].startswith("shadow:ETHUSDT:1700000000999:of:SHORT")


def test_flatten_for_xadd_handles_none_and_floats() -> None:
    env = {"ts_ms": 1, "sid": "x", "f": 1.5, "n": None, "k": "abc"}
    flat = _flatten_for_xadd(env)
    assert flat["ts_ms"] == "1"
    assert flat["f"] == "1.5"
    assert flat["n"] == ""  # None → empty string
    assert flat["k"] == "abc"


# ---------------------------------------------------------------------------
# emit_decision — fail-open & noop paths
# ---------------------------------------------------------------------------


def test_emit_decision_noop_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TP1_ADAPTIVE_EMIT_ENABLED", "0")
    # No exception, no XADD attempt. (Lack of a redis raise is enough.)
    emit_decision(
        decision=_mk_decision(),
        symbol="BTCUSDT", kind="of", side="LONG", regime="range",
        entry_price=10000.0, sl_price=9900.0,
        baseline_tp1_price=10115.0, baseline_tp1_rr=1.15,
        adaptive_tp1_price=10080.0,
        spread_bps=0.0, slippage_bps=0.0, fee_bps=4.0,
    )


def test_emit_decision_skips_when_policy_disabled() -> None:
    """skip_disabled reason should not increment counters or XADD."""
    # decision.reason='tp1_adaptive_skip_disabled' triggers early return.
    d = AdaptiveTP1Decision(
        enabled=False, apply=False, mode="off",
        reason="tp1_adaptive_skip_disabled",
        tp1_dist=None, tp1_rr=None, p_hit=None, p_hit_baseline=None,
        ev_baseline_r=0.0, ev_adaptive_r=0.0, ev_delta_r=0.0,
        cost_r=0.0, samples=0, baseline_rr=None, grid_evaluated=(),
    )
    emit_decision(
        decision=d,
        symbol="BTCUSDT", kind="of", side="LONG", regime="range",
        entry_price=10000.0, sl_price=9900.0,
        baseline_tp1_price=10115.0, baseline_tp1_rr=1.15,
        adaptive_tp1_price=None,
        spread_bps=0.0, slippage_bps=0.0, fee_bps=4.0,
    )


def test_emit_decision_xadd_with_fake_redis(monkeypatch: pytest.MonkeyPatch) -> None:
    """Inject FakeRedis via monkeypatching the lazy getter."""
    fake = _FakeRedis()
    import core.tp1_adaptive_metrics as m
    monkeypatch.setattr(m, "_get_redis_client", lambda: fake)
    emit_decision(
        decision=_mk_decision(),
        symbol="BTCUSDT", kind="of", side="LONG", regime="range",
        entry_price=10000.0, sl_price=9900.0,
        baseline_tp1_price=10115.0, baseline_tp1_rr=1.15,
        adaptive_tp1_price=10080.0,
        spread_bps=2.0, slippage_bps=1.0, fee_bps=4.0,
        ts_ms=1700000000000, sid="of:BTCUSDT:1",
    )
    assert fake.last is not None
    stream, fields = fake.last
    assert stream == "stream:tp1_adaptive_shadow_events"
    assert fields["sid"] == "of:BTCUSDT:1"
    assert fields["symbol"] == "BTCUSDT"
    assert fields["reason_code"] == "tp1_adaptive_shadow"
    assert fake.kwargs.get("approximate") is True


def test_emit_decision_redis_failure_does_not_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Bomb:
        def xadd(self, *a, **k):
            raise RuntimeError("redis down")
    import core.tp1_adaptive_metrics as m
    monkeypatch.setattr(m, "_get_redis_client", lambda: _Bomb())
    # Must not raise.
    emit_decision(
        decision=_mk_decision(),
        symbol="BTCUSDT", kind="of", side="LONG", regime="range",
        entry_price=10000.0, sl_price=9900.0,
        baseline_tp1_price=10115.0, baseline_tp1_rr=1.15,
        adaptive_tp1_price=10080.0,
        spread_bps=0.0, slippage_bps=0.0, fee_bps=4.0,
    )
