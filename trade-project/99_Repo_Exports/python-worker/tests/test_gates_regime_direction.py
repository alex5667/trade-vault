"""Tests for direction-aware regime gate (block counter-trend trades).

Background: 2026-05-18 audit found 780 LONG trades opened during
``regime=trending_bear`` (winrate 22.9%, -119.1% cumulative PnL). The pre-fix
``check_regime_gate`` never received ``side`` and only blocked specific
``kind`` values (breakout/extreme). This suite covers the direction filter.
"""
from __future__ import annotations

from types import SimpleNamespace

from handlers.crypto_orderflow.components.gates import GateOrchestrator


def _new_gates() -> GateOrchestrator:
    return GateOrchestrator(None, None, None, None, None)  # type: ignore


def test_dir_gate_off_by_default(monkeypatch):
    """Without REGIME_DIRECTION_GATE_MODE set, gate should ABSTAIN
    (legacy strict gate is also off by default)."""
    monkeypatch.delenv("REGIME_DIRECTION_GATE_MODE", raising=False)
    monkeypatch.delenv("REGIME_GATE_STRICT", raising=False)
    gates = _new_gates()
    ctx = SimpleNamespace(market_regime="trending_bear", symbol="BTCUSDT")
    dec = gates.check_regime_gate(ctx, kind="breakout", side="LONG")
    assert dec.decision == "ABSTAIN"
    assert dec.reason_code == "OK"


def test_dir_gate_enforce_blocks_long_in_bear(monkeypatch):
    monkeypatch.setenv("REGIME_DIRECTION_GATE_MODE", "enforce")
    gates = _new_gates()
    ctx = SimpleNamespace(market_regime="trending_bear", symbol="BTCUSDT")
    dec = gates.check_regime_gate(ctx, kind="breakout", side="LONG")
    assert dec.decision == "DENY"
    assert dec.reason_code == "VETO_REGIME_COUNTER_TREND_LONG"
    assert dec.notes["regime"] == "trending_bear"
    assert dec.notes["side"] == "LONG"


def test_dir_gate_enforce_blocks_short_in_bull(monkeypatch):
    monkeypatch.setenv("REGIME_DIRECTION_GATE_MODE", "enforce")
    gates = _new_gates()
    ctx = SimpleNamespace(market_regime="trending_bull", symbol="ETHUSDT")
    dec = gates.check_regime_gate(ctx, kind="breakout", side="SHORT")
    assert dec.decision == "DENY"
    assert dec.reason_code == "VETO_REGIME_COUNTER_TREND_SHORT"


def test_dir_gate_allows_trend_following(monkeypatch):
    monkeypatch.setenv("REGIME_DIRECTION_GATE_MODE", "enforce")
    gates = _new_gates()
    # SHORT in bear is trend-following → ALLOW.
    ctx_bear = SimpleNamespace(market_regime="trending_bear", symbol="BTCUSDT")
    dec = gates.check_regime_gate(ctx_bear, kind="breakout", side="SHORT")
    assert dec.decision == "ALLOW"
    assert dec.reason_code == "OK"
    # LONG in bull is trend-following → ALLOW.
    ctx_bull = SimpleNamespace(market_regime="trending_bull", symbol="ETHUSDT")
    dec = gates.check_regime_gate(ctx_bull, kind="breakout", side="LONG")
    assert dec.decision == "ALLOW"
    assert dec.reason_code == "OK"


def test_dir_gate_shadow_mode_does_not_deny(monkeypatch):
    monkeypatch.setenv("REGIME_DIRECTION_GATE_MODE", "shadow")
    gates = _new_gates()
    ctx = SimpleNamespace(market_regime="trending_bear", symbol="BTCUSDT")
    dec = gates.check_regime_gate(ctx, kind="breakout", side="LONG")
    assert dec.decision == "SHADOW_DENY"
    assert dec.reason_code == "VETO_REGIME_COUNTER_TREND_LONG"
    assert dec.notes["dir_gate_mode"] == "shadow"


def test_dir_gate_neutral_regime_allows(monkeypatch):
    monkeypatch.setenv("REGIME_DIRECTION_GATE_MODE", "enforce")
    gates = _new_gates()
    for r in ("range", "squeeze", "mixed", "expansion", "na", ""):
        ctx = SimpleNamespace(market_regime=r, symbol="BTCUSDT")
        for side in ("LONG", "SHORT"):
            dec = gates.check_regime_gate(ctx, kind="breakout", side=side)
            assert dec.decision == "ALLOW", f"regime={r!r} side={side} expected ALLOW, got {dec}"


def test_dir_gate_unknown_side_fails_open(monkeypatch):
    monkeypatch.setenv("REGIME_DIRECTION_GATE_MODE", "enforce")
    gates = _new_gates()
    ctx = SimpleNamespace(market_regime="trending_bear", symbol="BTCUSDT")
    # Missing side → ALLOW (fail-open to avoid blocking the whole flow on a bug).
    dec = gates.check_regime_gate(ctx, kind="breakout", side="")
    assert dec.decision == "ALLOW"


def test_dir_gate_accepts_int_side(monkeypatch):
    """Direction sometimes arrives as +1/-1 integer — must still gate."""
    monkeypatch.setenv("REGIME_DIRECTION_GATE_MODE", "enforce")
    gates = _new_gates()
    ctx = SimpleNamespace(market_regime="trending_bear", symbol="BTCUSDT")
    dec = gates.check_regime_gate(ctx, kind="breakout", side=1)  # 1 == LONG
    assert dec.decision == "DENY"
    assert dec.reason_code == "VETO_REGIME_COUNTER_TREND_LONG"


def test_dir_gate_custom_bear_labels(monkeypatch):
    monkeypatch.setenv("REGIME_DIRECTION_GATE_MODE", "enforce")
    monkeypatch.setenv("REGIME_BEAR_LABELS", "downtrend,collapse")
    gates = _new_gates()
    ctx = SimpleNamespace(market_regime="downtrend", symbol="BTCUSDT")
    dec = gates.check_regime_gate(ctx, kind="breakout", side="LONG")
    assert dec.decision == "DENY"
    assert dec.reason_code == "VETO_REGIME_COUNTER_TREND_LONG"
    # Default "trending_bear" is no longer in the set → ALLOW.
    ctx2 = SimpleNamespace(market_regime="trending_bear", symbol="BTCUSDT")
    dec2 = gates.check_regime_gate(ctx2, kind="breakout", side="LONG")
    assert dec2.decision == "ALLOW"


def test_dir_gate_redis_fallback(monkeypatch):
    """When ctx.market_regime is empty, fall back to Redis `regime:{SYMBOL}`."""
    monkeypatch.setenv("REGIME_DIRECTION_GATE_MODE", "enforce")
    monkeypatch.setenv("REGIME_GATE_REDIS_FALLBACK", "1")
    # Force cache miss between runs.
    monkeypatch.setenv("REGIME_GATE_REDIS_CACHE_TTL_S", "0")

    class FakeRedis:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def get(self, key: str):
            self.calls.append(key)
            return b"trending_bear" if key == "regime:BTCUSDT" else None

    fake = FakeRedis()
    portfolio = SimpleNamespace(r=fake)
    gates = GateOrchestrator(None, None, portfolio, None, None)  # type: ignore
    ctx = SimpleNamespace(market_regime="", symbol="BTCUSDT")
    dec = gates.check_regime_gate(ctx, kind="breakout", side="LONG")
    assert dec.decision == "DENY"
    assert dec.reason_code == "VETO_REGIME_COUNTER_TREND_LONG"
    assert "regime:BTCUSDT" in fake.calls


def test_dir_gate_redis_fallback_disabled(monkeypatch):
    monkeypatch.setenv("REGIME_DIRECTION_GATE_MODE", "enforce")
    monkeypatch.setenv("REGIME_GATE_REDIS_FALLBACK", "0")

    class FakeRedis:
        def get(self, key: str):  # noqa: ARG002
            return b"trending_bear"

    portfolio = SimpleNamespace(r=FakeRedis())
    gates = GateOrchestrator(None, None, portfolio, None, None)  # type: ignore
    ctx = SimpleNamespace(market_regime="", symbol="BTCUSDT")
    dec = gates.check_regime_gate(ctx, kind="breakout", side="LONG")
    # No regime → no counter-trend signal → ALLOW.
    assert dec.decision == "ALLOW"


def test_dir_gate_redis_cache(monkeypatch):
    """Redis fallback caches result within TTL window."""
    monkeypatch.setenv("REGIME_DIRECTION_GATE_MODE", "enforce")
    monkeypatch.setenv("REGIME_GATE_REDIS_CACHE_TTL_S", "60")

    class FakeRedis:
        def __init__(self) -> None:
            self.calls = 0

        def get(self, key: str):  # noqa: ARG002
            self.calls += 1
            return b"trending_bear"

    fake = FakeRedis()
    portfolio = SimpleNamespace(r=fake)
    gates = GateOrchestrator(None, None, portfolio, None, None)  # type: ignore
    ctx = SimpleNamespace(market_regime="", symbol="BTCUSDT")
    for _ in range(5):
        gates.check_regime_gate(ctx, kind="breakout", side="LONG")
    assert fake.calls == 1  # cached
