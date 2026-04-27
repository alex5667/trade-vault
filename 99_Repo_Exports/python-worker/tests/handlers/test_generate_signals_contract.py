from __future__ import annotations

import types

import pytest

from handlers.crypto_orderflow_handler import CryptoOrderFlowHandler


class _FakeEmitter:
    def __init__(self) -> None:
        self.payloads = []
    def emit(self, payload, labels=None, dedup=True) -> bool:
        self.payloads.append(payload)
        return True


class _FakeCalibrator:
    def __init__(self, out: float) -> None:
        self._out = out
        self.calls = []
    def calibrate(self, *, kind: str, symbol: str, score: float, ts_ms: int) -> float:
        self.calls.append((kind, symbol, score, ts_ms))
        return float(self._out)


class _FakeConfirm:
    def __init__(self, veto: bool, conf: float, reason_code: str = "OK") -> None:
        self.veto = veto
        self.conf_factor01 = conf
        self.parts = {"x": 1.0}
        self.reason_code = reason_code
        self.reason_u16 = 0


class _FakeConfirmEngine:
    def __init__(self, res):
        self._res = res
    def validate(self, kind, ctx, l2, l3, level_price):
        return self._res


def test_generate_signals_returns_strict_bool_false_on_none_ctx():
    h = CryptoOrderFlowHandler(symbol="BTCUSDT")
    assert h._generate_signals(None) is False


def test_generate_signals_final_score_contract_raw_times_conf():
    h = CryptoOrderFlowHandler(symbol="BTCUSDT")
    # monkeypatch detector output (avoid depending on your real detector)
    h._detect_candidates = lambda ctx: [
        types.SimpleNamespace(kind="breakout", side=1, raw_score=40.0, level_price=100.0, level_key="L", reasons=[], meta={})
    ]
    ctx = types.SimpleNamespace(symbol="BTCUSDT", ts=1_000, price=100.0, atr=1.0, pivots=None, market_regime="trend")
    ok = h._generate_signals(ctx)
    assert ok is True
    assert len(h._emitter.payloads) == 1
    p = h._emitter.payloads[0]
    assert p["raw_score"] == 40.0
    assert p["final_score"] == pytest.approx(10.0)  # 40 * 0.25
    assert 0.0 <= p["confidence"] <= 100.0


def test_generate_signals_uses_calibrator_for_confidence_pct():
    cal = _FakeCalibrator(77.7)
    h = CryptoOrderFlowHandler(
        emitter=_FakeEmitter(),
        confirmations_engine=_FakeConfirmEngine(_FakeConfirm(False, 0.50)),
        calibrator=cal,
    )
    h._detect_candidates = lambda ctx: [
        types.SimpleNamespace(kind="breakout", side=1, raw_score=20.0, level_price=100.0, level_key="L", reasons=[], meta={})
    ]
    ctx = types.SimpleNamespace(symbol="BTCUSDT", ts=123_000, price=100.0, atr=1.0, pivots=None, market_regime="trend")
    assert h._generate_signals(ctx) is True
    p = h._emitter.payloads[0]
    assert p["confidence"] == pytest.approx(77.7)
    assert len(cal.calls) == 1
    kind, sym, score, ts_ms = cal.calls[0]
    assert kind == "breakout"
    assert sym == "BTCUSDT"
    assert ts_ms == 123_000


def test_calibrator_confidence_is_sanitized_and_clamped():
    cal = _FakeCalibrator(float("inf"))
    h = CryptoOrderFlowHandler(
        emitter=_FakeEmitter(),
        confirmations_engine=_FakeConfirmEngine(_FakeConfirm(False, 1.0)),
        calibrator=cal,
    )
    h._detect_candidates = lambda ctx: [
        types.SimpleNamespace(kind="breakout", side=1, raw_score=80.0, level_price=100.0, level_key="L", reasons=[], meta={})
    ]
    ctx = types.SimpleNamespace(symbol="BTCUSDT", ts=1, price=100.0, atr=1.0, pivots=None, market_regime="trend")
    assert h._generate_signals(ctx) is True
    p = h._emitter.payloads[0]
    # inf must not leak
    assert 0.0 <= p["confidence"] <= 95.0
