from __future__ import annotations

import math

import pytest

from handlers.confidence_pct_provider import build_confidence_pct_fn


class _PctCal:
    def __init__(self, out: float) -> None:
        self.out = out
        self.calls = []
    def pct(self, *, kind: str, symbol: str, value: float) -> float:
        self.calls.append((kind, symbol, value))
        return float(self.out)


class _ConfPctCal:
    def __init__(self, out: float) -> None:
        self.out = out
        self.calls = []
    def confidence_pct(self, *, kind: str, symbol: str, final_score: float, ts_ms: int) -> float:
        self.calls.append((kind, symbol, final_score, ts_ms))
        return float(self.out)


class _CalibrateSvc:
    def __init__(self, out: float) -> None:
        self.out = out
        self.calls = []
    def calibrate(self, *, kind: str, symbol: str, score: float, ts_ms: int) -> float:
        self.calls.append((kind, symbol, score, ts_ms))
        return float(self.out)


def test_provider_prefers_confidence_pct_signature():
    cal = _ConfPctCal(77.7)
    fn = build_confidence_pct_fn(cal, cap_pct=95.0)
    v = fn("breakout", "BTCUSDT", 12.3, 123000)
    assert v == pytest.approx(77.7)
    assert len(cal.calls) == 1


def test_provider_supports_pct_signature():
    cal = _PctCal(55.5)
    fn = build_confidence_pct_fn(cal, cap_pct=95.0)
    v = fn("absorption", "ETHUSDT", -9.0, 1)
    assert v == pytest.approx(55.5)
    assert len(cal.calls) == 1
    kind, sym, value = cal.calls[0]
    assert kind == "absorption"
    assert sym == "ETHUSDT"
    assert value == pytest.approx(-9.0)


def test_provider_supports_calibrate_signature_and_clamps():
    cal = _CalibrateSvc(123.0)  # above cap
    fn = build_confidence_pct_fn(cal, cap_pct=95.0)
    v = fn("breakout", "BTCUSDT", 1.0, 7)
    assert v == 95.0
    assert len(cal.calls) == 1


def test_provider_fail_open_on_nan_inf():
    cal = _ConfPctCal(float("inf"))
    fn = build_confidence_pct_fn(cal, cap_pct=95.0)
    v = fn("breakout", "BTCUSDT", 1.0, 7)
    # inf must be sanitized
    assert 0.0 <= v <= 95.0


def test_provider_none_calibrator_fallback_is_monotone_and_finite():
    fn = build_confidence_pct_fn(None, cap_pct=95.0)
    v0 = fn("k", "S", 0.0, 0)
    v1 = fn("k", "S", 1.0, 0)
    v2 = fn("k", "S", 10.0, 0)
    assert 0.0 <= v0 <= 95.0
    assert 0.0 <= v1 <= 95.0
    assert 0.0 <= v2 <= 95.0
    assert math.isfinite(v0) and math.isfinite(v1) and math.isfinite(v2)
    assert v0 <= v1 <= v2
