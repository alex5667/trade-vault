from __future__ import annotations

import types
from unittest.mock import MagicMock, patch

import pytest

from handlers.crypto_orderflow_handler import CryptoOrderFlowHandler


def _make_handler(**kwargs):
    """Create a lightweight test stub for unit-testing _generate_signals."""
    from handlers.crypto_orderflow.config.runtime_config import _RuntimeCfg
    from handlers.confidence_pct_provider import build_confidence_pct_fn

    emitter = kwargs.pop("emitter", None) or _FakeEmitter()
    confirmations_engine = kwargs.pop("confirmations_engine", None)
    calibrator = kwargs.pop("calibrator", None)
    symbol = kwargs.pop("symbol", "BTCUSDT")

    # Build without __init__ to avoid Redis wiring
    h = object.__new__(CryptoOrderFlowHandler)
    h.symbol = symbol
    h.logger = MagicMock()
    h.redis = MagicMock()
    h._emitter = emitter
    h._confirmations = confirmations_engine
    h._sigm = None
    h._cfg = _RuntimeCfg.from_env()
    h._calibrator = calibrator
    h._use_calibrator = calibrator is not None
    h._confidence_cap = 95.0
    h._conf_pct_fn = build_confidence_pct_fn(calibrator=calibrator, cap_pct=95.0)
    # Stubs for gate methods
    h._last_l2_snapshot = None
    h._metrics = None
    h._apply_regime_gate = lambda signal_kind, ctx: (True, "OK")
    h._emit_veto_metric = lambda kind, ctx, reason_code: None
    h._observe_soft_penalty = lambda *a, **kw: None
    # default confirmations that passes OK if none injected
    if h._confirmations is None:
        _ok_conf = MagicMock()
        _ok_conf.validate.return_value = types.SimpleNamespace(
            veto=False, conf_factor01=0.25, parts={}, reason_code="OK", reason_u16=0
        )
        h._confirmations = _ok_conf
    return h


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
    def validate(self, *args, **kwargs):
        return self._res


def test_generate_signals_returns_strict_bool_false_on_none_ctx():
    h = _make_handler()
    assert h._generate_signals(None) is False


def test_generate_signals_final_score_contract_raw_times_conf():
    h = _make_handler()
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
    emitter = _FakeEmitter()
    confirm = _FakeConfirmEngine(_FakeConfirm(False, 0.50))
    h = _make_handler(emitter=emitter, confirmations_engine=confirm, calibrator=cal)
    h._emitter = emitter
    h._confirmations = confirm
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
    emitter = _FakeEmitter()
    confirm = _FakeConfirmEngine(_FakeConfirm(False, 1.0))
    h = _make_handler(emitter=emitter, confirmations_engine=confirm, calibrator=cal)
    h._emitter = emitter
    h._confirmations = confirm
    h._detect_candidates = lambda ctx: [
        types.SimpleNamespace(kind="breakout", side=1, raw_score=80.0, level_price=100.0, level_key="L", reasons=[], meta={})
    ]
    ctx = types.SimpleNamespace(symbol="BTCUSDT", ts=1, price=100.0, atr=1.0, pivots=None, market_regime="trend")
    assert h._generate_signals(ctx) is True
    p = h._emitter.payloads[0]
    # inf must not leak
    assert 0.0 <= p["confidence"] <= 95.0
