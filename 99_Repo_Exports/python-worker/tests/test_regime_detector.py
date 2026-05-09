from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from regime.detector import RegimeDetector
from regime.types import RegimeFeatures


@dataclass
class _Cfg:
    regime_window_size: int = 3


@dataclass
class _HTF:
    pdh: float = 110.0
    pdl: float = 90.0
    pdm: float = 100.0


@dataclass
class _Ctx:
    symbol: str = "BTCUSDT"
    price: float = 100.0
    last_price: float | None = None
    vwap: float | None = 100.0
    daily_open: float | None = 100.0
    atr_14_bps: float | None = 60.0
    weak_progress_raw: float | None = 0.7
    ts_utc: float | None = 123.0


def test_compute_features_default_on_missing_symbol_or_price():
    hist = {}
    det = RegimeDetector(cfg=_Cfg(), history=hist, get_htf_levels=lambda s: _HTF(), compute_daily_open_cross_freq=lambda s: 0.5)
    ctx = object()
    f = det.compute_features(ctx)
    assert isinstance(f, RegimeFeatures)
    assert f.vwap_dev_bps is None


def test_compute_features_handles_nan_inf_and_never_raises():
    hist = {}
    det = RegimeDetector(cfg=_Cfg(), history=hist, get_htf_levels=lambda s: _HTF(), compute_daily_open_cross_freq=lambda s: float("nan"))
    ctx = _Ctx(price=float("nan"), vwap=float("inf"), daily_open=-1.0, atr_14_bps=float("inf"), weak_progress_raw=float("nan"))
    f = det.compute_features(ctx)
    assert isinstance(f, RegimeFeatures)
    # because price invalid -> defaults
    assert f.vwap_dev_bps is None


def test_update_history_window_is_stable_maxlen():
    hist = {}
    det = RegimeDetector(cfg=_Cfg(regime_window_size=3), history=hist, get_htf_levels=lambda s: _HTF(), compute_daily_open_cross_freq=lambda s: 0.25)
    ctx = _Ctx(price=100.0, vwap=99.0, daily_open=101.0)
    for i in range(15):
        ctx.ts_utc = 1000.0 + i
        ctx.price = 100.0 + i
        det.update_history(ctx)
    assert "BTCUSDT" in hist
    assert len(hist["BTCUSDT"]) == 10  # guard enforces minimum 10 samples
    assert getattr(hist["BTCUSDT"], "maxlen", None) == 10
    # NOTE: detector enforces minimum 10 samples window guard; adapt if you change _maxlen() clamp.


def test_delta_dir_bias_responds_to_persistent_vwap_side():
    hist = {}
    det = RegimeDetector(cfg=_Cfg(regime_window_size=20), history=hist, get_htf_levels=lambda s: _HTF(), compute_daily_open_cross_freq=lambda s: 0.1)
    ctx = _Ctx(price=100.0, vwap=99.0, daily_open=100.0)
    # push mostly vwap_side=+1
    for i in range(12):
        ctx.ts_utc = 1000.0 + i
        ctx.price = 100.0 + i
        ctx.vwap = 99.0
        det.update_history(ctx)
    f = det.compute_features(ctx)
    assert f.delta_dir_bias is None or (-1.0 <= f.delta_dir_bias <= 1.0)
    if f.delta_dir_bias is not None:
        assert f.delta_dir_bias > 0.0


def test_optional_hypothesis_property_based_if_available():
    hyp = pytest.importorskip("hypothesis")
    st = pytest.importorskip("hypothesis.strategies")

    @dataclass
    class Ctx:
        symbol: str
        price: Any
        vwap: Any = None
        daily_open: Any = None
        atr_14_bps: Any = None
        weak_progress_raw: Any = None
        ts_utc: Any = None

    float_any = st.floats(allow_nan=True, allow_infinity=True, width=64)
    sym = st.text(min_size=1, max_size=10)
    ctxs = st.builds(Ctx, symbol=sym, price=float_any, vwap=float_any, daily_open=float_any, atr_14_bps=float_any, weak_progress_raw=float_any, ts_utc=float_any)

    det = RegimeDetector(cfg=_Cfg(regime_window_size=20), history={}, get_htf_levels=lambda s: _HTF(), compute_daily_open_cross_freq=lambda s: 0.5)

    @hyp.given(ctx=ctxs)
    def _prop(ctx):
        det.update_history(ctx)
        f = det.compute_features(ctx)
        assert isinstance(f, RegimeFeatures)

    _prop()
