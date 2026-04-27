from __future__ import annotations

from core.microbar import MicroBar
from core.swing_detector import SwingDetector, SwingPoint
from core.divergence_engine import DivergenceEngine, DivergenceEvent
from core.rsi import StreamingRSI


def _bar(ts: int, o: float, h: float, l: float, c: float, cvd: float) -> MicroBar:
    return MicroBar(
        symbol="BTCUSDT",
        tf_ms=1000,
        start_ts_ms=ts,
        end_ts_ms=ts + 1000,
        open=o, high=h, low=l, close=c,
        vol=1.0,
        delta_sum=0.0,
        cvd_close=cvd,
        vwap=c,
        tick_count=1,
    )


def test_swing_detector_fractal_pivot():
    sd = SwingDetector(left=2, right=2, min_bp=0.0, min_range_bp=0.0)
    
    # High pattern: 1, 2, 5, 2, 1 => pivot at 5
    bars = [
        _bar(0, 1, 1, 1, 1, 0),
        _bar(1000, 2, 2, 2, 2, 0),
        _bar(2000, 5, 5, 5, 5, 0),
        _bar(3000, 2, 2, 2, 2, 0),
        _bar(4000, 1, 1, 1, 1, 0),
    ]
    
    swings = []
    for b in bars:
        swings.extend(sd.update(b))
        
    assert len(swings) >= 1
    highs = [sh for sh in swings if sh.kind == "high"]
    assert any(sh.price == 5.0 for sh in highs)


def test_divergence_engine_bearish_regular():
    de = DivergenceEngine(min_strength=0.0, min_price_bp=0.0)
    
    # Two pivot highs: 
    # 1) Price 100, CVD 10
    # 2) Price 110, CVD 5   => Bearish Regular (Price HH, CVD LH)
    
    s1 = SwingPoint(kind="high", ts_ms=1000, price=100.0, cvd=10.0, bar_start_ts_ms=0, bar_end_ts_ms=1000)
    s2 = SwingPoint(kind="high", ts_ms=2000, price=110.0, cvd=5.0, bar_start_ts_ms=1000, bar_end_ts_ms=2000)
    
    out1 = de.update_swing(s1, trend_bias="none")
    out2 = de.update_swing(s2, trend_bias="none")
    
    assert out1 == []
    assert len(out2) == 1
    assert out2[0].kind == "bearish_regular"


def test_rsi_streaming_rising():
    r = StreamingRSI(period=14)
    # Strictly rising price => RSI should be high
    for x in range(1, 21):
        r.update(float(x))
        
    assert r.value is not None
    assert r.value > 80.0
