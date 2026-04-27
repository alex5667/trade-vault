from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import math
import time
from dataclasses import dataclass

import pytest

try:
    hyp = pytest.importorskip("hypothesis")
    from hypothesis import given, strategies as st
    HAS_HYPOTHESIS = True
except pytest.skip.Exception:
    # hypothesis not available, skip these tests
    hyp = None
    given = lambda *args, **kwargs: lambda f: f  # noop decorator
    class MockSt:
        def integers(self, *args, **kwargs): return None
        def floats(self, *args, **kwargs): return None
        def one_of(self, *args, **kwargs): return None
        def text(self, *args, **kwargs): return None
    st = MockSt()
    HAS_HYPOTHESIS = False

from orderflow.tick_sanitizer import normalize_ts_ms, sanitize_tick


@dataclass
class Tick:
    ts: int
    bid: float
    ask: float
    last: float
    volume: float
    flags: int
    is_buyer_maker: bool | None = None


def _isfinite(x: float) -> bool:
    try:
        return math.isfinite(float(x))
    except Exception:
        return False


@given(st.integers(min_value=-10**12, max_value=10**15))
def test_normalize_ts_ms_seconds_vs_ms(ts: int):
    out = normalize_ts_ms(ts)
    assert out is None or isinstance(out, int)
    if out is None:
        return
    if ts < 1_000_000_000_000:
        assert out == ts * 1000
    else:
        assert out == ts


@given(
    ts=st.integers(min_value=0, max_value=10**15),
    bid=st.floats(allow_nan=True, allow_infinity=True, width=64),
    ask=st.floats(allow_nan=True, allow_infinity=True, width=64),
    last=st.floats(allow_nan=True, allow_infinity=True, width=64),
    volume=st.floats(allow_nan=True, allow_infinity=True, width=64),
    flags=st.one_of(st.integers(), st.text(), st.floats(allow_nan=True, allow_infinity=True)),
)
def test_sanitize_tick_never_leaks_nan_inf(ts, bid, ask, last, volume, flags):
    """
    Инвариант:
      - sanitize_tick никогда не падает на шумных входах
      - если возвращает tick, то ключевые поля конечны (finite) и пригодны для mid/скоринга
    """
    # Делает вероятность пройти watermark не нулевой
    now_ms = get_ny_time_millis()
    # ts может быть в секундах: подстроим ближе к now
    ts = now_ms if ts % 2 == 0 else (now_ms // 1000)

    t = Tick(ts=ts, bid=bid, ask=ask, last=last, volume=volume, flags=flags)  # type: ignore[arg-type]
    out = sanitize_tick(t, logger=None)
    if out is None:
        return
    assert isinstance(out.ts, int)
    assert out.ts >= 1_000_000_000_000  # после нормализации это ms
    assert _isfinite(out.bid) and out.bid > 0.0
    assert _isfinite(out.ask) and out.ask > 0.0
    assert _isfinite(out.last) and out.last > 0.0
    assert _isfinite(out.volume) and out.volume >= 0.0
    assert isinstance(out.flags, int)
