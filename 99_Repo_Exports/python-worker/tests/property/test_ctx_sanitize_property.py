from __future__ import annotations

import math
from types import SimpleNamespace

import pytest

try:
    hyp = pytest.importorskip("hypothesis")
    from hypothesis import given
    from hypothesis import strategies as st
    HAS_HYPOTHESIS = True
except pytest.skip.Exception:
    # hypothesis not available, skip these tests
    hyp = None
    given = lambda *args, **kwargs: lambda f: f  # noop decorator
    class MockSt:
        def floats(self, *args, **kwargs): return None
    st = MockSt()
    HAS_HYPOTHESIS = False

from common.sanitize_ctx import sanitize_ctx_inplace


def _isfinite(x) -> bool:
    try:
        return x is None or (isinstance(x, (int, float)) and math.isfinite(float(x)))
    except Exception:
        return True


@given(
    price=st.floats(allow_nan=True, allow_infinity=True, width=64),
    spread_bps=st.floats(allow_nan=True, allow_infinity=True, width=64),
    obi_avg=st.floats(allow_nan=True, allow_infinity=True, width=64),
    micro=st.floats(allow_nan=True, allow_infinity=True, width=64),
    c2t=st.floats(allow_nan=True, allow_infinity=True, width=64),
    geo=st.floats(allow_nan=True, allow_infinity=True, width=64),
)
def test_sanitize_ctx_inplace_never_leaks_nan_inf(price, spread_bps, obi_avg, micro, c2t, geo):
    """
    Инвариант для 6.3:
      sanitize_ctx_inplace переводит NaN/Inf -> None и не падает.
    """
    ctx = SimpleNamespace(
        price=price,
        spread_bps=spread_bps,
        obi_avg=obi_avg,
        microprice_shift_bps_20=micro,
        cancel_to_trade_bid_5s=c2t,
        geometry_score=geo,
        data_quality_flags=[],
    )
    sanitize_ctx_inplace(ctx, logger=None)
    assert _isfinite(ctx.price)
    assert _isfinite(ctx.spread_bps)
    assert _isfinite(ctx.obi_avg)
    assert _isfinite(ctx.microprice_shift_bps_20)
    assert _isfinite(ctx.cancel_to_trade_bid_5s)
    assert _isfinite(ctx.geometry_score)
    # flags всегда list (fail-open схема)
    assert isinstance(getattr(ctx, "data_quality_flags", None), list)
