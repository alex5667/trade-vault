from __future__ import annotations

import math

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given
from hypothesis import strategies as st

from handlers.confirmations.l2_common import sanitize_book, wall_distance_bps
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level

nan = float("nan")
inf = float("inf")


@given(
    p=st.one_of(st.floats(allow_nan=True, allow_infinity=True), st.integers()),
    s=st.one_of(st.floats(allow_nan=True, allow_infinity=True), st.integers()),
    n=st.one_of(st.floats(allow_nan=True, allow_infinity=True), st.integers()),
)
def test_sanitize_book_strips_non_finite_and_non_positive(p, s, n):
    lvls = [L2Level(price=float(p), size=float(s), notional=float(n))]
    out = sanitize_book(lvls, max_levels=10, min_notional=0.0)
    for x in out:
        assert math.isfinite(float(x.price)) and float(x.price) > 0.0
        assert math.isfinite(float(x.size)) and float(x.size) > 0.0
        assert math.isfinite(float(x.notional)) and float(x.notional) > 0.0


def test_wall_distance_none_on_bad_ref_price():
    lvls = [L2Level(price=100.0, size=1.0, notional=10000.0)]
    assert wall_distance_bps(ref_price=nan, levels=lvls, min_wall_notional=1.0) is None
    assert wall_distance_bps(ref_price=inf, levels=lvls, min_wall_notional=1.0) is None
    assert wall_distance_bps(ref_price=0.0, levels=lvls, min_wall_notional=1.0) is None


@given(
    ref=st.floats(min_value=1.0, max_value=1e6, allow_nan=False, allow_infinity=False),
    p1=st.floats(min_value=1.0, max_value=1e6, allow_nan=False, allow_infinity=False),
    p2=st.floats(min_value=1.0, max_value=1e6, allow_nan=False, allow_infinity=False),
)
def test_wall_distance_is_min_distance(ref, p1, p2):
    lvls = [
        L2Level(price=float(p1), size=1.0, notional=50000.0),
        L2Level(price=float(p2), size=1.0, notional=50000.0),
    ]
    d = wall_distance_bps(ref_price=ref, levels=lvls, min_wall_notional=1.0)
    assert d is not None
    expected = min(abs(p1 - ref) / ref * 10_000.0, abs(p2 - ref) / ref * 10_000.0)
    assert abs(float(d) - float(expected)) < 1e-9
