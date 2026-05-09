from __future__ import annotations

import pytest

hypothesis = pytest.importorskip("hypothesis")
from dataclasses import dataclass

from hypothesis import assume, given
from hypothesis import strategies as st

from handlers.confirmations.l3_quality import L3QualityPolicy
from handlers.geometry.geometry_quality import GeoHit, GeometryQualityPolicy, geometry_score_from_hit


@dataclass
class Ctx:
    ts: int
    # L3 fields (optional)
    cancel_to_trade_bid_5s: float | None = None
    cancel_to_trade_ask_5s: float | None = None
    microprice_shift_bps_20: float | None = None
    spread_bps: float | None = None
    # geometry fields (optional)
    geometry: object | None = None
    geo_zone_hit: object | None = None
    geo_zone_hits: list | None = None
    geometry_score: float | None = None
    data_quality_flags: list[str] | None = None


def test_l3_missing_is_neutral_not_veto():
    p = L3QualityPolicy(missing_score01=0.5)
    ctx = Ctx(ts=10_000)
    a = p.assess(ctx=ctx)
    assert a.veto is False
    assert a.available is False
    assert a.score01 == 0.5
    assert "l3_missing" in a.flags


def test_geometry_missing_is_neutral_not_veto():
    p = GeometryQualityPolicy(missing_score01=0.1)
    ctx = Ctx(ts=10_000)
    a = p.assess(ctx=ctx)
    assert a.veto is False
    assert a.available is False
    assert 0.0 < a.score01 <= 1.0
    assert "htf_missing" in a.flags


@given(
    strength1=st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False),
    strength2=st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False),
    dist1=st.floats(min_value=0.0, max_value=200.0, allow_nan=False, allow_infinity=False),
    dist2=st.floats(min_value=0.0, max_value=200.0, allow_nan=False, allow_infinity=False),
)
def test_geometry_score_monotone_strength_and_distance(strength1, strength2, dist1, dist2):
    # strength↑ => score↑, dist↓ => score↑
    assume(strength2 >= strength1)
    assume(dist2 >= dist1)

    a = geometry_score_from_hit(GeoHit("pdh", strength1, dist1, None))
    b = geometry_score_from_hit(GeoHit("pdh", strength2, dist1, None))
    assert float(b) >= float(a) - 1e-9

    c = geometry_score_from_hit(GeoHit("pdh", strength1, dist1, None))
    d = geometry_score_from_hit(GeoHit("pdh", strength1, dist2, None))
    assert float(d) <= float(c) + 1e-9
