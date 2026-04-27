import math
from types import SimpleNamespace

import pytest

from signal_scoring.geometry import (
    compute_geometry_context,
    distance_to_score,
    geometry_score,
    normalize_zone_strength,
)


def test_normalize_zone_strength_accepts_percent_and_ratio():
    assert normalize_zone_strength(None) == 0.0
    assert normalize_zone_strength(float("nan")) == 0.0
    assert normalize_zone_strength(0.0) == 0.0
    assert normalize_zone_strength(1.0) == 1.0
    assert normalize_zone_strength(50.0) == 0.5
    assert normalize_zone_strength(120.0) == 1.0


def test_distance_to_score_monotonic_in_bps():
    # Strict monotonic decrease for increasing bps distance
    prev = distance_to_score(dist_bps=0.0, dist_rel_atr=None)
    for d in [1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0]:
        cur = distance_to_score(dist_bps=d, dist_rel_atr=None)
        assert 0.0 <= cur <= 1.0
        assert cur <= prev
        prev = cur


def test_distance_to_score_monotonic_in_atr():
    # With fixed bps, increasing rel-atr must not increase score
    base = distance_to_score(dist_bps=10.0, dist_rel_atr=0.0)
    for a in [0.1, 0.3, 0.5, 1.0, 2.0, 5.0]:
        cur = distance_to_score(dist_bps=10.0, dist_rel_atr=a)
        assert cur <= base + 1e-12
        base = cur


def test_geometry_score_monotonic_strength_and_distance():
    # strength up -> score up (same distance)
    s1 = geometry_score(zone_strength01=0.2, dist_bps=10.0, dist_rel_atr=0.3)
    s2 = geometry_score(zone_strength01=0.8, dist_bps=10.0, dist_rel_atr=0.3)
    assert s2 > s1

    # distance up -> score down (same strength)
    d1 = geometry_score(zone_strength01=0.8, dist_bps=5.0, dist_rel_atr=0.2)
    d2 = geometry_score(zone_strength01=0.8, dist_bps=50.0, dist_rel_atr=2.0)
    assert d2 < d1


def test_compute_geometry_context_returns_required_fields_and_sorted():
    price = 100.0
    atr = 2.0
    zones = [
        {"zone_type": "pdh", "zone_price": 101.0, "zone_strength": 0.9},
        {"zone_type": "pdl", "zone_price": 90.0, "zone_strength": 0.9},
        {"zone_type": "pdm", "zone_price": 99.8, "zone_strength": 0.7},
    ]

    hits, top, score = compute_geometry_context(price=price, atr=atr, zones=zones)
    assert isinstance(hits, list)
    assert top is not None
    assert 0.0 <= score <= 1.0
    assert abs(score - float(top["score"])) < 1e-12

    # Required fields in each hit
    for h in hits:
        for k in ("zone_type", "zone_strength", "zone_price", "dist_bps", "dist_rel_atr", "score", "meta"):
            assert k in h
        assert 0.0 <= float(h["zone_strength"]) <= 1.0
        assert 0.0 <= float(h["score"]) <= 1.0
        assert float(h["dist_bps"]) >= 0.0
        # dist_rel_atr may be None only if atr missing; here atr exists
        assert h["dist_rel_atr"] is None or float(h["dist_rel_atr"]) >= 0.0

    # Sorted best-first by score
    scores = [float(h["score"]) for h in hits]
    assert scores == sorted(scores, reverse=True)


def test_compute_geometry_context_handles_bad_inputs_fail_open():
    hits, top, score = compute_geometry_context(price=None, atr=None, zones=[])
    assert hits == []
    assert top is None
    assert score == 0.0

    hits, top, score = compute_geometry_context(price=float("nan"), atr=1.0, zones=[{"zone_price": 1, "zone_type": "x"}])
    assert hits == []
    assert top is None
    assert score == 0.0

    hits, top, score = compute_geometry_context(price=100.0, atr=float("inf"), zones=[{"zone_price": 101, "zone_type": "x", "zone_strength": 50}])
    assert len(hits) == 1
    assert top is not None
    assert 0.0 <= score <= 1.0
