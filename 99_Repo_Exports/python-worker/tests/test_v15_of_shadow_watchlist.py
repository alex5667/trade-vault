"""test_v15_of_shadow_watchlist.py — guard the shadow-feature watchlist.

Asserts:
  1. The watchlist is non-empty and groups are stable.
  2. Watchlist keys are NOT in V15_OF_NUMERIC_KEYS (the whole point — these
     are shadow features that must NOT bump the prod schema).
  3. Source-health keys flow from `core.source_health_v1.SOURCE_HEALTH_FEATURE_KEYS`
     (single source of truth — no drift).
  4. Coverage exporter wiring picks up the watchlist (KEY_TO_GROUP vs
     SHADOW_KEY_TO_GROUP are disjoint planes).
"""
from __future__ import annotations


def test_watchlist_non_empty():
    from core.v15_of_shadow_watchlist_v1 import SHADOW_WATCHLIST_KEYS, SHADOW_WATCHLIST_GROUPS
    assert len(SHADOW_WATCHLIST_KEYS) > 30, (
        f"shadow watchlist looks empty ({len(SHADOW_WATCHLIST_KEYS)} keys); "
        "P1/P2 shadow features should number well over 30."
    )
    assert len(SHADOW_WATCHLIST_GROUPS) > 0


def test_watchlist_disjoint_from_v15_of():
    """Shadow features must NOT live in V15_OF_NUMERIC_KEYS — the watchlist
    exists exactly because these keys are not yet schema-promoted."""
    from core.ml_feature_schema_v15_of import V15_OF_NUMERIC_KEYS
    from core.v15_of_shadow_watchlist_v1 import SHADOW_WATCHLIST_KEYS

    overlap = set(SHADOW_WATCHLIST_KEYS) & set(V15_OF_NUMERIC_KEYS)
    assert overlap == set(), (
        f"{len(overlap)} watchlist keys leaked into V15_OF_NUMERIC_KEYS: "
        f"{sorted(overlap)[:10]}. Either promote them (and remove from "
        "watchlist) or revert the schema addition."
    )


def test_source_health_keys_in_watchlist():
    from core.source_health_v1 import SOURCE_HEALTH_FEATURE_KEYS
    from core.v15_of_shadow_watchlist_v1 import SHADOW_WATCHLIST_KEYS

    missing = [k for k in SOURCE_HEALTH_FEATURE_KEYS if k not in SHADOW_WATCHLIST_KEYS]
    assert missing == [], (
        f"source_health_v1 keys not picked up by watchlist: {missing}"
    )


def test_watchlist_has_p1_and_p2_groups():
    from core.v15_of_shadow_watchlist_v1 import SHADOW_WATCHLIST_GROUPS
    p1 = [g for g in SHADOW_WATCHLIST_GROUPS if g.startswith("p1_")]
    p2 = [g for g in SHADOW_WATCHLIST_GROUPS if g.startswith("p2_")]
    assert len(p1) >= 5, f"expected ≥5 P1 groups, got {len(p1)}"
    assert len(p2) >= 5, f"expected ≥5 P2 groups, got {len(p2)}"


def test_exporter_picks_up_shadow_watchlist():
    from orderflow_services.v15_of_coverage_exporter_v1 import (
        SHADOW_KEY_TO_GROUP,
        SHADOW_GROUP_SIZES,
        SHADOW_TOTAL_KEYS,
    )
    from core.v15_of_shadow_watchlist_v1 import SHADOW_WATCHLIST_KEYS

    assert SHADOW_TOTAL_KEYS == len(SHADOW_WATCHLIST_KEYS)
    assert sum(SHADOW_GROUP_SIZES.values()) == SHADOW_TOTAL_KEYS
    # Every watchlist key must map to a group
    missing = [k for k in SHADOW_WATCHLIST_KEYS if k not in SHADOW_KEY_TO_GROUP]
    assert missing == []


def test_exporter_v15_and_shadow_planes_are_disjoint():
    """Prod schema metrics (KEY_TO_GROUP) and shadow metrics
    (SHADOW_KEY_TO_GROUP) must not overlap — otherwise a key would be
    double-counted and the promotion gauge becomes meaningless."""
    from orderflow_services.v15_of_coverage_exporter_v1 import (
        KEY_TO_GROUP,
        SHADOW_KEY_TO_GROUP,
    )
    overlap = set(KEY_TO_GROUP) & set(SHADOW_KEY_TO_GROUP)
    assert overlap == set(), (
        f"{len(overlap)} keys appear in both prod and shadow planes: "
        f"{sorted(overlap)[:10]}"
    )


def test_compute_window_with_shadow_keys():
    from orderflow_services.v15_of_coverage_exporter_v1 import (
        _compute_window,
        SHADOW_KEY_TO_GROUP,
    )
    sample_keys = list(SHADOW_KEY_TO_GROUP)[:2]
    if len(sample_keys) < 2:
        return  # tolerated — registry may be small in CI
    k1, k2 = sample_keys

    records = [
        {"indicators": {k1: 1.0}},
        {"indicators": {k1: 0.0, k2: 5.0}},
        {"indicators": {}},
    ]
    stats = _compute_window(records, keys=SHADOW_KEY_TO_GROUP)
    s1 = stats[k1]
    assert s1["coverage"] > 0
    # k2 was only in 1 of 3 records
    s2 = stats[k2]
    assert s2["coverage"] > 0
