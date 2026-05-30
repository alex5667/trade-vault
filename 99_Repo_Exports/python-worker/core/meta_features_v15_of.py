from __future__ import annotations

"""Meta-features schema v15_of.

v15_of = v14_of (META_FEAT_V14_OF_COLS) + 9 keys from Phase 8.3/8.4/8.5/P4
that are produced by external_features_payload_v1.

Append-only: v14_of ⊆ v15_of. v14_of remains valid and hash-locked.

Created 2026-05-19 alongside v14_of to keep the Meta-feature registry
in step with [[audit-v14of-v15of-canary-shadow-2026-05-19]].

Note: this is the "Meta path" feature subset, separate from the full
v15_of vector used by feature_registry. The full vector count is pinned
by ``core.ml_feature_schema_v15_of._EXPECTED_KEYS``. Production v15_of model
training (when it begins — gated by tools/check_v15_of_readiness.py)
should pin one or the other depending on the model role:
  - LR baseline (lightweight, latency-sensitive) → meta_feat_v15_of
  - GBDT / edge_stack challenger (full schema)   → V15_OF_NUMERIC_KEYS
"""

import hashlib
from typing import Any

from core.meta_features_v14_of import (
    META_FEAT_V14_OF_COLS,
    META_FEAT_V14_OF_TRANSFORMS,
    build_meta_features_v14_of,
)


META_FEAT_V15_OF_NAME = "meta_feat_v15_of"
META_FEAT_V15_OF_VERSION = 15


META_FEAT_V15_OF_NEW_COLS: list[str] = [
    # ── Phase 8.3 taker ratio (z-scored)
    "taker_buy_sell_ratio_z",
    # ── Phase 8.4 Hawkes/VPIN (toxicity proxies, well-defined producers)
    "hawkes_taker_buy_lam",
    "hawkes_taker_sell_lam",
    "hawkes_buy_sell_lam_ratio",
    "vpin_tox_z",
    # ── Phase 8.5 cross-venue
    "cross_venue_dislocation_z",
    # ── Phase 4.10 PIT priors 7d
    "prior_winrate_symbol_kind_7d",
    "prior_ev_r_symbol_kind_7d",
    # ── Phase 4.12 macro calendar
    "macro_event_severity",
]


META_FEAT_V15_OF_COLS: list[str] = list(META_FEAT_V14_OF_COLS) + list(META_FEAT_V15_OF_NEW_COLS)


META_FEAT_V15_OF_HASH: str = hashlib.sha1(
    ",".join(META_FEAT_V15_OF_COLS).encode("utf-8")
).hexdigest()


META_FEAT_V15_OF_TRANSFORMS: dict[str, Any] = dict(META_FEAT_V14_OF_TRANSFORMS)
META_FEAT_V15_OF_TRANSFORMS.update(
    {
        "taker_buy_sell_ratio_z":       {"type": "clip", "lo": -5.0, "hi": 5.0},
        "hawkes_taker_buy_lam":         "log1p",
        "hawkes_taker_sell_lam":        "log1p",
        "hawkes_buy_sell_lam_ratio":    {"type": "clip", "lo": 0.0, "hi": 10.0},
        "vpin_tox_z":                   {"type": "clip", "lo": -5.0, "hi": 5.0},
        "cross_venue_dislocation_z":    {"type": "clip", "lo": -5.0, "hi": 5.0},
        "prior_winrate_symbol_kind_7d": {"type": "clip", "lo": 0.0, "hi": 1.0},
        "prior_ev_r_symbol_kind_7d":    {"type": "clip", "lo": -3.0, "hi": 3.0},
        "macro_event_severity":         {"type": "clip", "lo": 0.0, "hi": 5.0},
    }
)


def build_meta_features_v15_of(
    evidence: dict[str, Any],
    indicators: dict[str, Any],
    **kwargs,
) -> tuple[dict[str, float], list[str]]:
    """Build meta_feat_v15_of (v14_of base + Phase 8.3/8.4/8.5/P4 subset)."""
    feat, missing = build_meta_features_v14_of(
        evidence=evidence, indicators=indicators, **kwargs
    )

    ind_v4: dict[str, Any] = kwargs.get("indicators_with_v4") or {}
    if not isinstance(ind_v4, dict):
        ind_v4 = {}

    def _get(key: str, default: float = 0.0) -> float:
        for src in (ind_v4, indicators, evidence):
            if not isinstance(src, dict):
                continue
            v = src.get(key)
            if v is not None:
                try:
                    return float(v)
                except (TypeError, ValueError):
                    pass
        return default

    for k in META_FEAT_V15_OF_NEW_COLS:
        feat[k] = _get(k, 0.0)

    for k in META_FEAT_V15_OF_COLS:
        if k not in feat:
            feat[k] = 0.0
            if k not in missing:
                missing.append(k)

    return feat, missing
