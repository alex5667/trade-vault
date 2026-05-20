from __future__ import annotations

"""Meta-features schema v14_of.

v14_of = v13_of (META_FEAT_V13_OF_COLS) + OG rule-gate consensus (4 keys) +
Phase 8.1 OE composites (10 keys) that are produced by
core/v14_of_features.build_og_payload and
core/external_features_payload_v1.build_external_features_payload at serving
time.

Additive over v13_of. v13_of remains valid and hash-locked.

Created 2026-05-19 to close the SCHEMAS dict gap identified in
[[audit-v14of-v15of-canary-shadow-2026-05-19]]: the Meta-feature registry in
core/meta_schema_registry.py previously stopped at v13_of, so any model
with `schema_name="meta_feat_v14_of"` would fall back to v1 features in
of_confirm_engine schema-guard and be forced to SHADOW.

Train==Serve guarantee
----------------------
Every new key here is written into `indicators` (or `indicators_with_v4`)
by of_confirm_engine BEFORE the meta-feature builder runs:

  - og_*           → core/v14_of_features.build_og_payload (called at
                     of_confirm_engine.py:~5589)
  - OE composites  → core/external_features_payload_v1.build_external_features_payload
                     (called at of_confirm_engine.py:~5614)

Both are guarded by fail-open counters (og_payload_fail_open_total,
external_features_payload_fail_open_total) so a missing producer is
observable rather than silent.

Notes
-----
- This is the "Meta path" feature set (small, ~50 cols total via v13_of base),
  separate from the "ml_confirm gate" full-schema vector used by
  edge_stack_v1 + LR baseline trained by tools/nightly_v14_of_train_bundle.
  The full-schema vector is sourced from
  core.ml_feature_schema_v14_of.V14_OF_NUMERIC_KEYS (359 keys).
- Models trained with `schema_name="meta_feat_v14_of"` should serialize
  their `features` list as `META_FEAT_V14_OF_COLS` and write
  `schema_hash=META_FEAT_V14_OF_HASH`; the engine schema-guard then
  matches and ENFORCE may be enabled.
"""

import hashlib
from typing import Any

from core.meta_features_v13_of import (
    META_FEAT_V13_OF_COLS,
    META_FEAT_V13_OF_TRANSFORMS,
    build_meta_features_v13_of,
)

META_FEAT_V14_OF_NAME = "meta_feat_v14_of"
META_FEAT_V14_OF_VERSION = 14


# ──────────────────────────────────────────────────────────────────────────────
# New columns added on top of META_FEAT_V13_OF_COLS.
# Picked deliberately: each key has a known producer in current of_confirm_engine
# wiring (build_og_payload or build_external_features_payload) and is not
# already present in v13_of base.
# ──────────────────────────────────────────────────────────────────────────────
META_FEAT_V14_OF_NEW_COLS: list[str] = [
    # ── OG rule-gate consensus (subset; full 16 og_* lives in feature_registry v14_of)
    "og_have_minus_need",
    "og_ok",
    "og_score_minus_threshold",
    "og_gate_bits_count",
    # ── Phase 8.1 OE composites (deriv / breadth / sentiment / vol-regime)
    "taker_buy_sell_imbalance",
    "force_order_imbalance_1m",
    "oi_confirmation_score",
    "squeeze_risk_score",
    "liq_impulse_score",
    "market_breadth_ret_24h",
    "market_breadth_vol_z",
    "deribit_btc_iv_z",
    "deribit_eth_iv_z",
    "fear_greed_index",
]


META_FEAT_V14_OF_COLS: list[str] = list(META_FEAT_V13_OF_COLS) + list(META_FEAT_V14_OF_NEW_COLS)


META_FEAT_V14_OF_HASH: str = hashlib.sha1(
    ",".join(META_FEAT_V14_OF_COLS).encode("utf-8")
).hexdigest()


# Transforms: v13_of base + new-key transforms.
# - og_* are bounded counts / signed numbers → clip-ish
# - OE composites are mostly z-scored or [-1, 1]-ish → clip
META_FEAT_V14_OF_TRANSFORMS: dict[str, Any] = dict(META_FEAT_V13_OF_TRANSFORMS)
META_FEAT_V14_OF_TRANSFORMS.update(
    {
        "og_have_minus_need":      {"type": "clip", "lo": -8.0, "hi": 8.0},
        "og_ok":                   "identity",
        "og_score_minus_threshold":{"type": "clip", "lo": -3.0, "hi": 3.0},
        "og_gate_bits_count":      {"type": "clip", "lo": 0.0,  "hi": 16.0},
        "taker_buy_sell_imbalance":{"type": "clip", "lo": -1.0, "hi": 1.0},
        "force_order_imbalance_1m":{"type": "clip", "lo": -1.0, "hi": 1.0},
        "oi_confirmation_score":   {"type": "clip", "lo": -3.0, "hi": 3.0},
        "squeeze_risk_score":      {"type": "clip", "lo": 0.0,  "hi": 1.0},
        "liq_impulse_score":       {"type": "clip", "lo": -3.0, "hi": 3.0},
        "market_breadth_ret_24h":  {"type": "clip", "lo": -0.5, "hi": 0.5},
        "market_breadth_vol_z":    {"type": "clip", "lo": -5.0, "hi": 5.0},
        "deribit_btc_iv_z":        {"type": "clip", "lo": -5.0, "hi": 5.0},
        "deribit_eth_iv_z":        {"type": "clip", "lo": -5.0, "hi": 5.0},
        "fear_greed_index":        {"type": "clip", "lo": 0.0,  "hi": 100.0},
    }
)


def build_meta_features_v14_of(
    evidence: dict[str, Any],
    indicators: dict[str, Any],
    **kwargs,
) -> tuple[dict[str, float], list[str]]:
    """Build meta_feat_v14_of (v13_of base + og_* + Phase 8.1 OE composites).

    Sources lookup order: indicators_with_v4 → indicators → evidence.
    Missing keys default to 0.0 and are appended to the `missing` list so the
    caller (OFConfirmEngine) can attribute via `feature_missing_total`.
    """
    feat, missing = build_meta_features_v13_of(
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

    for k in META_FEAT_V14_OF_NEW_COLS:
        feat[k] = _get(k, 0.0)

    # Ensure full column coverage
    for k in META_FEAT_V14_OF_COLS:
        if k not in feat:
            feat[k] = 0.0
            if k not in missing:
                missing.append(k)

    return feat, missing
