from __future__ import annotations

import hashlib
from typing import Any

from core.meta_features_v7 import (
    META_FEAT_V7_COLS,
    META_FEAT_V7_TRANSFORMS,
    build_meta_features_v7,
)

# ---------------------------------------------------------------------
# META-FEATURE SCHEMA V8
#
# Goal (P1): Expand MetaModelLR feature inventory using already-computed
# indicators/evidence (no new detectors).
#
# Key additions:
# - Strength / quality: OBI/OFI z-scores and stability proxies
# - Iceberg geometry
# - Absorption quality (level-based)
# - Runtime stability flags (e.g., churn_hi)
# - Data quality hard features (to be wired in later P2/F):
#   tick_gap_p95_ms, tick_missing_seq_ema, book_missing_seq_ema
#
# Important: The builder is fail-soft:
#   - missing keys -> 0.0 and appended to 'missing' list
#   - type issues -> 0.0 and appended to 'missing' list
# ---------------------------------------------------------------------

META_FEAT_V8_NAME = "meta_feat_v8"
META_FEAT_V8_VERSION = 8

META_FEAT_V8_NEW_COLS: list[str] = [
    "obi",
    "obi_z",
    "obi_stable_secs",
    "obi_stacking",
    "obi_concentration",
    "ofi",
    "ofi_z",
    "ofi_stability_score",
    "ofi_stable_secs",
    "iceberg_refresh",
    "iceberg_duration",
    "iceberg_dist_bp",
    "absorption_volume",
    "abs_lvl_score",
    "abs_lvl_ladder",
    "abs_lvl_eff",
    "abs_lvl_poc_edge",
    "data_health_veto_book_evidence",
    "cvd_quarantine_active",
    "expected_slippage_bps",
    "book_churn_hi",
    "tick_gap_p95_ms",
    "tick_missing_seq_ema",
    "book_missing_seq_ema",
]

META_FEAT_V8_COLS: list[str] = list(META_FEAT_V7_COLS) + list(META_FEAT_V8_NEW_COLS)
META_FEAT_V8_HASH: str = hashlib.sha1(",".join(META_FEAT_V8_COLS).encode("utf-8")).hexdigest()

META_FEAT_V8_TRANSFORMS = dict(META_FEAT_V7_TRANSFORMS)
for k in ("obi_z", "ofi_z", "abs_lvl_score"):
    META_FEAT_V8_TRANSFORMS.setdefault(k, {"type": "clip", "lo": -8.0, "hi": 8.0})
for k in ("obi", "ofi"):
    META_FEAT_V8_TRANSFORMS.setdefault(k, {"type": "clip", "lo": -5.0, "hi": 5.0})
for k in (
    "obi_stable_secs",
    "ofi_stable_secs",
    "iceberg_refresh",
    "iceberg_duration",
    "iceberg_dist_bp",
    "absorption_volume",
    "expected_slippage_bps",
    "tick_gap_p95_ms",
    "tick_missing_seq_ema",
    "book_missing_seq_ema",
):
    META_FEAT_V8_TRANSFORMS.setdefault(k, {"type": "log1p"})
for k in (
    "obi_stacking",
    "obi_concentration",
    "ofi_stability_score",
    "abs_lvl_ladder",
    "abs_lvl_eff",
    "data_health_veto_book_evidence",
    "cvd_quarantine_active",
    "book_churn_hi",
):
    META_FEAT_V8_TRANSFORMS.setdefault(k, {"type": "clip", "lo": 0.0, "hi": 1.0})

def _try_get_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def build_meta_features_v8(
    evidence: dict[str, Any],
    indicators: dict[str, Any],
    **kwargs,
) -> tuple[dict[str, float], list[str]]:
    """Build meta_feat_v8 (v7 base + quality/absorption/iceberg + DQ hard features)."""

    feat, missing = build_meta_features_v7(evidence=evidence, indicators=indicators, **kwargs)
    nested_ind = evidence.get("indicators") if isinstance(evidence, dict) else None
    if not isinstance(nested_ind, dict):
        nested_ind = {}

    for k in META_FEAT_V8_NEW_COLS:
        v = None
        if isinstance(evidence, dict) and k in evidence:
            v = _try_get_float(evidence.get(k))
        elif isinstance(indicators, dict) and k in indicators:
            v = _try_get_float(indicators.get(k))
        elif k in nested_ind:
            v = _try_get_float(nested_ind.get(k))

        if v is None:
            feat[k] = 0.0
            if k not in missing:
                missing.append(k)
        else:
            feat[k] = float(v)
            while k in missing:
                missing.remove(k)

    for k in META_FEAT_V8_COLS:
        if k not in feat:
            feat[k] = 0.0
            if k not in missing:
                missing.append(k)
        else:
            while k in missing and feat.get(k) != 0.0:
                missing.remove(k)

    return feat, missing
