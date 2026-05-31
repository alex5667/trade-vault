from __future__ import annotations

"""Meta-features schema v13_of.

v13_of = v10 + missing masks + age/freshness + session/time context + execution cost.

Additive over v10 — v10 remains valid and hash-locked.

New feature groups:
  A. Missing masks — explicit 1.0 signal when source data was absent/stale.
     Model sees "no data" instead of silently substituted 0.0.
  B. Age / freshness — raw ms for model to learn staleness sensitivity.
  C. Session / time context — written by OFConfirmEngine Phase 7.5/8.2
     into indicators_with_v4; no new computation needed at serving time.
  D. Execution / cost ratios — exec_risk_bps relative to tp1/sl horizons,
     fill probability proxy, eta_fill.
  E. Derived interactions — asia_weekend_flag (session_asia × weekend_flag).

Train==Serve: all source keys are written by OFConfirmEngine before the
meta-feature builder is invoked. No serving-only computation here.

Staleness thresholds (ms):
  book:   5 000 ms  (5 s)
  ofi:    5 000 ms
  obi:    5 000 ms
  atr:   30 000 ms  (30 s)
  liqmap: 60 000 ms  (60 s = 1 min)
"""

import hashlib
import math
from typing import Any

from core.meta_features_v10 import (
    META_FEAT_V10_COLS,
    META_FEAT_V10_TRANSFORMS,
    build_meta_features_v10,
)

META_FEAT_V13_OF_NAME = "meta_feat_v13_of"
META_FEAT_V13_OF_VERSION = 13

_BOOK_STALE_MS = 5_000.0
_OFI_STALE_MS = 5_000.0
_OBI_STALE_MS = 5_000.0
_ATR_STALE_MS = 30_000.0
_LIQMAP_STALE_MS = 60_000.0

META_FEAT_V13_OF_NEW_COLS: list[str] = [
    # A. Missing masks
    "book_missing_mask",
    "ofi_missing_mask",
    "obi_missing_mask",
    "liqmap_missing_mask",
    "atr_missing_mask",
    "spread_missing_mask",      # alias of v10's spread_bps_missing
    "feature_missing_count",    # sum of masks above
    # B. Age / freshness
    "book_age_ms",              # alias of book_staleness_ms
    "atr_age_ms",
    "liqmap_5m_age_ms",
    "liqmap_1h_age_ms",
    # C. Session / time context
    "session_asia",
    "session_europe",
    "session_us",
    "session_overlap_eu_us",
    "weekend_flag",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "asia_weekend_flag",        # session_asia × weekend_flag
    # D. Execution / cost
    "exec_cost_to_tp1_ratio",
    "exec_cost_to_sl_ratio",
    "spread_percentile_1h",
    "fill_prob_proxy",
    "fill_prob_decay_slope",
    "eta_fill_sec",
]

META_FEAT_V13_OF_COLS: list[str] = list(META_FEAT_V10_COLS) + list(META_FEAT_V13_OF_NEW_COLS)
META_FEAT_V13_OF_HASH: str = hashlib.sha1(
    ",".join(META_FEAT_V13_OF_COLS).encode("utf-8")
).hexdigest()

META_FEAT_V13_OF_TRANSFORMS: dict[str, Any] = dict(META_FEAT_V10_TRANSFORMS)
META_FEAT_V13_OF_TRANSFORMS.update(
    {
        # A. Missing masks — binary
        "book_missing_mask":        "identity",
        "ofi_missing_mask":         "identity",
        "obi_missing_mask":         "identity",
        "liqmap_missing_mask":      "identity",
        "atr_missing_mask":         "identity",
        "spread_missing_mask":      "identity",
        "feature_missing_count":    "identity",   # count [0,6]
        # B. Age — log1p (right-skewed; default -1.0 for absent)
        "book_age_ms":              "log1p",
        "atr_age_ms":               "log1p",
        "liqmap_5m_age_ms":         "log1p",
        "liqmap_1h_age_ms":         "log1p",
        # C. Session — binary / cyclical
        "session_asia":             "identity",
        "session_europe":           "identity",
        "session_us":               "identity",
        "session_overlap_eu_us":    "identity",
        "weekend_flag":             "identity",
        "hour_sin":                 "identity",
        "hour_cos":                 "identity",
        "dow_sin":                  "identity",
        "dow_cos":                  "identity",
        "asia_weekend_flag":        "identity",
        # D. Exec / cost
        "exec_cost_to_tp1_ratio":   {"type": "clip", "lo": 0.0, "hi": 5.0},
        "exec_cost_to_sl_ratio":    {"type": "clip", "lo": 0.0, "hi": 5.0},
        "spread_percentile_1h":     {"type": "clip", "lo": 0.0, "hi": 1.0},
        "fill_prob_proxy":          {"type": "clip", "lo": 0.0, "hi": 1.0},
        "fill_prob_decay_slope":    {"type": "clip", "lo": -1.0, "hi": 0.0},
        "eta_fill_sec":             "log1p",
    }
)


def _missing_mask(val: float, *, stale_ms: float) -> float:
    """1.0 if val is absent (-1) or exceeds stale_ms threshold, else 0.0."""
    if not math.isfinite(val) or val < 0.0:
        return 1.0
    return 1.0 if val > stale_ms else 0.0


def build_meta_features_v13_of(
    evidence: dict[str, Any],
    indicators: dict[str, Any],
    **kwargs,
) -> tuple[dict[str, float], list[str]]:
    """Build meta_feat_v13_of (v10 base + missing masks, session, exec cost)."""

    feat, missing = build_meta_features_v10(evidence=evidence, indicators=indicators, **kwargs)

    # Unified lookup: indicators_with_v4 → indicators → evidence
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

    # ------------------------------------------------------------------
    # B. Age / freshness (raw ms; -1.0 = absent)
    # ------------------------------------------------------------------
    book_age = _get("book_staleness_ms", -1.0)  # v1-v10 already have this in feat
    feat["book_age_ms"] = max(book_age, 0.0)   # log1p needs >= 0; mask captures absent

    atr_age = _get("atr_age_ms", -1.0)
    feat["atr_age_ms"] = max(atr_age, 0.0)

    liqmap_5m_age = _get("liqmap_5m_age_ms", -1.0)
    feat["liqmap_5m_age_ms"] = max(liqmap_5m_age, 0.0)

    liqmap_1h_age = _get("liqmap_1h_age_ms", -1.0)
    feat["liqmap_1h_age_ms"] = max(liqmap_1h_age, 0.0)

    # ------------------------------------------------------------------
    # A. Missing masks (derive from ages + v10's spread_bps_missing)
    # ------------------------------------------------------------------
    feat["book_missing_mask"] = _missing_mask(book_age, stale_ms=_BOOK_STALE_MS)
    feat["ofi_missing_mask"] = _missing_mask(_get("ofi_age_ms", -1.0), stale_ms=_OFI_STALE_MS)
    feat["obi_missing_mask"] = _missing_mask(_get("obi_age_ms", -1.0), stale_ms=_OBI_STALE_MS)
    feat["atr_missing_mask"] = _missing_mask(atr_age, stale_ms=_ATR_STALE_MS)
    feat["liqmap_missing_mask"] = _missing_mask(liqmap_5m_age, stale_ms=_LIQMAP_STALE_MS)
    feat["spread_missing_mask"] = feat.get("spread_bps_missing", 0.0)  # already in v10
    feat["feature_missing_count"] = (
        feat["book_missing_mask"]
        + feat["ofi_missing_mask"]
        + feat["obi_missing_mask"]
        + feat["atr_missing_mask"]
        + feat["liqmap_missing_mask"]
        + feat["spread_missing_mask"]
    )

    # ------------------------------------------------------------------
    # C. Session / time context (written by OFConfirmEngine Phase 7.5/8.2)
    # ------------------------------------------------------------------
    feat["session_asia"] = float(bool(_get("session_asia", 0.0)))
    feat["session_europe"] = float(bool(_get("session_europe", 0.0)))
    feat["session_us"] = float(bool(_get("session_us", 0.0)))
    feat["session_overlap_eu_us"] = float(bool(_get("session_overlap_eu_us", 0.0)))
    feat["weekend_flag"] = float(bool(_get("weekend_flag", 0.0)))
    feat["hour_sin"] = _get("hour_sin", 0.0)
    feat["hour_cos"] = _get("hour_cos", 1.0)   # cos(0) = 1.0 at midnight
    feat["dow_sin"] = _get("dow_sin", 0.0)
    feat["dow_cos"] = _get("dow_cos", 1.0)
    feat["asia_weekend_flag"] = feat["session_asia"] * feat["weekend_flag"]

    # ------------------------------------------------------------------
    # D. Execution / cost
    # ------------------------------------------------------------------
    exec_cost = _get("exec_risk_bps", 0.0)

    tp1_bps = (
        _get("tp1_bps", 0.0)
        or _get("pred_tp1_bps", 0.0)
        or _get("liqmap_gate_reward_bps", 0.0)
    )
    sl1_bps = _get("sl1_bps", 0.0) or _get("atr_bps", 0.0)

    _eps = 1e-6
    feat["exec_cost_to_tp1_ratio"] = exec_cost / max(tp1_bps, _eps) if tp1_bps > 0.0 else 0.0
    feat["exec_cost_to_sl_ratio"] = exec_cost / max(sl1_bps, _eps) if sl1_bps > 0.0 else 0.0
    feat["spread_percentile_1h"] = _get("spread_percentile_1h", 0.0)
    feat["fill_prob_proxy"] = _get("fill_prob_proxy", 0.0)
    feat["fill_prob_decay_slope"] = _get("fill_prob_decay_slope", 0.0)
    feat["eta_fill_sec"] = max(_get("eta_fill_sec", 0.0), 0.0)

    # Ensure full column coverage
    for k in META_FEAT_V13_OF_COLS:
        if k not in feat:
            feat[k] = 0.0
            if k not in missing:
                missing.append(k)

    return feat, missing
