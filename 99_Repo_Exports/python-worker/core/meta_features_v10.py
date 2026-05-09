from __future__ import annotations

"""Meta-features schema v10.

v10 = v9 + scenario-awareness features.

Intent:
  - v9 stays stable as champion (hash-locked).
  - v10 adds features that describe HOW the scenario was determined:
      • trend_dir_source (encoded as int): hidden_div=3, regime=2, direction=1, dz_bypass=1, none=0
      • hidden_div_used (0/1): hidden divergence was the primary trend source
      • scenario_dz_bypass (0/1): trend_dir was derived from delta_z sign (strong momentum)
      • scenario_dz_bypass_th: threshold used for dz_bypass (for model awareness)
      • scenario_is_reversal (0/1): signal came from a sweep→reversal path
      • scenario_is_continuation (0/1): signal came from continuation path
      • of_confirm_score: the OFC engine quality score [0,1]
      • strong_gate_have / strong_gate_need: confirmation counts
      • data_health: overall data quality scalar [0,1]
      • spread_bps_missing (0/1): spread was not available from real sources

  These features let the model learn:
    1. Was the scenario classification high or low confidence?
    2. Was trend following validated (hidden_div) or assumed (fallback)?
    3. Did we need a delta_z bypass (very strong momentum context)?

  Train==Serve: all these keys are already written into indicators/evidence
  by of_confirm_engine.py before meta features are built — no new computation needed,
  just surfacing what's already there.

Notes:
  - trend_dir_source_int: hidden_div=3, regime=2, direction_or_dz=1, no_source=0
    (ordinal encoding — higher = more reliable)
  - Do NOT re-hash v9: v10 has a new distinct hash.
"""


import hashlib
from typing import Any

from core.meta_features_v9 import (
    META_FEAT_V9_COLS,
    META_FEAT_V9_TRANSFORMS,
    _try_get_float,
    build_meta_features_v9,
)

META_FEAT_V10_NAME = "meta_feat_v10"
META_FEAT_V10_VERSION = 10

# ---- New columns added by v10 ----
META_FEAT_V10_NEW_COLS: list[str] = [
    # Scenario source reliability (ordinal: 3=hidden_div, 2=regime, 1=fallback/dz, 0=none)
    "trend_dir_source_int",
    # Binary flags for specific sources
    "hidden_div_used",          # 1 if hidden divergence was the trend source
    "scenario_dz_bypass",       # 1 if delta_z magnitude triggered trend_dir bypass
    # Context about the bypass threshold (for the model to learn what strength triggered it)
    "scenario_dz_bypass_th",
    # Scenario type flags
    "scenario_is_reversal",     # 1 if swept into reversal scenario
    "scenario_is_continuation", # 1 if entered continuation scenario
    # Quality of the OF decision
    "of_confirm_score",         # OFConfirmEngine quality score [0,1]
    "strong_gate_have",         # Number of evidence legs confirmed
    "strong_gate_need",         # Number required for ok=True
    # Data quality context that affects legitimacy of other features
    "data_health",              # Overall data quality [0,1]
    "spread_bps_missing",       # 1 if spread_bps came from static default
]

META_FEAT_V10_COLS: list[str] = list(META_FEAT_V9_COLS) + list(META_FEAT_V10_NEW_COLS)
META_FEAT_V10_HASH: str = hashlib.sha1(
    ",".join(META_FEAT_V10_COLS).encode("utf-8")
).hexdigest()

# ---- Transforms ----
META_FEAT_V10_TRANSFORMS: dict[str, str] = dict(META_FEAT_V9_TRANSFORMS)
META_FEAT_V10_TRANSFORMS.update(
    {
        "trend_dir_source_int":     "identity",   # ordinal [0,3]
        "hidden_div_used":          "identity",   # binary
        "scenario_dz_bypass":       "identity",   # binary
        "scenario_dz_bypass_th":    "log1p",      # threshold (typically 4.0)
        "scenario_is_reversal":     "identity",   # binary
        "scenario_is_continuation": "identity",   # binary
        "of_confirm_score":         "clip(0,1)",  # engine score in [0,1]
        "strong_gate_have":         "identity",   # count [0,3+]
        "strong_gate_need":         "identity",   # count [2,4]
        "data_health":              "clip(0,1)",  # quality [0,1]
        "spread_bps_missing":       "identity",   # binary
    }
)


# Ordinal encoding for trend_dir_source string → int
_TREND_DIR_SOURCE_ORD: dict[str, int] = {
    "hidden_div": 3,
    "regime":     2,
    "direction":  1,
    "dz_bypass":  1,   # same tier as direction fallback
    "":           0,
}


def _encode_trend_dir_source(source: str) -> float:
    """Map trend_dir_source string to ordinal float (higher = more reliable)."""
    return float(_TREND_DIR_SOURCE_ORD.get(str(source).strip().lower(), 0))


def build_meta_features_v10(
    evidence: dict[str, Any],
    indicators: dict[str, Any],
    **kwargs,
) -> tuple[dict[str, float], list[str]]:
    """Build meta_feat_v10 (v9 base + scenario awareness features)."""

    feat, missing = build_meta_features_v9(evidence=evidence, indicators=indicators, **kwargs)

    # Merge lookup: indicators takes priority over evidence for runtime keys
    def _get(key: str, default: float = 0.0) -> float:
        v = indicators.get(key) if isinstance(indicators, dict) else None
        if v is None and isinstance(evidence, dict):
            v = evidence.get(key)
        if v is None:
            return default
        return _try_get_float(v) or default

    # --- trend_dir_source_int ---
    td_src_raw = ""
    if isinstance(indicators, dict):
        td_src_raw = (indicators.get("trend_dir_source", "") or "")
    if not td_src_raw and isinstance(evidence, dict):
        td_src_raw = (evidence.get("trend_dir_source", "") or "")

    # dz_bypass also signals the "direction" tier
    dz_bp = _get("scenario_dz_bypass", 0.0)
    td_src_int = _encode_trend_dir_source(td_src_raw)
    if dz_bp >= 1.0 and td_src_int == 0.0:
        td_src_int = 1.0

    feat["trend_dir_source_int"]     = td_src_int
    feat["hidden_div_used"]          = _get("hidden_div_used", 0.0)
    feat["scenario_dz_bypass"]       = dz_bp
    feat["scenario_dz_bypass_th"]    = _get("scenario_dz_bypass_th", 0.0)

    # --- Scenario type flags (from indicators["scenario"] or evidence["scenario"]) ---
    scn_raw = ""
    if isinstance(indicators, dict):
        scn_raw = (indicators.get("scenario", "") or "")
    if not scn_raw and isinstance(evidence, dict):
        scn_raw = (evidence.get("scenario", "") or "")
    # Also check of_confirm dict (where scenario is stored post-build)
    if not scn_raw and isinstance(evidence, dict):
        ofc = evidence.get("of_confirm") or {}
        if isinstance(ofc, dict):
            scn_raw = (ofc.get("scenario", "") or "")

    feat["scenario_is_reversal"]     = 1.0 if scn_raw == "reversal" else 0.0
    feat["scenario_is_continuation"] = 1.0 if scn_raw == "continuation" else 0.0

    # --- OFC quality ---
    feat["of_confirm_score"]  = _get("of_confirm_score", 0.0)
    feat["strong_gate_have"]  = _get("strong_gate_have", 0.0)
    feat["strong_gate_need"]  = _get("strong_gate_need", 0.0)

    # --- Data quality context ---
    feat["data_health"]       = _get("data_health", 1.0)
    feat["spread_bps_missing"] = _get("spread_bps_missing", 0.0)

    # Mark missing where value is default (0.0 for binary, but don't flag correctly defaulted ones)
    for k in META_FEAT_V10_NEW_COLS:
        if k not in feat:
            feat[k] = 0.0
            if k not in missing:
                missing.append(k)

    # Ensure full column coverage
    for k in META_FEAT_V10_COLS:
        if k not in feat:
            feat[k] = 0.0
            if k not in missing:
                missing.append(k)

    return feat, missing
