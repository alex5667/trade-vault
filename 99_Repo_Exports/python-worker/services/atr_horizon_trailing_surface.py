from __future__ import annotations

"""atr_horizon_trailing_surface.py — Phase 2.6: trailing surface builder.

Computes the offset ATR mult & absolute values for dynamic trailing based on the 
selected horizon ATR profile (meta.atr_profile).

Fail-open logic: never causes an exception if keys are missing.
"""


from dataclasses import asdict, dataclass
from typing import Any


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v or default)
    except Exception:
        return default


def _ensure_dict(v: Any) -> dict[str, Any]:
    return dict(v) if isinstance(v, dict) else {}


@dataclass(frozen=True)
class TrailingSurface:
    mode: str
    atr_tf_ms: int
    atr_value: float
    atr_pct: float
    baseline_offset_atr_mult: float
    baseline_offset_distance_px: float
    selected_offset_atr_mult: float
    selected_offset_distance_px: float
    reason_code: str


def build_trailing_surface(
    signal_payload: dict[str, Any],
    pos_atr: float,
    offset_mult: float,
) -> dict[str, Any]:
    """Phase 2.6: compute trailing offset surface from selected ATR (meta.atr_profile).

    Returns a flat dict (via asdict) — safe for JSON serialisation and meta enrichment.
    Calling site decides whether to apply `selected` attributes (controlled by canary).

    Args:
        signal_payload: The raw state or pos.signal_payload containing `meta.atr_profile`.
        pos_atr: The legacy ATR extracted at entry, acting as the baseline.
        offset_mult: The configured mult returned from `_resolve_trailing_tp1_offset_atr`.
    """
    signal = _ensure_dict(signal_payload)
    meta = _ensure_dict(signal.get("meta"))
    atr_profile = _ensure_dict(meta.get("atr_profile"))

    # Baseline calculations
    pos_atr = _safe_float(pos_atr, 0.0)
    baseline_offset_mult = _safe_float(offset_mult, 0.0)
    baseline_offset_distance_px = max(0.0, pos_atr * baseline_offset_mult)

    # Selected (Candidate) calculations based on Horizon ATR
    atr_value = _safe_float(atr_profile.get("atr_value"), 0.0)

    # If no ATR profile exists, the "selected" surface gracefully falls back to baseline config
    if atr_value <= 0.0:
        return asdict(TrailingSurface(
            mode="fallback_to_baseline",
            atr_tf_ms=0,
            atr_value=pos_atr,
            atr_pct=0.0,
            baseline_offset_atr_mult=baseline_offset_mult,
            baseline_offset_distance_px=baseline_offset_distance_px,
            selected_offset_atr_mult=baseline_offset_mult,
            selected_offset_distance_px=baseline_offset_distance_px,
            reason_code="ATR_PROFILE_NOT_FOUND",
        ))

    # Calculate Candidate offset using the same multiplier against the fresh ATR
    # Note: Phase 2.6 uses the static legacy multiplier against the dynamic horizon-aware ATR value
    selected_offset_distance_px = max(0.0, atr_value * baseline_offset_mult)

    return asdict(TrailingSurface(
        mode="candidate",
        atr_tf_ms=int(_safe_float(atr_profile.get("atr_tf_ms"), 0)),
        atr_value=atr_value,
        atr_pct=_safe_float(atr_profile.get("atr_pct", 0.0)),
        baseline_offset_atr_mult=baseline_offset_mult,
        baseline_offset_distance_px=baseline_offset_distance_px,
        selected_offset_atr_mult=baseline_offset_mult,
        selected_offset_distance_px=selected_offset_distance_px,
        reason_code="ATR_PROFILE_APPLIED",
    ))
