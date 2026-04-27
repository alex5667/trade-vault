from __future__ import annotations

from typing import Any, Dict, Optional, Tuple
import math


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x) if x is not None else d
    except Exception:
        return d


def _get_attr_or_key(obj: Any, name: str, default: Any = None) -> Any:
    """
    Uniform access helper:
      - if obj is dict -> obj.get(name)
      - else -> getattr(obj, name, default)
    """
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def compute_fp_edge_absorb(
    *,
    direction: str,
    now_ts_ms: int,
    last_edge: Any,
    cfg: Dict[str, Any],
    indicators: Dict[str, Any],
) -> Tuple[bool, float, int, str]:
    """
    Footprint Edge Absorb evidence (tick-level / microbar-derived).

    Expected last_edge fields (object or dict):
      - ts_ms: int epoch ms
      - p90: float (scale)
      - value: float (raw edge absorb value)
      - bias: "LONG"/"SHORT" (directional expectation)
      - range_expansion: int 0/1 (1 => edge absorb happened with range expansion -> risk of fake impulse)

    We define normalized strength:
      strength = value / p90  (if p90>0 else 0)

    Gate rules (deterministic, fail-open):
      - freshness: age_ms in [0..fp_edge_valid_ms] (default 30000ms, aligned with strategy.py)
      - direction match: bias == direction
      - if fp_edge_require_no_range_expansion=1 -> require range_expansion==0
      - require strength >= fp_edge_min_strength (default 1.0)

    Returns:
      (ok, strength, range_expansion, bias)
    """
    ok = False
    strength = 0.0
    rng = 0
    bias = ""

    if last_edge is None:
        indicators["fp_edge_age_ms"] = -1
        indicators["fp_edge_absorb"] = 0
        return ok, strength, rng, bias

    ts = _i(_get_attr_or_key(last_edge, "ts_ms", 0), 0)
    age = (now_ts_ms - ts) if ts > 0 else 10**9
    indicators["fp_edge_age_ms"] = int(age)

    valid_ms = _i(cfg.get("fp_edge_valid_ms", 30000), 30000)
    if not (0 <= age <= valid_ms):
        indicators["fp_edge_absorb"] = 0
        return ok, strength, rng, bias

    p90 = _f(_get_attr_or_key(last_edge, "p90", 0.0), 0.0)
    val = _f(_get_attr_or_key(last_edge, "value", 0.0), 0.0)
    # allow precomputed strength if producer already provided it
    pre = _get_attr_or_key(last_edge, "strength", None)
    if pre is not None:
        strength = _f(pre, 0.0)
    else:
        strength = (val / p90) if p90 > 0 else 0.0

    bias = _s(_get_attr_or_key(last_edge, "bias", ""), "").upper()
    rng = _i(_get_attr_or_key(last_edge, "range_expansion", 0), 0)

    indicators["fp_edge_strength"] = float(strength)
    indicators["fp_edge_range_expansion"] = int(rng)
    indicators["fp_edge_bias"] = str(bias)

    dir_ok = int(bias == str(direction).upper())
    indicators["fp_edge_dir_ok"] = dir_ok

    require_no_rng = bool(int(cfg.get("fp_edge_require_no_range_expansion", 1) or 1))
    min_strength = _f(cfg.get("fp_edge_min_strength", 1.0), 1.0)

    ok = bool(dir_ok == 1 and strength >= min_strength and ((not require_no_rng) or (rng == 0)))
    indicators["fp_edge_absorb"] = 1 if ok else 0

    return ok, float(strength), int(rng), str(bias)

