from __future__ import annotations

"""World-practice tracker snapshots -> stable indicator dict (v1).

Why
----
Different hot-path components attach tracker outputs in slightly different ways:
- bar_processor writes volatility regime snapshot into runtime.dynamic_cfg
- book_processor writes book_resilience snapshot into runtime.dynamic_cfg
- tick_processor needs a deterministic, fail-open extraction layer so that:
    * gates can rely on stable keys
    * Prometheus gauges see non-zero values
    * ML dataset builders can keep schema stable

This module is intentionally tiny and dependency-free (no Redis/DB).
"""

from typing import Any, Dict
import math


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else float(d)
    except Exception:
        return float(d)


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return int(d)


def _s(x: Any, d: str = "na") -> str:
    try:
        s = str(x or "").strip()
        return s if s else d
    except Exception:
        return d


def extract_world_practice_indicators(dynamic_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Extract stable indicator keys from runtime.dynamic_cfg (fail-open).

    Output keys (subset):
      - vol_fast_bps, vol_slow_bps, vol_ratio, vol_ratio_z, vol_regime_label
      - res_active, res_recovered, res_recovery_ms, res_speed_per_s, res_min_ratio, res_curr_ratio

    All numeric outputs are finite (NaN/Inf -> default).
    """
    dc = dynamic_cfg or {}

    out: Dict[str, Any] = {}

    # Vol regime
    out["vol_fast_bps"] = _f(dc.get("vol_fast_bps", 0.0), 0.0)
    out["vol_slow_bps"] = _f(dc.get("vol_slow_bps", 0.0), 0.0)
    out["vol_ratio"] = _f(dc.get("vol_ratio", 0.0), 0.0)
    out["vol_ratio_z"] = _f(dc.get("vol_ratio_z", 0.0), 0.0)
    out["vol_regime_label"] = _s(dc.get("vol_regime_label", "na"), "na")

    # Book resilience
    out["res_active"] = _i(dc.get("res_active", dc.get("active", 0)), 0)
    out["res_recovered"] = _i(dc.get("res_recovered", dc.get("recovered", 0)), 0)
    out["res_recovery_ms"] = _i(dc.get("res_recovery_ms", dc.get("t_recover_ms", 0)), 0)
    out["res_speed_per_s"] = _f(dc.get("res_speed_per_s", dc.get("speed", 0.0)), 0.0)
    out["res_min_ratio"] = _f(dc.get("res_min_ratio", 0.0), 0.0)
    out["res_curr_ratio"] = _f(dc.get("res_curr_ratio", 0.0), 0.0)

    return out
