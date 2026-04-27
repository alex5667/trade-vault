from __future__ import annotations

import os
from typing import List


def env_truthy(name: str, default: str = "0") -> bool:
    """
    Minimal env->bool parser used in hot-path decisions.
    Intentionally dependency-free.
    """
    try:
        v = (os.getenv(name, default) or "").strip().lower()
        return v in {"1", "true", "yes", "on"}
    except Exception:
        return False


def cost_edge_reason_codes(primary_reason: str) -> List[str]:
    """
    Unified Cost/Edge veto reason-code mapping.

    Default (new truth):
      - emit ONLY EdgeCostGateDecision.reason_code (primary_reason)

    Optional (migration) dual-emit:
      - EDGE_DUAL_EMIT_LEGACY_THIN_COST=1 -> also emit legacy code "VETO_EDGE_THIN_COST"
        to keep existing dashboards alive during rollout.
      - default is OFF.
    """
    r = str(primary_reason or "VETO_EDGE_COST_UNKNOWN")
    out = [r]
    if env_truthy("EDGE_DUAL_EMIT_LEGACY_THIN_COST", "0"):
        out.append("VETO_EDGE_THIN_COST")
    return out
