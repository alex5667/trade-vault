from __future__ import annotations
"""
Cost/EV gate compatibility helpers.

Goals:
  - Single source of truth: EdgeCostGate.evaluate() returns a decision object.
  - Legacy call-sites (ok_edge, edge_details) remain supported without duplicating formulas.
  - Optional dashboard migration helper: dual-emit legacy reason_code VETO_EDGE_THIN_COST.

IMPORTANT:
  - No thresholds/formulas here. Only mapping/compat + best-effort diagnostics.
  - Must never raise (hot-path safe).
"""


import os
import math
from typing import Any, Dict, Tuple, List


def _isfinite_num(x: Any) -> bool:
    try:
        return isinstance(x, (int, float)) and math.isfinite(float(x))
    except Exception:
        return False


def decision_to_legacy_tuple(dec: Any) -> Tuple[bool, Dict[str, float]]:
    """
    Convert a decision object into legacy tuple:
      (ok_edge: bool, details: dict[str, float])

    Legacy semantics used by older code:
      - ok_edge True  => allowed
      - ok_edge False => veto
    """
    try:
        apply = bool(getattr(dec, "apply", True))
        veto = bool(getattr(dec, "veto", False))
        ok = (not apply) or (not veto)

        details: Dict[str, float] = {}
        for k in (
            "expected_move_bps",
            "threshold_bps",
            "fees_bps",
            "slippage_bps",
            "k",
        ):
            v = getattr(dec, k, None)
            if _isfinite_num(v):
                details[k] = float(v)

        # EV-mode extras (safe for dashboards / debugging; ignore if missing)
        for k in (
            "p_hit_tp1",
            "p_min",
            "tp1_bps",
            "stop_bps",
            "ev_bps",
        ):
            v = getattr(dec, k, None)
            if _isfinite_num(v):
                details[k] = float(v)

        # mode is not numeric but sometimes useful
        # Keep it out of numeric details to preserve "dict[str,float]" contract.
        return bool(ok), details
    except Exception:
        # Fail-open: do not block signal flow.
        return True, {}


def attach_cost_edge_veto_fields(ctx: Any, dec: Any) -> None:
    """
    Attach minimal veto diagnostics to ctx (best-effort).
    This is intentionally small and stable.
    """
    try:
        setattr(ctx, "veto_reason_code", str(getattr(dec, "reason_code", "") or "VETO_EDGE_COST"))
        setattr(ctx, "veto_reason", f"COST_EDGE: {getattr(dec, 'reason_code', 'VETO_EDGE_COST')}")
        # Numeric fields (only if finite)
        for k in ("expected_move_bps", "threshold_bps", "fees_bps", "slippage_bps", "k"):
            v = getattr(dec, k, None)
            if _isfinite_num(v):
                setattr(ctx, f"veto_{k}", float(v))
        # EV extras
        for k in ("p_hit_tp1", "p_min", "tp1_bps", "stop_bps", "ev_bps"):
            v = getattr(dec, k, None)
            if _isfinite_num(v):
                setattr(ctx, f"veto_{k}", float(v))
        # Notes (bounded)
        note = str(getattr(dec, "notes", "") or "")
        if note:
            setattr(ctx, "veto_notes", note[:256])
    except Exception:
        return


def maybe_dual_emit_legacy_thin_cost(*, emit_veto_metric: Any, kind: str, ctx: Any, reason_code: str) -> List[str]:
    """
    Optional compatibility for dashboards relying on legacy reason_code: VETO_EDGE_THIN_COST.

    ENV:
      EDGE_DUAL_EMIT_LEGACY_THIN_COST=1  => also emits VETO_EDGE_THIN_COST (in addition to reason_code).
    Default: OFF.
    """
    emitted: List[str] = []
    try:
        rc = str(reason_code or "VETO_EDGE_COST")
        try:
            emit_veto_metric(kind=kind, ctx=ctx, reason_code=rc)
            emitted.append(rc)
        except Exception:
            # fail-open: metrics must never break pipeline
            pass

        dual = (os.getenv("EDGE_DUAL_EMIT_LEGACY_THIN_COST", "0") or "").strip().lower() in {"1", "true", "yes", "on"}
        if dual and rc != "VETO_EDGE_THIN_COST":
            try:
                emit_veto_metric(kind=kind, ctx=ctx, reason_code="VETO_EDGE_THIN_COST")
                emitted.append("VETO_EDGE_THIN_COST")
            except Exception:
                pass
    except Exception:
        return emitted
    return emitted
