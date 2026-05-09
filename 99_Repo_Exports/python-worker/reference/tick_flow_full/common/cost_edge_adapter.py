from __future__ import annotations

from typing import Any

from common.safe_numbers import safe_float


def decision_to_legacy_tuple(dec: Any) -> tuple[bool, dict[str, float]]:
    """
    Convert EdgeCostGateDecision-like object to legacy (ok_edge, edge_details) tuple.

    Legacy contract used by older call-sites:
      ok_edge: bool
      edge_details: dict[str, float] (for logs/metrics)

    Policy:
      - if gate not applied -> ok_edge=True
      - else ok_edge = not veto
    """
    apply = True
    try:
        apply = bool(getattr(dec, "apply", True))
    except Exception:
        apply = True

    veto = False
    try:
        veto = bool(getattr(dec, "veto", False))
    except Exception:
        veto = False

    ok_edge = (not apply) or (not veto)

    # keep keys stable for dashboards/log parsing
    details: dict[str, float] = {
        "expected_move_bps": safe_float(getattr(dec, "expected_move_bps", None)),
        "threshold_bps": safe_float(getattr(dec, "threshold_bps", None)),
        "fees_bps": safe_float(getattr(dec, "fees_bps", None)),
        "slippage_bps": safe_float(getattr(dec, "slippage_bps", None)),
        "k": safe_float(getattr(dec, "k", None)),
    }
    return ok_edge, details


def attach_cost_edge_veto_fields(ctx: Any, dec: Any) -> None:
    """
    Attach minimal veto diagnostics to ctx in a backward-compatible way.
    This MUST be best-effort and never raise.

    IMPORTANT:
      Older code sometimes expected fields like delay_ms/spread_* (from other gates).
      We only attach those fields if the decision actually provides them.
    """
    try:
        ctx.veto_reason_code = str(getattr(dec, "reason_code", "") or "")
        ctx.veto_expected_move_bps = safe_float(getattr(dec, "expected_move_bps", None))
        ctx.veto_threshold_bps = safe_float(getattr(dec, "threshold_bps", None))
        ctx.veto_fees_bps = safe_float(getattr(dec, "fees_bps", None))
        ctx.veto_slippage_bps = safe_float(getattr(dec, "slippage_bps", None))
        ctx.veto_k = safe_float(getattr(dec, "k", None))
        ctx.veto_mode = str(getattr(dec, "mode", "") or "")
        ctx.veto_notes = str(getattr(dec, "notes", "") or "")

        # Optional fields (only if present on decision object)
        for name in (
            "delay_ms",
            "spread_bps",
            "spread_ema_bps",
            "burst_flip_ratio",
            "cancel_to_trade",
        ):
            if hasattr(dec, name):
                try:
                    setattr(ctx, f"veto_{name}", getattr(dec, name))
                except Exception:
                    pass
    except Exception:
        return
