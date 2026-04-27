from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from core.of_confirm_contract import pack_bits


from core.signal_payload import StrongGateDecision


def _truth(x: Any) -> int:
    return 1 if bool(x) else 0


def eval_reversal(
    *,
    direction: str,
    delta_z: float,
    weak_progress: bool,
    sweep_recent: bool,
    reclaim_recent: bool,
    obi_stable: bool,
    iceberg_strict: bool,
    abs_lvl_ok: bool = False,
    fp_edge_absorb: bool = False,
    ofi_leg: bool = False,
    cfg: Dict[str, Any],
) -> StrongGateDecision:
    """
    Reversal requires 2 of 3:
      A) deltaSpikeZ + weakProgress
      B) sweep + reclaim
      C) obi_stable OR iceberg_strict OR fp_edge_absorb OR ofi_leg
    """
    zmin = float(cfg.get("strong_z_min", 2.0))
    A = _truth(abs(delta_z) >= zmin and weak_progress)
    B = _truth(sweep_recent and reclaim_recent)
    use_ice = bool(cfg.get("strong_use_iceberg", True))
    C = _truth(
        obi_stable
        or (iceberg_strict if use_ice else False)
        or fp_edge_absorb
        or ofi_leg
    )

    # Absorption-on-level can count as one confirmation (configurable)
    if bool(int(cfg.get("abs_lvl_enable", 1))) and bool(abs_lvl_ok):
        mode = str(cfg.get("abs_lvl_counts_as", "A")).upper()  # "A" or "C"
        if mode == "C":
            C = 1
        else:
            A = 1

    need = int(cfg.get("strong_need_reversal", 2))
    have = A + B + C
    ok = have >= need
    bits = pack_bits(bool(A), bool(B), bool(C), bool(abs_lvl_ok))
    return StrongGateDecision(
        ok=ok,
        scenario="reversal",
        need=need,
        have=have,
        a=A, b=B, c=C,
        reason="reversal_gate",
        gate_bits=int(bits),
    )


def hidden_trend_dir(last_div_kind: Optional[str]) -> Optional[str]:
    if not last_div_kind:
        return None
    k = str(last_div_kind)
    if k == "bullish_hidden":
        return "LONG"
    if k == "bearish_hidden":
        return "SHORT"
    return None


def eval_continuation(
    *,
    direction: str,
    trend_dir: Optional[str],
    hidden_ctx_recent: bool,
    iceberg_strict: bool,
    obi_stable: bool,
    cont_ctx_recent: bool,
    abs_lvl_ok: bool = False,
    ofi_leg: bool = False,
    fp_edge_absorb: bool = False,
    cfg: Dict[str, Any],
    trend_dir_source: str = "none",
) -> StrongGateDecision:
    """
    Continuation requires 2 of 3:
      A) hidden_ctx_recent AND direction==trend_dir
      B) obi_stable OR iceberg_strict OR ofi_leg OR fp_edge_absorb
      C) cont_ctx_recent (countertrend absorption observed recently)
    """
    if trend_dir is None:
        return StrongGateDecision(ok=False, scenario="continuation", need=2, have=0, a=0, b=0, c=0, reason="no_trend_dir")

    is_aligned = (str(direction).upper() == str(trend_dir).upper())
    
    # If trend context is derived from regime/direction fallback (not hidden div),
    # we treat being robustly aligned with the HTF regime as a proxy for the context leg (A).
    fallback_en = bool(int(cfg.get("strong_cont_allow_fallback_a", 0)))
    fallback_a = fallback_en and (trend_dir_source in ("regime", "direction"))
    
    A = _truth((hidden_ctx_recent or fallback_a) and is_aligned)
    use_ice = bool(cfg.get("strong_use_iceberg", True))
    B = _truth(
        obi_stable
        or (iceberg_strict if use_ice else False)
        or ofi_leg
        or fp_edge_absorb
    )

    # Optional: abs_lvl_ok can assist in continuation too
    if bool(int(cfg.get("abs_lvl_enable", 1))) and bool(abs_lvl_ok):
        mode = str(cfg.get("abs_lvl_counts_as", "A")).upper()
        if mode == "B":
            B = 1
        else:
            A = 1

    C = _truth(cont_ctx_recent)
    need = int(cfg.get("strong_need_continuation", 2))
    have = A + B + C
    ok = have >= need
    bits = pack_bits(bool(A), bool(B), bool(C), bool(abs_lvl_ok))
    return StrongGateDecision(
        ok=ok,
        scenario="continuation",
        need=need,
        have=have,
        a=A, b=B, c=C,
        reason="continuation_gate",
        gate_bits=int(bits),
    )
