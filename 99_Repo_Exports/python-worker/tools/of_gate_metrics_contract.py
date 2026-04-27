from typing import Any, Dict, Tuple

def is_gate_row(r: Dict[str, Any]) -> bool:
    """Check if a row from Redis stream is an actual of_gate record."""
    t = str(r.get("type") or "").strip().lower()
    return t == "of_gate" or t == "of_gate_metrics_v1" or (t == "" and "ok" in r)

def derive_ok_fields(r: Dict[str, Any]) -> Tuple[int, int, str, str]:
    """
    Robust extraction of ok and ok_soft, plus their sources.
    Returns: (ok, ok_soft, ok_src, ok_soft_src)
    """
    ok_src = str(r.get("ok_src") or "missing")
    ok_soft_src = str(r.get("ok_soft_src") or "missing")
    
    ok_raw = r.get("ok")
    if ok_raw is not None:
        try:
            ok = int(float(ok_raw))
        except Exception:
            ok = 0
            ok_src = "parse_error"
    else:
        ok = 0

    soft_raw = r.get("ok_soft")
    if soft_raw is not None:
        try:
            soft = int(float(soft_raw))
        except Exception:
            soft = 0
            ok_soft_src = "parse_error"
    else:
        soft = 0

    return ok, soft, ok_src, ok_soft_src

def scenario_key(r: Dict[str, Any]) -> str:
    """Extract standard scenario."""
    from core.ok_fields import get_scenario
    return get_scenario(r) or "na"
