from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _as_dict_maybe_json(x: Any) -> Dict[str, Any]:
    """Accept dict or JSON-encoded dict; return {} on failure."""
    if isinstance(x, dict):
        return x
    if isinstance(x, str):
        s = x.strip()
        if s.startswith('{') and s.endswith('}'):
            try:
                v = json.loads(s)
                return v if isinstance(v, dict) else {}
            except Exception:
                return {}
    return {}


def compute_r_mult_from_pnl_risk(pnl: float, risk_usd: float) -> Tuple[float, str]:
    """Compute r-multiple from pnl/risk. Returns (r_mult, source)."""
    if float(risk_usd) <= 0.0:
        return 0.0, 'no_risk'
    return float(pnl) / float(risk_usd), 'pnl_over_risk'


def compute_r_mult_from_closed(fields: Dict[str, Any]) -> Tuple[float, str]:
    """Compute r-multiple from a POSITION_CLOSED payload.

    Preference order:
      1) explicit r_mult (or R)
      2) pnl/risk_usd (or meta.risk_usd)
      3) heuristic for sparse CLOSE events: reason contains TP/WIN → 1.0

    Returns (r_mult, source).
    """
    # 1) explicit
    if 'r_mult' in fields and fields.get('r_mult') is not None:
        r = _f(fields.get('r_mult'), 0.0)
        return float(r), 'r_mult'
    if 'R' in fields and fields.get('R') is not None:
        r = _f(fields.get('R'), 0.0)
        return float(r), 'R'

    meta = _as_dict_maybe_json(fields.get('meta') or fields.get('metadata'))

    # 2) pnl/risk
    pnl = _f(fields.get('pnl') or fields.get('pnl_net') or 0.0, 0.0)
    risk = _f(fields.get('risk_usd') or 0.0, 0.0)
    if risk <= 0.0:
        risk = _f(meta.get('risk_usd') or 0.0, 0.0)
    if pnl == 0.0:
        pnl = _f(meta.get('pnl') or meta.get('pnl_net') or 0.0, 0.0)
    if risk > 0.0:
        r, _ = compute_r_mult_from_pnl_risk(pnl, risk)
        return float(r), 'pnl_over_risk'

    # 3) heuristic
    rsn = str(fields.get('reason') or '').upper()
    rsn_raw = str(fields.get('reason_raw') or '').upper()
    if 'TP' in rsn or 'TP' in rsn_raw or 'WIN' in rsn:
        return 1.0, 'heuristic_tp'

    return 0.0, 'missing'


def compute_y_from_r_mult(r_mult: float, r_min: float) -> int:
    return 1 if float(r_mult) >= float(r_min) else 0


def compute_y_and_r_from_closed(fields: Dict[str, Any], *, r_min: float = 0.5) -> Tuple[int, float, str]:
    r_mult, src = compute_r_mult_from_closed(fields)
    y = compute_y_from_r_mult(r_mult, r_min)
    return int(y), float(r_mult), str(src)
