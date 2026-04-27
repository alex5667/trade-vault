from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple


def normalize_side(v: Any) -> str:
    """
    Normalize direction to domain canonical runtime strings: 'LONG' / 'SHORT'.
    Your codebase has mixed representations:
      - domain/models.py uses Literal 'LONG'/'SHORT' (uppercase)
      - some other modules use Enum with 'long'/'short' (lowercase)
    This function makes writes & recovery stable.
    """
    if v is None:
        return "LONG"
    s = str(v).strip()
    sl = s.lower()
    if sl in ("long", "buy"):
        return "LONG"
    if sl in ("short", "sell"):
        return "SHORT"
    su = s.upper()
    return su if su in ("LONG", "SHORT") else "LONG"


def parse_bool01(v: Any, default: bool = False) -> bool:
    """
    Parse redis hash flags stored as '1'/'0' or truthy strings.
    """
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def parse_json_list(v: Any) -> List[Any]:
    if v is None:
        return []
    try:
        if isinstance(v, list):
            return v
        s = str(v).strip()
        if not s:
            return []
        x = json.loads(s)
        return x if isinstance(x, list) else []
    except Exception:
        return []


def parse_json_dict(v: Any) -> Dict[str, Any]:
    if v is None:
        return {}
    try:
        if isinstance(v, dict):
            return v
        s = str(v).strip()
        if not s:
            return {}
        x = json.loads(s)
        return x if isinstance(x, dict) else {}
    except Exception:
        return {}


def extract_tp_levels(h: Dict[str, str]) -> List[float]:
    """
    Extract tp_levels with backward compatibility:
      - prefer tp_levels JSON
      - fallback to tp1/tp2/tp3 scalars
    """
    tps = []
    if h.get("tp_levels"):
        tps = parse_json_list(h.get("tp_levels"))
    if not tps:
        tps = [h.get("tp1") or 0, h.get("tp2") or 0, h.get("tp3") or 0]
    out: List[float] = []
    for x in tps:
        try:
            fx = float(x)
            if fx > 0:
                out.append(fx)
        except Exception:
            pass
    return out[:3]


def extract_profile(h: Dict[str, str]) -> str:
    """
    Read both keys for compatibility.
    """
    return str(h.get("trail_profile") or h.get("trailing_profile") or "")


def extract_tp_fills(h: Dict[str, str]) -> Tuple[Dict[int, float], Dict[int, int]]:
    """
    Reconstruct tp_fill_prices/tp_fill_times from persisted per-level scalars:
      tp1_fill_price, tp1_fill_ts, ...
    """
    prices: Dict[int, float] = {}
    times: Dict[int, int] = {}
    for lvl in (1, 2, 3):
        p = h.get(f"tp{lvl}_fill_price")
        t = h.get(f"tp{lvl}_fill_ts")
        if p is not None:
            try:
                fp = float(p)
                if fp > 0:
                    prices[lvl] = fp
            except Exception:
                pass
        if t is not None:
            try:
                it = int(float(t))
                if it > 0:
                    times[lvl] = it
            except Exception:
                pass
    return prices, times
