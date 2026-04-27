from __future__ import annotations

import json
from typing import Dict


def parse_map(raw: str) -> Dict[str, float]:
    """Parse JSON map string to dict[str, float].
    
    Handles:
    - Empty/None strings -> {}
    - Invalid JSON -> {}
    - Non-string keys -> skipped
    - Non-numeric values -> skipped
    - Keys are normalized to uppercase and stripped
    
    Args:
        raw: JSON string (e.g., '{"BTCUSDT":0.1,"ETHUSDT":"0.2"}')
        
    Returns:
        Dict mapping symbol (uppercase) to share (float)
    """
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        if not isinstance(obj, dict):
            return {}
        out: Dict[str, float] = {}
        for k, v in obj.items():
            if not isinstance(k, str):
                continue
            kk = k.strip().upper()
            try:
                out[kk] = float(v)
            except Exception:
                continue
        return out
    except Exception:
        return {}


def dump_map(m: Dict[str, float]) -> str:
    """Serialize dict to JSON string with stable ordering.
    
    Keys are sorted alphabetically for diff-friendly output.
    Values are rounded to 6 decimal places.
    
    Args:
        m: Dict mapping symbol to share
        
    Returns:
        JSON string (compact, no spaces)
    """
    items = sorted(m.items(), key=lambda kv: kv[0])
    obj = {k: round(float(v), 6) for k, v in items}
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def clamp_map(m: Dict[str, float], floor: float) -> Dict[str, float]:
    """Clamp all values in map to maximum floor.
    
    Used for freeze operations: ensures no share exceeds floor.
    
    Args:
        m: Dict mapping symbol to share
        floor: Maximum allowed share (all values clamped to <= floor)
        
    Returns:
        New dict with clamped values
    """
    out: Dict[str, float] = {}
    for k, v in m.items():
        try:
            out[k] = min(float(v), float(floor))
        except Exception:
            out[k] = float(floor)
    return out


def merge_updates(base: Dict[str, float], updates: Dict[str, float]) -> Dict[str, float]:
    """Merge updates into base map.
    
    Updates override base values. New keys are added.
    
    Args:
        base: Base map
        updates: Updates to apply
        
    Returns:
        New dict with merged values
    """
    out = dict(base)
    for k, v in updates.items():
        out[k] = float(v)
    return out

