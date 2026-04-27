from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional, Tuple


def _now_ms() -> int:
    return int(time.time() * 1000)


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x) if x is not None else d
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


DEFAULT_OVERRIDES_KEY = os.getenv("CFG_ENTRY_POLICY_OVERRIDES_KEY", "cfg:entry_policy:overrides")


def parse_overrides_json(raw: str) -> Tuple[int, Dict[str, str]]:
    """
    Format:
      {"version": 3, "updated_ts_ms": 123, "overrides": {"SMT_COH_THRESHOLD":"0.67","ENTRY_POLICY_SHADOW":"1"}}
    Returns (version, overrides_map)
    """
    try:
        d = json.loads(raw)
        if not isinstance(d, dict):
            return 0, {}
        ver = _i(d.get("version", 0), 0)
        ov = d.get("overrides", {})
        if not isinstance(ov, dict):
            return ver, {}
        out: Dict[str, str] = {}
        for k, v in ov.items():
            if k is None:
                continue
            out[str(k)] = _s(v, "")
        return ver, out
    except Exception:
        return 0, {}


async def fetch_overrides(*, r, key: Optional[str] = None) -> Tuple[int, Dict[str, str]]:
    k = key or DEFAULT_OVERRIDES_KEY
    try:
        raw = await r.get(k)
        if not raw:
            return 0, {}
        return parse_overrides_json(raw)
    except Exception:
        return 0, {}


def apply_overrides_to_env(base: Dict[str, str], overrides: Dict[str, str]) -> Dict[str, str]:
    """
    Returns merged env map (strings). Does not mutate base.
    """
    out = dict(base)
    for k, v in overrides.items():
        out[str(k)] = str(v)
    return out


def get_effective_numeric(*, env: Dict[str, str], key: str, kind: str, default: float) -> float:
    """
    kind: "f" or "i"
    """
    if key not in env:
        return float(default)
    if kind == "i":
        return float(_i(env.get(key), int(default)))
    return float(_f(env.get(key), float(default)))
