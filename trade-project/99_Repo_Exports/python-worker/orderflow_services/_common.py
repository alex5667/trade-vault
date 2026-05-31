"""Shared helpers for orderflow_services modules."""
from __future__ import annotations

import json
from typing import Any

from utils.time_utils import get_ny_time_millis


def _now_ms() -> int:
    return get_ny_time_millis()


def _as_str(x: Any, default: str = "") -> str:
    try:
        if x is None:
            return default
        if isinstance(x, (bytes, bytearray)):
            return x.decode("utf-8", "ignore")
        return str(x)
    except Exception:
        return default


def _as_int(x: Any, default: int = 0) -> int:
    try:
        if x is None or isinstance(x, bool):
            return default
        if isinstance(x, (int, float)):
            return int(x)
        s = _as_str(x).strip()
        return int(float(s)) if s else default
    except Exception:
        return default


def _as_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or isinstance(x, bool):
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = _as_str(x).strip()
        return float(s) if s else default
    except Exception:
        return default


def _parse_list(raw: Any, *, upper: bool = True) -> list[str]:
    raw = (raw or "").strip() if raw is not None else ""
    if not raw:
        return []
    xs: list[str] = []
    for p in str(raw).replace(";", ",").split(","):
        s = p.strip()
        if upper:
            s = s.upper()
        if s and s not in xs:
            xs.append(s)
    return xs


def _load_json(raw: Any, default: dict | None = None) -> dict:
    try:
        if raw is None:
            return default if default is not None else {}
        s = raw if isinstance(raw, str) else _as_str(raw)
        if not s.strip():
            return default if default is not None else {}
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else (default if default is not None else {})
    except Exception:
        return default if default is not None else {}


def _load_json_file(path: str) -> dict | None:
    try:
        with open(path, encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None
