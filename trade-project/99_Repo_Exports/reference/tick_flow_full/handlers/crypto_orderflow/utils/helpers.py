from __future__ import annotations

import math
from typing import Any, Optional, List

from handlers.base_orderflow_handler import Tick


def _to_str(x: Any) -> str:
    """
    Безопасное преобразование в float с fallback.
    """
    if x is None:
        return ""
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", errors="ignore")
    return str(x)


def _f(x: Any, default: float = 0.0) -> float:
    """
    Безопасное преобразование в float с fallback.
    """
    try:
        if x is None:
            return float(default)
        return float(x)
    except Exception:
        return float(default)


def _b(x: Any) -> bool:
    """
    Безопасное преобразование в bool.
    """
    try:
        return bool(x)
    except Exception:
        return False


def _depth_sum(levels: Any, depth: int = 5) -> float:
    """
    Суммирует объем на первых N уровнях книги.

    Args:
        levels: [[price, vol], ...] or [["price","vol"], ...]
        depth: Количество уровней для суммирования

    Returns:
        Суммарный объем
    """
    if not levels or depth <= 0:
        return 0.0

    # Для одиночных вызовов используем CPU (быстро)
    s = 0.0
    n = 0
    for lv in levels:
        if not lv or len(lv) < 2:
            continue
        try:
            s += float(lv[1])
            n += 1
        except Exception:
            continue
        if n >= depth:
            break
    return float(s)


def _is_trade_tick(tick: Tick) -> bool:
    """
    Унифицированная проверка является ли тик трейдом.
    Гарантирует консистентность между RS трекером, delta классификацией и L3.
    """
    return bool(tick.flags & 1) or bool(tick.last and tick.volume and tick.volume > 0)


def _parse_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = _to_str(v).strip().lower()
    if s in {"true", "1", "yes", "y"}:
        return True
    if s in {"false", "0", "no", "n"}:
        return False
    return None
