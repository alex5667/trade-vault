from __future__ import annotations

from typing import Optional, Any
import math

def finite_f(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if not math.isfinite(v):
            return None
        return v
    except Exception:
        return None

def label_outcome(outcome: str, realized_r: Optional[float], *, eps_r: float = 0.05) -> Optional[int]:
    """
    Разметка для confidence calibration:
      y=1: прибыльный исход
      y=0: убыточный исход
      None: исключить из обучения (нет входа/нет результата/нейтральная зона)

    Бизнес-логика (оптимально для реального confidence):
      - target_hit: win (1)
      - stop_hit: loss (0)
      - manual_exit: по realized_R (win/loss), около 0 -> исключить
      - expired_no_target: по realized_R (win/loss), около 0 -> исключить
      - expired_no_entry/unknown: исключить
      - breakeven: исключить
    """
    o = str(outcome or "").strip().lower()

    if o in {"expired_no_entry", "unknown"}:
        return None

    if o == "target_hit":
        return 1
    if o == "stop_hit":
        return 0

    if o in {"breakeven", "expired_no_target", "manual_exit"}:
        rr = realized_r
        if rr is None:
            return None
        if rr > eps_r:
            return 1
        if rr < -eps_r:
            return 0
        return None

    # Всё остальное — не учим (чтобы не вносить шум)
    return None
