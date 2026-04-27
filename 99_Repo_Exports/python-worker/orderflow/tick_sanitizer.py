from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""
Санитизация "сырых" тиков (Tick) на входе в обработчик.

Требования (4.2 + 6.3):
  - normalize tick.ts: секунды -> мс (эвристика < 1e12)
  - watermark: будущее/прошлое -> drop + метрика
  - NaN/Inf в bid/ask/last/volume не должны проходить дальше

Этот модуль не зависит от конкретного Tick dataclass — работает через getattr/setattr.
"""

import os
import time
from typing import Any, Optional

from common.sanitize_math import finite_float, is_finite_number


def normalize_ts_ms(ts: Any) -> Optional[int]:
    """
    Нормализация времени:
      - если < 1e12 считаем секунды и умножаем на 1000
      - иначе считаем, что это уже миллисекунды
    """
    v = finite_float(ts, default=None)
    if v is None:
        return None
    t = int(v)
    if t < 1_000_000_000_000:  # ~2001-09-09 в ms — всё меньше почти точно секунды
        t *= 1000
    return int(t)


def _now_ms() -> int:
    return get_ny_time_millis()


def sanitize_tick(tick: Any, *, logger: Optional[Any] = None) -> Optional[Any]:
    """
    Возвращает:
      - tick (мутированный) если он годится
      - None если tick надо дропнуть (плохой ts или нет валидной цены)
    """
    # ---- ts normalization + watermark ----
    ts_ms = normalize_ts_ms(getattr(tick, "ts", None))
    if ts_ms is None:
        return None

    # watermark окна (по умолчанию довольно мягкие)
    past_ms = int(os.getenv("TICK_WATERMARK_PAST_MS", "120000"))    # 2 минуты назад
    future_ms = int(os.getenv("TICK_WATERMARK_FUTURE_MS", "5000"))  # 5 секунд вперёд
    now = _now_ms()
    if ts_ms < (now - past_ms):
        # слишком старый тик
        try:
            setattr(tick, "ts", ts_ms)
        except Exception:
            pass
        return None
    if ts_ms > (now + future_ms):
        # тик из будущего — отдельный класс багов
        try:
            setattr(tick, "ts", ts_ms)
        except Exception:
            pass
        return None

    # ---- price fields ----
    bid = finite_float(getattr(tick, "bid", None), default=None)
    ask = finite_float(getattr(tick, "ask", None), default=None)
    last = finite_float(getattr(tick, "last", None), default=None)

    # Если bid/ask битые, но last валиден — делаем fail-open:
    # mid будет от last, а bid/ask подставим last (чтобы downstream не падал).
    if bid is None or ask is None or bid <= 0.0 or ask <= 0.0:
        if last is None or last <= 0.0:
            return None
        bid = last
        ask = last

    # ---- volume/flags ----
    vol = finite_float(getattr(tick, "volume", 0.0), default=0.0)
    if vol is None or vol < 0.0:
        vol = 0.0
    flags = getattr(tick, "flags", 0)
    if not isinstance(flags, int):
        # fail-open: лучше 0, чем падение на битых типах
        flags = 0

    # ---- commit sanitized fields ----
    try:
        setattr(tick, "ts", int(ts_ms))
        setattr(tick, "bid", float(bid))
        setattr(tick, "ask", float(ask))
        setattr(tick, "last", float(last if (last is not None and is_finite_number(last)) else float((bid + ask) / 2.0)))
        setattr(tick, "volume", float(vol))
        setattr(tick, "flags", int(flags))
    except Exception as e:  # pragma: no cover
        if logger is not None:
            try:
                logger.warning(f"sanitize_tick failed to set attrs: {e}")
            except Exception:
                pass
        return None

    return tick
