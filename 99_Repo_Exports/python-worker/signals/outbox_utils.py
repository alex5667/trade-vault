# outbox_utils.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from common.time_utils import normalize_epoch_ms_best_effort


# -------- ts helpers --------

def ensure_ts_ms(ts: int | float | None) -> int:
    """
    Гарантирует миллисекунды. Делегирует в canonical normalize_epoch_ms_best_effort.
    - если пришли секунды (10 цифр) -> *1000
    - если None -> now_ms (via canonical implementation)
    """
    return normalize_epoch_ms_best_effort(ts)


def normalize_to_bucket(ts_ms: int, bucket_ms: int) -> int:
    """
    Нормализует ts_ms на начало бакета (например, 60_000).
    """
    b = max(int(bucket_ms), 1)
    return (int(ts_ms) // b) * b


# -------- kind / subtype helpers --------

def normalize_kind(kind: str | None, *, subtype: str | None = None) -> str:
    """
    Не даём уходить в outbox с kind="custom".
    Если всё же пришло "custom" или пусто — используем subtype или "unknown".
    """
    k = (kind or "").strip().lower()
    st = (subtype or "").strip().lower()

    if not k or k == "custom":
        return st or "unknown"

    return k


# -------- level_key helpers --------

def nearest_pivot_key(price: float, pivots: Dict[str, Any] | None) -> str:
    """
    Возвращает ближайший pivot key (PP/R1/S1...), или "na".
    """
    if not pivots or price <= 0:
        return "na"

    best_k = "na"
    best_d = 1e18
    for k, lvl in pivots.items():
        try:
            lvl_f = float(lvl)
        except Exception:
            continue
        d = abs(lvl_f - float(price))
        if d < best_d:
            best_d = d
            best_k = str(k)
    return best_k


def price_bin_key(price: float, step: float) -> str:
    """
    Бин цены для стабилизации дедупа (например, шаг 0.5$ или 1.0$ для XAU).
    """
    if step <= 0:
        return f"px{price:.2f}"
    b = round(float(price) / float(step)) * float(step)
    # чтобы ключ был стабилен (0.5 -> 2764.0 и т.п.)
    if step >= 1:
        return f"px{b:.1f}"
    return f"px{b:.2f}"


def build_level_key_extreme(
    *,
    price: float,
    pivots: Dict[str, Any] | None,
    z: float | None = None,
    price_step: float = 0.5,
    z_step: float = 1.0,
    include_z_bin: bool = False,
) -> str:
    """
    EXTREME: вместо "na" делаем (nearest_pivot + price-bin) [+ z-bin опционально]
    Примеры:
      "PP:px2764.0"
      "R1:px2766.5:z3"
    """
    pv = nearest_pivot_key(price, pivots)
    px = price_bin_key(price, price_step)

    if include_z_bin and z is not None and z_step > 0:
        zb = int(abs(float(z)) // float(z_step))
        return f"{pv}:{px}:z{zb}"
    return f"{pv}:{px}"


def build_level_key_breakout(lvl: Optional[str]) -> Optional[str]:
    """
    BREAKOUT: если нет lvl — возвращаем None (и сигнал НЕ публикуем).
    """
    if not lvl:
        return None
    return str(lvl)


def build_level_key_sweep(*, price: float, pivots: Dict[str, Any] | None, fallback: str = "na") -> str:
    """
    SWEEP/OBI_SPIKE/ABSORPTION: nearest pivot достаточно, но стабильно.
    """
    k = nearest_pivot_key(price, pivots)
    return k or fallback


# -------- publish result wrapper (удобно для счетчиков) --------

@dataclass(frozen=False)
class PublishResult:
    sent: bool
    dedup: bool
    msg_id: Optional[str]
    confidence: Optional[float] = None
