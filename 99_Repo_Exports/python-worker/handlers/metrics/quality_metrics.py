from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class QualityCounters:
    l2_missing: int = 0
    l2_stale: int = 0
    l2_veto: int = 0
    l3_missing: int = 0
    htf_missing: int = 0
    atr_fallback: int = 0
    hlc_fallback: int = 0


class QualityMetrics:
    """
    "Жёстче": метрики должны быть наблюдаемыми.
    Интеграция:
      - если в ctx есть health_metrics с методом inc(name, value=1) => шлём туда
      - иначе держим локальные counters (для unit-тестов/отладки)
    """

    def __init__(self) -> None:
        self.c = QualityCounters()

    def _inc_local(self, name: str, v: int = 1) -> None:
        if not hasattr(self.c, name):
            return
        setattr(self.c, name, int(getattr(self.c, name)) + int(v))

    def inc(self, ctx: Any, name: str, v: int = 1) -> None:
        # 1) внешний HealthMetrics (если есть)
        hm = getattr(ctx, "health_metrics", None) or getattr(ctx, "health", None)
        if hm is not None:
            fn = getattr(hm, "inc", None)
            if callable(fn):
                try:
                    fn(name, v)
                    return
                except Exception:
                    pass
        # 2) локально
        self._inc_local(name, v)

    def record_flags(self, ctx: Any, flags: list[str]) -> None:
        s = set(flags or [])
        if "l2_missing" in s:
            self.inc(ctx, "l2_missing", 1)
        if "l2_stale" in s or "l2_no_ts" in s:
            self.inc(ctx, "l2_stale", 1)
        if "l3_missing" in s:
            self.inc(ctx, "l3_missing", 1)
        if "htf_missing" in s:
            self.inc(ctx, "htf_missing", 1)
        if "atr_fallback" in s:
            self.inc(ctx, "atr_fallback", 1)
        if "hlc_fallback" in s:
            self.inc(ctx, "hlc_fallback", 1)
