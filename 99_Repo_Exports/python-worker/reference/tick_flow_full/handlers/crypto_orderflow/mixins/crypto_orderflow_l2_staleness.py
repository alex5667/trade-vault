from __future__ import annotations

"""
L2 Staleness logic for CryptoOrderFlowHandler.

This module contains all L2 staleness detection and quality flag management.
"""

import os
from typing import Any

# Import L2 staleness helper functions (these would need to be extracted)
from utils.time_utils import get_ny_time_millis


class CryptoOrderFlowL2StalenessMixin:
    """
    Mixin class containing L2 staleness logic for CryptoOrderFlowHandler.
    """

    def _l2_max_stale_ms(self) -> int:
        # Дефолт под крипто-тик/книгу: 1500ms — достаточно, чтобы отсечь "мертвую" книгу.
        # Можно переопределять через env.
        return int(os.getenv("L2_MAX_STALE_MS", "1500"))

    def _ctx_quality_flags(self, ctx: Any) -> list[str]:
        v = getattr(ctx, "data_quality_flags", None)
        if isinstance(v, list):
            return v
        # fail-open: создаём список
        flags: list[str] = []
        try:
            ctx.data_quality_flags = flags
        except Exception:
            pass
        return flags

    def _get_l2_from_ctx(self, ctx: Any) -> Any:
        # поддерживаем несколько имен, чтобы не зависеть от конкретной реализации
        for k in ("l2", "l2_snapshot", "book", "orderbook", "l2_book"):
            try:
                v = getattr(ctx, k, None)
                if v is not None:
                    return v
            except Exception:
                continue
            try:
                if isinstance(ctx, dict) and k in ctx:
                    return ctx.get(k)
            except Exception:
                pass
        return None

    def _mark_l2_staleness(self, *, ctx: Any, kind: str) -> tuple[bool, int | None]:
        """
        Возвращает (stale_or_missing, age_ms).
        Также:
          - обновляет l2_stale_rate (gauge через MissingRateTracker)
          - добавляет флаги качества в ctx.data_quality_flags
        """
        now_ms = get_ny_time_millis()
        l2 = self._get_l2_from_ctx(ctx)
        flags = self._ctx_quality_flags(ctx)

        if l2 is None:
            if "l2_missing" not in flags:
                flags.append("l2_missing")
            # missing считаем stale для метрики staleness (иначе rate будет "розовым")
            tr = getattr(self, "_l2_stale", None)
            if tr is not None:
                try:
                    tr.mark(miss=True)
                    tr.maybe_export(getattr(self, "_m2", None))
                except Exception:
                    pass
            return True, None

        max_age = self._l2_max_stale_ms()
        stale = False
        age_ms: int | None = None

        # Use helper functions for staleness detection
        # These would need to be imported or defined
        _extract_ts_ms = getattr(self, "_extract_ts_ms", None)
        _is_stale = getattr(self, "_is_stale", None)

        try:
            if _extract_ts_ms is not None:
                ts_ms = _extract_ts_ms(l2)
                if ts_ms is not None:
                    age_ms = now_ms - int(ts_ms)
                    try:
                        ctx.l2_age_ms = int(age_ms)
                    except Exception:
                        pass
            if _is_stale is not None:
                stale = bool(_is_stale(obj=l2, now_ms=now_ms, max_age_ms=max_age))
        except Exception:
            stale = True

        if stale:
            if "l2_stale" not in flags:
                flags.append("l2_stale")

        tr = getattr(self, "_l2_stale", None)
        if tr is not None:
            try:
                tr.mark(miss=bool(stale))
                tr.maybe_export(getattr(self, "_m2", None))
            except Exception:
                pass

        return bool(stale), age_ms
