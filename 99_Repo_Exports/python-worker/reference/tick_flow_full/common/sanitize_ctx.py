from __future__ import annotations

"""
Санитизация OrderflowContext (ctx) на входе в unified pipeline.

Задачи:
  1) Не допускать NaN/Inf в "горячих" числах (spread/obi/microprice/cancel_to_trade/...).
  2) При плохих значениях не падать, а:
       - заменить на None (или нейтральное значение для score-полей)
       - добавить data_quality_flags (для логов/метрик/дебага)
       - продолжить (fail-open), если возможно.

Это ровно то, что нужно для property-based тестов 6.3:
  - "случайные тики с шумом: никакие NaN/Inf не пробивают пайплайн"
"""

from typing import Any, Optional

from common.sanitize_math import finite_float
from common.strict_mode import strict_contracts_enabled


# "Горячие" поля ctx, которые часто используются в сравнениях/скоринге/валидации.
# Список намеренно короткий и практичный: меньше риск повредить редкие поля.
_HOT_FLOAT_FIELDS: tuple[str, ...] = (
    "price"
    "last_price"
    "bid"
    "ask"
    "atr"
    "atr_14_bps"
    "atr_quantile"
    "spread_bps"
    "obi"
    "obi_avg"
    "obi_5"
    "obi_20"
    "obi_50"
    "microprice_shift_bps_20"
    "cancel_to_trade_bid_5s"
    "cancel_to_trade_ask_5s"
    "cancel_to_trade_bid_20s"
    "cancel_to_trade_ask_20s"
    "taker_rate_ema"
    "weak_progress"
    "weak_progress_raw"
    "weak_progress_ratio"
    "regime_trend_score"
    "regime_range_score"
    "market_regime_score"
    "geometry_score"
    "htf_level_dist_bps"
)


def _push_flag(ctx: Any, flag: str) -> None:
    try:
        flags = getattr(ctx, "data_quality_flags", None)
        if flags is None:
            setattr(ctx, "data_quality_flags", [flag])
            return
        if isinstance(flags, list):
            flags.append(flag)
            return
        # если кто-то положил не list — заменяем fail-open
        setattr(ctx, "data_quality_flags", [flag, "dq_flags_schema_fail_open"])
    except Exception:
        # вообще ничего не делаем: санитизация не должна валить пайплайн
        pass


def sanitize_ctx_inplace(ctx: Any, *, logger: Optional[Any] = None) -> None:
    """
    Мутирует ctx "на месте". Ничего не возвращает: fail-open.
    """
    for name in _HOT_FLOAT_FIELDS:
        try:
            if not hasattr(ctx, name):
                continue
            v = getattr(ctx, name)
            # Только float/int санитизируем. Списки/объекты не трогаем.
            if isinstance(v, (int, float)):
                fv = finite_float(v, default=None)
                if fv is None:
                    setattr(ctx, name, None)
                    _push_flag(ctx, f"nan_inf:{name}")
        except Exception as e:
            _push_flag(ctx, f"sanitize_err:{name}")
            if logger is not None:
                try:
                    logger.warning(f"sanitize_ctx_inplace failed for {name}: {e}")
                except Exception:
                    pass

    # "жёсткий" контракт для CI/dev: если после санитизации есть nan_inf флаги — падаем.
    # В проде выключено (fail-open), чтобы не ронять pipeline.
    if strict_contracts_enabled():
        try:
            flags = getattr(ctx, "data_quality_flags", None)
            if isinstance(flags, list) and any(isinstance(x, str) and x.startswith("nan_inf:") for x in flags):
                raise AssertionError(f"STRICT: NaN/Inf leaked into ctx (flags={flags})")
        except Exception:
            # если flags сломаны — это тоже сигнал, но strict должен ловить
            raise
