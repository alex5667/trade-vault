"""
Централизованная конфигурация (ENV → объект).
Ничего не импортирует из ваших модулей, чтобы избежать циклов.
"""

import os
from dataclasses import dataclass, field

from core.redis_keys import RedisStreams as RS
import contextlib


def _f(v: str, default: float) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _i(v: str, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


@dataclass
class Config:
    # Redis
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # Базовые ключи и каналы
    # NOTE: fallback использует RS.TICK_TPL — единственный canonical источник шаблона.
    # Не дублировать строку "stream:tick_{symbol}" в этом файле; менять только RS.TICK_TPL.
    tick_stream_tpl: str = os.getenv("TICK_STREAM_TPL", RS.TICK_TPL)
    # last_tick_key_tpl: локальный ключ, не является Redis Stream — не зеркалится в keys.go.
    # Placeholder {symbol} (lowercase) — canonical. {SYMBOL} uppercase — legacy, deprecated.
    last_tick_key_tpl: str = os.getenv("LAST_TICK_KEY_TPL", "last:tick:{symbol}")
    pivots_key: str = os.getenv("PIVOTS_KEY", "pivots:latest")
    dom_levels_key_tpl: str = os.getenv("DOM_LEVELS_KEY_TPL", "book:levels:{symbol}")
    ohlc_m1_list_tpl: str = os.getenv("OHLC_M1_LIST_TPL", "ohlc:m1:{symbol}")

    # Go-gateway
    gateway_url: str = os.getenv("GATEWAY_URL", "http://127.0.0.1:8090")
    orders_push_path: str = os.getenv("ORDERS_PUSH_PATH", "/orders/push")
    balance_path: str = os.getenv("BALANCE_PATH", "/account/balance")
    runtime_atr_path: str = os.getenv("RUNTIME_ATR_PATH", "/runtime/atr")

    # Символ и тайминги
    symbol: str = os.getenv("SYMBOL")
    poll_ms: int = _i(os.getenv("HUB_POLL_MS", "500"), 500)
    cooldown_sec: int = _i(os.getenv("HUB_COOLDOWN_SEC", "180"), 180)
    dedupe_ttl_sec: int = _i(os.getenv("HUB_DEDUPE_TTL_SEC", "900"), 900)

    # ATR / риск
    atr_period: int = _i(os.getenv("ATR_PERIOD", "14"), 14)
    atr_sl_mult: float = _f(os.getenv("ATR_SL_MULTIPLIER", "1.5"), 1.5)
    atr_tp_mults: list[float] = field(default_factory=lambda: [2.0, 3.0, 4.0])
    risk_pct: float = _f(os.getenv("RISK_PERCENT", "5.0"), 5.0)
    min_lot: float = _f(os.getenv("MIN_LOT", "0.01"), 0.01)
    max_lot: float = _f(os.getenv("MAX_LOT", "1.0"), 1.0)
    lot_step: float = _f(os.getenv("LOT_STEP", "0.01"), 0.01)

    # Спеки инструмента (фолбэк)
    point: float = _f(os.getenv("SPEC_POINT", "0.1"), 0.1)
    tick_value_per_lot: float = _f(os.getenv("SPEC_TICK_VALUE_PER_LOT", "1.0"), 1.0)

    # Пороги детекторов
    z_delta_thr: float = _f(os.getenv("Z_DELTA_THR", "3.0"), 3.0)
    z_extreme_thr: float = _f(os.getenv("Z_EXTREME_THR", "4.5"), 4.5)
    speed_z_thr: float = _f(os.getenv("SPEED_Z_THR", "3.0"), 3.0)

    # Запись и отладка
    notify_stream: str = os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)
    orders_sent_key_tpl: str = os.getenv("ORDERS_SENT_KEY_TPL", "orders:sent:{SID}")
    logger_name: str = os.getenv("LOGGER_NAME", "aggregated_hub")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


def load_config() -> Config:
    cfg = Config()
    atr_env = os.getenv("ATR_TP_MULTIPLIERS")
    if atr_env:
        with contextlib.suppress(Exception):
            cfg.atr_tp_mults = [float(x) for x in atr_env.split(",") if x.strip()]
    return cfg


