# services/time_be_exit_policy.py
import os
import math
from dataclasses import dataclass
from typing import Tuple, Any

from common.log import setup_logger

logger = setup_logger("TimeBeExitPolicy")

@dataclass(frozen=True)
class TimeBeExitConfig:
    enabled: bool
    mode: str
    after_ms: int
    min_pnl_net_bps: float
    max_loss_net_bps: float
    require_no_tp1: bool
    disable_when_trailing: bool
    max_price_age_ms: int

def load_time_be_exit_config() -> TimeBeExitConfig:
    """Загружает конфигурацию TIME_BE_EXIT из переменных окружения."""
    return TimeBeExitConfig(
        enabled=os.getenv("TIME_BE_EXIT_ENABLED", "0") == "1",
        mode=os.getenv("TIME_BE_EXIT_MODE", "SHADOW").upper(),
        after_ms=int(os.getenv("TIME_BE_EXIT_AFTER_MS", "900000")),
        min_pnl_net_bps=float(os.getenv("TIME_BE_EXIT_MIN_PNL_NET_BPS", "1.5")),
        max_loss_net_bps=float(os.getenv("TIME_BE_EXIT_MAX_LOSS_NET_BPS", "-2.0")),
        require_no_tp1=os.getenv("TIME_BE_EXIT_REQUIRE_NO_TP1", "1") == "1",
        disable_when_trailing=os.getenv("TIME_BE_EXIT_DISABLE_WHEN_TRAILING", "1") == "1",
        max_price_age_ms=int(os.getenv("TIME_BE_EXIT_MAX_PRICE_AGE_MS", "5000")),
    )

def should_time_be_exit(
    pos: Any, 
    now_ms: int, 
    pnl_net_bps: float, 
    last_price_ts_ms: int,
    cfg: TimeBeExitConfig
) -> Tuple[bool, str, str]:
    """
    Оценивает, должна ли позиция быть закрыта по времени около безубытка.
    
    Returns:
        (should_close: bool, reason_code: str, mode: str)
        should_close == True только если это не SHADOW режим. 
        В SHADOW режиме вернется (False, reason_code, "SHADOW") 
        (для метрик 'would close').
    """
    if not cfg.enabled:
        return False, "DISABLED", cfg.mode

    if not pos or pos.entry_ts_ms <= 0:
        return False, "INVALID_POS", cfg.mode

    age_ms = now_ms - pos.entry_ts_ms
    if age_ms < cfg.after_ms:
        return False, "TIME_BE_EXIT_TOO_YOUNG", cfg.mode

    if cfg.require_no_tp1 and getattr(pos, "tp1_hit", False):
        return False, "TIME_BE_EXIT_TP1_ALREADY_HIT_SKIP", cfg.mode

    if cfg.disable_when_trailing and getattr(pos, "trailing_active", False):
        return False, "TIME_BE_EXIT_TRAILING_ACTIVE_SKIP", cfg.mode

    price_age_ms = now_ms - last_price_ts_ms
    if price_age_ms > cfg.max_price_age_ms:
        return False, "TIME_BE_EXIT_PRICE_STALE", cfg.mode

    # Если мы дошли сюда, возраст сделки подходит и блокировок нет
    # Проверяем pnl_net_bps
    reason = ""
    if pnl_net_bps >= cfg.min_pnl_net_bps:
        reason = "TIME_BE_EXIT_PROFIT_FLAT"
    elif pnl_net_bps >= cfg.max_loss_net_bps:
        reason = "TIME_BE_EXIT_NEAR_FLAT"
    else:
        return False, "NOT_BREAKEVEN", cfg.mode

    # По всем параметрам сделка подлежит закрытию
    is_shadow = (cfg.mode == "SHADOW")
    if is_shadow:
        return False, f"{reason}_SHADOW", cfg.mode

    return True, reason, cfg.mode
