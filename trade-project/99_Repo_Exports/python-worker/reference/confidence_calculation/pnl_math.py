from __future__ import annotations

"""
Модуль для корректного расчета P&L с учетом спецификаций символов.

Устраняет хардкод *100 и другие несоответствия в расчетах P&L.
Поддерживает как линейную модель (contract_size), так и тиковую модель (tick_size/tick_value).
"""

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


def safe_div(num: float, den: float, default: float = 0.0) -> float:
    """Безопасное деление (защита от ZeroDivisionError)."""
    try:
        if abs(den) < 1e-12:
            return default
        return float(num / den)
    except Exception:
        return default


@dataclass(frozen=True)
class SymbolSpec:
    """
    Спецификация символа для расчета P&L.
    
    Поддерживает две модели расчета:
    1. Линейная модель: pnl = (exit - entry) * contract_size * lot
    2. Тиковая модель: pnl = ticks * tick_value * lot, где ticks = (exit - entry) / tick_size
    """
    # Линейная модель: pnl = (exit-entry) * contract_size * lot
    contract_size: float = 1.0

    # Тиковая модель: pnl = ticks * tick_value * lot
    tick_size: float | None = None
    tick_value: float | None = None

    # Для "пунктов" в метриках/трейлинге (если нужно)
    point_size: float | None = None  # напр., XAUUSD=0.01

    # Fallback множитель на случай, если ничего нет (НЕЖЕЛАТЕЛЕН)
    legacy_multiplier: float | None = None

    # ✅ Комиссии и swap
    commission_rate: float | None = None  # % от объема (например, 0.001 = 0.1%)
    commission_per_lot: float | None = None  # Фиксированная комиссия за лот
    swap_long: float | None = None  # Swap для LONG позиций (за день)
    swap_short: float | None = None  # Swap для SHORT позиций (за день)

    # ✅ Трейлинг после TP1
    trailing_enabled: bool = False  # общий флаг трейлинга по символу
    trailing_after_tp1_enabled: bool = False  # именно "после TP1"
    trailing_tp1_offset_atr: float = 0.0  # множитель ATR
    trailing_profile_default: str = ""  # дефолтный профиль трейлинга (например "rocket_v1")
    trailing_min_lock_r: float = 0.0  # минимальная фиксация в R после TP1 (например 0.25)

    # ✅ Параметры стоп-лосса и RR уровней (для калибровки под волатильность)
    stop_atr_mult: float = 1.0  # множитель ATR для SL (калибруется под шум символа)
    rr_levels: list[float] = field(default_factory=lambda: [1.0, 2.0, 3.0])  # RR уровни для TP1/TP2/TP3

    @property
    def uses_ticks(self) -> bool:
        """Проверяет, можно ли использовать тиковую модель."""
        return (self.tick_size is not None) and (self.tick_value is not None) and self.tick_size > 0

    def pnl_money(self, entry: float, exit: float, lot: float, side: str, symbol: str = None) -> float:
        """
        Расчет P&L в денежных единицах.
        
        Args:
            entry: Цена входа
            exit: Цена выхода
            lot: Размер позиции в лотах (для крипты - количество монет, для Forex - стандартные лоты)
            side: "LONG" или "SHORT"
            symbol: Символ инструмента (опционально, для определения типа инструмента)
        
        Returns:
            P&L в денежных единицах (положительное = прибыль, отрицательное = убыток)
        """
        if lot <= 0:
            return 0.0

        diff = (exit - entry) if side == "LONG" else (entry - exit)

        # ✅ ИСПРАВЛЕНИЕ: Для крипты используем правильную формулу ПЕРЕД тиковой моделью
        # Для крипты: lot = position_size_usd / entry_price (количество монет)
        # PnL = diff * lot = diff * (position_size_usd / entry_price) = (diff / entry_price) * position_size_usd
        # Это эквивалентно: diff * lot (без умножения на contract_size)
        # ✅ ИСПРАВЛЕНИЕ: Используем суффиксы для определения крипты (исключая XAU)
        is_crypto = symbol and symbol.upper().endswith(('USDT', 'USDC', 'BUSD')) and not symbol.upper().startswith('XAU')

        if is_crypto:
            # Для крипты: lot уже пересчитан из position_size_usd, поэтому просто diff * lot
            # Но нужно учесть, что lot = position_size_usd / entry_price
            # Поэтому: diff * lot = diff * (position_size_usd / entry_price) = (diff / entry_price) * position_size_usd
            # Это правильная формула для крипты
            return diff * lot

        if self.uses_ticks:
            ticks = diff / self.tick_size
            return ticks * self.tick_value * lot

        if self.contract_size:
            return diff * self.contract_size * lot

        if self.legacy_multiplier:
            return diff * lot * self.legacy_multiplier

        # Последний fallback (лучше явно сконфигурировать SymbolSpec)
        return diff * lot

    def risk_money(self, entry: float, sl: float, lot: float, side: str, symbol: str = None) -> float:
        """
        Расчет 1R (риск в денежных единицах).
        
        Args:
            entry: Цена входа
            sl: Цена стоп-лосса
            lot: Размер позиции в лотах
            side: "LONG" или "SHORT"
            symbol: Символ (важен для крипты, чтобы выбрать правильную модель расчета)
        
        Returns:
            Абсолютное значение риска (1R) в денежных единицах
        """
        r = self.pnl_money(entry, sl, lot, side, symbol=symbol)
        return abs(r)

    def calculate_fees(
        self,
        entry_price: float,
        exit_price: float,
        lot: float,
        side: str,
        duration_ms: int,
    ) -> float:
        """
        Расчет комиссий для позиции.
        
        Комиссии рассчитываются относительно размера позиции:
        - Для крипты: lot = количество монет, position_size = entry_price * lot * contract_size
        - Для Forex: lot = стандартные лоты, position_size = entry_price * lot * contract_size
        
        Args:
            entry_price: Цена входа
            exit_price: Цена выхода
            lot: Размер позиции в лотах (для крипты - количество монет)
            side: "LONG" или "SHORT"
            duration_ms: Длительность позиции в миллисекундах
        
        Returns:
            Общая сумма комиссий (commission + swap)
        """
        if lot <= 0:
            return 0.0

        total_fees = 0.0

        # 1. Комиссия на вход и выход
        if self.commission_rate is not None and self.commission_rate > 0:
            # Процент от объема позиции
            # Для крипты: entry_price * lot * contract_size = размер позиции в USDT
            # Для Forex: entry_price * lot * contract_size = размер позиции в базовой валюте
            entry_value = abs(entry_price * lot * self.contract_size)
            exit_value = abs(exit_price * lot * self.contract_size)
            # Комиссия = процент от размера позиции (на вход и выход)
            total_fees += entry_value * self.commission_rate
            total_fees += exit_value * self.commission_rate
        elif self.commission_per_lot is not None and self.commission_per_lot > 0:
            # Фиксированная комиссия за лот (вход + выход)
            total_fees += self.commission_per_lot * lot * 2

        # 2. Swap (если позиция держалась более суток)
        duration_days = duration_ms / (1000.0 * 60 * 60 * 24)
        if duration_days >= 1.0:
            swap_rate = None
            if side == "LONG" and self.swap_long is not None:
                swap_rate = self.swap_long
            elif side == "SHORT" and self.swap_short is not None:
                swap_rate = self.swap_short

            if swap_rate is not None:
                position_value = abs(entry_price * lot * self.contract_size)
                swap = position_value * swap_rate * int(duration_days)
                total_fees += abs(swap)

        return total_fees

    def calculate_risk_lot(
        self,
        entry_price: float,
        sl_price: float,
        side: str,
        deposit: float,
        risk_percent: float,
        leverage: float = 1.0,
        lot_step: float = 0.01,
        max_lot: float = 10.0,
    ) -> float:
        """
        Рассчитывает размер позиции на основе риска (универсально для всех инструментов).
        
        Args:
            entry_price: Цена входа
            sl_price: Цена стоп-лосса
            side: "LONG" или "SHORT"
            deposit: Размер депозита в USD
            risk_percent: Процент риска на сделку (например, 10.0 для 10%)
            leverage: Плечо (например, 1000 для 1:1000)
            lot_step: Шаг лота (для округления)
            max_lot: Максимальный лот (для безопасности)
        
        Returns:
            Размер лота с учетом риска
        """
        # Расстояние до SL
        sl_distance = abs(entry_price - sl_price)
        if sl_distance <= 0:
            return lot_step

        # Риск в USD
        risk_usd = deposit * (risk_percent / 100.0)

        # Потеря на 1 лот при срабатывании SL
        # Используем pnl_money для точного расчета
        try:
            loss_per_lot = abs(self.pnl_money(entry_price, sl_price, 1.0, side, symbol=None))  # symbol не нужен для calculate_risk_lot
        except Exception:
            # Fallback: простой расчет
            loss_per_lot = sl_distance * self.contract_size

        if loss_per_lot <= 0:
            return lot_step

        # Размер лота исходя из риска
        lot_risk = risk_usd / loss_per_lot

        # Максимальный лот по марже
        position_value = entry_price * self.contract_size
        lot_max = (deposit * leverage) / position_value if position_value > 0 else max_lot

        # Выбираем минимум
        lot = min(lot_risk, lot_max, max_lot)

        # Округляем до lot_step (вниз)
        lot = (int(lot / lot_step)) * lot_step

        # Минимум lot_step
        lot = max(lot, lot_step)

        return lot


def calculate_position_size(
    symbol: str,
    entry_price: float,
    sl_price: float,
    side: str = "LONG",
    deposit: float = None,
    risk_percent: float = None,
    leverage: float = None,
    lot_step: float = 0.01,
    max_lot: float = 10.0,
    redis_client = None
) -> tuple[float, float, float, float]:
    """
    Универсальная функция расчета размера позиции на основе риска.
    
    Автоматически определяет specs инструмента и рассчитывает lot.
    
    Args:
        symbol: Символ инструмента (XAUUSD, BTCUSDT, etc)
        entry_price: Цена входа
        sl_price: Цена стоп-лосса
        side: "LONG" или "SHORT"
        deposit: Размер депозита (если None, берется из ENV)
        risk_percent: Процент риска (если None, берется из ENV)
        leverage: Плечо (если None, берется из ENV)
        lot_step: Шаг лота
        max_lot: Максимальный лот
        redis_client: Redis клиент (опционально)
    
    Returns:
        tuple: (lot, position_size_usd, deposit, leverage) - для крипты position_size_usd содержит размер в USDT
    """
    import os

    # Defaults из ENV
    if deposit is None:
        deposit = float(os.getenv("ACCOUNT_DEPOSIT_USD", "100"))
    if risk_percent is None:
        risk_percent = float(os.getenv("RISK_PERCENT", "5.0"))
    # Санитарка: если риск задан как доля (<0.5), трактуем как 100*x %
    if 0 < risk_percent < 0.5:
        risk_percent *= 100.0
    if leverage is None:
        leverage = float(os.getenv("ACCOUNT_LEVERAGE", "100"))

    # Максимальная доля маржи от депозита (по умолчанию = risk_percent, можно задать MAX_MARGIN_PERCENT)
    max_margin_percent = float(os.getenv("MAX_MARGIN_PERCENT", str(risk_percent)))
    if 0 < max_margin_percent < 0.5:
        max_margin_percent *= 100.0

    # Получаем spec для символа
    try:
        if redis_client:
            info = get_symbol_info(symbol, redis_client)
        else:
            info = _get_default_symbol_info(symbol)
        spec = spec_from_symbol_info(info)
        # ✅ Если lot_step не передан явно - берем из конфига символа
        if lot_step == 0.01 and "lot_step" in info:
            lot_step = float(info["lot_step"])
    except Exception:
        spec = SymbolSpec()

    # Определяем тип инструмента (крипта или нет)
    is_crypto = symbol.upper().endswith(('USDT', 'USDC', 'BUSD')) and not symbol.upper().startswith('XAU')

    # Риск в USD
    risk_usd = deposit * (risk_percent / 100.0)

    # Для крипты работаем с фьючерсами: считаем номинал, возвращаем маржу
    if is_crypto:
        # Расстояние до SL в пунктах цены
        sl_distance = abs(entry_price - sl_price)
        if sl_distance <= 0:
            return lot_step, risk_usd, deposit, leverage

        # Номинал, чтобы риск по SL был <= risk_usd
        # notional = risk_usd * (entry_price / sl_distance)
        notional_usd = risk_usd * (entry_price / sl_distance)

        # Ограничения по депозиту и плечу
        max_notional_by_margin = deposit * leverage  # весь депозит под плечо
        max_notional_by_risk_leverage = risk_usd * leverage  # риск * плечо
        max_notional_by_margin_cap = deposit * (max_margin_percent / 100.0) * leverage  # потолок по марже
        notional_usd = min(notional_usd, max_notional_by_margin, max_notional_by_risk_leverage, max_notional_by_margin_cap)

        # Маржа = номинал / плечо — то, что реально резервируется
        position_size_usd = notional_usd / leverage if leverage > 1 else notional_usd
        # Кэп маржи на max_margin_percent от депозита
        margin_cap = deposit * (max_margin_percent / 100.0)
        position_size_usd = min(position_size_usd, margin_cap)
        # Пересчитаем номинал после кэпа маржи
        notional_usd = min(notional_usd, margin_cap * leverage)

        # Lot для крипты = номинал / entry_price (количество монет)
        lot = notional_usd / entry_price if entry_price > 0 else lot_step

        # Округляем lot
        lot = (int(lot / lot_step)) * lot_step
        lot = max(lot, lot_step)

        return lot, position_size_usd, deposit, leverage

    # Для остальных инструментов (XAUUSD, Forex)
    lot = spec.calculate_risk_lot(
        entry_price=entry_price,
        sl_price=sl_price,
        side=side,
        deposit=deposit,
        risk_percent=risk_percent,
        leverage=leverage,
        lot_step=lot_step,
        max_lot=max_lot,
    )

    # position_size_usd для не-крипты = lot * entry_price * contract_size
    position_size_usd = lot * entry_price * spec.contract_size

    return lot, position_size_usd, deposit, leverage


def spec_from_symbol_info(info: Mapping[str, Any]) -> SymbolSpec:
    """
    Адаптер для создания SymbolSpec из словаря с информацией о символе.
    
    Поддерживает разные naming-схемы для совместимости с различными источниками данных.
    
    Args:
        info: Словарь с информацией о символе (может содержать различные варианты имен полей)
    
    Returns:
        SymbolSpec с заполненными полями
    """
    # Тики
    tick_size = _to_float(
        info.get("tick_size") or
        info.get("tickSize") or
        info.get("ticks_size") or
        info.get("point")  # point часто используется как tick_size
    )
    tick_value = _to_float(
        info.get("tick_value") or
        info.get("tickValue") or
        info.get("ticks_value") or
        info.get("tick_value_per_lot") or
        info.get("pip_value")  # pip_value может быть эквивалентом tick_value
    )

    # Линейная модель
    contract_size = _to_float(
        info.get("contract_size") or
        info.get("contractSize") or
        info.get("multiplier") or
        1.0
    )

    point_size = _to_float(
        info.get("point_size") or
        info.get("pointSize") or
        info.get("point") or
        info.get("pip_size")
    )

    legacy_multiplier = _to_float(
        info.get("legacy_multiplier") or
        info.get("pnl_multiplier")
    )

    # ✅ Комиссии
    commission_rate = _to_float(
        info.get("commission_rate") or
        info.get("commissionRate") or
        info.get("fee_rate")
    )
    commission_per_lot = _to_float(
        info.get("commission_per_lot") or
        info.get("commissionPerLot") or
        info.get("commission")
    )
    swap_long = _to_float(
        info.get("swap_long") or
        info.get("swapLong") or
        info.get("swap_buy")
    )
    swap_short = _to_float(
        info.get("swap_short") or
        info.get("swapShort") or
        info.get("swap_sell")
    )

    # ✅ Трейлинг конфиг (из Redis symbol_specs или прямые поля)
    trailing_cfg = info.get("trailing", {}) or {}
    if not isinstance(trailing_cfg, dict):
        trailing_cfg = {}

    trailing_enabled = bool(
        trailing_cfg.get("enabled", info.get("trailing_enabled", False))
    )
    trailing_after_tp1_enabled = bool(
        trailing_cfg.get("after_tp1_enabled", info.get("trailing_after_tp1_enabled", False))
    )
    trailing_tp1_offset_atr = float(
        trailing_cfg.get("tp1_offset_atr", info.get("trailing_tp1_offset_atr", 0.0) or 0.0)
    )
    trailing_profile_default = str(
        trailing_cfg.get("profile_default", info.get("trailing_profile_default", "") or "")
    )
    trailing_min_lock_r = float(
        trailing_cfg.get("min_lock_r", info.get("trailing_min_lock_r", 0.0) or 0.0)
    )

    # ✅ Параметры стоп-лосса и RR уровней
    stop_atr_mult = float(
        trailing_cfg.get("stop_atr_mult", info.get("stop_atr_mult", 1.0) or 1.0)
    )
    rr_levels_raw = trailing_cfg.get("rr_levels", info.get("rr_levels", [1.0, 2.0, 3.0]))
    if isinstance(rr_levels_raw, list):
        rr_levels = [float(x) for x in rr_levels_raw if isinstance(x, (int, float))]
    elif isinstance(rr_levels_raw, str):
        # Парсим строку типа "1.0,2.0,3.0"
        try:
            rr_levels = [float(x.strip()) for x in rr_levels_raw.split(",") if x.strip()]
        except Exception:
            rr_levels = [1.0, 2.0, 3.0]
    else:
        rr_levels = [1.0, 2.0, 3.0]

    # Если tick_size есть, но tick_value нет — не включаем тиковую модель
    if not tick_size or not tick_value:
        tick_size, tick_value = None, None

    return SymbolSpec(
        contract_size=contract_size or 1.0,
        tick_size=tick_size,
        tick_value=tick_value,
        point_size=point_size,
        legacy_multiplier=legacy_multiplier,
        commission_rate=commission_rate,
        commission_per_lot=commission_per_lot,
        swap_long=swap_long,
        swap_short=swap_short,
        trailing_enabled=trailing_enabled,
        trailing_after_tp1_enabled=trailing_after_tp1_enabled,
        trailing_tp1_offset_atr=trailing_tp1_offset_atr,
        trailing_profile_default=trailing_profile_default,
        trailing_min_lock_r=trailing_min_lock_r,
        stop_atr_mult=stop_atr_mult,
        rr_levels=rr_levels,
    )


def _to_float(x: Any) -> float | None:
    """Безопасное преобразование в float."""
    try:
        if x is None:
            return None
        v = float(x)
        return v
    except Exception:
        return None


def _merge_trailing_cfg(base_info: dict, symbol: str, redis_client) -> None:
    """
    Подмешивает trailing-конфигурацию из symbol:trailing_cfg:{symbol} в base_info.

    Модифицирует base_info in-place.
    """
    try:
        key = f"symbol:trailing_cfg:{symbol.upper()}"
        trailing_cfg = redis_client.hgetall(key) or {}
    except Exception:
        trailing_cfg = {}

    if trailing_cfg:
        # Пример: кладём в отдельный блок
        base_info.setdefault("trailing_cfg", {})
        base_info["trailing_cfg"].update(trailing_cfg)

        # Можно сразу прокинуть как верхнеуровневые поля:
        if "tp1_offset_atr" in trailing_cfg:
            base_info["trailing_tp1_offset_atr"] = float(trailing_cfg["tp1_offset_atr"])
        if "stop_atr_mult" in trailing_cfg:
            base_info["stop_atr_mult"] = float(trailing_cfg["stop_atr_mult"])
        if "trailing_after_tp1_enabled" in trailing_cfg:
            base_info["trailing_after_tp1_enabled"] = trailing_cfg["trailing_after_tp1_enabled"].lower() == "true"


def get_symbol_info(symbol: str, redis_client=None) -> dict:
    """
    Получить информацию о символе из Redis или вернуть defaults.
    
    Args:
        symbol: Торговый символ (например, "XAUUSD", "BTCUSDT")
        redis_client: Опциональный Redis клиент (если None, будет создан новый)
    
    Returns:
        Словарь с информацией о символе для использования в spec_from_symbol_info()
    """
    import json

    # Пытаемся получить из Redis
    if redis_client is None:
        try:
            from core.redis_client import get_redis
            redis_client = get_redis()
        except Exception:
            redis_client = None

    if redis_client:
        try:
            key = f"symbol_specs:{symbol}"
            raw = redis_client.get(key)
            if raw:
                if isinstance(raw, bytes):
                    raw = raw.decode('utf-8')
                data = json.loads(raw)
                # 🔧 ДОБАВКА: перекрываем trailing-настройки из symbol:trailing_cfg:{symbol}
                _merge_trailing_cfg(data, symbol, redis_client)
                return data
        except Exception:
            pass

    # Fallback: defaults на основе символа
    defaults = _get_default_symbol_info(symbol)
    # 🔧 ДОБАВКА: пробуем добавить trailing cfg даже для defaults
    if redis_client:
        _merge_trailing_cfg(defaults, symbol, redis_client)
    return defaults


def _get_default_symbol_info(symbol: str) -> dict:
    """
    Возвращает дефолтные значения для символа.
    
    Приоритет комиссий:
    1. Из SymbolSpec (если установлено)
    2. Из ENV переменных (если установлено)
    3. Дефолтные значения для типа инструмента
    """
    symbol_upper = symbol.upper()

    # ✅ Читаем ENV конфигурацию для комиссий
    def _env_float(key: str, default: float | None = None) -> float | None:
        val = os.getenv(key)
        if val is None:
            return default
        try:
            return float(val)
        except Exception:
            return default

    # XAUUSD (золото)
    if symbol_upper == "XAUUSD" or symbol_upper.startswith("XAU"):
        return {
            "point": 0.01,
            "tick_value_per_lot": 1.0,  # $1 за 0.01 на 1 lot
            "contract_size": 100.0,
            "tick_size": 0.01,
            "tick_value": 1.0,
            # ✅ Комиссии (ENV или defaults)
            "commission_per_lot": _env_float("FOREX_COMMISSION_PER_LOT", 7.0),
            "commission_rate": _env_float("FOREX_COMMISSION_RATE"),
            "swap_long": _env_float("FOREX_SWAP_LONG", -0.0001),
            "swap_short": _env_float("FOREX_SWAP_SHORT", 0.00005),
        }

    # Криптовалюты (BTCUSDT, ETHUSDT и т.д.)
    if "BTC" in symbol_upper or "ETH" in symbol_upper or "USDT" in symbol_upper:
        # ✅ Для BTC используем шаг 0.001 (Binance standard), для остальных 0.01
        lot_step = 0.001 if "BTC" in symbol_upper else 0.01

        return {
            "point": 1e-8,
            "tick_value_per_lot": 1.0,  # Для крипты обычно 1:1 (если лот = монеты)
            "contract_size": 1.0,
            "tick_size": 1e-8,
            "tick_value": 1.0,
            "lot_step": lot_step,
            # ✅ Комиссии (ENV или defaults)
            # Binance Futures taker ~0.04% (0.0004), maker ~0.02%.
            # We use 0.0003 (0.05%) as the global baseline.
            "commission_rate": _env_float("CRYPTO_COMMISSION_RATE", 0.0003),
            "commission_per_lot": _env_float("CRYPTO_COMMISSION_PER_LOT"),
            "swap_long": _env_float("CRYPTO_SWAP_LONG", 0.0),
            "swap_short": _env_float("CRYPTO_SWAP_SHORT", 0.0),
            # ✅ Rocket v1: дефолтные настройки трейлинга для крипты
            "trailing_profile_default": "rocket_v1",
            "trailing_after_tp1_enabled": True,
            "trailing_tp1_offset_atr": 0.6,
            "trailing_min_lock_r": 0.25,
        }

    # Общие defaults
    return {
        "point": 0.01,
        "tick_value_per_lot": 1.0,
        "contract_size": 1.0,
        "tick_size": 0.01,
        "tick_value": 1.0,
        # ✅ Комиссии (ENV или defaults)
        "commission_rate": _env_float("DEFAULT_COMMISSION_RATE", 0.0003),  # 0.05%
        "commission_per_lot": _env_float("DEFAULT_COMMISSION_PER_LOT"),
        "swap_long": _env_float("DEFAULT_SWAP_LONG", 0.0),
        "swap_short": _env_float("DEFAULT_SWAP_SHORT", 0.0),
    }

