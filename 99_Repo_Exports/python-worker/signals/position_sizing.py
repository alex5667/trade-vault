"""
Position Sizing - расчет размера позиции на основе риска и ATR.

ФУНКЦИОНАЛ:
- Автоматический расчет лота от депозита
- Учет плеча (leverage)
- Расчет риска на основе ATR
- Ограничение по марже

ИСПОЛЬЗОВАНИЕ:
- lot = suggest_lot(price=1880.0, atr=2.5)
"""

import os


# Параметры счета (из переменных окружения)
DEPOSIT = float(os.getenv("ACCOUNT_DEPOSIT_USD", "100"))
LEVERAGE = float(os.getenv("ACCOUNT_LEVERAGE", "100"))     # 1:100
RISK_PCT = float(os.getenv("RISK_PERCENT", "5.0"))          # 5% по умолчанию
CONTRACT = float(os.getenv("XAU_CONTRACT_SIZE", "100"))     # 100 oz = $100 на 1$ движения
LOT_STEP = float(os.getenv("XAU_LOT_STEP", "0.01"))


def suggest_lot(price: float, atr: float, deposit: float = None, 
                risk_pct: float = None, leverage: float = None) -> float:
    """
    Рассчитывает оптимальный размер лота на основе риска и ATR.
    
    Логика:
    1. Определяем допустимый риск в USD (депозит * risk%)
    2. Вычисляем потенциальную потерю на 1 лот при движении на 1 ATR
    3. Определяем размер лота исходя из риска
    4. Проверяем ограничения по марже
    5. Округляем до lot_step
    
    Args:
        price: Текущая цена инструмента
        atr: Значение ATR для расчета риска
        deposit: Размер депозита (если None, берется из env)
        risk_pct: Процент риска на сделку (если None, берется из env)
        leverage: Плечо (если None, берется из env)
        
    Returns:
        Размер лота (округленный до lot_step)
    """
    # Используем параметры из env, если не переданы явно
    _deposit = deposit if deposit is not None else DEPOSIT
    _risk_pct = risk_pct if risk_pct is not None else RISK_PCT
    _leverage = leverage if leverage is not None else LEVERAGE
    
    # Минимальный ATR для избежания деления на ноль
    if atr <= 0:
        atr = 1.0
    
    # Риск в USD
    risk_usd = _deposit * (_risk_pct / 100.0)
    
    # Потеря на 1 лот при движении на 1 ATR
    # Для : 1 лот = 100 oz, 1$ движения = 100$ P/L
    loss_per_lot = atr * CONTRACT
    
    # Новая логика (Fixed Notional): Размер лота зависит только от размера Депозита, Риска и Плеча.
    # Notional = Deposit * Risk% * Leverage
    target_notional = _deposit * (_risk_pct / 100.0) * _leverage
    
    # lot * price * CONTRACT = Notional
    lot_notional_based = target_notional / (price * CONTRACT) if price > 0 else 0
    
    # Максимальный лот по марже (не превышаем доступную маржу)
    # По новой логике, наша целевая позиция и так равна максимально допустимой позиции по размеру риск-капитала и плеча.
    lot_max = (_deposit * _leverage) / (price * CONTRACT) if price > 0 else 0
    
    lot = min(lot_notional_based, lot_max)
    
    # Округляем до lot_step (вниз)
    lot = (int(lot / LOT_STEP)) * LOT_STEP
    
    # Минимум lot_step
    lot = max(lot, LOT_STEP)
    
    return lot


def calculate_risk_usd(lot: float, atr: float) -> float:
    """
    Рассчитывает риск в USD для заданного лота и ATR.
    
    Args:
        lot: Размер лота
        atr: Значение ATR
        
    Returns:
        Риск в USD
    """
    return lot * atr * CONTRACT


def calculate_margin_required(lot: float, price: float, leverage: float = None) -> float:
    """
    Рассчитывает требуемую маржу для позиции.
    
    Args:
        lot: Размер лота
        price: Цена инструмента
        leverage: Плечо (если None, берется из env)
        
    Returns:
        Требуемая маржа в USD
    """
    _leverage = leverage if leverage is not None else LEVERAGE
    return (lot * price * CONTRACT) / _leverage


def validate_lot(lot: float, price: float, deposit: float = None, leverage: float = None) -> dict:
    """
    Проверяет корректность размера лота и возвращает детали.
    
    Args:
        lot: Размер лота для проверки
        price: Цена инструмента
        deposit: Размер депозита (если None, берется из env)
        leverage: Плечо (если None, берется из env)
        
    Returns:
        Словарь с информацией о валидации
    """
    _deposit = deposit if deposit is not None else DEPOSIT
    _leverage = leverage if leverage is not None else LEVERAGE
    
    margin_required = calculate_margin_required(lot, price, _leverage)
    margin_available = _deposit
    
    is_valid = margin_required <= margin_available
    margin_level = (margin_available / margin_required * 100) if margin_required > 0 else 0
    
    return {
        "is_valid": is_valid,
        "lot": lot,
        "margin_required": margin_required,
        "margin_available": margin_available,
        "margin_level": margin_level,
        "free_margin": margin_available - margin_required
    }

