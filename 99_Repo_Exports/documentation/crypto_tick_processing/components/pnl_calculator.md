# PNL Calculator - Детальная документация

## Обзор

**PNL Calculator** - комплексный модуль для расчета прибыли/убытков, размеров позиций и комиссий с учетом спецификаций различных торговых инструментов. Поддерживает как традиционные рынки (Forex, CFD), так и криптовалюты с их специфическими моделями ценообразования.

**Расположение**: `python-worker/services/pnl_math.py`

**Назначение**: Обеспечение точных и一致ных расчетов P&L, риск-менеджмента и комиссий для различных типов финансовых инструментов.

## Архитектурные принципы

### 1. Multi-Asset Support
- **Forex**: Contract-based модель (100,000 единиц базовой валюты)
- **Crypto**: Quantity-based модель (количество монет)
- **Commodities**: Tick-based модель (тик-цена и тик-значение)
- **CFD/Indices**: Contract multiplier модель

### 2. Precision-First Design
- **Floating-point safety**: Защита от precision loss
- **Zero-division protection**: Safe division с defaults
- **Boundary validation**: Проверка входных данных

### 3. Extensible Specification System
- **SymbolSpec dataclass**: Структурированные спецификации
- **Dynamic loading**: Загрузка из Redis/external sources
- **Fallback mechanisms**: Graceful degradation

## Детальная структура

### SymbolSpec - Спецификация символа

**Основная структура данных для описания торгового инструмента:**

```python
@dataclass(frozen=True)
class SymbolSpec:
    # Линейная модель: pnl = (exit-entry) * contract_size * lot
    contract_size: float = 1.0

    # Тиковая модель: pnl = ticks * tick_value * lot
    tick_size: Optional[float] = None
    tick_value: Optional[float] = None

    # Для пунктов в метриках/трейлинге
    point_size: Optional[float] = None

    # Fallback множитель (нежелателен)
    legacy_multiplier: Optional[float] = None

    # Комиссии и swap
    commission_rate: Optional[float] = None      # % от объема
    commission_per_lot: Optional[float] = None   # Фиксированная комиссия
    swap_long: Optional[float] = None           # Swap для LONG
    swap_short: Optional[float] = None          # Swap для SHORT

    # Трейлинг после TP1
    trailing_enabled: bool = False
    trailing_after_tp1_enabled: bool = False
    trailing_tp1_offset_atr: float = 0.0
    trailing_profile_default: str = ""
    trailing_min_lock_r: float = 0.0

    # Параметры стоп-лосса и RR уровней
    stop_atr_mult: float = 1.0
    rr_levels: List[float] = field(default_factory=lambda: [1.0, 2.0, 3.0])
```

#### Свойства и методы

```python
@property
def uses_ticks(self) -> bool:
    """Проверяет возможность использования тиковой модели."""
    return (self.tick_size is not None) and (self.tick_value is not None) and self.tick_size > 0
```

## Детальная логика методов

### Расчет P&L (pnl_money)

**Основной метод расчета прибыли/убытков:**

```python
def pnl_money(self, entry: float, exit: float, lot: float, side: str, symbol: str = None) -> float:
    """
    Расчет P&L в денежных единицах с учетом типа инструмента.
    """

    if lot <= 0:
        return 0.0

    # Расчет разницы цен
    diff = (exit - entry) if side == "LONG" else (entry - exit)

    # Определение типа инструмента
    is_crypto = symbol and symbol.upper().endswith(('USDT', 'USDC', 'BUSD')) and not symbol.upper().startswith('XAU')

    if is_crypto:
        # Для крипты: lot = position_size_usd / entry_price
        # P&L = diff * lot = (diff / entry_price) * position_size_usd
        return diff * lot

    # Для традиционных инструментов
    if self.uses_ticks:
        # Тиковая модель
        ticks = diff / self.tick_size
        return ticks * self.tick_value * lot
    else:
        # Линейная модель
        return diff * self.contract_size * lot
```

#### Специфика расчетов по типам инструментов

**Криптовалюты (BTCUSDT, ETHUSDT):**
```
lot = position_size_usd / entry_price  # Количество монет
P&L = (exit - entry) * lot для LONG
P&L = (entry - exit) * lot для SHORT
```

**Forex (EURUSD, GBPJPY):**
```
contract_size = 100,000 (стандартный лот)
P&L = (exit - entry) * contract_size * lot
```

**Commodities с тиковой моделью (XAUUSD):**
```
tick_size = 0.01 (0.01 USD)
tick_value = 0.01 (0.01 USD за тик)
ticks = (exit - entry) / tick_size
P&L = ticks * tick_value * lot
```

### Расчет комиссий (calculate_fees)

```python
def calculate_fees(
    self,
    position_size: float,
    entry_price: float,
    exit_price: float,
    lot: float,
    symbol: str,
    holding_days: float = 0.0
) -> Dict[str, float]:
    """
    Расчет всех типов комиссий и сборов.
    """

    fees = {
        "commission": 0.0,      # Торговые комиссии
        "swap": 0.0,           # Swap за удержание
        "spread": 0.0,         # Spread (опционально)
        "total": 0.0           # Итого
    }

    # 1. Торговые комиссии
    if self.commission_rate:
        # Процент от объема
        volume = position_size * 2  # entry + exit
        fees["commission"] = volume * self.commission_rate

    elif self.commission_per_lot:
        # Фиксированная комиссия за лот
        fees["commission"] = self.commission_per_lot * lot * 2  # entry + exit

    # 2. Swap за удержание позиции
    if holding_days > 0:
        swap_rate = self.swap_long if side == "LONG" else self.swap_short
        if swap_rate:
            fees["swap"] = abs(swap_rate) * holding_days * lot

    # 3. Spread (опционально)
    if self.point_size:
        spread_points = 2  # entry + exit spread
        fees["spread"] = spread_points * self.point_size * lot

    # Итого
    fees["total"] = sum(fees.values())

    return fees
```

### Расчет размера позиции (calculate_position_size)

**Универсальная функция расчета позиции на основе риска:**

```python
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
    redis_client = None,
) -> tuple[float, float, float, float]:
```

#### Этапы расчета:

1. **Загрузка параметров**
   ```python
   deposit = deposit or float(os.getenv("ACCOUNT_DEPOSIT_USD", "100"))
   risk_percent = risk_percent or float(os.getenv("RISK_PERCENT", "5.0"))
   leverage = leverage or float(os.getenv("ACCOUNT_LEVERAGE", "100"))
   ```

2. **Расчет риска в деньгах**
   ```python
   risk_amount = deposit * (risk_percent / 100.0)
   ```

3. **Расчет стоп-расстояния**
   ```python
   sl_distance = abs(entry_price - sl_price)
   ```

4. **Расчет размера позиции**
   ```python
   # Для крипты
   if symbol.endswith(('USDT', 'USDC', 'BUSD')):
       # position_size_usd = risk_amount / (sl_distance / entry_price)
       # lot = position_size_usd / entry_price
       position_size_usd = risk_amount / (sl_distance / entry_price)
       lot = position_size_usd / entry_price

   # Для Forex
   else:
       # pip_value = (contract_size / entry_price) * pip_size
       # lot = risk_amount / (sl_distance * pip_value)
       pip_value = 0.0001 if symbol.endswith('JPY') else 0.0001 * 10
       lot = risk_amount / (sl_distance * pip_value)
   ```

5. **Применение ограничений**
   ```python
   # Проверка маржи
   margin_required = position_size_usd / leverage
   if margin_required > deposit * (max_margin_percent / 100.0):
       lot *= 0.8  # Уменьшение позиции

   # Округление до шага лота
   lot = round(lot / lot_step) * lot_step
   lot = min(lot, max_lot)  # Ограничение максимума
   ```

6. **Пересчет финальных значений**
   ```python
   position_size_usd = lot * entry_price  # Для крипты
   return lot, position_size_usd, deposit, leverage
   ```

## Модели ценообразования

### Линейная модель (Linear Model)

**Принцип:** P&L = (exit - entry) × contract_size × lot

**Примеры:**
- **Forex EURUSD**: contract_size = 100,000, lot = 0.01
- **CFD на индексы**: contract_size = 1, lot = 10

**Формула:**
```
P&L = (exit - entry) × contract_size × lot
```

### Тиковая модель (Tick Model)

**Принцип:** P&L = ticks × tick_value × lot, где ticks = (exit - entry) / tick_size

**Примеры:**
- **XAUUSD**: tick_size = 0.01, tick_value = 0.01
- **WTI Oil**: tick_size = 0.01, tick_value = 1.0

**Формула:**
```
ticks = (exit - entry) / tick_size
P&L = ticks × tick_value × lot
```

### Крипто модель (Crypto Model)

**Принцип:** P&L = (exit - entry) × lot, где lot = position_size_usd / entry_price

**Примеры:**
- **BTCUSDT**: lot = 0.001 (0.001 BTC), entry = 50,000
- **ETHUSDT**: lot = 0.01 (0.01 ETH), entry = 3,000

**Формула:**
```
position_size_usd = lot × entry_price
P&L = (exit - entry) × lot для LONG
```

## Спецификации символов

### Получение спецификации (spec_from_symbol_info)

```python
def spec_from_symbol_info(symbol: str, redis_client = None) -> SymbolSpec:
    """
    Получение спецификации символа из различных источников.
    """

    # 1. Попытка из Redis хеша symbol:specs:{symbol}
    if redis_client:
        spec_data = redis_client.hgetall(f"symbol:specs:{symbol}")
        if spec_data:
            return SymbolSpec(**spec_data)

    # 2. Хардкод для известных символов
    specs = {
        "BTCUSDT": SymbolSpec(
            contract_size=1.0,
            commission_rate=0.001,  # 0.1%
            trailing_enabled=True,
            stop_atr_mult=1.0
        ),
        "EURUSD": SymbolSpec(
            contract_size=100000.0,
            tick_size=0.00001,
            tick_value=1.0,
            commission_rate=0.00002,  # 2 pips
            swap_long=-0.5,
            swap_short=0.2
        ),
        "XAUUSD": SymbolSpec(
            contract_size=100.0,  # 100 oz
            tick_size=0.01,
            tick_value=0.01,
            commission_rate=0.0005
        )
    }

    return specs.get(symbol.upper(), SymbolSpec())
```

## Конфигурационные параметры

### Переменные окружения

**Риск-менеджмент:**
- `ACCOUNT_DEPOSIT_USD`: Размер депозита (default: 100)
- `RISK_PERCENT`: Процент риска на сделку (default: 5.0)
- `ACCOUNT_LEVERAGE`: Плечо (default: 100)
- `MAX_MARGIN_PERCENT`: Максимальная доля маржи (default: risk_percent)

**Размеры позиций:**
- `LOT_STEP`: Шаг лота (default: 0.01)
- `MAX_LOT`: Максимальный лот (default: 10.0)

### Структура конфигурации

```python
# Пример полной спецификации для BTCUSDT
btc_spec = SymbolSpec(
    contract_size=1.0,
    commission_rate=0.001,  # 0.1%
    commission_per_lot=0.0,
    swap_long=0.0,
    swap_short=0.0,
    trailing_enabled=True,
    trailing_after_tp1_enabled=True,
    trailing_tp1_offset_atr=0.5,
    trailing_profile_default="rocket_v1",
    trailing_min_lock_r=0.25,
    stop_atr_mult=1.0,
    rr_levels=[1.0, 2.0, 3.0]
)
```

## Производительность и оптимизации

### Оптимизации расчетов

1. **Vectorized operations**: Для batch расчетов
2. **Cached specifications**: Кеш спецификаций символов
3. **Lazy loading**: Загрузка спецификаций по требованию

### Precision handling

```python
def safe_round(value: float, decimals: int = 8) -> float:
    """Безопасное округление с учетом floating-point precision."""
    return round(float(value), decimals)
```

### Memory optimization

- **Immutable specs**: SymbolSpec как frozen dataclass
- **Shared objects**: Переиспользование спецификаций
- **Minimal state**: Только необходимые поля

## Мониторинг и метрики

### Метрики расчетов

```python
def log_pnl_calculation(self, symbol: str, entry: float, exit: float,
                       lot: float, pnl: float, side: str) -> None:
    """Логирование расчетов P&L для анализа."""

    metrics = {
        "symbol": symbol,
        "entry": entry,
        "exit": exit,
        "lot": lot,
        "pnl": pnl,
        "side": side,
        "pnl_percent": (pnl / (entry * lot)) * 100 if entry > 0 else 0,
        "timestamp": time.time()
    }

    # Отправка в метрики
    self._send_metrics("pnl_calculation", metrics)
```

### Валидация расчетов

```python
def validate_pnl_calculation(self, expected_pnl: float, calculated_pnl: float,
                           tolerance: float = 0.01) -> bool:
    """Валидация корректности расчетов P&L."""

    diff = abs(expected_pnl - calculated_pnl)
    if diff > tolerance:
        self.logger.warning(f"P&L validation failed: expected={expected_pnl}, calculated={calculated_pnl}, diff={diff}")
        return False

    return True
```

## Обработка ошибок

### Fail-Safe механизмы

1. **Invalid inputs**: Возврат 0.0 с логированием
2. **Division by zero**: Safe division с defaults
3. **Missing specs**: Fallback на стандартные значения
4. **Precision errors**: Округление и валидация

### Error recovery

```python
def safe_pnl_calculation(self, *args, **kwargs) -> float:
    """Безопасный расчет P&L с обработкой исключений."""

    try:
        return self.pnl_money(*args, **kwargs)
    except Exception as e:
        self.logger.error(f"P&L calculation error: {e}")
        # Fallback: простая формула
        entry, exit, lot, side = args[:4]
        diff = (exit - entry) if side == "LONG" else (entry - exit)
        return diff * lot  # Простейший fallback
```

## Типичные проблемы и решения

### Проблема: Некорректный P&L для крипты
**Симптомы**: P&L не соответствует ожиданию для crypto символов
**Решения**:
- Проверить определение типа инструмента (USDT suffix)
- Убедиться что lot = position_size_usd / entry_price
- Проверить формулу: P&L = (exit - entry) * lot

### Проблема: Неправильные комиссии
**Симптомы**: Комиссии рассчитываются неверно
**Решения**:
- Проверить commission_rate vs commission_per_lot
- Убедиться в правильном объеме (entry + exit)
- Проверить тип комиссии (% vs фиксированная)

### Проблема: Размер позиции превышает лимиты
**Симптомы**: Lot > max_lot или margin > deposit
**Решения**:
- Проверить расчет маржи: position_size_usd / leverage
- Убедиться в правильном max_margin_percent
- Добавить дополнительные проверки и округление

### Проблема: Precision loss в расчетах
**Симптомы**: Маленькие ошибки накапливаются
**Решения**:
- Использовать Decimal для точных расчетов
- Округлять промежуточные результаты
- Добавить tolerance в сравнениях

## Интеграция с другими компонентами

### TradeMonitorService

```python
# Расчет P&L при закрытии позиции
spec = spec_from_symbol_info(position.symbol)
pnl = spec.pnl_money(position.entry_price, close_price, position.lot, position.side, position.symbol)

# Расчет комиссий
fees = spec.calculate_fees(position_size, position.entry_price, close_price, position.lot, position.symbol, holding_days)
net_pnl = pnl - fees['total']
```

### ReportingService

```python
# Агрегация P&L по стратегиям
total_pnl = sum(
    spec.pnl_money(trade.entry, trade.exit, trade.lot, trade.side, trade.symbol)
    for trade in trades
)

# Расчет метрик эффективности
win_rate = len([t for t in trades if t.pnl > 0]) / len(trades)
avg_win = sum(t.pnl for t in trades if t.pnl > 0) / len([t for t in trades if t.pnl > 0])
avg_loss = sum(t.pnl for t in trades if t.pnl < 0) / len([t for t in trades if t.pnl < 0])
profit_factor = abs(sum(t.pnl for t in trades if t.pnl > 0) / sum(t.pnl for t in trades if t.pnl < 0))
```

## Заключение

PNL Calculator предоставляет точную и универсальную систему расчетов для различных типов финансовых инструментов. Его модульная архитектура и поддержка множественных моделей ценообразования обеспечивают корректность расчетов в сложных торговых системах.
