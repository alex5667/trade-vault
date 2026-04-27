# 📖 Объяснение: Как работает `_get_symbol_specs()`

## 🎯 Назначение

Метод `_get_symbol_specs()` возвращает **спецификацию торгового инструмента** (SymbolSpecs) - параметры, необходимые для корректной работы с конкретным символом (BTCUSD, ETHUSD, XAUUSD и т.д.).

## 📋 Архитектура

```
BaseOrderFlowHandler (абстрактный класс)
    ↓
    _get_symbol_specs() - абстрактный метод (должен быть переопределен)
    ↓
CryptoOrderFlowHandler (реализация для криптовалют)
    ↓
    _get_symbol_specs() - конкретная реализация
```

## 🔍 Как это работает

### 1. Вызов метода

Метод вызывается при инициализации обработчика:

```python
class BaseOrderFlowHandler:
    def __init__(self, symbol: str, config: Optional[OrderFlowConfig] = None):
        self.symbol = symbol
        self.config = config or get_config(symbol, use_env=True)
        self.specs = self._get_symbol_specs()  # ← Вызов здесь
```

### 2. Реализация в CryptoOrderFlowHandler

```python
def _get_symbol_specs(self) -> SymbolSpecs:
    """Возвращает спецификацию для криптовалюты"""
    try:
        # Попытка получить из централизованного реестра
        return get_specs(self.symbol)
    except ValueError:
        # Если символ не найден - используем generic fallback
        return self._get_generic_crypto_specs()
```

### 3. Функция `get_specs()` из instrument_config.py

```python
def get_specs(symbol: str) -> SymbolSpecs:
    """
    Получает спецификацию для указанного символа.

    Ищет в централизованном реестре INSTRUMENT_SPECS.
    """
    if symbol in INSTRUMENT_SPECS:
        return INSTRUMENT_SPECS[symbol]

    raise ValueError(f"Unknown symbol: {symbol}")
```

### 4. Централизованный реестр

В `instrument_config.py` есть словарь со всеми зарегистрированными инструментами:

```python
INSTRUMENT_SPECS: Dict[str, SymbolSpecs] = {
    "XAUUSD": XAUUSD_SPECS,
    "BTCUSD": BTCUSD_SPECS,
    "BTCUSDT": BTCUSD_SPECS,  # Алиас
    "ETHUSD": ETHUSD_SPECS,
    "ETHUSDT": ETHUSD_SPECS,  # Алиас
    "BNBUSD": BNBUSD_SPECS,
    "BNBUSDT": BNBUSD_SPECS,  # Алиас
}
```

### 5. Fallback для неизвестных символов

Если символ не найден в реестре, используется generic спецификация:

```python
def _get_generic_crypto_specs(self) -> SymbolSpecs:
    """Generic спецификация для криптовалют (fallback)"""
    return SymbolSpecs(
        symbol=self.symbol,
        contract_size=1.0,
        pip_value=0.01,
        lot_step=0.001,
        min_lot=0.001,
        max_lot=1000.0,
        tick_value=0.01,
        point_value=0.01,
        price_decimals=2,   # Большинство крипты: 2 знака
        volume_decimals=3   # Объем до 3 знаков (0.001 BTC)
    )
```

## 📊 Что содержит SymbolSpecs?

```python
@dataclass
class SymbolSpecs:
    symbol: str              # Символ (BTCUSD, ETHUSD и т.д.)
    contract_size: float     # Размер контракта
    pip_value: float         # Стоимость пипса
    lot_step: float          # Шаг лота (0.01, 0.001 и т.д.)
    min_lot: float           # Минимальный лот
    max_lot: float           # Максимальный лот
    tick_value: float        # Минимальное изменение цены
    point_value: float       # Значение пойнта
    price_decimals: int      # Количество знаков после запятой для цены
    volume_decimals: int     # Количество знаков после запятой для объема
```

## 🔄 Пример работы

### Пример 1: BTCUSD (найден в реестре)

```python
handler = CryptoOrderFlowHandler("BTCUSD")

# 1. Вызывается _get_symbol_specs()
# 2. Вызывается get_specs("BTCUSD")
# 3. Находится в INSTRUMENT_SPECS["BTCUSD"]
# 4. Возвращается BTCUSD_SPECS:

SymbolSpecs(
    symbol="BTCUSD",
    contract_size=1.0,
    pip_value=0.01,
    lot_step=0.01,
    min_lot=0.01,
    max_lot=1000.0,
    tick_value=0.01,
    point_value=0.01,
    price_decimals=2,      # BTC: $90000.00
    volume_decimals=2      # Объем: 0.01 BTC
)
```

### Пример 2: SOLUSD (не найден в реестре)

```python
handler = CryptoOrderFlowHandler("SOLUSD")

# 1. Вызывается _get_symbol_specs()
# 2. Вызывается get_specs("SOLUSD")
# 3. НЕ найден в INSTRUMENT_SPECS → ValueError
# 4. Используется fallback: _get_generic_crypto_specs()

SymbolSpecs(
    symbol="SOLUSD",
    contract_size=1.0,
    pip_value=0.01,
    lot_step=0.001,        # Generic: более мелкий шаг
    min_lot=0.001,         # Generic: более мелкий минимальный лот
    max_lot=1000.0,
    tick_value=0.01,
    point_value=0.01,
    price_decimals=2,
    volume_decimals=3      # Generic: 3 знака для объема
)
```

## 🎯 Зачем это нужно?

### 1. Корректные расчеты позиций

Спецификация используется для:

- Расчет размера позиции (lot)
- Расчет P&L (profit/loss)
- Валидация параметров ордера

### 2. Разные инструменты - разные параметры

**Криптовалюты (BTC, ETH):**

- `lot_step = 0.01`
- `volume_decimals = 2`
- `price_decimals = 2`

**Forex (XAUUSD):**

- `lot_step = 0.01`
- `volume_decimals = 2`
- `price_decimals = 2`
- `contract_size = 100.0` (для золота)

**Generic crypto (fallback):**

- `lot_step = 0.001` (более мелкий)
- `volume_decimals = 3` (больше знаков)

### 3. Централизованное управление

Все спецификации хранятся в одном месте (`instrument_config.py`), что упрощает:

- Добавление новых инструментов
- Изменение параметров
- Поддержку кода

## 🔧 Как добавить новый инструмент?

### Вариант 1: Добавить в реестр (рекомендуется)

```python
# В instrument_config.py

# 1. Создать спецификацию
SOLUSD_SPECS = SymbolSpecs(
    symbol="SOLUSD",
    contract_size=1.0,
    pip_value=0.01,
    lot_step=0.01,
    min_lot=0.01,
    max_lot=1000.0,
    tick_value=0.01,
    point_value=0.01,
    price_decimals=2,
    volume_decimals=2
)

# 2. Добавить в реестр
INSTRUMENT_SPECS["SOLUSD"] = SOLUSD_SPECS
INSTRUMENT_SPECS["SOLUSDT"] = SOLUSD_SPECS  # Алиас
```

### Вариант 2: Использовать fallback

Если инструмент не добавлен в реестр, автоматически используется `_get_generic_crypto_specs()`.

## 📝 Итоговая схема

```
┌─────────────────────────────────────────┐
│ CryptoOrderFlowHandler.__init__()       │
│   self.specs = self._get_symbol_specs() │
└─────────────────┬───────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│ _get_symbol_specs()                      │
│   try:                                   │
│     return get_specs(self.symbol)       │
│   except ValueError:                    │
│     return _get_generic_crypto_specs()  │
└─────────────────┬───────────────────────┘
                  │
        ┌─────────┴─────────┐
        │                   │
        ▼                   ▼
┌──────────────────┐  ┌──────────────────┐
│ get_specs()       │  │ Generic fallback │
│   INSTRUMENT_     │  │ (для неизвестных) │
│   SPECS[symbol]   │  │                  │
└──────────────────┘  └──────────────────┘
```

## ✅ Преимущества такого подхода

1. **Гибкость**: Можно использовать как реестр, так и fallback
2. **Расширяемость**: Легко добавлять новые инструменты
3. **Безопасность**: Всегда возвращается валидная спецификация
4. **Централизация**: Все параметры в одном месте
5. **Полиморфизм**: Каждый обработчик может переопределить метод
