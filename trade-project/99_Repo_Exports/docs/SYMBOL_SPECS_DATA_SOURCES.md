# 📊 Откуда берутся данные для SymbolSpecs?

## 🎯 Источники данных

Данные для `SymbolSpecs` берутся из **нескольких источников**, в зависимости от конфигурации:

### 1. 📝 Hardcoded пресеты (основной источник)

**Файл:** `python-worker/core/instrument_config.py`

Значения определяются **вручную** на основе характеристик инструмента на бирже:

```python
BTCUSD_SPECS = SymbolSpecs(
    symbol="BTCUSD",
    contract_size=1.0,              # ← Из спецификации биржи
    pip_value=0.01,                 # ← Стандарт для USD пар
    lot_step=0.001,                 # ← Минимальный шаг лота на бирже
    min_lot=0.001,                   # ← Минимальный размер позиции
    max_lot=1000.0,                  # ← Максимальный размер позиции
    tick_value=0.01,                 # ← Минимальное изменение цены
    point_value=0.01,                # ← Значение пункта
    price_decimals=2,                # ← Точность цены на бирже ($50000.00)
    volume_decimals=3                # ← Точность объема (0.001 BTC)
)
```

### 2. 🔍 Откуда берутся конкретные значения?

#### **contract_size** (Размер контракта)
- **Forex (XAUUSD)**: `100.0` - стандартный размер контракта для золота (100 унций)
- **Crypto (BTCUSD, ETHUSD)**: `1.0` - для крипты обычно 1:1 (1 BTC = 1 контракт)
- **Источник**: Спецификация инструмента на бирже (MT5, Binance и т.д.)

#### **lot_step** (Шаг лота)
- **BTCUSD**: `0.001` - можно купить 0.001, 0.002, 0.003 BTC и т.д.
- **ETHUSD**: `0.01` - можно купить 0.01, 0.02, 0.03 ETH и т.д.
- **XAUUSD**: `0.01` - стандартный шаг для Forex
- **Источник**: Минимальный шаг размера позиции на бирже

#### **min_lot / max_lot** (Минимальный/максимальный лот)
- **BTCUSD**: `min_lot=0.001, max_lot=1000.0`
- **ETHUSD**: `min_lot=0.01, max_lot=1000.0`
- **XAUUSD**: `min_lot=0.01, max_lot=100.0`
- **Источник**: Ограничения биржи на размер позиции

#### **price_decimals** (Точность цены)
- **BTCUSD**: `2` - цена отображается как $50000.00 (2 знака)
- **ETHUSD**: `2` - цена как $3000.00
- **XAGUSD**: `3` - серебро как $25.123 (3 знака)
- **Источник**: Формат цены на бирже

#### **volume_decimals** (Точность объема)
- **BTCUSD**: `3` - объем как 0.001 BTC
- **ETHUSD**: `2` - объем как 0.01 ETH
- **XAUUSD**: `2` - объем как 0.01 лота
- **Источник**: Формат объема на бирже

#### **tick_value / point_value** (Значение тика/пункта)
- Обычно `0.01` для большинства инструментов
- Определяет минимальное изменение цены
- **Источник**: Спецификация инструмента на бирже

### 3. 🔄 Альтернативные источники (если доступны)

#### A. Redis (для динамических значений)

В системе есть поддержка загрузки из Redis:

```python
# go-gateway/internal/risk/symbol_specs.go
func LoadSymbolSpecs(ctx context.Context, rdb *redis.Client, symbol string) SymbolSpecs {
    key := "symbol_specs:" + symbol
    raw, err := rdb.Get(ctx, key).Bytes()
    if err == nil {
        var sp SymbolSpecs
        if json.Unmarshal(raw, &sp) == nil {
            return sp
        }
    }
    // Fallback на defaults
}
```

**Ключ в Redis:** `symbol_specs:{SYMBOL}`

**Формат JSON:**
```json
{
  "point": 0.1,
  "tick_value_per_lot": 1.0,
  "min_lot": 0.01,
  "max_lot": 10.0,
  "lot_step": 0.01,
  "contract_size": 100.0,
  "price_decimals": 1,
  "volume_decimals": 2
}
```

#### B. Environment Variables (для переопределения)

Можно переопределить через переменные окружения:

```bash
SPEC_POINT=0.1
SPEC_TICK_VALUE_PER_LOT=1.0
SPEC_CONTRACT_SIZE=100.0
```

#### C. API биржи (потенциально)

Теоретически можно получать из API биржи:
- **Binance API**: `/api/v3/exchangeInfo` → `filters` → `LOT_SIZE`, `PRICE_FILTER`
- **MT5 API**: `SymbolInfoDouble()` → `SYMBOL_VOLUME_MIN`, `SYMBOL_VOLUME_MAX`
- **Другие биржи**: их специфичные API

## 📋 Текущая реализация

### В `instrument_config.py`:

```python
# 1. Определяются пресеты для каждого инструмента
BTCUSD_SPECS = SymbolSpecs(
    symbol="BTCUSD",
    contract_size=1.0,
    pip_value=0.01,
    lot_step=0.001,
    min_lot=0.001,
    max_lot=1000.0,
    tick_value=0.01,
    point_value=0.01,
    price_decimals=2,
    volume_decimals=3
)

# 2. Регистрируются в централизованном реестре
INSTRUMENT_SPECS: Dict[str, SymbolSpecs] = {
    "BTCUSD": BTCUSD_SPECS,
    "ETHUSD": ETHUSD_SPECS,
    ...
}

# 3. Получаются через функцию
def get_specs(symbol: str) -> SymbolSpecs:
    if symbol in INSTRUMENT_SPECS:
        return INSTRUMENT_SPECS[symbol]
    raise ValueError(f"Unknown symbol: {symbol}")
```

### Fallback для неизвестных символов:

```python
def _get_generic_crypto_specs(self) -> SymbolSpecs:
    """Generic спецификация для криптовалют (fallback)"""
    return SymbolSpecs(
        symbol=self.symbol,
        contract_size=1.0,
        pip_value=0.01,
        lot_step=0.001,      # Консервативный шаг
        min_lot=0.001,
        max_lot=1000.0,
        tick_value=0.01,
        point_value=0.01,
        price_decimals=2,
        volume_decimals=3    # Больше знаков для безопасности
    )
```

## 🔍 Как определить правильные значения?

### 1. Для криптовалют (Binance):

```python
# Пример для BTCUSDT на Binance
# Из документации Binance API:

# LOT_SIZE filter:
#   minQty: 0.00001      → min_lot = 0.00001
#   maxQty: 9000         → max_lot = 9000.0
#   stepSize: 0.00001    → lot_step = 0.00001

# PRICE_FILTER:
#   tickSize: 0.01       → tick_value = 0.01
#   priceDecimals: 2    → price_decimals = 2

BTCUSDT_SPECS = SymbolSpecs(
    symbol="BTCUSDT",
    contract_size=1.0,
    lot_step=0.00001,        # Из LOT_SIZE.stepSize
    min_lot=0.00001,         # Из LOT_SIZE.minQty
    max_lot=9000.0,          # Из LOT_SIZE.maxQty
    tick_value=0.01,         # Из PRICE_FILTER.tickSize
    price_decimals=2,        # Из PRICE_FILTER
    volume_decimals=5        # Из LOT_SIZE.stepSize (5 знаков)
)
```

### 2. Для Forex (MT5):

```python
# Из MT5 SymbolInfo:
#   SYMBOL_VOLUME_MIN: 0.01     → min_lot = 0.01
#   SYMBOL_VOLUME_MAX: 100.0    → max_lot = 100.0
#   SYMBOL_VOLUME_STEP: 0.01    → lot_step = 0.01
#   SYMBOL_TRADE_TICK_SIZE: 0.01 → tick_value = 0.01
#   SYMBOL_TRADE_TICK_VALUE: 0.01 → point_value = 0.01

XAUUSD_SPECS = SymbolSpecs(
    symbol="XAUUSD",
    contract_size=100.0,      # Стандарт для золота
    lot_step=0.01,
    min_lot=0.01,
    max_lot=100.0,
    tick_value=0.01,
    point_value=0.01,
    price_decimals=2,
    volume_decimals=2
)
```

## 🎯 Приоритет источников

```
1. INSTRUMENT_SPECS реестр (instrument_config.py)  ← Текущий основной источник
   ↓ (если не найден)
2. Redis (symbol_specs:{SYMBOL})                    ← Динамический источник
   ↓ (если не найден)
3. Environment Variables                            ← Переопределение
   ↓ (если не найден)
4. Generic fallback (_get_generic_crypto_specs)     ← Безопасный fallback
```

## 📝 Рекомендации

### Для добавления нового инструмента:

1. **Проверьте спецификацию на бирже:**
   - Binance: `/api/v3/exchangeInfo`
   - MT5: `SymbolInfo*()` функции
   - Другие биржи: их документацию

2. **Определите значения:**
   - `lot_step` = минимальный шаг размера позиции
   - `min_lot` = минимальный размер позиции
   - `max_lot` = максимальный размер позиции
   - `price_decimals` = количество знаков в цене
   - `volume_decimals` = количество знаков в объеме

3. **Добавьте в реестр:**
   ```python
   NEW_SYMBOL_SPECS = SymbolSpecs(
       symbol="NEWSYMBOL",
       contract_size=1.0,
       lot_step=0.001,
       min_lot=0.001,
       max_lot=1000.0,
       tick_value=0.01,
       point_value=0.01,
       price_decimals=2,
       volume_decimals=3
   )
   
   INSTRUMENT_SPECS["NEWSYMBOL"] = NEW_SYMBOL_SPECS
   ```

## ✅ Итог

**Текущий источник данных:**
- ✅ Hardcoded пресеты в `instrument_config.py`
- ✅ Определены вручную на основе характеристик биржи
- ✅ Fallback для неизвестных символов

**Потенциальные улучшения:**
- 🔄 Загрузка из Redis (частично реализовано в go-gateway)
- 🔄 Загрузка из API биржи (не реализовано)
- 🔄 Автоматическое обновление при изменении на бирже













