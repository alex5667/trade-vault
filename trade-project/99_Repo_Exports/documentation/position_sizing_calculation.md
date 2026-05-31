# Расчет размера позиции (Position Sizing)

Документация описывает, где и как происходит расчет размера позиции (lot/volume) для различных сервисов, а также где задаются параметры депозита и риска.

**Версия:** 1.0  
**Дата:** 2025-11-27  
**Команда:** Senior Go/Python Developer + Senior Trading Systems Analyst

---

## 📋 Содержание

1. [Обзор](#обзор)
2. [Параметры расчета](#параметры-расчета)
3. [Сервисы и их логика расчета](#сервисы-и-их-логика-расчета)
4. [Формулы расчета](#формулы-расчета)
5. [Конфигурация](#конфигурация)

---

## 🎯 Обзор

Размер позиции (lot/volume) рассчитывается по-разному в зависимости от сервиса:

- **CryptoOrderFlow** — использует значение из индикаторов (delta, tick_qty) или дефолтное значение
- **AggregatedHub / FilteredSignalWriter** — рассчитывает на основе риска и ATR через `PositionSizer`
- **UnifiedSignalGenerator** — рассчитывает на основе риска и расстояния до SL

---

## ⚙️ Параметры расчета

### Переменные окружения

| Переменная                    | Описание                    | Значение по умолчанию | Где используется                    |
| ----------------------------- | --------------------------- | --------------------- | ----------------------------------- |
| `ACCOUNT_DEPOSIT_USD`         | Размер депозита в USD       | `100`                 | Все сервисы с расчетом по риску     |
| `ACCOUNT_LEVERAGE`            | Плечо (leverage)            | `100` (1:100)         | Position sizing для XAUUSD и крипты |
| `RISK_PERCENT` или `RISK_PCT` | Процент риска на сделку     | `5.0` (5%)            | Все сервисы с расчетом по риску     |
| `XAU_CONTRACT_SIZE`           | Размер контракта для XAUUSD | `100` (oz)            | Position sizing для XAUUSD          |
| `XAU_LOT_STEP`                | Шаг лота для XAUUSD         | `0.01`                | Округление лота                     |

### Параметры в конфигурации

| Параметр      | Описание                 | Значение по умолчанию |
| ------------- | ------------------------ | --------------------- |
| `risk_pct`    | Процент риска на сделку  | `5.0`                 |
| `atr_sl_mult` | Множитель ATR для SL     | `1.5`                 |
| `min_lot`     | Минимальный размер лота  | `0.01`                |
| `max_lot`     | Максимальный размер лота | `10.0`                |
| `lot_step`    | Шаг округления лота      | `0.01`                |

---

## 🔧 Сервисы и их логика расчета

### 1. CryptoOrderFlow Service

**Файл:** `python-worker/services/crypto_orderflow_service.py`  
**Метод:** `_calculate_levels()` (строки 704-751)

**Логика:**

- Лот **НЕ рассчитывается** на основе риска
- Лот берется из индикаторов или используется дефолтное значение:

```python
lot = indicators.get("lot")
if lot is None:
    lot = indicators.get("tick_qty") or indicators.get("delta") or 1.0
    lot = max(float(lot), cfg.get("min_lot", 0.01))
```

**Источники лота:**

1. `indicators["lot"]` — если указан явно
2. `indicators["tick_qty"]` — количество тиков
3. `indicators["delta"]` — значение дельты
4. Дефолтное значение `1.0` или `min_lot` из конфигурации

**Пример:**

```
BTCUSDT LONG @ 90912.80, Volume 1.46 lot
```

В этом случае `lot = 1.46` берется из `indicators["delta"]` или `indicators["tick_qty"]`.

**Конфигурация:**

- Настройки берутся из `config:orderflow:{symbol}` (Redis Hash)
- Параметры: `min_lot`, `max_lot`, `lot_step`

---

### 2. AggregatedHub / FilteredSignalWriter

**Файл:** `python-worker/core/filtered_signal_writer.py`  
**Метод:** `_select_lot_and_sl_tp()` (строки 86-99)

**Логика:**

- Использует `PositionSizer` из `risk/position_sizer.py`
- Рассчитывает лот на основе:
  - Баланса счета (`balance`)
  - Процента риска (`risk_pct`)
  - ATR значения (`atr`)
  - Множителя ATR для SL (`atr_sl_mult`)

```python
def _select_lot_and_sl_tp(self, side: str, entry: float, atr: float):
    specs = self._get_specs()
    bal = self._get_balance() or 10_000.0  # Баланс из gateway или дефолт
    ps = PositionSizer(specs)
    lot, stop_dist = ps.size_by_atr(bal, self.cfg.risk_pct, atr, self.cfg.atr_sl_mult)
    # ...
```

**Источники параметров:**

- `balance` — из `gateway_url/balance_path` или дефолт `10000.0`
- `risk_pct` — из конфигурации (`self.cfg.risk_pct`)
- `atr` — переданный параметр
- `atr_sl_mult` — из конфигурации (`self.cfg.atr_sl_mult`)

**Конфигурация:**

- `risk_pct` — из `Config` объекта (обычно из ENV `RISK_PERCENT`)
- `atr_sl_mult` — из конфигурации символа

---

### 3. PositionSizer (risk/position_sizer.py)

**Файл:** `risk/position_sizer.py`  
**Метод:** `size_by_atr()` (строки 38-48)

**Формула:**

```
money_risk = balance * (risk_pct / 100.0)
stop_dist = atr * atr_sl_mult
ticks = stop_dist / point
lot = money_risk / (ticks * tick_value_per_lot)
```

**Параметры:**

- `balance` — баланс счета в USD
- `risk_pct` — процент риска (например, 1.0 = 1%)
- `atr` — значение ATR
- `atr_sl_mult` — множитель ATR для расчета дистанции SL
- `point` — размер тика в цене (из `SymbolSpecs`)
- `tick_value_per_lot` — денежная ценность 1 тика на 1 лот (из `SymbolSpecs`)

**Пример расчета:**

```
balance = 10000 USD
risk_pct = 1.0% (0.01)
atr = 57.69
atr_sl_mult = 0.6
point = 0.01
tick_value_per_lot = 0.01

money_risk = 10000 * 0.01 = 100 USD
stop_dist = 57.69 * 0.6 = 34.614
ticks = 34.614 / 0.01 = 3461.4
lot = 100 / (3461.4 * 0.01) = 100 / 34.614 ≈ 2.89 lot
```

---

### 4. UnifiedSignalGenerator

**Файл:** `python-worker/core/unified_signal_generator.py`  
**Метод:** `_calculate_lot_size()` (строки 488-506)

**Логика:**

- Рассчитывает лот на основе риска и расстояния до SL
- Используется для XAUUSD и криптовалют

```python
def _calculate_lot_size(self, risk_amount: float, sl_distance: float) -> float:
    contract_size = 100 if self.symbol == "XAUUSD" else 1
    lot = risk_amount / (sl_distance * contract_size)
    lot = round(lot / self.specs.lot_step) * self.specs.lot_step
    lot = max(self.specs.min_lot, min(lot, self.specs.max_lot))
    return lot
```

**Параметры:**

- `risk_amount` — сумма риска в USD (рассчитывается как `balance * risk_pct / 100`)
- `sl_distance` — расстояние до SL в цене
- `contract_size` — размер контракта (100 для XAUUSD, 1 для крипты)

---

## 📐 Формулы расчета

### Формула PositionSizer.size_by_atr()

```
money_risk = balance * (risk_pct / 100.0)
stop_dist = atr * atr_sl_mult
ticks = max(stop_dist / point, 1.0)
lot = money_risk / (ticks * tick_value_per_lot)
lot = round_to_step(lot, lot_step)
lot = clamp(lot, min_lot, max_lot)
```

**Где:**

- `money_risk` — допустимый риск в USD
- `stop_dist` — дистанция до SL в цене
- `ticks` — количество тиков до SL
- `lot` — размер позиции

### Формула для криптовалют (contract_size = 1)

```
risk_amount = balance * (risk_pct / 100.0)
lot = risk_amount / sl_distance
```

### Формула для XAUUSD (contract_size = 100)

```
risk_amount = balance * (risk_pct / 100.0)
lot = risk_amount / (sl_distance * 100)
```

---

## 🔧 Конфигурация

### Переменные окружения (docker-compose.yml)

```yaml
environment:
  - ACCOUNT_DEPOSIT_USD=100 # Размер депозита
  - ACCOUNT_LEVERAGE=100 # Плечо 1:100
  - RISK_PERCENT=5.0 # 5% риска на сделку
  - RISK_PCT=5.0 # Альтернативное имя
  - XAU_CONTRACT_SIZE=100 # Размер контракта XAUUSD
  - XAU_LOT_STEP=0.01 # Шаг лота XAUUSD
```

### Конфигурация в коде

**python-worker/core/config.py:**

```python
ACCOUNT_DEPOSIT_USD = float(os.getenv("ACCOUNT_DEPOSIT_USD", "100"))
ACCOUNT_LEVERAGE = float(os.getenv("ACCOUNT_LEVERAGE", "100"))
RISK_PERCENT = float(os.getenv("RISK_PERCENT", "5.0"))
```

**python-worker/signals/position_sizing.py:**

```python
DEPOSIT = float(os.getenv("ACCOUNT_DEPOSIT_USD", "100"))
LEVERAGE = float(os.getenv("ACCOUNT_LEVERAGE", "100"))
RISK_PCT = float(os.getenv("RISK_PERCENT", "5.0"))
```

### Конфигурация символов (Redis)

**Ключ:** `config:orderflow:{symbol}` (Hash)

**Поля:**

- `min_lot` — минимальный лот
- `max_lot` — максимальный лот
- `lot_step` — шаг округления
- `risk_pct` — процент риска (если используется)
- `atr_sl_mult` — множитель ATR для SL
- `stop_atr_mult` — альтернативное имя для `atr_sl_mult`

### Конфигурация TradeMonitor

**Файл:** `python-worker/services/trade_monitor.py`

```python
self.default_lot = float(monitor_cfg.get("default_lot", 1.0))
self.risk_pct = float(monitor_cfg.get("risk_pct", 1.0))
self.stop_atr_mult = float(monitor_cfg.get("stop_atr_mult", 1.0))
```

**Источник:** `config/signal_tracker_config.json` или ENV переменные

---

## 📊 Примеры

### Пример 1: CryptoOrderFlow (BTCUSDT)

**Сигнал:**

```
BTCUSDT LONG @ 90912.80, Volume 1.46 lot
```

**Расчет:**

- Лот берется из `indicators["delta"]` или `indicators["tick_qty"]`
- Если `delta = 1.46`, то `lot = 1.46`
- Минимум: `min_lot = 0.01` (из конфигурации)

**Параметры НЕ используются:**

- `ACCOUNT_DEPOSIT_USD` — не используется
- `RISK_PERCENT` — не используется
- Расчет по риску — не выполняется

---

### Пример 2: AggregatedHub (XAUUSD)

**Параметры:**

- `balance = 100 USD`
- `risk_pct = 5.0%`
- `atr = 10.0`
- `atr_sl_mult = 1.5`
- `point = 0.01`
- `tick_value_per_lot = 0.01`

**Расчет:**

```
money_risk = 100 * 0.05 = 5 USD
stop_dist = 10.0 * 1.5 = 15.0
ticks = 15.0 / 0.01 = 1500
lot = 5 / (1500 * 0.01) = 5 / 15 = 0.33 lot
```

**Результат:** `lot = 6.67` (округлено до `lot_step`)

---

### Пример 3: UnifiedSignalGenerator (BTCUSDT)

**Параметры:**

- `balance = 100 USD`
- `risk_pct = 5.0%`
- `entry = 90912.80`
- `sl = 90878.19`
- `contract_size = 1` (крипта)

**Расчет:**

```
risk_amount = 100 * 0.05 = 5 USD
sl_distance = 90912.80 - 90878.19 = 34.61
lot = 5 / (34.61 * 1) = 0.14 lot
```

**Результат:** `lot = 2.89` (округлено до `lot_step`)

---

## 🔍 Где задаются параметры

### 1. Размер депозита

**Переменная окружения:**

```bash
ACCOUNT_DEPOSIT_USD=100
```

**Используется в:**

- `python-worker/core/config.py`
- `python-worker/signals/position_sizing.py`
- `python-worker/core/filtered_signal_writer.py` (через `_get_balance()`)

**Альтернативный источник:**

- API Gateway: `{gateway_url}/balance` (для `FilteredSignalWriter`)

---

### 2. Процент риска на сделку

**Переменные окружения:**

```bash
RISK_PERCENT=5.0
# или
RISK_PCT=5.0
```

**Используется в:**

- `python-worker/core/config.py`
- `python-worker/signals/position_sizing.py`
- `python-worker/core/filtered_signal_writer.py`
- `python-worker/services/trade_monitor.py`
- `risk/position_sizer.py`

**Конфигурационные файлы:**

- `python-worker/config/signal_tracker_config.json`
- `docker-compose.yml` (environment variables)

---

### 3. Множитель ATR для SL

**Конфигурация:**

- `atr_sl_mult` в конфигурации символа
- `stop_atr_mult` в конфигурации TradeMonitor

**Значение по умолчанию:** `1.5` или `0.6` (зависит от сервиса)

---

## ⚠️ Важные замечания

1. **CryptoOrderFlow НЕ использует расчет по риску:**

   - Лот берется из индикаторов (delta, tick_qty)
   - Параметры `ACCOUNT_DEPOSIT_USD` и `RISK_PERCENT` не влияют на расчет

2. **Разные сервисы — разные формулы:**

   - CryptoOrderFlow: лот из индикаторов
   - AggregatedHub: расчет через `PositionSizer.size_by_atr()`
   - UnifiedSignalGenerator: расчет через `_calculate_lot_size()`

3. **Баланс может браться из разных источников:**

   - Переменная окружения `ACCOUNT_DEPOSIT_USD`
   - API Gateway (`{gateway_url}/balance`)
   - Дефолтное значение `100.0`

4. **Округление лота:**
   - Все расчеты округляются до `lot_step`
   - Ограничиваются `min_lot` и `max_lot`

---

## 📝 Резюме

**Для CryptoOrderFlow (BTCUSDT):**

- Лот = `indicators["delta"]` или `indicators["tick_qty"]`
- Параметры риска **НЕ используются**
- Конфигурация: `config:orderflow:{symbol}` → `min_lot`, `max_lot`

**Для AggregatedHub / FilteredSignalWriter:**

- Лот рассчитывается через `PositionSizer.size_by_atr()`
- Параметры: `ACCOUNT_DEPOSIT_USD`, `RISK_PERCENT`, `atr_sl_mult`
- Формула: `lot = (balance * risk_pct/100) / (ticks * tick_value_per_lot)`

**Для TradeMonitor:**

- Использует `default_lot` из конфигурации
- Параметры: `risk_pct`, `stop_atr_mult` (для расчета SL/TP)

---

**Последнее обновление:** 2025-11-27
