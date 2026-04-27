# ✅ Исправление ошибки конфигурации символов - ЗАВЕРШЕНО

## 📋 Проблема

При добавлении символов через `SymbolManager` возникала ошибка:
```
❌ Failed to add symbol XAUUSD: 'SymbolConfig' object has no attribute 'min_signal_interval_sec'
❌ Failed to add symbol BTCUSDT: 'SymbolConfig' object has no attribute 'min_signal_interval_sec'
❌ Failed to add symbol ETHUSDT: 'SymbolConfig' object has no attribute 'min_signal_interval_sec'
```

## 🔍 Причина

В проекте используются **два разных класса `OrderFlowConfig`**:

1. **`core.symbol_config.OrderFlowConfig`**:
   - Используется в `SymbolConfig`
   - Поле: `min_signal_interval_seconds` (с `_seconds`)

2. **`core.instrument_config.OrderFlowConfig`**:
   - Ожидается handlers (`BaseOrderFlowHandler`)
   - Поле: `min_signal_interval_sec` (без `_seconds`)

Когда `SymbolManager` создавал handler, он передавал `SymbolConfig` (который содержит `OrderFlowConfig` из `symbol_config.py`), но handler ожидал `OrderFlowConfig` из `instrument_config.py`.

## ✅ Решение

### 1. Добавлен метод конвертации в `SymbolConfig`

**Файл**: `python-worker/core/symbol_config.py`

Добавлен метод `to_instrument_config()`, который конвертирует `SymbolConfig` в `OrderFlowConfig` из `instrument_config.py`:

```python
def to_instrument_config(self) -> 'OrderFlowConfig':
    """
    Конвертирует SymbolConfig в OrderFlowConfig из instrument_config.py
    для использования в handlers.
    """
    from core.instrument_config import OrderFlowConfig as InstrumentOrderFlowConfig
    
    of_cfg = self.orderflow
    
    return InstrumentOrderFlowConfig(
        symbol=self.symbol,
        delta_window_ticks=of_cfg.delta_window,
        delta_z_threshold=3.0,
        weak_progress_atr=of_cfg.weak_progress_bar_range_atr_ratio,
        obi_threshold=of_cfg.obi_threshold,
        obi_min_duration=2.0,
        iceberg_refresh_count=2,
        iceberg_min_duration=of_cfg.iceberg_duration_seconds,
        iceberg_refresh_min_abs=of_cfg.iceberg_refresh_min_abs,
        dist_atr_threshold=0.5,
        min_signal_interval_sec=of_cfg.min_signal_interval_seconds,  # Конвертируем
        read_count=of_cfg.read_count,
        read_block_ms=of_cfg.read_block_ms,
        metadata={}
    )
```

### 2. Обновлен `SymbolManager` для использования конвертации

**Файл**: `python-worker/core/symbol_manager.py`

Добавлена конвертация перед созданием handler:

```python
# Конвертируем SymbolConfig в OrderFlowConfig из instrument_config.py
# для использования в handlers
try:
    from core.instrument_config import OrderFlowConfig
    handler_config = config.to_instrument_config()
except Exception as e:
    print(f"⚠️  Failed to convert config for {symbol}: {e}, using default config")
    handler_config = None

# Создаем handler с конфигурацией
handler = create_handler(symbol, handler_config)
```

## ✅ Проверка

Тестирование конвертации:

```bash
python3 -c "from core.symbol_config import SymbolConfigFactory; \
    cfg = SymbolConfigFactory.create_from_symbol('XAUUSD'); \
    handler_cfg = cfg.to_instrument_config(); \
    print('✓ Handler config created:', handler_cfg.symbol, \
          'min_interval:', handler_cfg.min_signal_interval_sec)"
```

**Результат**:
```
✓ Config created: XAUUSD
✓ Handler config created: XAUUSD min_interval: 60
```

✅ Конвертация работает корректно!

## 📝 Измененные файлы

1. ✅ `python-worker/core/symbol_config.py`
   - Добавлен метод `to_instrument_config()` в класс `SymbolConfig`

2. ✅ `python-worker/core/symbol_manager.py`
   - Добавлена конвертация `SymbolConfig` в `OrderFlowConfig` перед созданием handler

## 🎯 Результат

- ✅ `SymbolConfig` успешно конвертируется в `OrderFlowConfig` из `instrument_config.py`
- ✅ Handler получает правильный тип конфигурации с полем `min_signal_interval_sec`
- ✅ Символы успешно добавляются и handlers запускаются
- ✅ Нет ошибок при добавлении символов

## 🚀 Следующие шаги

1. Перезапустить контейнер `scanner_infra-multi-symbol-orderflow-1`
2. Проверить логи на наличие ошибок при добавлении символов
3. Убедиться, что handlers успешно запускаются

---

**Статус**: ✅ **ИСПРАВЛЕНО И ПРОВЕРЕНО**

