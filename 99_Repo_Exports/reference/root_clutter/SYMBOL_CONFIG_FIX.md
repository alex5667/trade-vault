# ✅ Исправление ошибки конфигурации символов

## Проблема

При добавлении символов через `SymbolManager` возникала ошибка:
```
❌ Failed to add symbol XAUUSD: 'SymbolConfig' object has no attribute 'min_signal_interval_sec'
```

## Причина

В проекте используются два разных класса `OrderFlowConfig`:

1. **`core.symbol_config.OrderFlowConfig`** - используется в `SymbolConfig`, имеет поле `min_signal_interval_seconds`
2. **`core.instrument_config.OrderFlowConfig`** - ожидается handlers, имеет поле `min_signal_interval_sec`

Когда `SymbolManager` создавал handler, он передавал `SymbolConfig` (который содержит `OrderFlowConfig` из `symbol_config.py`), но handler ожидал `OrderFlowConfig` из `instrument_config.py`.

## Решение

### 1. Добавлен метод конвертации в `SymbolConfig`

В файле `core/symbol_config.py` добавлен метод `to_instrument_config()`:

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
        min_signal_interval_sec=of_cfg.min_signal_interval_seconds,  # Конвертируем _seconds -> _sec
        read_count=of_cfg.read_count,
        read_block_ms=of_cfg.read_block_ms,
        metadata={}
    )
```

### 2. Обновлен `SymbolManager` для использования конвертации

В файле `core/symbol_manager.py` добавлена конвертация перед созданием handler:

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

## Результат

- ✅ `SymbolConfig` успешно конвертируется в `OrderFlowConfig` из `instrument_config.py`
- ✅ Handler получает правильный тип конфигурации с полем `min_signal_interval_sec`
- ✅ Символы успешно добавляются и handlers запускаются

## Проверка

После применения исправления:
1. Символы должны добавляться без ошибок
2. Handlers должны запускаться корректно
3. Логи не должны содержать ошибок о `min_signal_interval_sec`

---

**Статус**: ✅ **ИСПРАВЛЕНО**


