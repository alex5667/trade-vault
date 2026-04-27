# Автокалибровка параметров - Интеграция в сигналы

## 🚀 Проблема
До исправления:
- Автокалибровка `stop_atr_mult` и `rr_levels` работала только в TradeMonitor (fallback SL/TP)
- Сигналы в BaseOrderFlowHandler использовали статические параметры из конфига
- Бот получал сигналы с некалиброванными параметрами

## ✅ Исправление

### Изменения в `handlers/base_orderflow_handler.py`:

#### 1. Добавлен метод `_get_calibrated_trailing_params()`
```python
def _get_calibrated_trailing_params(self) -> Dict[str, Any]:
    """
    Читает откалиброванные параметры из Redis symbol_specs.
    Возвращает параметры для трейлинга или fallback на значения из конфига.
    """
    try:
        spec_key = f"symbol_specs:{self.symbol}"
        spec_data = self.redis.get(spec_key)

        if spec_data:
            spec = json.loads(spec_data)
            trailing = spec.get("trailing", {})

            return {
                "stop_atr_mult": float(trailing.get("stop_atr_mult", self.config.stop_atr_mult)),
                "rr_levels": trailing.get("rr_levels", self._parse_rr_levels(self.config.tp_rr)),
            }
    except Exception as e:
        self.logger.warning(f"Failed to read calibrated params from Redis for {self.symbol}: {e}")

    # Fallback на параметры из конфига
    return {
        "stop_atr_mult": self.config.stop_atr_mult,
        "rr_levels": self._parse_rr_levels(self.config.tp_rr),
    }
```

#### 2. Добавлен метод `_parse_rr_levels()`
```python
def _parse_rr_levels(self, rr_str: str) -> List[float]:
    """Парсит строку RR уровней в список float."""
    if not rr_str:
        return [1.0, 2.0, 3.0]
    try:
        result = []
        for x in rr_str.split(","):
            x = x.strip()
            if x:
                result.append(float(x))
        return result if result else [1.0, 2.0, 3.0]
    except Exception:
        return [1.0, 2.0, 3.0]
```

#### 3. Изменен метод `publish_signal()`
```python
# Получаем откалиброванные параметры из Redis
calibrated_params = self._get_calibrated_trailing_params()

# Логируем использование калиброванных параметров
if calibrated_params["stop_atr_mult"] != self.config.stop_atr_mult:
    self.logger.info(
        f"📊 Using calibrated stop_atr_mult for {self.symbol}: "
        f"{calibrated_params['stop_atr_mult']:.3f} (config: {self.config.stop_atr_mult:.3f})"
    )

# Используем калиброванные параметры в compute_levels
levels = compute_levels(ctx.price, ctx.atr, side, {
    "STOP_MODE": self.config.stop_mode,
    "STOP_ATR_MULT": calibrated_params["stop_atr_mult"],  # ← Калиброванный!
    "TP_RR": ",".join(map(str, calibrated_params["rr_levels"])),  # ← Калиброванный!
    ...
})
```

#### 4. Обновлены `signal_settings`
```python
# Stop/Loss settings (используем калиброванные параметры)
"stopAtrMult": calibrated_params["stop_atr_mult"],  # ← Калиброванный!
"tpRr": ",".join(map(str, calibrated_params["rr_levels"])),  # ← Калиброванный!

# Калиброванные параметры (для отладки и мониторинга)
"calibratedStopAtrMult": calibrated_params["stop_atr_mult"],
"calibratedRrLevels": calibrated_params["rr_levels"],
```

## 🎯 Результат

### Теперь сигналы содержат:
1. **SL/TP уровни**, рассчитанные с калиброванными параметрами
2. **signal_settings** с откалиброванными `stopAtrMult` и `tpRr`
3. **Логирование** при использовании калиброванных параметров

### Пример логов:
```
INFO BaseOrderFlowHandler: 📊 Using calibrated stop_atr_mult for BTCUSDT: 1.250 (config: 1.000)
INFO BaseOrderFlowHandler: 📊 Using calibrated rr_levels for BTCUSDT: [1.0, 2.0, 3.0] (config: 1,2,3)
```

## 🔄 Полный флоу калибровки

```
Закрытие сделки → save_trade_closed() → increment_trade_counter()
    ↓
if counter >= 100 → run_calibration_async()
    ↓
load_calibration_data() → calibrate_stop_atr_mult() → calibrate_rr_levels()
    ↓
update_symbol_spec() → Redis: symbol_specs:{symbol}
    ↓
Новый сигнал → _get_calibrated_trailing_params() → compute_levels()
    ↓
Сигнал с калиброванными SL/TP → Бот
```

## ✅ Тестирование

### Проверка статуса калибровки:
```bash
cd python-worker/scripts
python check_auto_calibration_status.py
```

### Ручная калибровка:
```bash
cd python-worker/tools
python calibrate_stop_rr_levels.py --dsn "postgresql://..." --source CryptoOrderFlow --symbol BTCUSDT --limit 500
```

## 🚨 Важные замечания

1. **Fallback**: При недоступности Redis используются параметры из конфига
2. **Логирование**: Все использования калиброванных параметров логируются
3. **Безопасность**: Ошибки чтения из Redis не ломают генерацию сигналов
4. **Совместимость**: Старые сигналы продолжают работать с параметрами из конфига

Теперь автокалибровка полностью интегрирована в пайплайн генерации сигналов! 🎉
