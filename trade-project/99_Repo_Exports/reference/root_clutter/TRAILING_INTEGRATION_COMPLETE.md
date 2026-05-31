# ✅ Автоматический трейлинг настроен

## Выполненные задачи

### 1. ✅ Создан модуль `services/trailing_size_recommender.py`
- Полная реализация логики расчета рекомендаций по трейлингу
- Анализ MFE_R и расчет оптимального lock_r
- Конвертация в TRAILING_TP1_OFFSET_ATR
- Все комментарии сохранены согласно спецификации

### 2. ✅ Исправлена интеграция в систему
- Обновлена функция `_merge_trailing_cfg()` в `pnl_math.py`
- Исправлены несоответствия названий полей
- Добавлено поле `trailing_after_tp1_enabled` в автозапись

### 3. ✅ Настроена автозапись рекомендаций
- Скрипт `tools/recommend_trailing_from_redis.py` записывает в Redis
- Формат: `symbol:trailing_cfg:{SYMBOL}`
- Применяется автоматически через `get_symbol_info()`

### 4. ✅ Настроен периодический запуск
- Systemd service: `trailing-recommender.service`
- Systemd timer: `trailing-recommender.timer` (каждые 6 часов)
- Скрипт установки: `install_trailing_timer.sh`

## Как использовать

### Ручной запуск

```bash
cd /home/alex/front/trade/scanner_infra

# Анализ и автозапись
TRAILING_AUTOTUNE_ENABLED=true python3 tools/recommend_trailing_from_redis.py \
  --source CryptoOrderFlow \
  --symbols ETHUSDT,BTCUSDT \
  --auto-write \
  --conf-threshold 0.6
```

### Автоматический запуск

```bash
# Установка таймера
sudo ./install_trailing_timer.sh

# Проверка статуса
systemctl status trailing-recommender.timer
journalctl -u trailing-recommender.service
```

## Результаты тестирования

### ✅ Скрипт работает
```
### 🔧 Trailing calibration: CryptoOrderFlow

**ETHUSDT**
- Все win-сделки: n_total=5, n_wins=4, lock_r≈1.00R → TP1_OFFSET_ATR≈1.00
  - MFE_R avg/median≈2.90/2.90, giveback_R≈1.25, ratio≈0.44
  - σ(MFE_R)≈0.47, σ(giveback_ratio)≈0.02, confidence≈0.68

- 🔄 Автообновление: выбрана рекомендация all (TP1_OFFSET_ATR≈1.000, lock_r≈1.000, confidence≈0.68)
```

### ✅ Данные записываются в Redis
```redis
HGETALL symbol:trailing_cfg:ETHUSDT
tp1_offset_atr: "1.000000"
lock_r: "1.000000"
confidence: "0.6779"
stop_atr_mult: "1.000000"
trailing_after_tp1_enabled: "true"
updated_at_ms: "1766205782436"
```

### ✅ Интеграция работает
- `get_symbol_info()` подмешивает trailing-конфигурацию
- Дефолтные значения применяются при недоступности Redis
- SymbolSpec получает правильные параметры трейлинга

## Архитектура

```
trades:closed (Redis Stream)
    ↓
tools/recommend_trailing_from_redis.py
    ↓
symbol:trailing_cfg:{SYMBOL} (Redis Hash)
    ↓
get_symbol_info() → _merge_trailing_cfg()
    ↓
spec_from_symbol_info() → SymbolSpec.trailing_tp1_offset_atr
    ↓
base_orderflow_handler.py (применяет настройки)
```

## Мониторинг

```bash
# Проверка рекомендаций
redis-cli HGETALL symbol:trailing_cfg:ETHUSDT

# Статус таймера
systemctl status trailing-recommender.timer

# Логи
journalctl -u trailing-recommender.service -n 20
```

## Следующие шаги

1. **Запустить на продакшене** с реальными данными сделок
2. **Мониторить эффективность** рекомендаций в торговле
3. **Добавить больше символов** (SOLUSDT, ADAUSDT и т.д.)
4. **Настроить алерты** при низкой confidence
5. **Добавить A/B тестирование** разных стратегий трейлинга

## Файлы

- `services/trailing_size_recommender.py` - основной модуль
- `tools/recommend_trailing_from_redis.py` - CLI инструмент
- `trailing-recommender.service` - systemd service
- `trailing-recommender.timer` - systemd timer
- `TRAILING_AUTOMATION_SETUP.md` - подробная документация
- `install_trailing_timer.sh` - скрипт установки

🎯 **Система готова к автоматической настройке трейлинга на основе истории сделок!**
