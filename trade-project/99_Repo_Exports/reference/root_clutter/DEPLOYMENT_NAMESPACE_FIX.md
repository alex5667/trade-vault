# Quick Deployment Guide: Namespace Isolation Fix

## Проблема
Race condition между `scanner-trade-monitor` и `scanner-signal-tracker` при дедупликации SID в Redis → пропуск сигналов.

## Решение
Добавлена изоляция через `TM_NAMESPACE` ENV var.

## Развертывание

### 1. Пересборка (если нужно)
```bash
cd /home/alex/front/trade/scanner_infra
docker-compose build python-worker
```

### 2. Перезапуск сервисов
```bash
# Остановка
docker-compose stop scanner-trade-monitor scanner-signal-tracker

# Запуск с новыми ENV vars (они уже в docker-compose)
docker-compose up -d scanner-trade-monitor scanner-signal-tracker
```

### 3. Верификация
```bash
# Проверяем логи на наличие namespace
docker-compose logs scanner-trade-monitor | grep namespace
# Ожидаем: 🔖 TradeMonitorService namespace: trade-monitor

docker-compose logs scanner-signal-tracker | grep namespace
# Ожидаем: 🔖 TradeMonitorService namespace: signal-tracker

# Проверяем Redis ключи
redis-cli KEYS "dedup:trade_monitor:*:sid:*" | head -10
# Должны видеть ключи с разными namespace
```

## Откат (если нужен)
```bash
# Удалить TM_NAMESPACE из docker-compose файлов
# Перезапустить сервисы (будет использоваться "default" namespace)
docker-compose restart scanner-trade-monitor scanner-signal-tracker
```

## Тесты
```bash
cd /home/alex/front/trade/scanner_infra/python-worker
python -m pytest tests/test_trade_monitor_namespace.py tests/test_trade_monitor_race_condition.py -v
# Ожидаем: 20 passed
```

## Детали
См. `python-worker/services/NAMESPACE_ISOLATION_FIX.md`

