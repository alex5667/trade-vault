# 🔥 HOTFIX: Docker Compose Profiles Error

## Проблема

При запуске системы возникает ошибка:

```
ERROR: Service "multi-symbol-orderflow" was pulled in as a dependency of service
"signal-performance-tracker" but is not enabled by the active profiles.
```

## Root Cause

В `docker-compose.yml`:

- ✅ `multi-symbol-orderflow` имеет профиль `default`
- ❌ `signal-performance-tracker` **НЕ имел** профиля

Docker Compose требует, чтобы зависимые сервисы имели **одинаковые профили**.

## Исправление

**Файл**: `docker-compose.yml`

**Изменение**:

```yaml
signal-performance-tracker:
  profiles:
    - default # ← ДОБАВЛЕНО
  build:
    context: .
    dockerfile: python-worker/Dockerfile
```

## Применение исправления

### Вариант 1: Уже исправлено (если используете обновленный docker-compose.yml)

```bash
# Просто запустите
make up-bg
```

### Вариант 2: Если ошибка осталась

```bash
# 1. Убедитесь, что файл обновлен
grep -A 2 "signal-performance-tracker:" docker-compose.yml | grep profiles

# Должно показать:
#   profiles:
#     - default

# 2. Если нет, добавьте вручную в docker-compose.yml после строки signal-performance-tracker:
#    profiles:
#      - default

# 3. Запустите систему
make down
make up-bg
```

### Вариант 3: Запуск без профилей (временное решение)

```bash
# Запустить явно указав профиль
docker-compose --profile default up -d

# Или запустить все сервисы игнорируя профили
docker-compose --profile "*" up -d
```

## Проверка

После исправления:

```bash
# Проверьте, что оба сервиса запустились
docker ps | grep -E "(multi-symbol-orderflow|signal-tracker)"

# Ожидаемый вывод:
scanner-signal-tracker        Up X seconds
scanner_infra-multi-symbol-orderflow-1   Up X seconds

# Проверьте статус
make tracker-status
```

## Дополнительная информация

### Что такое Docker Compose Profiles?

Profiles позволяют группировать сервисы для выборочного запуска:

```yaml
services:
  prod-service:
    profiles:
      - production # Запускается только с --profile production

  dev-service:
    profiles:
      - development # Запускается только с --profile development

  common-service:
    profiles:
      - default # Запускается всегда (с make up)
```

### Профили в нашей системе

**Сервисы с профилем `default`** (запускаются всегда):

- `multi-symbol-orderflow`
- `signal-performance-tracker`

**Сервисы без профиля** (запускаются всегда):

- `redis`
- `go-workers`
- `go-gateway`
- и другие основные сервисы

## Статус

- ✅ **ИСПРАВЛЕНО** в docker-compose.yml
- ✅ Добавлен профиль `default` к `signal-performance-tracker`
- ✅ Оба сервиса теперь в одном профиле
- ✅ Зависимости работают корректно

## Тестирование

```bash
# Запустите систему
make down
make up-bg

# Проверьте логи
make tracker-logs

# Ожидаемый вывод:
🔧 Загрузка конфигурации из: /app/python-worker/config/signal_tracker_config.json
✅ Конфигурация загружена из файла
📊 Символы: ['XAUUSD']
📊 Стратегии: ['orderflow', 'aggregated-hub']
📊 Периодические отчеты: True
📊 Интервал отчетов: 3ч
🚀 Запуск Signal Performance Tracker...
✅ Redis подключение установлено
✅ Все потоки запущены
```

---

**Статус**: ✅ FIXED  
**Дата**: 3 ноября 2025  
**Версия**: v1.0.1

---

_Senior Go/Python Developer + Senior Trading Systems Analyst_
