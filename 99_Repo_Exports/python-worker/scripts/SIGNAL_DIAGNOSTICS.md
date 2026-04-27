# Диагностика сигналов CryptoOrderFlow

## Быстрый запуск

```bash
# Из контейнера crypto-orderflow-service
docker exec scanner-crypto-orderflow python scripts/check_crypto_signals.py

# Или из хоста (если есть доступ к Redis)
cd python-worker
python3 scripts/check_crypto_signals.py
```

## Что проверяет скрипт

1. **Символы** - список активных символов из `crypto:symbols`
2. **Тики** - наличие тиков в `stream:tick_{symbol}`
3. **Сырые сигналы** - записи в `signals:crypto:raw`
4. **Структурированные сигналы** - записи в `signals:cryptoorderflow:{symbol}`
5. **Telegram stream** - сообщения в `notify:telegram`
6. **Гейтинг** - значение счетчика и параметр `every_n`
7. **Конфигурация** - пороги и настройки

## Типичные проблемы и решения

### 1. Тики не найдены

**Симптомы:**
```
❌ BTCUSDT: НЕТ записей в stream:tick_BTCUSDT
```

**Решения:**
- Проверьте, что тики публикуются в Redis
- Проверьте подключение к `redis-ticks`
- Проверьте логи сервиса, который публикует тики

### 2. Сигналы не генерируются

**Симптомы:**
```
❌ НЕТ сигналов в signals:crypto:raw
```

**Возможные причины:**
- Детектор не срабатывает (z-score < threshold)
- Фильтр confidence отсекает сигналы
- Cooldown блокирует частые сигналы

**Решения:**

#### Снизить порог детектора:
```bash
# В docker-compose.yml для crypto-orderflow-service
- BTC_DELTA_Z_THRESHOLD=2.0  # вместо 2.7
```

#### Снизить минимальную confidence:
```bash
- CRYPTO_SIGNAL_MIN_CONF=70  # вместо 80
```

#### Уменьшить cooldown:
```bash
# В config:orderflow:{symbol} в Redis
signal_cooldown_sec: 20  # вместо 45
```

### 3. Сигналы генерируются, но не идут в Telegram

**Симптомы:**
```
✅ Найдено сигналов в signals:crypto:raw
❌ НЕТ сообщений в notify:telegram
```

**Возможные причины:**
- Гейтинг `every_n=3` пропускает сигналы
- Бот читает другой Redis/stream
- Ошибки публикации в Redis

**Решения:**

#### Отключить гейтинг (для тестирования):
```bash
# В docker-compose.yml
- CRYPTO_NOTIFY_SIGNAL_EVERY_N=1
```

#### Проверить настройки бота:
- Убедитесь, что бот читает `notify:telegram`
- Убедитесь, что бот подключен к правильному Redis (`scanner-redis-worker-1:6379/0`)

#### Проверить логи сервиса:
```bash
docker logs scanner-crypto-orderflow | grep -E "(Telegram|notify|Не удалось)"
```

### 4. Гейтинг пропускает сигналы

**Симптомы:**
```
⚠️ Следующий сигнал будет пропущен (остаток 2 != 0)
```

**Решение:**
- Установить `CRYPTO_NOTIFY_SIGNAL_EVERY_N=1` для отправки всех сигналов
- Или дождаться, пока счетчик станет кратным `every_n`

## Проверка логов сервиса

### Просмотр логов в реальном времени:
```bash
docker logs -f scanner-crypto-orderflow
```

### Поиск конкретных событий:

#### Сигналы формируются:
```bash
docker logs scanner-crypto-orderflow | grep "Сигнал сформирован"
```

#### Сигналы фильтруются:
```bash
docker logs scanner-crypto-orderflow | grep -E "(Signal filtered|Пропуск)"
```

#### Успешная публикация в Telegram:
```bash
docker logs scanner-crypto-orderflow | grep "Сигнал отправлен в Telegram"
```

#### Ошибки публикации:
```bash
docker logs scanner-crypto-orderflow | grep "Не удалось опубликовать"
```

## Ручная проверка Redis

### Проверка тиков:
```bash
redis-cli -u redis://redis-ticks:6379/0 XREVRANGE stream:tick_BTCUSDT + - COUNT 5
```

### Проверка сырых сигналов:
```bash
redis-cli -u redis://scanner-redis-worker-1:6379/0 XREVRANGE signals:crypto:raw + - COUNT 5
```

### Проверка Telegram stream:
```bash
redis-cli -u redis://scanner-redis-worker-1:6379/0 XREVRANGE notify:telegram + - COUNT 5
```

### Проверка счетчика гейтинга:
```bash
redis-cli -u redis://scanner-redis-worker-1:6379/0 GET notify:telegram:signal_counter
```

## Настройка для тестирования

Для быстрого тестирования можно временно снизить пороги:

```yaml
# В docker-compose.yml для crypto-orderflow-service
environment:
  - BTC_DELTA_Z_THRESHOLD=2.0  # Снизить порог детектора
  - CRYPTO_SIGNAL_MIN_CONF=50   # Снизить минимальную confidence
  - CRYPTO_NOTIFY_SIGNAL_EVERY_N=1  # Отключить гейтинг
```

После тестирования верните значения обратно.

## Мониторинг в реальном времени

### Следить за генерацией сигналов:
```bash
docker logs -f scanner-crypto-orderflow | grep -E "(Сигнал сформирован|Signal filtered|Сигнал отправлен)"
```

### Следить за публикацией в Telegram:
```bash
docker logs -f scanner-crypto-orderflow | grep "Telegram"
```

## Контакты и поддержка

При возникновении проблем:
1. Запустите диагностический скрипт
2. Проверьте логи сервиса
3. Проверьте настройки в docker-compose.yml
4. Проверьте конфигурацию символов в Redis

