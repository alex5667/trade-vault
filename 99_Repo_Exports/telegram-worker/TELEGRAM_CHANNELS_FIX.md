# 🔧 Telegram Worker - Исправление проблемы с каналами

## ❌ Проблема

```
⚠️ Нет активных каналов для подписки
⚠️ Нет активных каналов
```

**Причина:** В Redis отсутствуют каналы для парсинга.

---

## ✅ Решение

### 1. Добавление каналов в Redis

**Метод 1: Через redis-cli**

```bash
# Добавить каналы в Set
redis-cli SADD telegram:channels:usernames "channel_username_1"
redis-cli SADD telegram:channels:usernames "channel_username_2"
redis-cli SADD telegram:channels:usernames "channel_username_3"

# Проверить добавленные каналы
redis-cli SMEMBERS telegram:channels:usernames
```

**Метод 2: Через Make команду (рекомендуется)**

```bash
# Добавить один канал
make add-telegram-channel CHANNEL=my_trading_signals

# Добавить несколько каналов
make add-telegram-channels CHANNELS="channel1,channel2,channel3"

# Посмотреть список каналов
make list-telegram-channels

# Удалить канал
make remove-telegram-channel CHANNEL=old_channel

# Очистить все каналы
make clear-telegram-channels
```

**Метод 3: Через Python скрипт**

```python
import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Список каналов для добавления
channels = [
    "crypto_signals_pro",
    "forex_elite_signals",
    "bitcoin_trading_group",
    "altcoin_pumps"
]

# Добавление
for channel in channels:
    r.sadd("telegram:channels:usernames", channel)
    print(f"✅ Added: @{channel}")

# Проверка
active_channels = r.smembers("telegram:channels:usernames")
print(f"\n📊 Total channels: {len(active_channels)}")
print(f"Channels: {active_channels}")
```

---

### 2. Формат имени канала

**Правильно:**

- `crypto_signals` (без @)
- `forex_trading_room`
- `bitcoin_analysis`

**Неправильно:**

- `@crypto_signals` (с @)
- `https://t.me/crypto_signals` (полная ссылка)
- `t.me/crypto_signals` (с доменом)

---

### 3. Проверка работы

**Шаг 1: Добавить тестовый канал**

```bash
redis-cli SADD telegram:channels:usernames "test_channel"
```

**Шаг 2: Проверить что каналы в Redis**

```bash
redis-cli SMEMBERS telegram:channels:usernames
```

Должно вернуть:

```
1) "test_channel"
```

**Шаг 3: Перезапустить telegram-worker**

```bash
# Через Docker
docker-compose restart scanner-telegram-worker

# Или через Make
make restart-telegram-worker

# Проверить логи
docker logs scanner-telegram-worker --tail 50
```

**Шаг 4: Проверить что каналы подписаны**

В логах должно появиться:

```
✅ Подписка на @test_channel установлена
📊 Всего подписано: 1 каналов
```

---

### 4. Типичные проблемы

#### Проблема 1: Канал приватный

**Ошибка:**

```
ChannelPrivateError: You don't have access to this channel
```

**Решение:**

1. Убедитесь что аккаунт Telegram подписан на канал
2. Если канал приватный - нужно быть участником
3. Добавьте аккаунт в канал вручную через Telegram

#### Проблема 2: Неверное имя канала

**Ошибка:**

```
ValueError: No entity found for "wrong_channel"
```

**Решение:**

1. Проверьте правильность username канала
2. Откройте канал в Telegram и скопируйте точное имя
3. Username видно в URL: `t.me/channel_name`

#### Проблема 3: Сессия не авторизована

**Ошибка:**

```
You must be logged in to do this
```

**Решение:**

1. Удалите старую сессию: `rm telegram-worker/sessions/*.session`
2. Перезапустите worker с кодом авторизации
3. Введите код из Telegram

---

### 5. Мониторинг каналов

**Проверка статистики:**

```bash
# Статистика по каналам
redis-cli HGETALL telegram:channel:stats

# Последние сообщения
redis-cli XREVRANGE signal:telegram:raw + - COUNT 10

# Парсенные сигналы
redis-cli XREVRANGE signal:telegram:parsed + - COUNT 10
```

**Проверка активности:**

```python
import redis
import json

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Получить все каналы
channels = r.smembers("telegram:channels:usernames")

print(f"📊 Всего каналов: {len(channels)}\n")

for channel in channels:
    # Статистика по каналу
    stats_key = f"telegram:channel:{channel}:stats"
    stats = r.hgetall(stats_key)

    if stats:
        print(f"📺 @{channel}")
        print(f"   Сообщений: {stats.get('messages', 0)}")
        print(f"   Последнее: {stats.get('last_message', 'N/A')}")
        print()
    else:
        print(f"⚠️ @{channel} - нет статистики")
```

---

### 6. Автоматизация (Make команды)

Добавьте в `Makefile`:

```makefile
# ========================================
# Telegram Channels Management
# ========================================

.PHONY: add-telegram-channel
add-telegram-channel:
	@echo "➕ Adding Telegram channel: $(CHANNEL)"
	@redis-cli SADD telegram:channels:usernames "$(CHANNEL)"
	@echo "✅ Channel added successfully"
	@make list-telegram-channels

.PHONY: add-telegram-channels
add-telegram-channels:
	@echo "➕ Adding multiple Telegram channels..."
	@for channel in $$(echo "$(CHANNELS)" | tr ',' ' '); do \
		redis-cli SADD telegram:channels:usernames "$$channel"; \
		echo "  ✅ Added: @$$channel"; \
	done
	@make list-telegram-channels

.PHONY: remove-telegram-channel
remove-telegram-channel:
	@echo "➖ Removing Telegram channel: $(CHANNEL)"
	@redis-cli SREM telegram:channels:usernames "$(CHANNEL)"
	@echo "✅ Channel removed"
	@make list-telegram-channels

.PHONY: list-telegram-channels
list-telegram-channels:
	@echo "\n📋 Active Telegram Channels:"
	@redis-cli SMEMBERS telegram:channels:usernames | while read channel; do \
		echo "  • @$$channel"; \
	done
	@echo ""

.PHONY: clear-telegram-channels
clear-telegram-channels:
	@echo "⚠️ WARNING: This will remove ALL channels!"
	@read -p "Are you sure? (yes/no): " confirm && [ "$$confirm" = "yes" ] || exit 1
	@redis-cli DEL telegram:channels:usernames
	@echo "✅ All channels removed"

.PHONY: restart-telegram-worker
restart-telegram-worker:
	@echo "🔄 Restarting Telegram Worker..."
	@docker-compose restart scanner-telegram-worker
	@echo "✅ Telegram Worker restarted"
	@sleep 2
	@docker logs scanner-telegram-worker --tail 20

.PHONY: check-telegram-channels
check-telegram-channels:
	@echo "🔍 Checking Telegram channels..."
	@COUNT=$$(redis-cli SCARD telegram:channels:usernames); \
	if [ "$$COUNT" -eq "0" ]; then \
		echo "❌ No channels configured!"; \
		echo ""; \
		echo "Add channels with:"; \
		echo "  make add-telegram-channel CHANNEL=my_channel"; \
		echo "  make add-telegram-channels CHANNELS=\"channel1,channel2,channel3\""; \
	else \
		echo "✅ Found $$COUNT channel(s)"; \
		make list-telegram-channels; \
	fi
```

---

### 7. Пример настройки

**Полный цикл от нуля до работы:**

```bash
# 1. Проверить текущее состояние
make check-telegram-channels

# 2. Добавить каналы (замените на реальные)
make add-telegram-channels CHANNELS="crypto_signals,forex_pro,btc_analysis"

# 3. Проверить что добавлены
make list-telegram-channels

# 4. Перезапустить worker
make restart-telegram-worker

# 5. Проверить логи
docker logs scanner-telegram-worker --tail 50 -f

# 6. Проверить парсинг (подождите несколько минут)
redis-cli XLEN signal:telegram:raw
redis-cli XLEN signal:telegram:parsed
```

---

### 8. Troubleshooting

**Если каналы есть, но сообщения не парсятся:**

```bash
# 1. Проверить что worker запущен
docker ps | grep telegram

# 2. Проверить логи на ошибки
docker logs scanner-telegram-worker --tail 100 | grep -i error

# 3. Проверить Redis streams
redis-cli XINFO STREAM signal:telegram:raw

# 4. Проверить сессию Telegram
ls -la telegram-worker/sessions/

# 5. Проверить переменные окружения
docker exec scanner-telegram-worker env | grep TG_
```

**Если нужно пересоздать сессию:**

```bash
# 1. Остановить worker
docker-compose stop scanner-telegram-worker

# 2. Удалить старую сессию
rm telegram-worker/sessions/*.session

# 3. Запустить заново (потребует код из Telegram)
docker-compose up -d scanner-telegram-worker

# 4. Следить за логами
docker logs scanner-telegram-worker -f
```

---

## ✅ Финальная проверка

После настройки выполните:

```bash
# Проверка 1: Каналы в Redis
redis-cli SMEMBERS telegram:channels:usernames

# Проверка 2: Worker запущен
docker ps | grep telegram

# Проверка 3: Логи без ошибок
docker logs scanner-telegram-worker --tail 20

# Проверка 4: Сырые сообщения появляются
redis-cli XLEN signal:telegram:raw

# Проверка 5: Парсенные сигналы появляются
redis-cli XLEN signal:telegram:parsed
```

**Все OK если:**

- ✅ Каналы показаны в SMEMBERS
- ✅ Worker работает (в `docker ps`)
- ✅ В логах: "✅ Подписка на @channel установлена"
- ✅ signal:telegram:raw > 0
- ✅ signal:telegram:parsed > 0

---

**Готово!** Telegram Worker настроен и парсит сигналы из каналов! 🎉
