# 🤖 XAUUSD Telegram Bot (NestJS/Telegraf)

**Telegram бот для XAUUSD Order Flow System v6.0**

---

## 📊 Возможности

- ✅ Отправка сигналов из Redis → Telegram
- ✅ Inline кнопки (Открыть, SL/TP, Отменить, x0.5/x1/x2)
- ✅ Обработка callbacks → Redis stream
- ✅ Consumer group (at-most-once delivery)
- ✅ Команды /start, /status

---

## 🚀 Quick Start

### 1. Установить зависимости

```bash
cd bot-nest
npm install
```

### 2. Настроить переменные окружения

```bash
export BOT_TOKEN=your_telegram_bot_token
export CHAT_ID=your_telegram_chat_id
export REDIS_URL=redis://scanner-redis:6379/0
```

**Получить BOT_TOKEN:**

1. Найдите @BotFather в Telegram
2. Отправьте `/newbot`
3. Следуйте инструкциям
4. Сохраните токен

**Получить CHAT_ID:**

1. Найдите @userinfobot в Telegram
2. Отправьте `/start`
3. Скопируйте ваш ID

### 3. Собрать и запустить

```bash
# Development
npm run dev

# Production
npm run build
npm start
```

---

## ⚙️ Конфигурация

**Environment variables:**

```bash
BOT_TOKEN=<your_bot_token>         # Telegram bot token (required)
CHAT_ID=<your_chat_id>             # Your Telegram chat ID (required)
REDIS_URL=redis://localhost:6379/0 # Redis connection string
BOT_GROUP=bot-sender-group         # Consumer group name
BOT_CONSUMER=bot-sender-1          # Consumer name
NOTIFY_STREAM=notify:telegram      # Signals stream
CALLBACKS_STREAM=bot:callbacks     # Callbacks stream
```

---

## 🔗 Integration Flow

```
Signal Handler
    ↓
notify:telegram stream
    ↓
Bot sender loop (XREADGROUP)
    ↓
Telegram message + buttons
    ↓
User clicks button
    ↓
Bot callback handler
    ↓
bot:callbacks stream
    ↓
Orders Router
```

---

## 📱 Команды бота

### /start

Приветствие и описание бота

### /status

Статистика системы:

- Количество сигналов
- Размер очереди ордеров
- Количество исполнений

---

## 🐛 Troubleshooting

### Бот не отправляет сообщения

```bash
# Проверить переменные
echo $BOT_TOKEN
echo $CHAT_ID

# Проверить Redis
redis-cli XLEN notify:telegram

# Проверить логи
npm start
```

### Кнопки не работают

```bash
# Проверить callback stream
redis-cli XLEN bot:callbacks
redis-cli XREVRANGE bot:callbacks + - COUNT 5

# Убедиться, что buttons в JSON формате
```

---

## 🔧 Deployment

### Systemd (user-level)

Создать `~/.config/systemd/user/telegram-bot.service`:

```ini
[Unit]
Description=XAUUSD Telegram Bot
After=network-online.target

[Service]
WorkingDirectory=%h/front/trade/scanner_infra/bot-nest
ExecStart=/usr/bin/node %h/front/trade/scanner_infra/bot-nest/dist/main.js
Restart=always
RestartSec=5
Environment=NODE_ENV=production
Environment=BOT_TOKEN=YOUR_TOKEN
Environment=CHAT_ID=YOUR_CHAT_ID
Environment=REDIS_URL=redis://scanner-redis:6379/0

[Install]
WantedBy=default.target
```

Запустить:

```bash
systemctl --user daemon-reload
systemctl --user enable --now telegram-bot.service
systemctl --user status telegram-bot.service
```

---

### Docker

Создать `Dockerfile`:

```dockerfile
FROM node:20-alpine

WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY . .
RUN npm run build

CMD ["node", "dist/main.js"]
```

Добавить в `docker-compose.yml`:

```yaml
telegram-bot:
  build: ./bot-nest
  environment:
    - BOT_TOKEN=${BOT_TOKEN}
    - CHAT_ID=${CHAT_ID}
    - REDIS_URL=redis://scanner-redis:6379/0
  restart: unless-stopped
  depends_on:
    - redis
```

---

## 📚 Дополнительная информация

**Telegraf documentation:**

- https://telegraf.js.org

**ioredis documentation:**

- https://github.com/redis/ioredis

**Redis Streams:**

- https://redis.io/docs/data-types/streams/

---

## ✅ Checklist

**Setup:**

- [ ] Node.js установлен
- [ ] Зависимости установлены (`npm install`)
- [ ] BOT_TOKEN настроен
- [ ] CHAT_ID настроен
- [ ] Redis доступен

**Running:**

- [ ] Бот запущен (`npm start` or systemd)
- [ ] Логи показывают "Listening for signals..."
- [ ] Сообщения приходят в Telegram
- [ ] Кнопки работают

**Integration:**

- [ ] notify:telegram stream публикуется
- [ ] bot:callbacks stream записывается
- [ ] orders_router обрабатывает callbacks

---

## 🎉 Готово!

Бот готов к использованию для XAUUSD Order Flow System v6.0!

**Успешной автоматизации! 🚀📈💰**
