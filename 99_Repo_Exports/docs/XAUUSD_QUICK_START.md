# ⚡ XAUUSD Quick Start Guide

**5 минут до полного запуска системы**

---

## 🎯 Цель

Запустить полный data flow для XAUUSD:

```
MT5 → Tick Ingest → Redis → OrderFlow → Hub → Notify → Telegram
```

---

## ✅ Pre-Requirements

- [x] Docker + Docker Compose запущены
- [x] Redis работает (`docker ps | grep redis`)
- [x] MT5 установлен под Wine (опционально)
- [x] Telegram Bot Token и Chat ID настроены

---

## 🚀 Быстрый запуск (без MT5)

### Шаг 1: Запустить систему

```bash
cd /home/alex/front/trade/scanner_infra
make up-bg
```

### Шаг 2: Проверить статус

```bash
make check-xauusd-services
```

### Шаг 3: Отправить тестовый тик вручную

```bash
curl -X POST http://localhost:8087/tick \
  -H 'Content-Type: application/json' \
  -d '{
    "symbol":"XAUUSD",
    "ts":'$(date +%s)'000,
    "bid":2055.25,
    "ask":2055.35,
    "last":2055.30,
    "volume":1.5,
    "flags":6
  }'
```

### Шаг 4: Проверить Redis

```bash
docker exec scanner-redis redis-cli XLEN stream:tick_XAUUSD
# Должно вернуть: 1 (или больше)
```

### Шаг 5: Посмотреть логи

```bash
# OrderFlow Handler
docker logs -f scanner_infra_multi-symbol-orderflow_1

# Или Aggregated Hub
docker logs -f scanner-aggregated-hub
```

---

## 🖥️ Запуск с MT5

### Шаг 1: Установить TickBridge EA

1. Скопировать файл:

```bash
cp /home/alex/front/trade/scanner_infra/mt5/TickBridge.mq5 \
   ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Experts/
```

2. Открыть MetaEditor в MT5
3. Скомпилировать `TickBridge.mq5`
4. Перетащить EA на график XAUUSD
5. В настройках указать:
   - **URL**: `http://localhost:8087/tick`
   - **Symbol**: `XAUUSD`
   - **Poll Interval**: `200` (ms)

### Шаг 2: Проверить отправку

```bash
# Смотреть логи в MT5 Experts
# Должны видеть: "Tick sent successfully"

# Или в Docker
docker logs -f scanner-tick-ingest
```

### Шаг 3: Мониторинг

```bash
watch -n 1 'docker exec scanner-redis redis-cli XLEN stream:tick_XAUUSD'
```

---

## 🔍 Диагностика

### Проблема: Нет тиков в Redis

**Проверить**:

```bash
# 1. Tick Ingest работает?
curl http://localhost:8087/health

# 2. MT5 отправляет?
# Смотреть MT5 Experts log

# 3. Тест вручную
curl -X POST http://localhost:8087/tick \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"XAUUSD","ts":1730000000000,"bid":2055.25,"ask":2055.35,"last":2055.30,"volume":1.5,"flags":6}'
```

### Проблема: OrderFlow Handler не запущен

```bash
# Проверить
docker ps | grep multi-symbol-orderflow

# Запустить
docker-compose up -d multi-symbol-orderflow

# Логи
docker logs -f scanner_infra_multi-symbol-orderflow_1
```

### Проблема: Нет сигналов

```bash
# Проверить thresholds
# Delta Z-score: 3.0 (высокий порог, нужен сильный сигнал)

# Посмотреть debug logs
docker logs scanner_infra_multi-symbol-orderflow_1 | grep "z_delta"
```

### Проблема: Telegram не получает сообщения

```bash
# 1. Notify Worker работает?
docker logs scanner-notify-worker --tail 50

# 2. Есть что-то в notify stream?
docker exec scanner-redis redis-cli XLEN notify:telegram

# 3. Telegram credentials правильные?
echo $TELEGRAM_BOT_TOKEN
echo $TELEGRAM_CHAT_ID
```

---

## 📊 Полная диагностика

```bash
# Comprehensive check
bash scripts/check_xauusd_flow.sh

# Результат покажет:
# ✓ Services status
# ✓ Redis streams
# ✓ Consumer groups
# ✓ HTTP endpoints
# ✓ Recent activity
# ✓ Recommendations
```

---

## 🎮 Makefile команды

```bash
make status                    # Статус всех контейнеров
make check-xauusd-services     # Проверка XAUUSD flow
make logs                      # Все логи
make orderflow-logs            # Логи OrderFlow handler
make hub-logs                  # Логи Aggregated Hub
make gateway-logs              # Логи Go Gateway
make telegram-logs             # Логи Telegram workers
make redis-stats               # Статистика Redis
```

---

## 🧪 Тестирование

### Тест 1: HTTP Endpoint

```bash
curl -X POST http://localhost:8087/tick \
  -H 'Content-Type: application/json' \
  -d '{
    "symbol":"XAUUSD",
    "ts":'$(date +%s)'000,
    "bid":2055.25,
    "ask":2055.35,
    "last":2055.30,
    "volume":1.5,
    "flags":6
  }'

# Expected: {"status":"ok","stream_id":"..."}
```

### Тест 2: Redis Stream

```bash
# Длина stream
docker exec scanner-redis redis-cli XLEN stream:tick_XAUUSD

# Последний тик
docker exec scanner-redis redis-cli XREVRANGE stream:tick_XAUUSD + - COUNT 1
```

### Тест 3: Consumer Groups

```bash
# Список групп
docker exec scanner-redis redis-cli XINFO GROUPS stream:tick_XAUUSD

# Pending messages
docker exec scanner-redis redis-cli XPENDING stream:tick_XAUUSD xauusd-signal-group
```

### Тест 4: Notifications

```bash
# Длина notify stream
docker exec scanner-redis redis-cli XLEN notify:telegram

# Последнее уведомление
docker exec scanner-redis redis-cli XREVRANGE notify:telegram + - COUNT 1
```

---

## 📈 Мониторинг

### Prometheus (порт 9090)

```bash
# Открыть в браузере
http://localhost:9090

# Полезные метрики:
# - orderflow_signals_generated_total
# - hub_signals_published_total
# - notify_messages_sent_total
```

### Grafana (порт 3001)

```bash
# Открыть в браузере
http://localhost:3001

# Credentials:
# Username: admin
# Password: admin
```

### Logи в реальном времени

```bash
# Все логи
docker-compose logs -f

# Только XAUUSD сервисы
docker-compose logs -f tick-ingest multi-symbol-orderflow aggregated-hub notify-worker
```

---

## 🔧 Настройка порогов

### Файл: `docker-compose.yml`

```yaml
# OrderFlow thresholds (XAUUSD)
XAU_DELTA_Z_THRESHOLD=3.0        # 👈 Понизить для больше сигналов (2.0-2.5)
XAU_OBI_THRESHOLD=0.5            # OBI threshold
XAU_MIN_SIGNAL_INTERVAL=60       # Минимальный интервал между сигналами (сек)

# Hub thresholds
HUB_CONFIDENCE_THR=0.25          # 👈 Минимальный confidence (25%)
HUB_MIN_SIG_INT_SEC=180          # Cooldown между сигналами (3 мин)
HUB_SIDE_LOCK_SEC=20             # Anti-dither lock (20 сек)
```

**После изменения**:

```bash
docker-compose restart multi-symbol-orderflow aggregated-hub
```

---

## 🎯 Типичные сценарии

### Сценарий 1: Первый запуск

```bash
# 1. Запустить систему
make up-bg

# 2. Подождать 30 секунд (сервисы стартуют)
sleep 30

# 3. Проверить
make check-xauusd-services

# 4. Отправить тестовый тик
curl -X POST http://localhost:8087/tick \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"XAUUSD","ts":'$(date +%s)'000,"bid":2055.25,"ask":2055.35,"last":2055.30,"volume":1.5,"flags":6}'

# 5. Проверить Redis
docker exec scanner-redis redis-cli XLEN stream:tick_XAUUSD
```

### Сценарий 2: Debugging сигналов

```bash
# 1. Включить debug mode
# В docker-compose.yml: LOG_LEVEL=DEBUG

# 2. Restart services
docker-compose restart multi-symbol-orderflow aggregated-hub

# 3. Watch logs
docker logs -f scanner_infra_multi-symbol-orderflow_1 | grep "z_delta"

# 4. Отправить много тиков для симуляции
for i in {1..200}; do
  curl -X POST http://localhost:8087/tick \
    -H 'Content-Type: application/json' \
    -d '{"symbol":"XAUUSD","ts":'$(date +%s)$(printf "%03d" $i)',"bid":'$(echo "2055 + $i * 0.01" | bc)',"ask":'$(echo "2055.1 + $i * 0.01" | bc)',"last":'$(echo "2055.05 + $i * 0.01" | bc)',"volume":1.5,"flags":6}'
  sleep 0.1
done
```

### Сценарий 3: Полный reset

```bash
# 1. Остановить все
make down

# 2. Очистить Redis
docker volume rm scanner_infra_scanner-redis-data

# 3. Rebuild и запуск
make rebuild

# 4. Проверить
make check-xauusd-services
```

---

## 📚 Документация

- **XAUUSD_ANALYSIS_SUMMARY.md** - Executive summary
- **XAUUSD_DATA_FLOW_ANALYSIS.md** - Полный техдок
- **XAUUSD_FLOW_DIAGRAM.md** - Визуальная диаграмма
- **ARCHITECTURE.md** - Общая архитектура
- **SERVICES.md** - Описание сервисов

---

## 🆘 Помощь

### Quick Commands

```bash
# Status check
make check-xauusd-services

# Full diagnostic
bash scripts/check_xauusd_flow.sh

# Logs
make logs

# Restart
make restart
```

### Troubleshooting

1. **Нет тиков** → Проверить MT5/TickBridge
2. **Нет сигналов** → Понизить thresholds
3. **Нет Telegram** → Проверить Bot Token/Chat ID
4. **Services unhealthy** → Посмотреть docker logs

---

**Status**: Ready to use! 🚀  
**Time to first signal**: ~2-5 минут (с live MT5 ticks)  
**Support**: См. документацию выше
