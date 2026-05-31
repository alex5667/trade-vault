# 🍷 MT5 под Wine + TickBridge - Полная установка

## 📋 Обзор

Запуск MetaTrader 5 под Wine на Ubuntu и настройка отправки тиков через HTTP в Redis Stream.

```
MT5 (Wine) → TickBridge EA → HTTP POST → FastAPI Server → Redis Stream
```

---

## 🚀 Установка MT5 под Wine

### Шаг 1: Установка Wine и зависимостей

```bash
# Ubuntu 22.04/24.04
sudo dpkg --add-architecture i386
sudo apt update

# Установка Wine stable
sudo mkdir -pm755 /etc/apt/keyrings
sudo wget -O /etc/apt/keyrings/winehq-archive.key https://dl.winehq.org/wine-builds/winehq.key

# Добавление репозитория (Ubuntu 22.04)
sudo wget -NP /etc/apt/sources.list.d/ https://dl.winehq.org/wine-builds/ubuntu/dists/jammy/winehq-jammy.sources

# Или для Ubuntu 24.04
# sudo wget -NP /etc/apt/sources.list.d/ https://dl.winehq.org/wine-builds/ubuntu/dists/noble/winehq-noble.sources

sudo apt update
sudo apt install --install-recommends winehq-stable winetricks xvfb

# Проверка версии
wine --version
```

### Шаг 2: Создание префикса для MT5

```bash
# Создаем отдельный префикс (изолированная среда)
export WINEPREFIX=$HOME/.wine-mt5

# Инициализация префикса (создаст структуру папок)
winecfg

# В открывшемся окне можно сразу закрыть (настройки по умолчанию подходят)
```

### Шаг 3: Скачивание и установка MT5

```bash
# Скачайте установщик MT5 от вашего брокера
# Например, RoboForex: https://my.roboforex.com/en/platforms/metatrader5/

# Запустите установщик через Wine
cd ~/Downloads
wine mt5setup.exe

# Следуйте инструкциям установщика
# Рекомендуем установить в стандартную папку:
# C:\Program Files\MetaTrader 5\
```

### Шаг 4: Первый запуск MT5

```bash
# Запуск с графическим интерфейсом (для первоначальной настройки)
export WINEPREFIX=$HOME/.wine-mt5
wine "C:\\Program Files\\MetaTrader 5\\terminal64.exe"
```

**При первом запуске:**

1. ✅ Войдите в торговый счет (логин/пароль от брокера)
2. ✅ Откройте график XAUUSD (правый клик → New Chart → XAUUSD)
3. ✅ Включите AutoTrading (кнопка на тулбаре)
4. ✅ Настройте WebRequest (см. ниже)

### Шаг 5: Запуск MT5 "без головы" (Headless mode)

После первоначальной настройки можно запускать без GUI:

```bash
# Создайте скрипт для запуска
cat > ~/start-mt5.sh << 'EOF'
#!/bin/bash
export WINEPREFIX=$HOME/.wine-mt5
xvfb-run -a wine "C:\\Program Files\\MetaTrader 5\\terminal64.exe" /portable
EOF

chmod +x ~/start-mt5.sh

# Запуск в фоне
nohup ~/start-mt5.sh > mt5.log 2>&1 &

# Проверка логов
tail -f mt5.log
```

---

## 📝 Установка TickBridge EA

### Шаг 1: Копирование файла

```bash
# Путь к папке экспертов в Wine префиксе
export WINEPREFIX=$HOME/.wine-mt5
EA_DIR="$WINEPREFIX/drive_c/Program Files/MetaTrader 5/MQL5/Experts"

# Копируем TickBridge.mq5
cp /home/alex/front/trade/scanner_infra/mt5/TickBridge.mq5 "$EA_DIR/"

echo "✅ TickBridge.mq5 скопирован в $EA_DIR"
```

### Шаг 2: Компиляция EA

Вариант A: Через MetaEditor (GUI)

```bash
# Запустите MetaEditor
export WINEPREFIX=$HOME/.wine-mt5
wine "C:\\Program Files\\MetaTrader 5\\metaeditor64.exe"

# В MetaEditor:
# 1. File → Open → Experts → TickBridge.mq5
# 2. Compile (F7)
# 3. Проверьте что нет ошибок
```

Вариант B: Через командную строку

```bash
export WINEPREFIX=$HOME/.wine-mt5
wine "C:\\Program Files\\MetaTrader 5\\metaeditor64.exe" /compile:"$EA_DIR/TickBridge.mq5"
```

**Результат:** Должен появиться файл `TickBridge.ex5`

### Шаг 3: Настройка WebRequest

**КРИТИЧНО! Без этого EA не сможет отправлять HTTP запросы**

В MT5:

1. Tools → Options
2. Expert Advisors
3. ✅ Включите "Allow WebRequest for listed URL"
4. Добавьте URL:
   ```
   http://127.0.0.1:8088
   ```
   Или если сервер на другой машине:
   ```
   http://YOUR_SERVER_IP:8088
   ```
5. OK

### Шаг 4: Прикрепление EA к графику

1. В MT5 откройте график XAUUSD (если еще не открыт)
2. В Navigator → Expert Advisors → найдите TickBridge
3. Перетащите на график XAUUSD
4. В диалоге настроек:
   - Endpoint: `http://127.0.0.1:8088/tick` (или IP вашего сервера)
   - TimeoutMs: `300`
   - EnableLogging: `true`
   - LogEveryNTicks: `100`
5. ✅ "Allow Algo Trading" должно быть включено
6. OK

**Проверка:** В углу графика должна появиться иконка с улыбкой ☺️

---

## 🔧 Настройка Tick Ingest Server

### Вариант 1: Docker (рекомендуется)

Уже настроено в `docker-compose.yml`:

```bash
# Запуск tick-ingest-server
docker-compose up -d tick-ingest-server

# Проверка логов
docker logs -f scanner-tick-ingest

# Проверка health
curl http://localhost:8088/health
```

### Вариант 2: Локально (без Docker)

```bash
cd /home/alex/front/trade/scanner_infra/python-worker

# Установка зависимостей
pip install fastapi uvicorn redis

# Запуск сервера
uvicorn services.tick_ingest_server:app --host 0.0.0.0 --port 8088

# Или в фоне
nohup uvicorn services.tick_ingest_server:app --host 0.0.0.0 --port 8088 > tick_ingest.log 2>&1 &
```

---

## ✅ Проверка работы

### 1. Проверка Tick Ingest Server

```bash
# Health check
curl http://localhost:8088/health

# Ожидается:
# {"status":"healthy","redis":"ok","stream":"stream:tick_XAUUSD","uptime_seconds":123}

# Статистика
curl http://localhost:8088/stats

# Ожидается:
# {"total_ticks":1234,"errors":0,"uptime_seconds":300,"ticks_per_second":4.11,...}
```

### 2. Проверка Redis Stream

```bash
# Длина стрима (должна расти)
docker exec scanner-redis redis-cli XLEN stream:tick_XAUUSD

# Последние тики
docker exec scanner-redis redis-cli XREVRANGE stream:tick_XAUUSD + - COUNT 5

# Должно показать JSON с тиками:
# {"ts":1234567890000,"bid":1880.50,"ask":1880.75,"last":1880.60,...}
```

### 3. Проверка MT5 EA

В MT5 откройте вкладку "Experts":

```
═══════════════════════════════════════════
  TickBridge EA инициализирован
═══════════════════════════════════════════
  Symbol: XAUUSD
  Endpoint: http://127.0.0.1:8088/tick
  Timeout: 300 ms
═══════════════════════════════════════════

📊 Статистика: 100 тиков | Success: 100 (100.0%) | Errors: 0
📊 Статистика: 200 тиков | Success: 200 (100.0%) | Errors: 0
...
```

### 4. Полная цепочка

```bash
# 1. Проверяем что MT5 EA работает (смотрим Experts лог в MT5)
# 2. Проверяем Tick Ingest Server
curl http://localhost:8088/stats

# 3. Проверяем Redis Stream
docker exec scanner-redis redis-cli XLEN stream:tick_XAUUSD

# 4. Проверяем обработчик XAU
docker logs scanner-python-worker | grep "XAU OrderFlow"

# 5. Проверяем сигналы в Telegram
docker exec scanner-redis redis-cli XREVRANGE notify:telegram + - COUNT 3
```

---

## 🐛 Troubleshooting

### Проблема: MT5 не запускается

```bash
# Проверка Wine
wine --version

# Пересоздание префикса
rm -rf ~/.wine-mt5
export WINEPREFIX=$HOME/.wine-mt5
winecfg

# Переустановка MT5
wine mt5setup.exe
```

### Проблема: EA не компилируется

```bash
# Проверьте путь к файлу
export WINEPREFIX=$HOME/.wine-mt5
ls -la "$WINEPREFIX/drive_c/Program Files/MetaTrader 5/MQL5/Experts/TickBridge.mq5"

# Попробуйте открыть в MetaEditor вручную
wine "C:\\Program Files\\MetaTrader 5\\metaeditor64.exe"
```

### Проблема: WebRequest error code 4014

**Причина:** URL не добавлен в список разрешенных

**Решение:**

1. Tools → Options → Expert Advisors
2. ✅ Allow WebRequest for listed URL
3. Добавьте: `http://127.0.0.1:8088`
4. Restart MT5

### Проблема: HTTP 503 (Service Unavailable)

**Причина:** Tick Ingest Server не запущен или недоступен

**Решение:**

```bash
# Проверьте что сервер запущен
curl http://localhost:8088/health

# Если нет - запустите
docker-compose up -d tick-ingest-server
# или
uvicorn services.tick_ingest_server:app --host 0.0.0.0 --port 8088
```

### Проблема: HTTP 422 (Validation Error)

**Причина:** Неверная структура данных от EA

**Решение:**
Проверьте логи Tick Ingest Server:

```bash
docker logs -f scanner-tick-ingest
```

Пересоберите EA с актуальной версией.

### Проблема: Нет тиков в Redis Stream

```bash
# 1. Проверьте что EA работает (лог MT5)
# 2. Проверьте Tick Ingest Server логи
docker logs scanner-tick-ingest

# 3. Попробуйте ручную отправку тика
curl -X POST http://localhost:8088/tick \
  -H "Content-Type: application/json" \
  -d '{"ts":1234567890000,"bid":1880.5,"ask":1880.75,"last":1880.6,"volume":10.5,"flags":2,"symbol":"XAUUSD"}'

# 4. Проверьте Redis
docker exec scanner-redis redis-cli XLEN stream:tick_XAUUSD
```

---

## 📊 Мониторинг

### Systemd сервисы (опционально)

Создайте systemd units для автозапуска:

**MT5 Service:**

```bash
sudo nano /etc/systemd/system/mt5-xauusd.service
```

```ini
[Unit]
Description=MetaTrader 5 for XAUUSD
After=network.target

[Service]
Type=simple
User=alex
WorkingDirectory=/home/alex
Environment="WINEPREFIX=/home/alex/.wine-mt5"
ExecStart=/usr/bin/xvfb-run -a /usr/bin/wine "C:\\Program Files\\MetaTrader 5\\terminal64.exe" /portable
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable mt5-xauusd
sudo systemctl start mt5-xauusd
sudo systemctl status mt5-xauusd
```

### Логирование

```bash
# MT5 логи
tail -f ~/.wine-mt5/drive_c/Program\ Files/MetaTrader\ 5/Logs/*.log

# Tick Ingest Server
docker logs -f scanner-tick-ingest

# XAU Handler
docker logs -f scanner-python-worker | grep XAU
```

---

## 🎯 Производительность

### Рекомендации

- **CPU:** 2+ cores для Wine + MT5
- **RAM:** 2GB+ для MT5
- **Network:** Низкая задержка до брокера важна для качества тиков

### Оптимизация

```bash
# Увеличьте приоритет процесса MT5
renice -n -10 -p $(pgrep terminal64)

# Ограничьте CPU для других процессов
# cpulimit -p PID -l 50
```

---

## ✅ Готово!

Теперь у вас полностью рабочий поток:

```
MT5 (Wine) → TickBridge EA → HTTP → FastAPI → Redis Stream → XAU Handler → Telegram
```

**Следующие шаги:**

1. Мониторинг стабильности
2. Настройка автозапуска
3. Backtesting параметров
4. Production deployment

---

**Версия:** 1.0.0  
**Дата:** 2025-10-25  
**Платформа:** Ubuntu 22.04/24.04 + Wine 8.x/9.x
