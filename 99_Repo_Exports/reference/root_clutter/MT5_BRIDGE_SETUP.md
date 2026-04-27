# Настройка MT5 Bridge Credentials

## 🚀 Быстрый Старт

### 1. Получите MT5 Demo Account

Перейдите на сайт **RoboForex**: https://my.roboforex.com/en/platforms/metatrader5/

1. **Зарегистрируйтесь** (если нет аккаунта)
2. **Создайте Demo счет** в разделе Platforms → MetaTrader 5
3. **Запишите credentials**:
   - **Login**: 8-значный номер (например: 12345678)
   - **Password**: пароль от счета
   - **Server**: обычно `RoboForex-Demo` или `RoboForex-DemoPro`

### 2. Настройте mt5-bridge.env

Файл уже создан в корне проекта: `mt5-bridge.env`

```bash
# Отредактируйте реальные credentials
nano mt5-bridge.env
```

**Замените значения:**
```bash
MT5_LOGIN=ВАШ_НАСТОЯЩИЙ_ЛОГИН    # Вместо 12345678
MT5_PASSWORD=ВАШ_НАСТОЯЩИЙ_ПАРОЛЬ # Вместо demo_password_replace_with_real
MT5_SERVER=RoboForex-Demo         # Проверьте точное название сервера
```

### 3. Запустите MT5 Bridge

```bash
# С profile mt5
docker-compose --profile mt5 up mt5-bridge

# Или локально
export $(cat mt5-bridge.env | xargs) && python -m mt5_bridge.main
```

---

## 📋 Детальная Настройка

### Структура mt5-bridge.env

```bash
# === ОБЯЗАТЕЛЬНЫЕ НАСТРОЙКИ ===
MT5_LOGIN=ВАШ_ЛОГИН_НАПРИМЕР_12345678
MT5_PASSWORD=ВАШ_ПАРОЛЬ_ОТ_СЧЕТА
MT5_SERVER=RoboForex-Demo

# === ДОПОЛНИТЕЛЬНЫЕ НАСТРОЙКИ ===
REDIS_DSN=redis://redis-worker-1:6379/0
MT5_SYMBOL_MAP='{"XAUUSD": "XAUUSD.m", "BTCUSDT": "BTCUSDT.m"}'
POLL_BLOCK_MS=500
LOG_LEVEL=INFO
```

### Проверка Credentials

**В MT5 Терминале:**
1. File → Login to Trade Account
2. Введите ваш Login/Password/Server
3. Убедитесь что подключение успешно

**Имена серверов RoboForex:**
- `RoboForex-Demo` - основной demo сервер
- `RoboForex-DemoPro` - pro demo сервер
- `RoboForex-ECN` - live ECN сервер
- `RoboForex-Pro` - live pro сервер

---

## 🔍 Поиск Credentials в Проекте

Проект использует **RoboForex** как основного брокера:

### Документация
- `mt5/README_MT5_SETUP.md` - установка MT5 под Wine
- `mt5/TickBridge.mq5` - EA для отправки тиков

### Типичные Настройки для RoboForex
```bash
MT5_SERVER=RoboForex-Demo
MT5_SYMBOL_MAP='{"XAUUSD": "XAUUSD.m", "EURUSD": "EURUSD."}'
```

### Если у вас уже есть аккаунт
Проверьте в MT5 терминале:
- **View → Navigator** - список доступных серверов
- **Tools → Options → Server** - текущий сервер

---

## 🧪 Тестирование Подключения

### 1. Проверка MT5 Bridge
```bash
# Запуск с логированием
docker-compose --profile mt5 up mt5-bridge 2>&1 | grep -E "(✅|❌|MT5)"

# Ожидаемый вывод:
# [mt5_bridge] ✅ MT5 connected successfully
# [mt5_bridge] ✅ Redis consumer initialized
# [mt5_bridge] 🚀 Bridge started - waiting for signals
```

### 2. Проверка Сигналов
```bash
# Проверить Redis streams
docker exec scanner-redis redis-cli XREAD STREAMS stream:signals:plans 0

# Если есть сигналы - MT5 Bridge должен их обработать
```

### 3. Ручное Тестирование
```bash
# Тест подключения к MT5
python -c "
import os
os.environ.update(dict(line.strip().split('=', 1) for line in open('mt5-bridge.env') if '=' in line))
import MetaTrader5 as mt5
if mt5.initialize():
    print('✅ MT5 initialized')
    if mt5.login(int(os.environ['MT5_LOGIN']), os.environ['MT5_PASSWORD'], os.environ['MT5_SERVER']):
        print('✅ Login successful')
    else:
        print('❌ Login failed:', mt5.last_error())
    mt5.shutdown()
else:
    print('❌ MT5 initialize failed')
"
```

---

## 🚨 Troubleshooting

### Проблема: "Invalid account"
```
Решение: Проверьте login/password/server в mt5-bridge.env
Убедитесь что используете demo account credentials
```

### Проблема: "Server not found"
```
Решение: Проверьте точное имя сервера в MT5 терминале
Возможные варианты: RoboForex-Demo, RoboForex-DemoPro
```

### Проблема: "No signals received"
```
Решение:
1. Проверьте что scanner_infra запущен: docker-compose ps
2. Проверьте Redis: docker exec scanner-redis redis-cli XLEN stream:signals:plans
3. Проверьте логи MT5 Bridge
```

### Проблема: Wine/MT5 не запускается
```
Решение:
1. Следуйте mt5/README_MT5_SETUP.md
2. Проверьте Wine: wine --version
3. Пересоздайте Wine prefix если проблемы
```

---

## 📊 Мониторинг

### Логи MT5 Bridge
```bash
docker-compose --profile mt5 logs -f mt5-bridge
```

### Состояние Сигналов
```bash
# Активные планы
curl -s http://localhost:8080/metrics | grep mt5_bridge_active_plans

# Вошедшие позиции
curl -s http://localhost:8080/metrics | grep mt5_bridge_entered_positions
```

### MT5 Терминал
- **Experts** вкладка - статус TickBridge EA
- **Terminal** вкладка - история ордеров
- **Logs** - ошибки и статус подключения

---

## 🎯 Следующие Шаги

1. ✅ **Настроить credentials** в `mt5-bridge.env`
2. ✅ **Запустить bridge**: `docker-compose --profile mt5 up mt5-bridge`
3. ✅ **Протестировать** получение сигналов
4. ✅ **Мониторить** логи и производительность
5. 🔄 **Перейти на live account** когда система отлажена

---

**MT5 Bridge готов к работе с вашими реальными credentials!** 🚀🤖📊
