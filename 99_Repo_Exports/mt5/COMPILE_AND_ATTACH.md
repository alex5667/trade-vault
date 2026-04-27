# MT5 Expert Advisors - Компиляция и Прикрепление

**Все EA уже исправлены с правильными портами!**

---

## 📝 Список EA с правильными настройками

### 1. OrderExecutor.mq5 ✅

```mql5
#property version   "6.00"

input string EndpointPoll    = "http://127.0.0.1:8090/orders/poll";     // ✅ CORRECT
input string EndpointConfirm = "http://127.0.0.1:8090/orders/confirm";  // ✅ CORRECT
input string SymbolToTrade   = "XAUUSD";
input int    PollIntervalMs  = 1000;
input int    Magic           = 777001;
```

### 2. OrderExecutorAdvanced.mq5 ✅

```mql5
#property version   "7.00"

input string EndpointPoll    = "http://127.0.0.1:8090/orders/poll";     // ✅ CORRECT (было 8089)
input string EndpointConfirm = "http://127.0.0.1:8090/orders/confirm";  // ✅ CORRECT (было 8089)
input string SymbolToTrade   = "XAUUSD";
input string TpSplitPerc     = "50,30,20";
input bool   EnableBreakeven = true;
input string TrailMode       = "ATR";
```

### 3. BookBridge.mq5 ✅

```mql5
#property version   "2.00"

input string EndpointBook = "http://127.0.0.1:8088/book";  // ✅ CORRECT
input int    MaxDepth = 10;
input int    TimeoutMs = 300;
```

### 4. TickBridge.mq5 ✅

```mql5
#property version   "1.00"

input string Endpoint = "http://127.0.0.1:8088/tick";  // ✅ CORRECT
input int    TimeoutMs = 300;
```

---

## 🔨 Шаг 1: Компиляция в MetaEditor

### Способ A: Через MetaEditor GUI

1. Нажмите **F4** в MT5 (откроется MetaEditor)
2. **File → Open**
3. Выберите каждый файл по очереди:
   ```
   MQL5/Experts/OrderExecutor.mq5
   MQL5/Experts/OrderExecutorAdvanced.mq5
   MQL5/Experts/BookBridge.mq5
   MQL5/Experts/TickBridge.mq5
   ```
4. Для каждого файла: нажмите **F7** (Compile)
5. Проверьте в окне "Errors" - **0 error(s), 0 warning(s)**

### Способ B: Через командную строку (Wine/Linux)

```bash
export WINEPREFIX=$HOME/.wine-mt5

# Компиляция всех EA
wine "C:\\Program Files\\MetaTrader 5\\metaeditor64.exe" /compile:"$EA_DIR/OrderExecutor.mq5"
wine "C:\\Program Files\\MetaTrader 5\\metaeditor64.exe" /compile:"$EA_DIR/OrderExecutorAdvanced.mq5"
wine "C:\\Program Files\\MetaTrader 5\\metaeditor64.exe" /compile:"$EA_DIR/BookBridge.mq5"
wine "C:\\Program Files\\MetaTrader 5\\metaeditor64.exe" /compile:"$EA_DIR/TickBridge.mq5"
```

**Результат:** Появятся файлы `.ex5` рядом с `.mq5`

---

## 🔧 Шаг 2: Настройка WebRequest (ОБЯЗАТЕЛЬНО!)

**Без этого EA не будут работать!**

1. В MT5: **Tools → Options**
2. Вкладка **Expert Advisors**
3. ✅ Включить "Allow WebRequest for listed URL"
4. Добавить оба URL (по одному на строку):
   ```
   http://127.0.0.1:8088
   http://127.0.0.1:8090
   ```
5. Нажать **OK**

---

## 🎯 Шаг 3: Прикрепление EA к графику XAUUSD

### 3.1 BookBridge (Обязательно для OBI)

1. Откройте график **XAUUSD** (любой таймфрейм)
2. **Navigator → Expert Advisors → BookBridge**
3. **Drag & Drop** на график
4. В диалоге:

**Inputs tab:**

```
EndpointBook        = "http://127.0.0.1:8088/book"
MaxDepth            = 10
TimeoutMs           = 300
EnableLogging       = true
LogEveryNUpdates    = 100
```

**Common tab:**

- ✅ Allow DLL imports
- ✅ Allow WebRequest for URL: http://127.0.0.1:8088
- ✅ Allow Algo Trading

5. **OK**

**Проверка в Experts tab:**

```
═══════════════════════════════════════════
  BookBridge EA инициализирован
═══════════════════════════════════════════
  Symbol: XAUUSD
  Endpoint: http://127.0.0.1:8088/book
  Max Depth: 10
  Timeout: 300 ms
═══════════════════════════════════════════

✅ Подписка на Market Book активирована для XAUUSD
```

---

### 3.2 OrderExecutor (Обязательно для торговли)

1. На том же или новом графике **XAUUSD**
2. **Navigator → Expert Advisors → OrderExecutor**
3. **Drag & Drop** на график
4. В диалоге:

**Inputs tab:**

```
EndpointPoll        = "http://127.0.0.1:8090/orders/poll"
EndpointConfirm     = "http://127.0.0.1:8090/orders/confirm"
PollIntervalMs      = 1000
SymbolToTrade       = "XAUUSD"
TpSplitPerc         = "50,30,20"
Slippage            = 20
Magic               = 777001
```

**Common tab:**

- ✅ Allow DLL imports
- ✅ Allow WebRequest for URL: http://127.0.0.1:8090
- ✅ Allow Algo Trading

5. **OK**

**Проверка в Experts tab:**

```
OrderExecutor v6.0 initialized
Poll endpoint: http://127.0.0.1:8090/orders/poll
Symbol: XAUUSD
Poll interval: 1000 ms
```

---

### 3.3 TickBridge (Опционально)

1. На том же или новом графике **XAUUSD**
2. **Navigator → Expert Advisors → TickBridge**
3. **Drag & Drop** на график
4. В диалоге:

**Inputs tab:**

```
Endpoint            = "http://127.0.0.1:8088/tick"
TimeoutMs           = 300
EnableLogging       = true
LogEveryNTicks      = 100
```

**Common tab:**

- ✅ Allow DLL imports
- ✅ Allow WebRequest for URL: http://127.0.0.1:8088
- ✅ Allow Algo Trading

5. **OK**

---

## ✅ Проверка работы

### В MT5 Experts tab:

Должны быть сообщения:

```
✅ BookBridge EA инициализирован
✅ Подписка на Market Book активирована для XAUUSD
✅ OrderExecutor v6.0 initialized
```

### В Docker logs:

```bash
# BookBridge отправляет данные
docker logs -f scanner-py-obi
# Каждый 10,000-й snapshot: 📖 Book #10000: XAUUSD OBI=0.123...

# OrderExecutor polling
docker logs -f scanner-go-gateway
# Каждый 10,000-й poll: → GET /orders/poll [#10000 polls]
```

---

## 🐛 Troubleshooting

### Проблема: "WebRequest is not allowed"

**Решение:**

1. Tools → Options → Expert Advisors
2. ✅ Allow WebRequest for listed URL
3. Добавить URL:
   ```
   http://127.0.0.1:8088
   http://127.0.0.1:8090
   ```

### Проблема: "MarketBookAdd failed"

**Причина:** Брокер не поддерживает DOM/Level II

**Решение:**

- Используйте ECN брокер (RoboForex, Pepperstone)
- Или отключите BookBridge (OBI не будет работать)

### Проблема: EA не отправляет данные

**Проверка:**

```bash
# 1. Docker сервисы запущены?
docker ps | grep -E "(scanner-py-obi|scanner-go-gateway)"

# 2. Endpoints доступны?
curl http://127.0.0.1:8088/healthz
curl http://127.0.0.1:8090/healthz

# 3. Тестовый POST
curl -X POST http://127.0.0.1:8088/book \
  -H "Content-Type: application/json" \
  -d '{"ts":1234567890000,"symbol":"XAUUSD","bids":[[2760.50,10.5]],"asks":[[2760.75,8.3]]}'
```

---

## 📚 Дополнительные материалы

- [MT5_INTEGRATION_GUIDE.md](../MT5_INTEGRATION_GUIDE.md) - полное руководство
- [QUICK_MT5_START.md](../QUICK_MT5_START.md) - быстрый старт
- [check_mt5_endpoints.sh](../check_mt5_endpoints.sh) - проверка endpoints

---

**Все настроено правильно. Просто скомпилируйте и прикрепите EA!** ✅
