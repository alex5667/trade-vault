//+------------------------------------------------------------------+
//|                                                   BookBridge.mq5 |
//|                   Order Book Bridge для передачи DOM через HTTP |
//|                                                                  |
//| НАЗНАЧЕНИЕ:                                                      |
//| - Подписка на Order Book (Depth of Market) для символа         |
//| - Отправка DOM snapshots через HTTP при изменениях              |
//| - Интеграция с FastAPI для расчета реального OBI               |
//|                                                                  |
//| УСТАНОВКА:                                                       |
//| 1. Скопируйте файл в папку MQL5/Experts/                       |
//| 2. Откройте MetaEditor и скомпилируйте                          |
//| 3. В MT5: Tools → Options → Expert Advisors                     |
//|    - Включите "Allow WebRequest for listed URL"                |
//|    - Добавьте: http://127.0.0.1:8088                           |
//| 4. Прикрепите EA к графику XAUUSD (ВМЕСТЕ с TickBridge)        |
//| 5. Включите "Allow Algo Trading"                               |
//|                                                                  |
//| ТРЕБОВАНИЯ:                                                      |
//| - Брокер должен предоставлять Level II данные (DOM/Market Book)|
//| - RoboForex, Pepperstone и другие ECN брокеры поддерживают     |
//|                                                                  |
//| ПАРАМЕТРЫ:                                                       |
//| - EndpointBook: URL FastAPI server /book                        |
//| - MaxDepth: Количество уровней DOM для отправки (default 10)   |
//| - TimeoutMs: Таймаут HTTP запроса (мс)                          |
//| - EnableLogging: Периодическое логирование                      |
//+------------------------------------------------------------------+

#property copyright "Scanner Infrastructure Team"
#property link      ""
#property version   "2.00"
#property strict

// Входные параметры
input string EndpointBook = "http://127.0.0.1:8088/book";  // URL Book endpoint
input int MaxDepth = 10;                                    // Максимальная глубина DOM
input int TimeoutMs = 300;                                  // Таймаут HTTP запроса (мс)
input bool EnableLogging = true;                            // Включить логирование
input int LogEveryNUpdates = 100;                           // Логировать каждое N-е обновление

// Глобальные переменные
int updateCounter = 0;
int successCounter = 0;
int errorCounter = 0;
datetime lastLogTime = 0;
bool subscriptionActive = false;

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
   Print("═══════════════════════════════════════════");
   Print("  BookBridge EA инициализирован");
   Print("═══════════════════════════════════════════");
   Print("  Symbol: ", _Symbol);
   Print("  Endpoint: ", EndpointBook);
   Print("  Max Depth: ", MaxDepth);
   Print("  Timeout: ", TimeoutMs, " ms");
   Print("═══════════════════════════════════════════");
   Print("");
   
   // Подписываемся на Market Book (DOM) для текущего символа
   if(MarketBookAdd(_Symbol))
   {
      subscriptionActive = true;
      Print("✅ Подписка на Market Book активирована для ", _Symbol);
      Print("");
      Print("⚠️ ВАЖНО: Проверьте настройки WebRequest!");
      Print("   Tools → Options → Expert Advisors");
      Print("   → Allow WebRequest for listed URL");
      Print("   → Добавьте: ", EndpointBook);
      Print("");
   }
   else
   {
      Print("❌ ОШИБКА: Не удалось подписаться на Market Book!");
      Print("   Возможные причины:");
      Print("   1. Брокер не предоставляет Level II данные");
      Print("   2. Символ не поддерживает DOM");
      Print("   3. Проблемы с подключением к серверу");
      Print("");
      Print("⚠️ EA будет остановлен");
      return(INIT_FAILED);
   }
   
   // Сброс счетчиков
   updateCounter = 0;
   successCounter = 0;
   errorCounter = 0;
   lastLogTime = TimeCurrent();
   
   return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
   // Отписываемся от Market Book
   if(subscriptionActive)
   {
      MarketBookRelease(_Symbol);
      subscriptionActive = false;
   }
   
   Print("═══════════════════════════════════════════");
   Print("  BookBridge EA остановлен");
   Print("═══════════════════════════════════════════");
   Print("  Всего обновлений: ", updateCounter);
   Print("  Успешно отправлено: ", successCounter);
   Print("  Ошибок: ", errorCounter);
   Print("  Success rate: ", (updateCounter > 0 ? (double)successCounter/updateCounter*100 : 0), "%");
   Print("═══════════════════════════════════════════");
}

//+------------------------------------------------------------------+
//| OnBookEvent - вызывается при изменении Order Book               |
//+------------------------------------------------------------------+
void OnBookEvent(const string &symbol)
{
   // Получаем текущий Order Book
   MqlBookInfo bookInfo[];
   if(!MarketBookGet(symbol, bookInfo))
   {
      if(errorCounter == 0) // Логируем только первую ошибку
         Print("❌ Ошибка получения Market Book для ", symbol);
      errorCounter++;
      return;
   }
   
   updateCounter++;
   
   // Разделяем на bids и asks
   double bids[][2];
   double asks[][2];
   ArrayResize(bids, 0);
   ArrayResize(asks, 0);
   
   for(int i = 0; i < ArraySize(bookInfo); i++)
   {
      if(bookInfo[i].volume_real > 0)
      {
         if(bookInfo[i].type == BOOK_TYPE_BUY)
         {
            // Bid level
            int n = ArraySize(bids);
            ArrayResize(bids, n + 1);
            bids[n][0] = bookInfo[i].price;
            bids[n][1] = bookInfo[i].volume_real;
         }
         else if(bookInfo[i].type == BOOK_TYPE_SELL)
         {
            // Ask level
            int n = ArraySize(asks);
            ArrayResize(asks, n + 1);
            asks[n][0] = bookInfo[i].price;
            asks[n][1] = bookInfo[i].volume_real;
         }
      }
   }
   
   // Сортировка: bids по убыванию цены, asks по возрастанию
   // Bubble sort для bids (descending)
   for(int i = 0; i < ArraySize(bids) - 1; i++)
   {
      for(int j = i + 1; j < ArraySize(bids); j++)
      {
         if(bids[i][0] < bids[j][0])
         {
            double temp0 = bids[i][0];
            double temp1 = bids[i][1];
            bids[i][0] = bids[j][0];
            bids[i][1] = bids[j][1];
            bids[j][0] = temp0;
            bids[j][1] = temp1;
         }
      }
   }
   
   // Bubble sort для asks (ascending)
   for(int i = 0; i < ArraySize(asks) - 1; i++)
   {
      for(int j = i + 1; j < ArraySize(asks); j++)
      {
         if(asks[i][0] > asks[j][0])
         {
            double temp0 = asks[i][0];
            double temp1 = asks[i][1];
            asks[i][0] = asks[j][0];
            asks[i][1] = asks[j][1];
            asks[j][0] = temp0;
            asks[j][1] = temp1;
         }
      }
   }
   
   // Ограничиваем глубину
   if(ArraySize(bids) > MaxDepth)
      ArrayResize(bids, MaxDepth);
   if(ArraySize(asks) > MaxDepth)
      ArrayResize(asks, MaxDepth);
   
   // Формируем JSON
   long ts = (long)(TimeCurrent()) * 1000; // миллисекунды
   
   string json = StringFormat("{\"ts\":%I64d,\"symbol\":\"%s\",\"bids\":[", ts, symbol);
   
   // Bids array
   for(int i = 0; i < ArraySize(bids); i++)
   {
      json += StringFormat("[%.5f,%.2f]", bids[i][0], bids[i][1]);
      if(i < ArraySize(bids) - 1)
         json += ",";
   }
   
   json += "],\"asks\":[";
   
   // Asks array
   for(int i = 0; i < ArraySize(asks); i++)
   {
      json += StringFormat("[%.5f,%.2f]", asks[i][0], asks[i][1]);
      if(i < ArraySize(asks) - 1)
         json += ",";
   }
   
   json += "]}";
   
   // Конвертируем в byte array для WebRequest
   char data[];
   StringToCharArray(json, data, 0, WHOLE_ARRAY, CP_UTF8);
   ArrayResize(data, ArraySize(data) - 1); // Удаляем trailing null
   
   // Подготовка запроса
   char result[];
   string headers = "Content-Type: application/json\r\n";
   string resp_headers = "";
   
   // Отправляем POST запрос
   int statusCode = WebRequest(
      "POST",
      EndpointBook,
      headers,
      TimeoutMs,
      data,
      result,
      resp_headers
   );
   
   // Обработка ответа
   if(statusCode == 200)
   {
      successCounter++;
      
      // Периодическое логирование
      if(EnableLogging && updateCounter % LogEveryNUpdates == 0)
      {
         double successRate = (double)successCounter / updateCounter * 100;
         Print("📊 Book: ", updateCounter, " обновлений | Success: ", 
               successCounter, " (", DoubleToString(successRate, 1), "%) | ",
               "Depth: ", ArraySize(bids), " bids, ", ArraySize(asks), " asks");
      }
   }
   else if(statusCode == -1)
   {
      // Ошибка WebRequest
      errorCounter++;
      
      if(errorCounter == 1)
      {
         int lastError = GetLastError();
         Print("❌ ОШИБКА WebRequest (Order Book)!");
         Print("   Error code: ", lastError);
         Print("   Проверьте:");
         Print("   1. Tools → Options → Expert Advisors");
         Print("   2. Allow WebRequest включен");
         Print("   3. URL добавлен: ", EndpointBook);
         Print("   4. FastAPI server запущен");
         Print("");
         Print("   (Дальнейшие ошибки не логируются)");
      }
   }
   else
   {
      // HTTP ошибка
      errorCounter++;
      
      if(errorCounter <= 3)
      {
         string response = CharArrayToString(result, 0, WHOLE_ARRAY, CP_UTF8);
         Print("❌ Book HTTP ", statusCode, ": ", response);
      }
   }
   
   // Периодическая статистика (каждые 60 секунд)
   datetime currentTime = TimeCurrent();
   if(currentTime - lastLogTime >= 60)
   {
      double successRate = (updateCounter > 0 ? (double)successCounter / updateCounter * 100 : 0);
      double updatesPerSec = (double)updateCounter / 60.0;
      
      Print("═══════════════════════════════════════════");
      Print("  Order Book статистика за минуту");
      Print("═══════════════════════════════════════════");
      Print("  Обновлений: ", updateCounter);
      Print("  Успешно: ", successCounter, " (", DoubleToString(successRate, 1), "%)");
      Print("  Ошибок: ", errorCounter);
      Print("  Rate: ", DoubleToString(updatesPerSec, 2), " обновлений/сек");
      Print("═══════════════════════════════════════════");
      
      // Сброс счетчиков
      updateCounter = 0;
      successCounter = 0;
      errorCounter = 0;
      lastLogTime = currentTime;
   }
}

//+------------------------------------------------------------------+

