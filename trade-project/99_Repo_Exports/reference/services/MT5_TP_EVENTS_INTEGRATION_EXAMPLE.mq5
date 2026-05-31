//+------------------------------------------------------------------+
//| MT5 TP Events Integration Example                                |
//| Пример интеграции публикации TP/SL событий в Redis               |
//+------------------------------------------------------------------+
//| Этот код показывает как публиковать события TP1/TP2/TP3/SL      |
//| из MT5 EA в Redis stream events:trades для TP1 Trailing System  |
//|                                                                  |
//| ИНТЕГРАЦИЯ:                                                       |
//| 1. Добавьте эти функции в ваш существующий MT5 EA               |
//| 2. Вызывайте PublishTPEvent() при достижении TP/SL уровней      |
//| 3. Убедитесь что go-gateway запущен и доступен                  |
//+------------------------------------------------------------------+

// Глобальные переменные для связи с gateway
string g_GatewayURL = "http://scanner-go-gateway:8090";
string g_EventsEndpoint = "/events/publish";  // Новый endpoint для событий

//+------------------------------------------------------------------+
//| Структура события для публикации                                 |
//+------------------------------------------------------------------+
struct TradeEvent
{
   string event_type;     // TP1_HIT, TP2_HIT, TP3_HIT, SL_HIT, etc
   string sid;            // Signal ID
   string symbol;         // Symbol (XAUUSD, etc)
   string position_id;    // MT5 ticket
   double price;          // Цена срабатывания
   double lot;            // Объём позиции
   long   timestamp;      // Timestamp в миллисекундах
   string source;         // "mt5"
};

//+------------------------------------------------------------------+
//| Публикация события TP/SL в Redis через go-gateway                |
//+------------------------------------------------------------------+
bool PublishTPEvent(TradeEvent &event)
{
   // Формируем JSON payload
   string json = "";
   json += "{";
   json += "\"event_type\":\"" + event.event_type + "\",";
   json += "\"sid\":\"" + event.sid + "\",";
   json += "\"symbol\":\"" + event.symbol + "\",";
   json += "\"position_id\":\"" + event.position_id + "\",";
   json += "\"ticket\":\"" + event.position_id + "\",";  // Дублируем для совместимости
   json += "\"price\":\"" + DoubleToString(event.price, _Digits) + "\",";
   json += "\"lot\":\"" + DoubleToString(event.lot, 2) + "\",";
   json += "\"ts\":\"" + IntegerToString(event.timestamp) + "\",";
   json += "\"source\":\"" + event.source + "\"";
   json += "}";
   
   // Отправляем в gateway
   char data[];
   char result[];
   string headers = "Content-Type: application/json\r\n";
   
   StringToCharArray(json, data, 0, StringLen(json));
   
   string url = g_GatewayURL + g_EventsEndpoint;
   
   int timeout = 3000;  // 3 секунды
   int res = WebRequest(
      "POST",
      url,
      headers,
      timeout,
      data,
      result,
      headers
   );
   
   if(res == 200)
   {
      Print("✅ Event published: ", event.event_type, " for ", event.sid);
      return true;
   }
   else
   {
      Print("❌ Failed to publish event: ", event.event_type, " status=", res);
      return false;
   }
}

//+------------------------------------------------------------------+
//| Пример: Проверка достижения TP уровней                           |
//+------------------------------------------------------------------+
void CheckTPLevels(string signal_id, ulong ticket, double open_price, 
                   double tp1, double tp2, double tp3, bool is_buy)
{
   static bool tp1_hit = false;
   static bool tp2_hit = false;
   static bool tp3_hit = false;
   
   // Получаем текущую позицию
   if(!PositionSelectByTicket(ticket))
      return;
   
   double current_price = is_buy ? SymbolInfoDouble(_Symbol, SYMBOL_BID) 
                                 : SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   
   // Проверяем TP1
   if(!tp1_hit)
   {
      bool tp1_reached = is_buy ? (current_price >= tp1) : (current_price <= tp1);
      
      if(tp1_reached)
      {
         tp1_hit = true;
         
         // Публикуем событие TP1_HIT
         TradeEvent event;
         event.event_type = "TP1_HIT";
         event.sid = signal_id;
         event.symbol = _Symbol;
         event.position_id = IntegerToString(ticket);
         event.price = current_price;
         event.lot = PositionGetDouble(POSITION_VOLUME);
         event.timestamp = TimeCurrent() * 1000;  // Миллисекунды
         event.source = "mt5";
         
         PublishTPEvent(event);
         
         Print("🎯 TP1 HIT: ", signal_id, " @ ", current_price);
      }
   }
   
   // Проверяем TP2
   if(tp1_hit && !tp2_hit)
   {
      bool tp2_reached = is_buy ? (current_price >= tp2) : (current_price <= tp2);
      
      if(tp2_reached)
      {
         tp2_hit = true;
         
         TradeEvent event;
         event.event_type = "TP2_HIT";
         event.sid = signal_id;
         event.symbol = _Symbol;
         event.position_id = IntegerToString(ticket);
         event.price = current_price;
         event.lot = PositionGetDouble(POSITION_VOLUME);
         event.timestamp = TimeCurrent() * 1000;
         event.source = "mt5";
         
         PublishTPEvent(event);
         
         Print("🎯 TP2 HIT: ", signal_id, " @ ", current_price);
      }
   }
   
   // Проверяем TP3
   if(tp2_hit && !tp3_hit)
   {
      bool tp3_reached = is_buy ? (current_price >= tp3) : (current_price <= tp3);
      
      if(tp3_reached)
      {
         tp3_hit = true;
         
         TradeEvent event;
         event.event_type = "TP3_HIT";
         event.sid = signal_id;
         event.symbol = _Symbol;
         event.position_id = IntegerToString(ticket);
         event.price = current_price;
         event.lot = PositionGetDouble(POSITION_VOLUME);
         event.timestamp = TimeCurrent() * 1000;
         event.source = "mt5";
         
         PublishTPEvent(event);
         
         Print("🎯 TP3 HIT: ", signal_id, " @ ", current_price);
      }
   }
}

//+------------------------------------------------------------------+
//| Пример: Публикация события открытия позиции                      |
//+------------------------------------------------------------------+
bool PublishPositionOpened(string signal_id, ulong ticket, double price, 
                          double lot, double sl, double tp1, double tp2, double tp3)
{
   TradeEvent event;
   event.event_type = "POSITION_OPENED";
   event.sid = signal_id;
   event.symbol = _Symbol;
   event.position_id = IntegerToString(ticket);
   event.price = price;
   event.lot = lot;
   event.timestamp = TimeCurrent() * 1000;
   event.source = "mt5";
   
   return PublishTPEvent(event);
}

//+------------------------------------------------------------------+
//| Пример: Публикация события SL                                    |
//+------------------------------------------------------------------+
bool PublishSLHit(string signal_id, ulong ticket, double price, double lot)
{
   TradeEvent event;
   event.event_type = "SL_HIT";
   event.sid = signal_id;
   event.symbol = _Symbol;
   event.position_id = IntegerToString(ticket);
   event.price = price;
   event.lot = lot;
   event.timestamp = TimeCurrent() * 1000;
   event.source = "mt5";
   
   return PublishTPEvent(event);
}

//+------------------------------------------------------------------+
//| ИНТЕГРАЦИЯ В ОСНОВНОЙ EA:                                         |
//|                                                                  |
//| 1. В OnInit():                                                   |
//|    - Настройте g_GatewayURL если нужно                          |
//|                                                                  |
//| 2. В OnTick():                                                   |
//|    - Вызывайте CheckTPLevels() для каждой активной позиции      |
//|                                                                  |
//| 3. После открытия позиции:                                       |
//|    - Вызовите PublishPositionOpened()                           |
//|                                                                  |
//| 4. При срабатывании SL:                                          |
//|    - Вызовите PublishSLHit()                                    |
//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
//| Пример использования в OnTick()                                  |
//+------------------------------------------------------------------+
/*
void OnTick()
{
   // Ваш существующий код...
   
   // Проверяем TP уровни для активных позиций
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      
      if(PositionSelectByTicket(ticket))
      {
         string comment = PositionGetString(POSITION_COMMENT);
         
         // Извлекаем signal_id из комментария
         // Предполагаем формат: "SIG:signal-XAUUSD-123456"
         if(StringFind(comment, "SIG:") >= 0)
         {
            string signal_id = StringSubstr(comment, 4);
            
            double open_price = PositionGetDouble(POSITION_PRICE_OPEN);
            bool is_buy = (PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY);
            
            // Получаем TP уровни (нужно сохранить их при открытии)
            double tp1 = 0, tp2 = 0, tp3 = 0;
            // TODO: Загрузить TP уровни из глобальных переменных или файла
            
            CheckTPLevels(signal_id, ticket, open_price, tp1, tp2, tp3, is_buy);
         }
      }
   }
}
*/

//+------------------------------------------------------------------+
//| Публикация TRAILING_MOVE события                                 |
//+------------------------------------------------------------------+
bool PublishTrailingMove(string signal_id, double new_sl, double current_price, string profile)
{
   // Формируем JSON для TRAILING_MOVE события
   string json = "";
   json += "{";
   json += "\"event_type\":\"TRAILING_MOVE\",";
   json += "\"sid\":\"" + signal_id + "\",";
   json += "\"symbol\":\"" + _Symbol + "\",";
   json += "\"new_sl\":\"" + DoubleToString(new_sl, _Digits) + "\",";
   json += "\"profile\":\"" + profile + "\",";
   json += "\"ts\":\"" + IntegerToString(TimeCurrent() * 1000) + "\",";
   json += "\"source\":\"mt5\",";
   json += "\"metadata\":{";
   json += "\"current_price\":\"" + DoubleToString(current_price, _Digits) + "\"";
   json += "}";
   json += "}";
   
   // Отправляем в gateway
   char data[];
   char result[];
   string headers = "Content-Type: application/json\r\n";
   
   StringToCharArray(json, data, 0, StringLen(json));
   
   string url = g_GatewayURL + g_EventsEndpoint;
   
   int timeout = 3000;
   int res = WebRequest(
      "POST",
      url,
      headers,
      timeout,
      data,
      result,
      headers
   );
   
   if(res == 200)
   {
      Print("✅ TRAILING_MOVE published: new_sl=", new_sl);
      return true;
   }
   else
   {
      Print("❌ Failed to publish TRAILING_MOVE: status=", res);
      return false;
   }
}

//+------------------------------------------------------------------+
//| Пример: Отслеживание движения trailing stop в OnTick()           |
//+------------------------------------------------------------------+
/*
// Глобальные переменные для отслеживания
double g_LastKnownSL = 0;
string g_SignalID = "";
string g_TrailingProfile = "rocket_v1";

void OnTick()
{
   // Ваш существующий код...
   
   // Проверяем активную позицию
   if(PositionSelect(_Symbol))
   {
      double current_sl = PositionGetDouble(POSITION_SL);
      double current_price = SymbolInfoDouble(_Symbol, SYMBOL_BID);
      
      // Если SL изменился значительно (> 5 пунктов)
      if(MathAbs(current_sl - g_LastKnownSL) > _Point * 5)
      {
         // Логируем движение trailing stop
         PublishTrailingMove(
            g_SignalID,
            current_sl,
            current_price,
            g_TrailingProfile
         );
         
         // Обновляем кэш
         g_LastKnownSL = current_sl;
         
         Print("📈 Trailing moved: SL=", current_sl, " Price=", current_price);
      }
   }
}
*/

//+------------------------------------------------------------------+
//| ТРЕБОВАНИЯ:                                                       |
//| 1. В MT5 добавьте в список разрешённых URL:                     |
//|    Tools -> Options -> Expert Advisors -> Allow WebRequest      |
//|    Добавьте: http://scanner-go-gateway:8090                     |
//|                                                                  |
//| 2. В go-gateway нужен endpoint /events/publish который          |
//|    принимает JSON и публикует в Redis stream events:trades      |
//|    (см. go-gateway/internal/events/trade_events.go)             |
//|                                                                  |
//| 3. Для trade_back анализа критично логировать TRAILING_MOVE!   |
//|    Это позволит рассчитать:                                     |
//|    - Как далеко удалось утащить SL                             |
//|    - Эффективность профилей трейлинга                           |
//|    - Winrate с учётом trailing protection                      |
//+------------------------------------------------------------------+

