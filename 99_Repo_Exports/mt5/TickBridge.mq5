string TrimString(const string value)
{
   string tmp = value;
   StringTrimLeft(tmp);
   StringTrimRight(tmp);
   return tmp;
}

//+------------------------------------------------------------------+
//|                                                   TickBridge.mq5 |
//|                   Tick Bridge + TP/SL Events to Go Gateway       |
//+------------------------------------------------------------------+
#property copyright "Scanner Infrastructure Team"
#property version   "1.11"
#property strict

//================= INPUTS =================
input string TickEndpoint      = "http://127.0.0.1:8088/tick";
input int    TickTimeoutMs     = 300;
input bool   EnableLogging     = true;
input int    LogEveryNTicks    = 100;

input string GatewayBaseURL    = "http://127.0.0.1:8090";
input string EventsEndpoint    = "/events/publish";
input bool   EnableTPTracking  = true;
input int    PositionsCheckSec = 1;

//================= GLOBALS =================
int      tickCounter           = 0;
int      errorCounter          = 0;
int      successCounter        = 0;
int      consecutiveHttpErrors = 0;
datetime lastLogTime           = 0;
int      symbolDigits          = 5;

#define MAX_POS 100

struct PositionState
{
   ulong  ticket;
   string sid;
   double tp1;
   double tp2;
   double tp3;
   double openPrice;
   double volume;
   bool   isBuy;
   bool   tp1Hit;
   bool   tp2Hit;
   bool   tp3Hit;
};

PositionState g_positions[MAX_POS];

const int MAX_HTTP_ERRORS_BEFORE_SILENCE = 20;
const int ERROR_SILENT_WINDOW            = 50;
int       errorSilenceCountdown          = 0;

//+------------------------------------------------------------------+
void ResetPositionState(PositionState &state)
{
   state.ticket      = 0;
   state.sid         = "";
   state.tp1         = 0.0;
   state.tp2         = 0.0;
   state.tp3         = 0.0;
   state.openPrice   = 0.0;
   state.volume      = 0.0;
   state.isBuy       = true;
   state.tp1Hit      = false;
   state.tp2Hit      = false;
   state.tp3Hit      = false;
}

void InitPosArrays()
{
   int i;
   for(i = 0; i < MAX_POS; i++)
   {
      ResetPositionState(g_positions[i]);
   }
}

int FindPositionIndex(const ulong ticket)
{
   int i;
   for(i = 0; i < MAX_POS; i++)
   {
      if(g_positions[i].ticket == ticket)
         return i;
   }
   return -1;
}

int FindFreePositionIndex()
{
   int i;
   for(i = 0; i < MAX_POS; i++)
   {
      if(g_positions[i].ticket == 0)
         return i;
   }

   return -1;
}

int AcquireSlotForTicket(const ulong ticket)
{
   int idx = FindPositionIndex(ticket);
   if(idx >= 0)
      return idx;

   idx = FindFreePositionIndex();
   if(idx < 0)
      return -1;

   ResetPositionState(g_positions[idx]);
   g_positions[idx].ticket = ticket;
   return idx;
}

void ReleaseSlot(const int idx)
{
   if(idx < 0 || idx >= MAX_POS)
      return;

   ResetPositionState(g_positions[idx]);
}

//+------------------------------------------------------------------+
struct ParsedComment
{
   bool   ok;
   string sid;
   double tp1;
   double tp2;
   double tp3;
};

ParsedComment ParseCommentForSignal(const string comment)
{
   ParsedComment result;
   string parts[];
   string tps[];
   int    cnt;
   int    tpCount;
   int    i;
   string trimmed;

   result.ok  = false;
   result.sid = "";
   result.tp1 = 0.0;
   result.tp2 = 0.0;
   result.tp3 = 0.0;

   if(StringLen(comment) == 0)
      return result;

   cnt = StringSplit(comment, '|', parts);
   if(cnt <= 0)
      return result;

   for(i = 0; i < cnt; i++)
   {
      trimmed = TrimString(parts[i]);
      if(StringLen(trimmed) == 0)
         continue;

      if(StringFind(trimmed, "SID:") == 0)
      {
         result.sid = StringSubstr(trimmed, 4);
      }
      else if(StringFind(trimmed, "TP:") == 0)
      {
         tpCount = StringSplit(StringSubstr(trimmed, 3), ',', tps);
         if(tpCount >= 1) result.tp1 = StringToDouble(TrimString(tps[0]));
         if(tpCount >= 2) result.tp2 = StringToDouble(TrimString(tps[1]));
         if(tpCount >= 3) result.tp3 = StringToDouble(TrimString(tps[2]));
      }
   }

   result.ok = (StringLen(result.sid) > 0);
   return result;
}

//+------------------------------------------------------------------+
bool PublishTradeEvent(const string event_type, const string sid, const string symbol, const ulong ticket, const double price, const double lot)
{
   int res;
   long ts_ms;
   string json;
   string url;
   string headers;
   string resp_headers;
   uchar data[];
   uchar result[];

   ts_ms = (long)TimeCurrent() * 1000;
   json = StringFormat(
      "{\"event_type\":\"%s\",\"sid\":\"%s\",\"symbol\":\"%s\",\"position_id\":\"%I64d\",\"ticket\":\"%I64d\",\"price\":\"%s\",\"lot\":\"%s\",\"ts\":\"%I64d\",\"source\":\"mt5\"}",
      event_type,
      sid,
      symbol,
      (long)ticket,
      (long)ticket,
      DoubleToString(price, symbolDigits),
      DoubleToString(lot, 2),
      ts_ms
   );

   StringToCharArray(json, data, 0, WHOLE_ARRAY, CP_UTF8);
   if(ArraySize(data) > 0)
      ArrayResize(data, ArraySize(data) - 1);

   url = GatewayBaseURL + EventsEndpoint;
   headers = "Content-Type: application/json\r\n";
   resp_headers = "";

   ResetLastError();
   res = WebRequest("POST", url, "", headers, 3000, data, ArraySize(data), result, resp_headers);

   if(res == 200)
   {
      Print("Event published: ", event_type, " sid=", sid, " ticket=", ticket);
      return true;
   }
   else
   {
      Print("Event publish failed: ", event_type, " http=", res, " err=", GetLastError());
      return false;
   }
}

//+------------------------------------------------------------------+
void CheckOneStoredPosition(const int idx)
{
   double bid;
   double ask;
   double current;
   double sl;
   bool   slHit;
   bool   stateChanged = false;
   PositionState state;

   if(idx < 0 || idx >= MAX_POS)
      return;

   state = g_positions[idx];

   if(state.ticket == 0)
      return;

   if(!PositionSelectByTicket(state.ticket))
   {
      ReleaseSlot(idx);
      return;
   }

   bid     = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   ask     = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   current = state.isBuy ? bid : ask;

   if(state.tp1 > 0.0 && !state.tp1Hit)
   {
      if((state.isBuy && current >= state.tp1) || (!state.isBuy && current <= state.tp1))
      {
         state.tp1Hit = true;
         PublishTradeEvent("TP1_HIT", state.sid, _Symbol, state.ticket, current, state.volume);
         stateChanged = true;
      }
   }

   if(state.tp2 > 0.0 && state.tp1Hit && !state.tp2Hit)
   {
      if((state.isBuy && current >= state.tp2) || (!state.isBuy && current <= state.tp2))
      {
         state.tp2Hit = true;
         PublishTradeEvent("TP2_HIT", state.sid, _Symbol, state.ticket, current, state.volume);
         stateChanged = true;
      }
   }

   if(state.tp3 > 0.0 && state.tp2Hit && !state.tp3Hit)
   {
      if((state.isBuy && current >= state.tp3) || (!state.isBuy && current <= state.tp3))
      {
         state.tp3Hit = true;
         PublishTradeEvent("TP3_HIT", state.sid, _Symbol, state.ticket, current, state.volume);
         stateChanged = true;
      }
   }

   sl = PositionGetDouble(POSITION_SL);
   if(sl > 0.0)
   {
      slHit = (state.isBuy && current <= sl) || (!state.isBuy && current >= sl);
      if(slHit)
      {
         PublishTradeEvent("SL_HIT", state.sid, _Symbol, state.ticket, current, state.volume);
         ReleaseSlot(idx);
         return;
      }
   }

   if(stateChanged)
      g_positions[idx] = state;
}

void SyncActivePositions(const int totalPositions)
{
   int    idx;
   int    limit;
   int    iterator;
   bool   isNew;
   ulong  ticket;
   string sym;
   string comment;
   ParsedComment parsed;

   limit = totalPositions;
   if(limit > MAX_POS)
      limit = MAX_POS;

   iterator = 0;
   while(iterator < limit)
   {
      ticket = PositionGetTicket(iterator);
      if(ticket == 0)
      {
         iterator++;
         continue;
      }

      if(!PositionSelectByTicket(ticket))
      {
         iterator++;
         continue;
      }

      sym = PositionGetString(POSITION_SYMBOL);
      if(sym != _Symbol)
      {
         iterator++;
         continue;
      }

      idx = FindPositionIndex(ticket);
      isNew = (idx < 0);

      if(isNew)
      {
         idx = AcquireSlotForTicket(ticket);
         if(idx < 0)
         {
            iterator++;
            continue;
         }
      }

      g_positions[idx].isBuy     = (PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY);
      g_positions[idx].volume    = PositionGetDouble(POSITION_VOLUME);
      g_positions[idx].openPrice = PositionGetDouble(POSITION_PRICE_OPEN);

      if(isNew)
      {
         comment = PositionGetString(POSITION_COMMENT);
         parsed  = ParseCommentForSignal(comment);

         if(parsed.ok)
         {
            g_positions[idx].sid    = parsed.sid;
            g_positions[idx].tp1    = parsed.tp1;
            g_positions[idx].tp2    = parsed.tp2;
            g_positions[idx].tp3    = parsed.tp3;
            g_positions[idx].tp1Hit = false;
            g_positions[idx].tp2Hit = false;
            g_positions[idx].tp3Hit = false;
            PublishTradeEvent("POSITION_OPENED", g_positions[idx].sid, _Symbol, g_positions[idx].ticket, g_positions[idx].openPrice, g_positions[idx].volume);
         }
         else
         {
            g_positions[idx].sid    = "";
            g_positions[idx].tp1    = 0.0;
            g_positions[idx].tp2    = 0.0;
            g_positions[idx].tp3    = 0.0;
            g_positions[idx].tp1Hit = false;
            g_positions[idx].tp2Hit = false;
            g_positions[idx].tp3Hit = false;
         }
      }

      iterator++;
   }
}

void SyncStoredPositionStates()
{
   int idx = 0;
   while(idx < MAX_POS)
   {
      if(g_positions[idx].ticket != 0)
         CheckOneStoredPosition(idx);
      idx++;
   }
}

//+------------------------------------------------------------------+
void SyncPositions()
{
   int total;

   if(!EnableTPTracking)
      return;

   total = PositionsTotal();
   SyncActivePositions(total);
   SyncStoredPositionStates();
}

//+------------------------------------------------------------------+
int OnInit()
{
   int sec;
   
   Print("============================================");
   Print("  TickBridge EA initialized v1.11");
   Print("============================================");
   Print("  Symbol: ", _Symbol);
   Print("  TickEndpoint: ", TickEndpoint);
   Print("  Gateway: ", GatewayBaseURL, EventsEndpoint);
   Print("============================================");

   tickCounter    = 0;
   errorCounter   = 0;
   successCounter = 0;
   consecutiveHttpErrors = 0;
   errorSilenceCountdown = 0;
   lastLogTime    = TimeCurrent();
   symbolDigits   = (int)SymbolInfoInteger(_Symbol, SYMBOL_DIGITS);
   if(symbolDigits <= 0)
      symbolDigits = 5;

   InitPosArrays();

   sec = PositionsCheckSec;
   if(sec < 1) sec = 1;
   EventSetTimer(sec);

   return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
   double sr;
   
   EventKillTimer();

   Print("============================================");
   Print("  TickBridge EA stopped");
   Print("============================================");
   Print("  Ticks sent: ", tickCounter);
   Print("  Success: ", successCounter);
   Print("  Errors: ", errorCounter);
   sr = (tickCounter>0) ? ((double)successCounter/(double)tickCounter*100.0) : 0.0;
   Print("  Success rate: ", DoubleToString(sr,1), "%");
   Print("============================================");
}

//+------------------------------------------------------------------+
void OnTimer()
{
   SyncPositions();
}

//+------------------------------------------------------------------+
void OnTick()
{
   MqlTick tick;
   string json;
   string headers;
   string resp_headers;
   uchar data[];
   uchar result[];
   int statusCode;
   double successRate;
   string response;
   
   if(!SymbolInfoTick(_Symbol, tick))
   {
      Print("Error getting tick for ", _Symbol);
      return;
   }

   tickCounter = tickCounter + 1;
   if(errorSilenceCountdown > 0)
      errorSilenceCountdown--;

   json = StringFormat(
      "{\"ts\":%I64d,\"bid\":%s,\"ask\":%s,\"last\":%s,\"volume\":%.2f,\"flags\":%u,\"symbol\":\"%s\"}",
      (long)tick.time_msc,
      DoubleToString(tick.bid, symbolDigits),
      DoubleToString(tick.ask, symbolDigits),
      DoubleToString(tick.last, symbolDigits),
      tick.volume,
      (uint)tick.flags,
      _Symbol
   );

   StringToCharArray(json, data, 0, WHOLE_ARRAY, CP_UTF8);
   if(ArraySize(data) > 0)
      ArrayResize(data, ArraySize(data)-1);

   resp_headers = "";
   headers = "Content-Type: application/json\r\n";

   statusCode = WebRequest("POST", TickEndpoint, "", headers, TickTimeoutMs, data, ArraySize(data), result, resp_headers);

   if(statusCode == 200)
   {
      successCounter = successCounter + 1;
      consecutiveHttpErrors = 0;

      if(EnableLogging && tickCounter % LogEveryNTicks == 0)
      {
         successRate = (double)successCounter / (double)tickCounter * 100.0;
         Print("TickBridge: ", tickCounter, " ticks | Success: ",
               successCounter, " (", DoubleToString(successRate, 1), "%) | Errors: ", errorCounter);
      }
   }
   else if(statusCode == -1)
   {
      errorCounter = errorCounter + 1;
      consecutiveHttpErrors = consecutiveHttpErrors + 1;
      if(consecutiveHttpErrors >= MAX_HTTP_ERRORS_BEFORE_SILENCE && errorSilenceCountdown == 0)
         errorSilenceCountdown = ERROR_SILENT_WINDOW;

      if(errorCounter == 1)
      {
         Print("WebRequest error (ticks) = ", GetLastError());
         Print("Check URL: ", TickEndpoint);
      }
   }
   else
   {
      errorCounter = errorCounter + 1;
      consecutiveHttpErrors = consecutiveHttpErrors + 1;
      if(consecutiveHttpErrors >= MAX_HTTP_ERRORS_BEFORE_SILENCE && errorSilenceCountdown == 0)
         errorSilenceCountdown = ERROR_SILENT_WINDOW;

      if((errorCounter <= 5 || EnableLogging) && errorSilenceCountdown == 0)
      {
         response = CharArrayToString(result, 0, -1, CP_UTF8);
         Print("HTTP ", statusCode, ": ", response);
      }
   }
}
//+------------------------------------------------------------------+


