//+------------------------------------------------------------------+
//|                                                   TickBridge.mq5 |
//|            Tick Bridge + TP/SL Events → Go Gateway (fixed)       |
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
int      tickCounter    = 0;
int      errorCounter   = 0;
int      successCounter = 0;
datetime lastLogTime    = 0;

#define MAX_POS 100
ulong  g_posTickets[MAX_POS];
double g_posTP1[MAX_POS];
double g_posTP2[MAX_POS];
double g_posTP3[MAX_POS];
bool   g_posTP1Hit[MAX_POS];
bool   g_posTP2Hit[MAX_POS];
bool   g_posTP3Hit[MAX_POS];
string g_posSID[MAX_POS];
bool   g_posIsBuy[MAX_POS];

//+------------------------------------------------------------------+
//| init arrays                                                      |
//+------------------------------------------------------------------+
void InitPosArrays()
{
   int i;
   for(i=0; i<MAX_POS; i++)
   {
      g_posTickets[i] = 0;
      g_posTP1[i]     = 0.0;
      g_posTP2[i]     = 0.0;
      g_posTP3[i]     = 0.0;
      g_posTP1Hit[i]  = false;
      g_posTP2Hit[i]  = false;
      g_posTP3Hit[i]  = false;
      g_posSID[i]     = "";
      g_posIsBuy[i]   = true;
   }
}

//+------------------------------------------------------------------+
//| find index helpers                                               |
//+------------------------------------------------------------------+
int FindPosIndexByTicket(ulong ticket)
{
   int i;
   for(i=0; i<MAX_POS; i++)
   {
      if(g_posTickets[i] == ticket)
         return i;
   }
   return -1;
}

int FindFreePosIndex()
{
   int i;
   for(i=0; i<MAX_POS; i++)
   {
      if(g_posTickets[i] == 0)
         return i;
   }
   return -1;
}

//+------------------------------------------------------------------+
//| Parse comment: "SID:xxx|TP:1,2,3"                                |
//+------------------------------------------------------------------+
bool ParseCommentForSignal(const string comment,
                           string &sid,
                           double &tp1,
                           double &tp2,
                           double &tp3)
{
   int i, cnt, n;
   string parts[], p, list, tps[];
   
   sid = "";
   tp1 = tp2 = tp3 = 0.0;

   if(StringLen(comment) == 0)
      return false;

   cnt = StringSplit(comment, '|', parts);
   if(cnt < 1)
      return false;

   for(i = 0; i < cnt; i++)
   {
      p = parts[i];
      if(StringFind(p, "SID:") == 0)
      {
         sid = StringSubstr(p, 4);
      }
      else if(StringFind(p, "TP:") == 0)
      {
         list = StringSubstr(p, 3);
         n = StringSplit(list, ',', tps);
         if(n >= 1) tp1 = StringToDouble(tps[0]);
         if(n >= 2) tp2 = StringToDouble(tps[1]);
         if(n >= 3) tp3 = StringToDouble(tps[2]);
      }
   }

   return (StringLen(sid) > 0);
}

//+------------------------------------------------------------------+
//| Publish trade event to gateway                                   |
//+------------------------------------------------------------------+
bool PublishTradeEvent(const string event_type,
                       const string sid,
                       const string symbol,
                       ulong   ticket,
                       double  price,
                       double  lot)
{
   int digits, res;
   long ts_ms;
   string json, url, headers, resp_headers;
   uchar data[], result[];

   digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   ts_ms = (long)TimeCurrent() * 1000;

   json = "{";
   json += "\"event_type\":\"" + event_type + "\",";
   json += "\"sid\":\"" + sid + "\",";
   json += "\"symbol\":\"" + symbol + "\",";
   json += "\"position_id\":\"" + IntegerToString((long)ticket) + "\",";
   json += "\"ticket\":\""      + IntegerToString((long)ticket) + "\",";
   json += "\"price\":\""       + DoubleToString(price, digits) + "\",";
   json += "\"lot\":\""         + DoubleToString(lot, 2) + "\",";
   json += "\"ts\":\""          + IntegerToString(ts_ms) + "\",";
   json += "\"source\":\"mt5\"";
   json += "}";

   StringToCharArray(json, data, 0, WHOLE_ARRAY, CP_UTF8);
   if(ArraySize(data) > 0)
      ArrayResize(data, ArraySize(data) - 1);

   url = GatewayBaseURL + EventsEndpoint;
   headers = "Content-Type: application/json\r\n";
   resp_headers = "";

   ResetLastError();
   res = WebRequest("POST",
                    url,
                    "",
                    headers,
                    3000,
                    data,
                    ArraySize(data),
                    result,
                    resp_headers);

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
//| Check one stored position                                        |
//+------------------------------------------------------------------+
void CheckOneStoredPosition(const int idx)
{
   ulong ticket;
   double volume, bid, ask, current, sl;
   bool hit1, hit2, hit3, sl_hit;

   ticket = g_posTickets[idx];
   if(ticket == 0)
      return;

   if(!PositionSelectByTicket(ticket))
   {
      g_posTickets[idx] = 0;
      return;
   }

   volume  = PositionGetDouble(POSITION_VOLUME);
   bid     = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   ask     = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   current = g_posIsBuy[idx] ? bid : ask;

   if(g_posTP1[idx] > 0.0 && !g_posTP1Hit[idx])
   {
      hit1 = g_posIsBuy[idx] ? (current >= g_posTP1[idx]) : (current <= g_posTP1[idx]);
      if(hit1)
      {
         g_posTP1Hit[idx] = true;
         PublishTradeEvent("TP1_HIT", g_posSID[idx], _Symbol, ticket, current, volume);
      }
   }

   if(g_posTP2[idx] > 0.0 && g_posTP1Hit[idx] && !g_posTP2Hit[idx])
   {
      hit2 = g_posIsBuy[idx] ? (current >= g_posTP2[idx]) : (current <= g_posTP2[idx]);
      if(hit2)
      {
         g_posTP2Hit[idx] = true;
         PublishTradeEvent("TP2_HIT", g_posSID[idx], _Symbol, ticket, current, volume);
      }
   }

   if(g_posTP3[idx] > 0.0 && g_posTP2Hit[idx] && !g_posTP3Hit[idx])
   {
      hit3 = g_posIsBuy[idx] ? (current >= g_posTP3[idx]) : (current <= g_posTP3[idx]);
      if(hit3)
      {
         g_posTP3Hit[idx] = true;
         PublishTradeEvent("TP3_HIT", g_posSID[idx], _Symbol, ticket, current, volume);
      }
   }

   sl = PositionGetDouble(POSITION_SL);
   if(sl > 0.0)
   {
      sl_hit = false;
      if(g_posIsBuy[idx]  && current <= sl) sl_hit = true;
      if(!g_posIsBuy[idx] && current >= sl) sl_hit = true;

      if(sl_hit)
      {
         PublishTradeEvent("SL_HIT", g_posSID[idx], _Symbol, ticket, current, volume);
         g_posTickets[idx] = 0;
      }
   }
}

//+------------------------------------------------------------------+
//| Sync positions from terminal to our slots                        |
//+------------------------------------------------------------------+
void SyncPositions()
{
   int total, i, k, idx, freeIdx;
   ulong ticket;
   string sym, comment, sid;
   double tp1, tp2, tp3, price, vol;
   bool ok;

   if(!EnableTPTracking)
      return;

   total = PositionsTotal();
   
   for(i = 0; i < total; i++)
   {
      if(!PositionSelectByIndex(i))
         continue;

      sym = PositionGetString(POSITION_SYMBOL);
      if(sym != _Symbol)
         continue;

      ticket = (ulong)PositionGetInteger(POSITION_TICKET);
      idx = FindPosIndexByTicket(ticket);

      if(idx < 0)
      {
         comment = PositionGetString(POSITION_COMMENT);
         ok = ParseCommentForSignal(comment, sid, tp1, tp2, tp3);

         freeIdx = FindFreePosIndex();
         if(freeIdx >= 0)
         {
            g_posTickets[freeIdx] = ticket;
            g_posSID[freeIdx]     = (ok ? sid  : "");
            g_posTP1[freeIdx]     = (ok ? tp1 : 0.0);
            g_posTP2[freeIdx]     = (ok ? tp2 : 0.0);
            g_posTP3[freeIdx]     = (ok ? tp3 : 0.0);
            g_posTP1Hit[freeIdx]  = false;
            g_posTP2Hit[freeIdx]  = false;
            g_posTP3Hit[freeIdx]  = false;
            g_posIsBuy[freeIdx]   = (PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY);

            if(ok && StringLen(sid) > 0)
            {
               price = PositionGetDouble(POSITION_PRICE_OPEN);
               vol   = PositionGetDouble(POSITION_VOLUME);
               PublishTradeEvent("POSITION_OPENED", sid, _Symbol, ticket, price, vol);
            }
         }
      }
   }

   for(k = 0; k < MAX_POS; k++)
   {
      if(g_posTickets[k] != 0)
         CheckOneStoredPosition(k);
   }
}

//+------------------------------------------------------------------+
//| OnInit                                                           |
//+------------------------------------------------------------------+
int OnInit()
{
   int sec;
   
   Print("============================================");
   Print("  TickBridge EA initialized (v1.11 + TP events)");
   Print("============================================");
   Print("  Symbol: ", _Symbol);
   Print("  TickEndpoint: ", TickEndpoint);
   Print("  Gateway: ", GatewayBaseURL, EventsEndpoint);
   Print("============================================");
   Print("Check: Tools -> Options -> Expert Advisors -> Allow WebRequest");
   Print("and add:");
   Print("   ", TickEndpoint);
   Print("   ", GatewayBaseURL);

   tickCounter    = 0;
   errorCounter   = 0;
   successCounter = 0;
   lastLogTime    = TimeCurrent();

   InitPosArrays();

   sec = PositionsCheckSec;
   if(sec < 1) sec = 1;
   EventSetTimer(sec);

   return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
//| OnDeinit                                                         |
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
//| OnTimer                                                          |
//+------------------------------------------------------------------+
void OnTimer()
{
   SyncPositions();
}

//+------------------------------------------------------------------+
//| OnTick                                                           |
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

   tickCounter++;

   json = StringFormat(
      "{\"ts\":%I64d,\"bid\":%.5f,\"ask\":%.5f,\"last\":%.5f,\"volume\":%.2f,\"flags\":%u,\"symbol\":\"%s\"}",
      (long)tick.time_msc,
      tick.bid,
      tick.ask,
      tick.last,
      tick.volume,
      (uint)tick.flags,
      _Symbol
   );

   StringToCharArray(json, data, 0, WHOLE_ARRAY, CP_UTF8);
   if(ArraySize(data) > 0)
      ArrayResize(data, ArraySize(data)-1);

   resp_headers = "";
   headers = "Content-Type: application/json\r\n";

   statusCode = WebRequest(
      "POST",
      TickEndpoint,
      "",
      headers,
      TickTimeoutMs,
      data,
      ArraySize(data),
      result,
      resp_headers
   );

   if(statusCode == 200)
   {
      successCounter++;
      if(EnableLogging && tickCounter % LogEveryNTicks == 0)
      {
         successRate = (double)successCounter / (double)tickCounter * 100.0;
         Print("TickBridge: ", tickCounter, " ticks | Success: ",
               successCounter, " (", DoubleToString(successRate, 1), "%) | Errors: ", errorCounter);
      }
   }
   else if(statusCode == -1)
   {
      errorCounter++;
      if(errorCounter == 1)
      {
         Print("WebRequest error (ticks) = ", GetLastError());
         Print("Check URL: ", TickEndpoint);
      }
   }
   else
   {
      errorCounter++;
      if(errorCounter <= 5)
      {
         response = CharArrayToString(result, 0, -1, CP_UTF8);
         Print("HTTP ", statusCode, ": ", response);
      }
   }
}
//+------------------------------------------------------------------+
