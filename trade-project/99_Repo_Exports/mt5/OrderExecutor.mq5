//+------------------------------------------------------------------+
//|                                              OrderExecutor.mq5    |
//|                            XAUUSD Order Executor via HTTP Bridge |
//+------------------------------------------------------------------+
#property copyright "XAUUSD Order Flow System"
#property version   "6.00"
#property strict

//--- Input parameters
input string EndpointPoll    = "http://127.0.0.1:8090/orders/poll";
input string EndpointConfirm = "http://127.0.0.1:8090/orders/confirm";
input int    PollIntervalMs  = 1000;        // Poll interval (ms)
input string SymbolToTrade   = "XAUUSD";    // Symbol to trade
input string TpSplitPerc     = "50,30,20";  // TP split % (future use)
input int    Slippage        = 20;          // Max slippage in points
input int    Magic           = 777001;      // Magic number

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
  {
   Print("OrderExecutor v6.0 initialized");
   Print("Poll endpoint: ", EndpointPoll);
   Print("Symbol: ", SymbolToTrade);
   Print("Poll interval: ", PollIntervalMs, " ms");
   
   EventSetTimer(PollIntervalMs / 1000);
   
   return(INIT_SUCCEEDED);
  }

//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
  {
   EventKillTimer();
   Print("OrderExecutor stopped. Reason: ", reason);
  }

//+------------------------------------------------------------------+
//| Round value to step                                              |
//+------------------------------------------------------------------+
double RoundToStep(double value, double step)
  {
   if(step <= 0) return value;
   return MathRound(value / step) * step;
  }

//+------------------------------------------------------------------+
//| Parse JSON value (simple parser for our use case)               |
//+------------------------------------------------------------------+
bool ParseJsonValue(const string &json, const string &key, string &val)
  {
   int p = StringFind(json, "\"" + key + "\":");
   if(p < 0) return false;
   
   int s = p + StringLen(key) + 3;
   
   // Detect string or number
   if(StringGetCharacter(json, s) == '\"')
     {
      int e = StringFind(json, "\"", s + 1);
      if(e < 0) return false;
      val = StringSubstr(json, s + 1, e - (s + 1));
      return true;
     }
   else
     {
      int e = s;
      while(e < StringLen(json))
        {
         int ch = StringGetCharacter(json, e);
         if((ch >= '0' && ch <= '9') || ch == '.' || ch == '-')
           {
            e++;
            continue;
           }
         break;
        }
      val = StringSubstr(json, s, e - s);
      return true;
     }
  }

//+------------------------------------------------------------------+
//| Parse first element from JSON array                             |
//+------------------------------------------------------------------+
bool ParseJsonArrayFirst(const string &json, const string &key, double &first)
  {
   int p = StringFind(json, "\"" + key + "\":[");
   if(p < 0) return false;
   
   int s = p + StringLen(key) + 4;
   int e = StringFind(json, "]", s);
   if(e < 0) return false;
   
   string arr = StringSubstr(json, s, e - s);
   int coma = StringFind(arr, ",");
   string firsts = (coma >= 0) ? StringSubstr(arr, 0, coma) : arr;
   first = (double)StringToDouble(firsts);
   
   return true;
  }

//+------------------------------------------------------------------+
//| Timer function - polls orders queue                             |
//+------------------------------------------------------------------+
void OnTimer()
  {
   // Poll order from HTTP bridge
   string url = EndpointPoll + "?symbol=" + SymbolToTrade;
   string headers = "Accept: application/json\r\n";
   uchar result[];
   string resp_headers = "";
   
   ResetLastError();
   int status = WebRequest("GET", url, headers, 3000, result, resp_headers);
   
   // 204 = no orders available
   if(status == 204 || ArraySize(result) == 0)
      return;
   
   // -1 = error
   if(status == -1)
     {
      int err = GetLastError();
      if(err != 0)
         Print("WebRequest error: ", err, " (check Expert Advisors settings - allow URL)");
      return;
     }
   
   // Parse response
   string body = CharArrayToString(result, 0, -1, CP_UTF8);
   if(StringLen(body) < 5)
      return;
   
   // Extract action
   string action;
   if(!ParseJsonValue(body, "action", action))
      return;
   
   // Route by action
   if(action == "open")
      ExecuteOpen(body);
   else if(action == "modify")
      ExecuteModify(body);
   else if(action == "resize")
      ExecuteResize(body);
   else if(action == "cancel")
      ExecuteCancel(body);
   else
      Print("Unknown action: ", action);
  }

//+------------------------------------------------------------------+
//| Execute open order                                               |
//+------------------------------------------------------------------+
void ExecuteOpen(const string &json)
  {
   Print("📥 Executing open order...");
   
   // Parse fields
   string lot_s, side, sid, entry_s, sl_s;
   ParseJsonValue(json, "lot", lot_s);
   ParseJsonValue(json, "side", side);
   ParseJsonValue(json, "sid", sid);
   if(StringLen(sid) == 0)
     {
      Print("❌ Empty SID in order payload: ", json);
     }
   else
     {
      Print("📎 Order SID: ", sid);
     }
   ParseJsonValue(json, "entry", entry_s);
   ParseJsonValue(json, "sl", sl_s);
   
   double lot   = (double)StringToDouble(lot_s);
   double entry = (double)StringToDouble(entry_s);
   double sl    = (double)StringToDouble(sl_s);
   double tp1   = 0.0;
   
   // Parse first TP
   ParseJsonArrayFirst(json, "tp_levels", tp1);
   
   // Prepare trade request
   MqlTradeRequest req;
   MqlTradeResult res;
   ZeroMemory(req);
   ZeroMemory(res);
   
   // Order type
   ENUM_ORDER_TYPE ot = (StringCompare(side, "LONG") == 0) ? ORDER_TYPE_BUY : ORDER_TYPE_SELL;
   
   req.action = TRADE_ACTION_DEAL;
   req.symbol = SymbolToTrade;
   req.type   = ot;
   req.magic  = Magic;
   req.volume = (lot > 0) ? lot : 0.1;
   req.deviation = Slippage;
   req.type_filling = ORDER_FILLING_FOK;  // Change to IOC if broker requires
   
   // Market price
   double price = (ot == ORDER_TYPE_BUY) ? 
                  SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK) :
                  SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);
   
   double tick_size = SymbolInfoDouble(SymbolToTrade, SYMBOL_TRADE_TICK_SIZE);
   int    digits    = (int)SymbolInfoInteger(SymbolToTrade, SYMBOL_DIGITS);
   
   req.price = NormalizeDouble(price, digits);
   
   // SL
   if(sl > 0.0)
      req.sl = NormalizeDouble(RoundToStep(sl, tick_size), digits);
   
   // TP (first level)
   if(tp1 > 0.0)
      req.tp = NormalizeDouble(RoundToStep(tp1, tick_size), digits);
   
   // Send order (temporarily disabled)
   // ResetLastError();
   // bool success = OrderSend(req, res);
   //
   // if(!success)
   //   {
   //    Print("❌ OrderSend failed: ", GetLastError());
   //    ConfirmExecution(sid, "failed", 0, 0.0, 0.0, 0.0, GetLastError());
   //    return;
   //   }
   //
   // Print("✅ Order opened: ", res.order, " @ ", res.price);
   //
   // // Confirm execution
   // if(StringLen(sid) == 0)
   //   {
   //    Print("⚠️ ConfirmExecution without SID - check producer payload");
   //   }
   // ConfirmExecution(sid, "opened", res.order, res.price, req.sl, req.tp, 0);

   Print("🚫 OrderSend disabled for XAUUSD - skipping execution");
  }

//+------------------------------------------------------------------+
//| Execute modify SL/TP                                             |
//+------------------------------------------------------------------+
void ExecuteModify(const string &json)
  {
   Print("📥 Executing modify SL/TP...");
   
   // Find position by symbol
   ulong ticket = 0;
   
   for(int i = 0; i < PositionsTotal(); i++)
     {
      string psym = PositionGetSymbol(i);
      if(psym == SymbolToTrade)
        {
         ticket = PositionGetInteger(POSITION_TICKET);
         break;
        }
     }
   
   if(ticket == 0)
     {
      Print("⚠️  No position found for ", SymbolToTrade);
      return;
     }
   
   // Parse SL/TP
   string sl_s;
   double sl = 0.0;
   double tp = 0.0;
   
   if(ParseJsonValue(json, "sl", sl_s))
      sl = (double)StringToDouble(sl_s);
   
   ParseJsonArrayFirst(json, "tp_levels", tp);
   
   // Modify position
   MqlTradeRequest req;
   MqlTradeResult res;
   ZeroMemory(req);
   ZeroMemory(res);
   
   req.action   = TRADE_ACTION_SLTP;
   req.symbol   = SymbolToTrade;
   req.position = ticket;
   req.sl       = sl;
   req.tp       = tp;
   
   // ResetLastError();
   // bool success = OrderSend(req, res);
   //
   // if(!success)
   //   {
   //    Print("❌ Modify failed: ", GetLastError());
   //    return;
   //   }
   //
   // Print("✅ Position modified: ", ticket, " SL=", sl, " TP=", tp);

   Print("🚫 Modify disabled for XAUUSD - skipping execution");
  }

//+------------------------------------------------------------------+
//| Execute resize (future: modify volume)                           |
//+------------------------------------------------------------------+
void ExecuteResize(const string &json)
  {
   // For now, just log
   Print("📥 Resize action received (not implemented)");
  }

//+------------------------------------------------------------------+
//| Execute cancel                                                    |
//+------------------------------------------------------------------+
void ExecuteCancel(const string &json)
  {
   // For now, just log
   Print("📥 Cancel action received (not implemented - manual close)");
  }

//+------------------------------------------------------------------+
//| Confirm execution to HTTP bridge                                 |
//+------------------------------------------------------------------+
void ConfirmExecution(string sid, string status, ulong order, double price, double sl, double tp, int error_code)
  {
   string j = StringFormat(
      "{\"sid\":\"%s\",\"status\":\"%s\",\"order\":%I64d,\"price\":%.5f,\"sl\":%.5f,\"tp\":%.5f,\"error\":%d}",
      sid, status, order, price, sl, tp, error_code
   );
   
   uchar body[];
   StringToCharArray(j, body, 0, WHOLE_ARRAY, CP_UTF8);
   ArrayResize(body, ArraySize(body) - 1);  // Remove null terminator
   
   uchar result[];
   string resp_headers = "";
   string headers = "Content-Type: application/json\r\n";
   
   ResetLastError();
   int st = WebRequest("POST", EndpointConfirm, headers, 3000, body, result, resp_headers);
   
   if(st == -1 || st >= 400)
      Print("⚠️  Confirm failed: ", GetLastError());
   else
      Print("✅ Execution confirmed");
  }
//+------------------------------------------------------------------+

