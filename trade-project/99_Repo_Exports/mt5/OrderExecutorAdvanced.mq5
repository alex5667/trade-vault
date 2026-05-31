//+------------------------------------------------------------------+
//|                                     OrderExecutorAdvanced.mq5    |
//|                         XAUUSD Order Executor v7 - Multi-TP +BE  |
//+------------------------------------------------------------------+
#property copyright "XAUUSD Order Flow System v7"
#property version   "7.00"
#property strict

//--- Input parameters
input string EndpointPoll    = "http://127.0.0.1:8090/orders/poll";
input string EndpointConfirm = "http://127.0.0.1:8090/orders/confirm";
input int    PollIntervalMs  = 1000;        // Poll interval (ms)
input string SymbolToTrade   = "XAUUSD";    // Symbol to trade
input string TpSplitPerc     = "50,30,20";  // TP split % (sum≈100)
input bool   EnableBreakeven = true;        // Move SL to entry after TP1
input string TrailMode       = "ATR";       // ATR | POINTS | OFF
input double TrailATRMult    = 0.8;         // ATR multiplier for trailing
input double TrailPoints     = 2.0;         // Points for trailing
input int    Magic           = 777007;      // Magic number (v7)
input string FillPolicy      = "IOC";       // IOC | FOK | RETURN
input int    Slippage        = 30;          // Max slippage

//--- v7.1: Advanced features
input string TpMode          = "AUTO";      // AUTO | HEDGING_SPLIT | NET_PARTIAL
input bool   BreakevenStructure = true;    // Use swing low/high for BE
input int    SwingLookback   = 5;           // Bars for swing detection
input bool   RetcodeNotify   = true;        // Send error notifications

//--- Runtime state
datetime lastPoll = 0;
bool breakevenApplied = false;
double g_tp_levels[3] = {0,0,0};
double g_tp_perc[3]   = {0,0,0};
bool   g_tp_done[3]   = {false,false,false};
double g_open_price   = 0.0;
double g_open_volume  = 0.0;
bool   g_is_hedging   = false;

//+------------------------------------------------------------------+
//| Helper functions                                                 |
//+------------------------------------------------------------------+
double RoundToStep(double v, double step)
  {
   if(step <= 0) return v;
   return MathRound(v / step) * step;
  }

int GetDigits()
  {
   return (int)SymbolInfoInteger(SymbolToTrade, SYMBOL_DIGITS);
  }

double GetPoint()
  {
   return SymbolInfoDouble(SymbolToTrade, SYMBOL_POINT);
  }

string Trim(const string &s)
  {
   string x = s;
   StringTrimLeft(x);
   StringTrimRight(x);
   return x;
  }

//+------------------------------------------------------------------+
//| JSON parsing                                                     |
//+------------------------------------------------------------------+
bool ParseJsonValue(const string &json, const string &key, string &val)
  {
   int p = StringFind(json, "\"" + key + "\":");
   if(p < 0) return false;
   
   int s = p + StringLen(key) + 3;
   
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

bool ParseJsonArray(const string &json, const string &key, double &a1, double &a2, double &a3, int &count)
  {
   a1 = a2 = a3 = 0.0;
   count = 0;
   
   int p = StringFind(json, "\"" + key + "\":[");
   if(p < 0) return false;
   
   int s = p + StringLen(key) + 4;
   int e = StringFind(json, "]", s);
   if(e < 0) return false;
   
   string arr = StringSubstr(json, s, e - s);
   int start = 0;
   int idx = 0;
   
   while(true)
     {
      int c = StringFind(arr, ",", start);
      string token = (c >= 0) ? StringSubstr(arr, start, c - start) : StringSubstr(arr, start);
      token = Trim(token);
      
      if(StringLen(token) > 0)
        {
         double v = StringToDouble(token);
         if(idx == 0) a1 = v;
         else if(idx == 1) a2 = v;
         else if(idx == 2) a3 = v;
         idx++;
        }
      
      if(c < 0) break;
      start = c + 1;
      if(idx >= 3) break;
     }
   
   count = idx;
   return true;
  }

//+------------------------------------------------------------------+
//| TP split percentages                                             |
//+------------------------------------------------------------------+
int SplitPercents(double &p1, double &p2, double &p3)
  {
   p1 = p2 = p3 = 0.0;
   string s = TpSplitPerc;
   
   int p = StringFind(s, ",");
   if(p < 0)
     {
      p1 = StringToDouble(Trim(s));
      return 1;
     }
   
   string a = Trim(StringSubstr(s, 0, p));
   string rest = StringSubstr(s, p + 1);
   int q = StringFind(rest, ",");
   
   if(q < 0)
     {
      p1 = StringToDouble(a);
      p2 = StringToDouble(Trim(rest));
      return 2;
     }
   
   p1 = StringToDouble(a);
   p2 = StringToDouble(Trim(StringSubstr(rest, 0, q)));
   p3 = StringToDouble(Trim(StringSubstr(rest, q + 1)));
   return 3;
  }

//+------------------------------------------------------------------+
//| Filling policy                                                   |
//+------------------------------------------------------------------+
ENUM_ORDER_TYPE_FILLING PickFilling()
  {
   string m = FillPolicy;
   StringToUpper(m);
   if(m == "IOC") return ORDER_FILLING_IOC;
   if(m == "FOK") return ORDER_FILLING_FOK;
   return ORDER_FILLING_RETURN;
  }

//+------------------------------------------------------------------+
//| Pre-trade spec checks                                            |
//+------------------------------------------------------------------+
bool CheckTradable()
  {
   long mode = (long)SymbolInfoInteger(SymbolToTrade, SYMBOL_TRADE_MODE);
   
   if(mode != SYMBOL_TRADE_MODE_FULL && 
      mode != SYMBOL_TRADE_MODE_LONGONLY && 
      mode != SYMBOL_TRADE_MODE_SHORTONLY)
     {
      Print("❌ Symbol not tradable: mode=", mode);
      
      if(RetcodeNotify)
        {
         string j = "{\"severity\":\"error\",\"msg\":\"SYMBOL_TRADE_MODE not FULL\"}";
         ConfirmJson(j);
        }
      
      return false;
     }
   
   return true;
  }

bool CheckAndRoundVolume(double &vol)
  {
   double vmin = SymbolInfoDouble(SymbolToTrade, SYMBOL_VOLUME_MIN);
   double vmax = SymbolInfoDouble(SymbolToTrade, SYMBOL_VOLUME_MAX);
   double vstep = SymbolInfoDouble(SymbolToTrade, SYMBOL_VOLUME_STEP);
   
   if(vmin <= 0 || vstep <= 0)
     {
      Print("❌ Spec error: volume steps unknown");
      return false;
     }
   
   if(vol < vmin) vol = vmin;
   if(vol > vmax) vol = vmax;
   
   vol = MathFloor(vol / vstep) * vstep;
   
   return (vol >= vmin);
  }

bool EnsureStopsDistance(double price, double &sl, double &tp)
  {
   int stop_lvl = (int)SymbolInfoInteger(SymbolToTrade, SYMBOL_TRADE_STOPS_LEVEL);
   double min_dist = stop_lvl * GetPoint();
   
   bool adjusted = false;
   
   if(sl > 0 && MathAbs(price - sl) < min_dist)
     {
      sl = (sl < price) ? (price - min_dist) : (price + min_dist);
      adjusted = true;
     }
   
   if(tp > 0 && MathAbs(price - tp) < min_dist)
     {
      tp = (tp > price) ? (price + min_dist) : (price - min_dist);
      adjusted = true;
     }
   
   if(adjusted)
      Print("⚠️  Stops adjusted to minimum distance: ", min_dist);
   
   return true;
  }

//+------------------------------------------------------------------+
//| Read ATR                                                         |
//+------------------------------------------------------------------+
double ReadATR()
  {
   int handle = iATR(SymbolToTrade, PERIOD_M1, 14);
   if(handle == INVALID_HANDLE) return 0.0;
   
   double buf[];
   ArraySetAsSeries(buf, true);
   
   if(CopyBuffer(handle, 0, 0, 2, buf) < 1)
      return 0.0;
   
   return buf[0];
  }

//+------------------------------------------------------------------+
//| Confirm to HTTP bridge                                          |
//+------------------------------------------------------------------+
void ConfirmJson(const string &j)
  {
   uchar body[];
   StringToCharArray(j, body, 0, WHOLE_ARRAY, CP_UTF8);
   ArrayResize(body, ArraySize(body) - 1);  // Remove null terminator
   
   uchar result[];
   string resp_headers = "";
   string headers = "Content-Type: application/json\r\n";
   
   ResetLastError();
   WebRequest("POST", EndpointConfirm, headers, 3000, body, result, resp_headers);
  }

//+------------------------------------------------------------------+
//| Send open confirmation                                           |
//+------------------------------------------------------------------+
void SendOpen(const string &sid, ulong order, double price, double sl, double tp, double vol)
  {
   string j = StringFormat(
      "{\"sid\":\"%s\",\"status\":\"opened\",\"order\":%I64d,\"price\":%.5f,\"sl\":%.5f,\"tp\":%.5f,\"volume\":%.2f}",
      sid, order, price, sl, tp, vol
   );
   ConfirmJson(j);
  }

//+------------------------------------------------------------------+
//| Open market order                                                |
//+------------------------------------------------------------------+
bool OpenMarket(double vol, bool isBuy, double sl, double tp, const string &comment, ulong &outOrder)
  {
   // Pre-trade checks
   if(!CheckTradable())
      return false;
   
   // Validate and round volume
   double v = vol;
   if(!CheckAndRoundVolume(v))
      return false;
   
   MqlTradeRequest req;
   MqlTradeResult res;
   ZeroMemory(req);
   ZeroMemory(res);
   
   req.action = TRADE_ACTION_DEAL;
   req.symbol = SymbolToTrade;
   req.type = isBuy ? ORDER_TYPE_BUY : ORDER_TYPE_SELL;
   req.magic = Magic;
   req.volume = v;  // Use validated volume
   req.type_filling = PickFilling();
   req.deviation = Slippage;
   req.comment = comment;
   
   int digits = GetDigits();
   double price = isBuy ? 
                  SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK) :
                  SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);
   double step = GetPoint();
   
   req.price = NormalizeDouble(price, digits);
   
   // Set stops
   if(sl > 0) req.sl = RoundToStep(sl, step);
   if(tp > 0) req.tp = RoundToStep(tp, step);
   
   // Ensure minimum distance
   EnsureStopsDistance(req.price, req.sl, req.tp);
   
   // Normalize
   if(req.sl > 0) req.sl = NormalizeDouble(req.sl, digits);
   if(req.tp > 0) req.tp = NormalizeDouble(req.tp, digits);
   
   // Send order
   ResetLastError();
   if(!OrderSend(req, res))
     {
      int err = GetLastError();
      Print("❌ OrderSend failed: ", err, " retcode=", res.retcode);
      
      // Notify about error
      if(RetcodeNotify)
        {
         string j = StringFormat(
            "{\"severity\":\"error\",\"action\":\"open\",\"retcode\":%d,\"error\":%d,\"comment\":\"%s\"}",
            (int)res.retcode, err, comment
         );
         ConfirmJson(j);
        }
      
      return false;
     }
   
   outOrder = res.order;
   Print("✅ Order opened: ", outOrder, " @ ", req.price, " vol=", v);
   return true;
  }

//+------------------------------------------------------------------+
//| Close partial volume (for netting)                               |
//+------------------------------------------------------------------+
bool ClosePartial(ulong position_ticket, bool isBuyPosition, double volume_to_close)
  {
   // In netting, close partially with opposite deal
   double v = volume_to_close;
   if(!CheckAndRoundVolume(v))
      return false;
   
   MqlTradeRequest req;
   MqlTradeResult res;
   ZeroMemory(req);
   ZeroMemory(res);
   
   req.action = TRADE_ACTION_DEAL;
   req.symbol = SymbolToTrade;
   req.magic = Magic;
   req.deviation = Slippage;
   req.volume = v;
   req.type_filling = PickFilling();
   
   if(isBuyPosition)
     {
      req.type = ORDER_TYPE_SELL;
      req.price = SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);
     }
   else
     {
      req.type = ORDER_TYPE_BUY;
      req.price = SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK);
     }
   
   req.price = NormalizeDouble(req.price, GetDigits());
   
   ResetLastError();
   if(!OrderSend(req, res))
     {
      int err = GetLastError();
      Print("❌ Partial close failed: ", err, " retcode=", res.retcode);
      
      if(RetcodeNotify)
        {
         string j = StringFormat(
            "{\"severity\":\"error\",\"action\":\"partial_close\",\"retcode\":%d,\"error\":%d}",
            (int)res.retcode, err
         );
         ConfirmJson(j);
        }
      
      return false;
     }
   
   Print("✅ Partial close: ", v, " lot @ ", req.price);
   return true;
  }

//+------------------------------------------------------------------+
//| Modify position                                                  |
//+------------------------------------------------------------------+
bool ModifyPosition(ulong ticket, double sl, double tp)
  {
   MqlTradeRequest req;
   MqlTradeResult res;
   ZeroMemory(req);
   ZeroMemory(res);
   
   req.action = TRADE_ACTION_SLTP;
   req.position = ticket;
   req.symbol = SymbolToTrade;
   req.sl = sl;
   req.tp = tp;
   
   ResetLastError();
   if(!OrderSend(req, res))
     {
      Print("❌ Modify failed: ", GetLastError(), " retcode=", res.retcode);
      return false;
     }
   
   return true;
  }

//+------------------------------------------------------------------+
//| Find position                                                    |
//+------------------------------------------------------------------+
bool FindPosition(ulong &ticket, double &price, double &volume, string &comment)
  {
   ticket = 0;
   price = 0;
   volume = 0;
   comment = "";
   
   for(int i = 0; i < PositionsTotal(); i++)
     {
      if(PositionGetSymbol(i) == SymbolToTrade && 
         PositionGetInteger(POSITION_MAGIC) == Magic)
        {
         ticket = (ulong)PositionGetInteger(POSITION_TICKET);
         price = PositionGetDouble(POSITION_PRICE_OPEN);
         volume = PositionGetDouble(POSITION_VOLUME);
         comment = PositionGetString(POSITION_COMMENT);
         return true;
        }
     }
   
   return false;
  }

//+------------------------------------------------------------------+
//| Get current mid price                                            |
//+------------------------------------------------------------------+
double GetMid()
  {
   double bid = SymbolInfoDouble(SymbolToTrade, SYMBOL_BID);
   double ask = SymbolInfoDouble(SymbolToTrade, SYMBOL_ASK);
   return (bid + ask) / 2.0;
  }

//+------------------------------------------------------------------+
//| Swing detection for structural breakeven                         |
//+------------------------------------------------------------------+
double LastSwingLow(int lookback)
  {
   double minv = DBL_MAX;
   
   for(int i = 1; i <= lookback; i++)
     {
      double v = iLow(SymbolToTrade, PERIOD_M1, i);
      if(v == 0) break;
      if(v < minv) minv = v;
     }
   
   return (minv == DBL_MAX) ? 0.0 : minv;
  }

double LastSwingHigh(int lookback)
  {
   double maxv = 0.0;
   
   for(int i = 1; i <= lookback; i++)
     {
      double v = iHigh(SymbolToTrade, PERIOD_M1, i);
      if(v == 0) break;
      if(v > maxv) maxv = v;
     }
   
   return maxv;
  }

//+------------------------------------------------------------------+
//| Breakeven and trailing logic                                     |
//+------------------------------------------------------------------+
void MaybeBreakevenAndTrail()
  {
   ulong ticket;
   double price, volume;
   string comment;
   
   if(!FindPosition(ticket, price, volume, comment))
      return;
   
   bool isBuy = PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY;
   double mid = GetMid();
   double currentSL = PositionGetDouble(POSITION_SL);
   double currentTP = PositionGetDouble(POSITION_TP);
   
   // === BREAKEVEN ===
   if(EnableBreakeven && !breakevenApplied && currentSL > 0)
     {
      double dist = MathAbs(price - currentSL);
      
      if(dist > GetPoint())
        {
         // Check if price moved >= 1 RR
         if((isBuy && mid >= price + dist) || (!isBuy && mid <= price - dist))
           {
            double newSL = 0.0;
            
            // Structural breakeven: use swing low/high
            if(BreakevenStructure)
              {
               if(isBuy)
                  newSL = LastSwingLow(SwingLookback);
               else
                  newSL = LastSwingHigh(SwingLookback);
               
               // Fallback to entry if swing not found
               if(newSL <= 0)
                  newSL = price;
              }
            else
              {
               newSL = price;  // Simple: move to entry
              }
            
            // Safety: ensure newSL is better than current
            if(newSL > 0)
              {
               double trail_ok = GetPoint();
               bool should_modify = false;
               
               if(isBuy && newSL > currentSL && newSL < mid - trail_ok)
                  should_modify = true;
               
               if(!isBuy && newSL < currentSL && newSL > mid + trail_ok)
                  should_modify = true;
               
               if(should_modify)
                 {
                  if(ModifyPosition(ticket, newSL, currentTP))
                    {
                     breakevenApplied = true;
                     Print("✅ Breakeven applied: SL moved to ", newSL,
                           BreakevenStructure ? " (structure)" : " (entry)");
                     
                     string j = StringFormat(
                        "{\"status\":\"breakeven\",\"position\":%I64d,\"sl\":%.5f,\"type\":\"%s\"}",
                        ticket, newSL, BreakevenStructure ? "structure" : "entry"
                     );
                     ConfirmJson(j);
                    }
                 }
              }
           }
        }
     }
   
   // === TRAILING ===
   string trail_mode_upper = TrailMode;
   StringToUpper(trail_mode_upper);
   
   if(trail_mode_upper != "OFF")
     {
      double trailDist = 0.0;
      
      if(trail_mode_upper == "ATR")
        {
         double atr = ReadATR();
         trailDist = atr * TrailATRMult;
        }
      else
        {
         trailDist = TrailPoints * GetPoint();
        }
      
      if(trailDist > 0)
        {
         double newSL = currentSL;
         
         if(isBuy)
           {
            double candidate = mid - trailDist;
            if(candidate > currentSL && candidate < mid)
              {
               newSL = candidate;
              }
           }
         else
           {
            double candidate = mid + trailDist;
            if((currentSL == 0.0 || candidate < currentSL) && candidate > mid)
              {
               newSL = candidate;
              }
           }
         
         if(newSL != currentSL)
           {
            if(ModifyPosition(ticket, newSL, currentTP))
              {
               Print("✅ Trailing: SL moved to ", newSL);
              }
           }
        }
     }
   
   // === NET_PARTIAL: Close at TP levels ===
   string tm = TpMode;
   StringToUpper(tm);
   bool usePartial = (tm == "NET_PARTIAL") || (tm == "AUTO" && !g_is_hedging);
   
   if(usePartial)
     {
      for(int i = 0; i < 3; i++)
        {
         if(g_tp_levels[i] <= 0 || g_tp_perc[i] <= 0 || g_tp_done[i])
            continue;
         
         bool hit = (isBuy && mid >= g_tp_levels[i]) || (!isBuy && mid <= g_tp_levels[i]);
         
         if(hit)
           {
            double curVol = PositionGetDouble(POSITION_VOLUME);
            double toClose = g_open_volume * g_tp_perc[i];
            
            if(toClose > curVol)
               toClose = curVol;
            
            double vmin = SymbolInfoDouble(SymbolToTrade, SYMBOL_VOLUME_MIN);
            
            if(toClose >= vmin)
              {
               if(ClosePartial(ticket, isBuy, toClose))
                 {
                  g_tp_done[i] = true;
                  
                  string j = StringFormat(
                     "{\"status\":\"partial_closed\",\"position\":%I64d,\"level\":%d,\"volume\":%.2f,\"price\":%.5f}",
                     ticket, i + 1, toClose, mid
                  );
                  ConfirmJson(j);
                  
                  Print("✅ Partial close at TP", i + 1, ": ", toClose, " lot @ ", mid);
                  
                  // After TP1, allow breakeven to be re-evaluated
                  if(i == 0 && EnableBreakeven)
                     breakevenApplied = false;
                 }
              }
           }
        }
     }
  }

//+------------------------------------------------------------------+
//| Expert initialization                                            |
//+------------------------------------------------------------------+
int OnInit()
  {
   Print("OrderExecutorAdvanced v7.1 initialized");
   Print("Symbol: ", SymbolToTrade);
   Print("TP Split: ", TpSplitPerc);
   Print("TP Mode: ", TpMode);
   Print("Breakeven: ", EnableBreakeven, " (Structure: ", BreakevenStructure, ")");
   Print("Trail Mode: ", TrailMode);
   Print("Fill Policy: ", FillPolicy);
   
   // Detect account type
   long mm = (long)AccountInfoInteger(ACCOUNT_MARGIN_MODE);
   g_is_hedging = (mm == ACCOUNT_MARGIN_MODE_RETAIL_HEDGING);
   Print("Account type: ", g_is_hedging ? "HEDGING" : "NETTING");
   
   int sec = (PollIntervalMs <= 0) ? 1 : (PollIntervalMs / 1000);
   if(sec < 1) sec = 1;
   
   // Initialize lastPoll
   lastPoll = TimeCurrent();
   
   EventSetTimer(sec);
   Print("Timer set: every ", sec, " seconds");
   Print("Polling will start in ", sec, " seconds...");
   
   return(INIT_SUCCEEDED);
  }

//+------------------------------------------------------------------+
//| Expert deinitialization                                          |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
  {
   EventKillTimer();
   Print("OrderExecutorAdvanced stopped. Reason: ", reason);
  }

//+------------------------------------------------------------------+
//| Timer function                                                    |
//+------------------------------------------------------------------+
void OnTimer()
  {
   static int timerCounter = 0;
   timerCounter++;
   
   // Debug: log every 10th timer call
   if(timerCounter % 10 == 1)
      Print("⏰ OnTimer called #", timerCounter);
   
   datetime now = TimeCurrent();
   
   // Rate limiting
   if((now - lastPoll) * 1000 < PollIntervalMs)
     {
      MaybeBreakevenAndTrail();
      return;
     }
   
   // Debug: first poll
   if(timerCounter <= 3)
      Print("📡 Polling now... URL: ", EndpointPoll, "?symbol=", SymbolToTrade);
   
   lastPoll = now;
   
   // Poll orders queue
   string url = EndpointPoll + "?symbol=" + SymbolToTrade;
   string headers = "Accept: application/json\r\n";
   uchar data[];
   uchar result[];
   string resp_headers = "";
   
   ResetLastError();
   int status = WebRequest("GET", url, headers, 3000, data, result, resp_headers);
   
   if(status == 204 || ArraySize(result) == 0)
     {
      MaybeBreakevenAndTrail();
      return;
     }
   
   if(status == -1)
     {
      int err = GetLastError();
      if(err != 0)
         Print("WebRequest error: ", err);
      
      MaybeBreakevenAndTrail();
      return;
     }
   
   // Parse response
   string body = CharArrayToString(result, 0, -1, CP_UTF8);
   if(StringLen(body) < 5)
     {
      MaybeBreakevenAndTrail();
      return;
     }
   
   // Extract action
   string action;
   if(!ParseJsonValue(body, "action", action))
     {
      MaybeBreakevenAndTrail();
      return;
     }
   
   // Route by action
   if(action == "open")
     {
      // Parse fields
      string lot_s, side, sid, sl_s;
      ParseJsonValue(body, "lot", lot_s);
      ParseJsonValue(body, "side", side);
      ParseJsonValue(body, "sid", sid);
      ParseJsonValue(body, "sl", sl_s);
      
      double lot = (double)StringToDouble(lot_s);
      bool isBuy = (StringCompare(side, "LONG") == 0);
      double sl = (double)StringToDouble(sl_s);
      
      // Parse TP levels
      double tp1 = 0, tp2 = 0, tp3 = 0;
      int tpn = 0;
      ParseJsonArray(body, "tp_levels", tp1, tp2, tp3, tpn);
      
      // Split lot
      double p1 = 0, p2 = 0, p3 = 0;
      int pc = SplitPercents(p1, p2, p3);
      
      double v1 = lot * p1 / 100.0;
      double v2 = (pc >= 2) ? lot * p2 / 100.0 : 0.0;
      double v3 = (pc >= 3) ? lot * p3 / 100.0 : 0.0;
      
      Print("📥 Opening ", side, " ", lot, " lot (split: ", v1, "/", v2, "/", v3, ")");
      
      // Save runtime TP/percentages for NET_PARTIAL mode
      g_open_price = GetMid();
      g_open_volume = lot;
      g_tp_perc[0] = p1 / 100.0;
      g_tp_perc[1] = p2 / 100.0;
      g_tp_perc[2] = p3 / 100.0;
      g_tp_levels[0] = tp1;
      g_tp_levels[1] = tp2;
      g_tp_levels[2] = tp3;
      g_tp_done[0] = g_tp_done[1] = g_tp_done[2] = false;
      
      // Determine mode
      bool useSplit = false;
      string tm = TpMode;
      StringToUpper(tm);
      
      if(tm == "HEDGING_SPLIT")
         useSplit = true;
      else if(tm == "NET_PARTIAL")
         useSplit = false;
      else  // AUTO
         useSplit = g_is_hedging;
      
      if(useSplit)
        {
         // HEDGING: open multiple orders with different TPs
         ulong ord = 0;
         if(v1 > 0)
           {
            if(OpenMarket(v1, isBuy, sl, tp1, sid, ord))
              {
               SendOpen(sid, ord, GetMid(), sl, tp1, v1);
              }
           }
         
         if(v2 > 0 && tpn >= 2)
           {
            ulong o2 = 0;
            OpenMarket(v2, isBuy, sl, tp2, sid, o2);
           }
         
         if(v3 > 0 && tpn >= 3)
           {
            ulong o3 = 0;
            OpenMarket(v3, isBuy, sl, tp3, sid, o3);
           }
        }
      else
        {
         // NETTING: open single order without TP, manage partials manually
         ulong ord = 0;
         if(OpenMarket(lot, isBuy, sl, 0.0, sid, ord))
           {
            string j = StringFormat(
               "{\"sid\":\"%s\",\"status\":\"opened_net\",\"order\":%I64d}",
               sid, ord
            );
            ConfirmJson(j);
           }
        }
      
      breakevenApplied = false;  // Reset
     }
   else if(action == "modify")
     {
      ulong ticket;
      double price, vol;
      string comment;
      
      if(FindPosition(ticket, price, vol, comment))
        {
         double tp1 = 0, tp2 = 0, tp3 = 0;
         int n = 0;
         ParseJsonArray(body, "tp_levels", tp1, tp2, tp3, n);
         
         string sls;
         double sl = 0.0;
         if(ParseJsonValue(body, "sl", sls))
            sl = StringToDouble(sls);
         
         ModifyPosition(ticket, sl, tp1);
        }
     }
   else if(action == "trail")
     {
      // Note: Cannot modify input parameter TrailMode at runtime
      // This action would require global variable instead
      Print("⚠️  Trail mode change requested but TrailMode is input parameter");
     }
   
   MaybeBreakevenAndTrail();
  }

//+------------------------------------------------------------------+
//| Trade transaction event                                          |
//+------------------------------------------------------------------+
void OnTradeTransaction(const MqlTradeTransaction &trans,
                        const MqlTradeRequest &req,
                        const MqlTradeResult &res)
  {
   if(trans.type == TRADE_TRANSACTION_DEAL_ADD)
     {
      ulong deal = trans.deal;
      
      if(HistoryDealSelect(deal))
        {
         string sym = HistoryDealGetString(deal, DEAL_SYMBOL);
         if(sym != SymbolToTrade) return;
         
         double profit = HistoryDealGetDouble(deal, DEAL_PROFIT);
         double price = HistoryDealGetDouble(deal, DEAL_PRICE);
         long type = (long)HistoryDealGetInteger(deal, DEAL_TYPE);
         ulong pos = (ulong)HistoryDealGetInteger(deal, DEAL_POSITION_ID);
         string comm = HistoryDealGetString(deal, DEAL_COMMENT);
         
         string j = StringFormat(
            "{\"symbol\":\"%s\",\"deal\":%I64d,\"position\":%I64d,\"type\":%d,\"price\":%.5f,\"profit\":%.2f,\"comment\":\"%s\",\"sid\":\"%s\"}",
            sym, deal, pos, (int)type, price, profit, comm, comm  // sid in comment
         );
         
         ConfirmJson(j);
         
         Print("💰 Deal closed: ", deal, " P&L: $", profit);
        }
     }
  }
//+------------------------------------------------------------------+

