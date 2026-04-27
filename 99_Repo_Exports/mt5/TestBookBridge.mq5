//+------------------------------------------------------------------+
//|                                            TestBookBridge.mq5    |
//|                                Test if BookBridge can send data  |
//+------------------------------------------------------------------+
#property copyright "Test Script"
#property version   "1.00"
#property script_show_inputs

//--- Input parameters
input string Endpoint = "http://127.0.0.1:8088/book";  // Book endpoint

//+------------------------------------------------------------------+
//| Script program start function                                    |
//+------------------------------------------------------------------+
void OnStart()
{
   Print("═══════════════════════════════════════════");
   Print("  BookBridge Test - Sending sample book data");
   Print("═══════════════════════════════════════════");
   Print("  Symbol: ", _Symbol);
   Print("  Endpoint: ", Endpoint);
   Print("");
   
   // Get current time in milliseconds
   long ts = (long)TimeCurrent() * 1000;
   
   // Create sample book data
   string json = StringFormat(
      "{\"ts\":%I64d,\"symbol\":\"%s\",\"bids\":[[2760.50,15.0],[2760.45,12.5]],\"asks\":[[2760.75,10.0],[2760.80,11.2]]}",
      ts, _Symbol
   );
   
   Print("Sending JSON:");
   Print(json);
   Print("");
   
   // Convert to byte array
   char data[];
   StringToCharArray(json, data, 0, WHOLE_ARRAY, CP_UTF8);
   ArrayResize(data, ArraySize(data) - 1); // Remove trailing null
   
   // Prepare request
   char result[];
   string headers = "Content-Type: application/json\r\n";
   string response_headers = "";
   
   // Send request
   ResetLastError();
   int status = WebRequest("POST", Endpoint, headers, 5000, data, result, response_headers);
   int last_error = GetLastError();
   
   Print("═══════════════════════════════════════════");
   Print("  Results:");
   Print("═══════════════════════════════════════════");
   Print("  HTTP Status: ", status);
   Print("  Last Error: ", last_error);
   
   if(status == 200)
   {
      string response = CharArrayToString(result, 0, -1, CP_UTF8);
      Print("  Response: ", response);
      Print("");
      Print("✅ BookBridge endpoint works!");
      Print("");
      Print("Now check Docker logs:");
      Print("  docker logs scanner-py-obi --tail 20");
      Print("");
      Print("You should see this request logged.");
   }
   else if(status == -1)
   {
      Print("");
      Print("❌ Request FAILED!");
      Print("  Error code: ", last_error);
      
      if(last_error == 5203)
      {
         Print("");
         Print("  Solution:");
         Print("  1. Add to WebRequest whitelist:");
         Print("     ", Endpoint);
      }
   }
   else
   {
      string response = CharArrayToString(result, 0, -1, CP_UTF8);
      Print("  Response: ", response);
      Print("");
      Print("⚠️  Unexpected status code!");
   }
   
   Print("");
   Print("Test completed.");
}
//+------------------------------------------------------------------+

