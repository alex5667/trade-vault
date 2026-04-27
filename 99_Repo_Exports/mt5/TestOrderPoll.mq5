//+------------------------------------------------------------------+
//|                                             TestOrderPoll.mq5    |
//|                              Test if OrderExecutor can poll data |
//+------------------------------------------------------------------+
#property copyright "Test Script"
#property version   "1.00"
#property script_show_inputs

//--- Input parameters
input string Endpoint = "http://127.0.0.1:8090/orders/poll";  // Poll endpoint
input string TestSymbol = "XAUUSD";  // Symbol to poll

//+------------------------------------------------------------------+
//| Script program start function                                    |
//+------------------------------------------------------------------+
void OnStart()
{
   Print("═══════════════════════════════════════════");
   Print("  OrderExecutor Test - Polling for orders");
   Print("═══════════════════════════════════════════");
   Print("  Symbol: ", TestSymbol);
   Print("  Endpoint: ", Endpoint);
   Print("");
   
   // Build URL with symbol
   string url = Endpoint + "?symbol=" + TestSymbol;
   
   Print("Polling URL: ", url);
   Print("");
   
   // Prepare request
   char data[];
   char result[];
   string headers = "Accept: application/json\r\n";
   string response_headers = "";
   
   // Send request
   ResetLastError();
   int status = WebRequest("GET", url, headers, 5000, data, result, response_headers);
   int last_error = GetLastError();
   
   Print("═══════════════════════════════════════════");
   Print("  Results:");
   Print("═══════════════════════════════════════════");
   Print("  HTTP Status: ", status);
   Print("  Last Error: ", last_error);
   
   if(status == 204)
   {
      Print("");
      Print("✅ Poll endpoint works! (queue is empty)");
      Print("");
      Print("Status 204 = No Content (empty queue)");
      Print("This is the expected response when no orders.");
   }
   else if(status == 200)
   {
      string response = CharArrayToString(result, 0, -1, CP_UTF8);
      Print("  Response: ", response);
      Print("");
      Print("✅ Poll endpoint works! (order received)");
      Print("");
      Print("There's an order in the queue!");
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
         Print("     http://127.0.0.1:8090");
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
   Print("Now check Docker logs:");
   Print("  docker logs scanner-go-gateway --tail 20");
   Print("");
   Print("You should see the poll request.");
   Print("");
   Print("Test completed.");
}
//+------------------------------------------------------------------+

