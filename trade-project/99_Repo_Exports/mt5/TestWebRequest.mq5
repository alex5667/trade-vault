//+------------------------------------------------------------------+
//|                                              TestWebRequest.mq5   |
//|                            Test script to verify WebRequest works |
//+------------------------------------------------------------------+
#property copyright "Test Script"
#property version   "1.00"
#property script_show_inputs

//--- Input parameters
input string TestURL = "http://127.0.0.1:8088/healthz";  // URL to test

//+------------------------------------------------------------------+
//| Script program start function                                    |
//+------------------------------------------------------------------+
void OnStart()
{
   Print("═══════════════════════════════════════════");
   Print("  WebRequest Test Script");
   Print("═══════════════════════════════════════════");
   Print("  Testing URL: ", TestURL);
   Print("");
   
   // Prepare request
   char data[];
   char result[];
   string headers = "Content-Type: application/json\r\n";
   string response_headers = "";
   
   // Reset last error
   ResetLastError();
   
   // Send request
   Print("Sending GET request...");
   int status = WebRequest("GET", TestURL, headers, 5000, data, result, response_headers);
   
   // Get last error
   int last_error = GetLastError();
   
   Print("═══════════════════════════════════════════");
   Print("  Results:");
   Print("═══════════════════════════════════════════");
   Print("  HTTP Status: ", status);
   Print("  Last Error: ", last_error);
   
   if(status == -1)
   {
      Print("");
      Print("❌ WebRequest FAILED!");
      Print("");
      
      switch(last_error)
      {
         case 5203:
            Print("  Error 5203: URL not allowed!");
            Print("  Solution:");
            Print("  1. Tools → Options → Expert Advisors");
            Print("  2. ✅ Allow WebRequest for listed URL");
            Print("  3. Add: ", TestURL);
            Print("  4. OK and restart MT5");
            break;
            
         case 5200:
            Print("  Error 5200: WebRequest method not allowed!");
            break;
            
         default:
            Print("  Unknown error code: ", last_error);
            Print("  Check MT5 documentation for error codes");
            break;
      }
   }
   else if(status == 200)
   {
      string response = CharArrayToString(result, 0, -1, CP_UTF8);
      Print("");
      Print("✅ WebRequest SUCCESS!");
      Print("");
      Print("  Response: ", response);
      Print("");
      Print("═══════════════════════════════════════════");
      Print("  Docker services are reachable from MT5!");
      Print("  You can now attach EA to the chart.");
      Print("═══════════════════════════════════════════");
   }
   else
   {
      Print("");
      Print("⚠️  Unexpected status: ", status);
      string response = CharArrayToString(result, 0, -1, CP_UTF8);
      Print("  Response: ", response);
   }
   
   Print("");
   Print("Test completed.");
}
//+------------------------------------------------------------------+

