//+------------------------------------------------------------------+
//|                                                    Test_Loop.mq5 |
//+------------------------------------------------------------------+
#property strict

void TestFunction()
{
   int total;
   int i;
   
   total = 10;
   
   for(i = 0; i < total; i = i + 1)
   {
      Print("i = ", i);
   }
}

int OnInit()
{
   TestFunction();
   return(INIT_SUCCEEDED);
}

void OnTick()
{
}
//+------------------------------------------------------------------+

