
import os
import sys
import redis
import json

# Adjust path to import services
sys.path.insert(0, "/app")
# print(f"sys.path: {sys.path}")

from services.trade_metrics_service import TradeMetricsService

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
r = redis.from_url(REDIS_URL, decode_responses=True)

entries = r.xrevrange("trades:closed", count=10)
print(f"Found {len(entries)} trades")

tm = TradeMetricsService()
m = tm.new_metrics()

for _id, fields in entries:
    order_id = fields.get('order_id') or fields.get('id')
    print(f"\n--- Stream ID {_id} / Order ID {order_id} ---")
    
    # Stream values
    s_pnl = fields.get('pnl_net')
    s_one_r = fields.get('one_r_money')
    print(f"STREAM: pnl_net={s_pnl}, one_r_money={s_one_r}")
    
    # Hash values
    if order_id:
        h_data = r.hgetall(f"order:{order_id}")
        h_pnl = h_data.get('pnl_net')
        h_one_r = h_data.get('one_r_money')
        print(f"HASH:   pnl_net={h_pnl}, one_r_money={h_one_r}")
    
    tm.accumulate_trade(m, fields)

tm.finalize(m)
print("\n--- Metrics ---")
print(f"cnt_r: {m.get('cnt_r')}")
print(f"sum_r: {m.get('sum_r')}")
print(f"median_r: {m.get('median_r')}")
print(f"std_r: {m.get('std_r')}")
print(f"sum_exit_eff_win: {m.get('sum_exit_eff_win')}")
print(f"cnt_exit_eff_win: {m.get('cnt_exit_eff_win')}")
print(f"avg_sl_atr: {m.get('avg_sl_atr')}")
