import sys
import redis
import logging
# [AUTOGRAVITY CLEANUP] sys.path.append("/home/alex/front/trade/scanner_infra/python-worker")

from services.trade_monitor import TradeMonitorService
from domain.tick_price import build_tick

r = redis.from_url("redis://go_gateway:fdb98a081579737da0d6a5b25746a3c9d63abdad70e7d47f0d24159726146130@redis-worker-1:6379/0", decode_responses=True)

class TestTM(TradeMonitorService):
    def __init__(self):
        self.r = r
        self.open_positions = {}
        self.shards = {}
        self._fsm_enabled = False
        self.logger = logging.getLogger("test")
        self.tp_ratios = [0.33, 0.33, 0.34]

tm = TestTM()
h = r.hgetall("order:a774da92-13fb-4c6a-b1c4-19a2d2f4a95a")
if not h:
    print("Could not find order")
    sys.exit(0)

# Simulate what TradeMonitorService._cache_initial_load() does
pos = tm._position_from_hash(h)
if pos:
    print("Recovered position:", pos.symbol, pos.direction, pos.entry_price, "SL:", pos.sl)

    # Let's hit the SL
    tick = {"symbol": "1000BONKUSDT", "price": 0.007, "ts": 1776397001795} # Current price > SL (0.00639) for SHORT
    
    # We will simulate what TradeMonitorService.on_tick does
    from domain.handlers import process_tick
    print("Before process_tick is_short:", pos.is_short(), "direction:", getattr(pos, 'direction', None))
    events, closed = process_tick(pos, build_tick(tick), tm._get_spec(pos.symbol), tm.tp_ratios, "level")
    
    print("Events:", len(events))
    for e in events:
        print(" -", e.event_type)
        
    if closed:
        print("Closed Trade! Reason:", closed.close_reason_raw)
    else:
        print("Trade did NOT close!")
