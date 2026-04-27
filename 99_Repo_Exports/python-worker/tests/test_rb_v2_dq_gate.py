import asyncio
import json
import pytest
from services.entry_policy_rollback_guard_v2 import RollbackGuardV2

class FakePipeline:
    def __init__(self, red):
        self.red = red
        self.cmds = []
    
    def lpush(self, k, v):
        self.cmds.append(("lpush", k, v))
        return self

    def ltrim(self, k, a, b):
        self.cmds.append(("ltrim", k, a, b))
        return self

    def set(self, k, v, ex=None, nx=False, px=None):
        self.cmds.append(("set", k, v, ex, nx, px))
        return self

    async def execute(self):
        for cmd in self.cmds:
             if cmd[0] == "lpush":
                 self.red.lists.setdefault(cmd[1], []).insert(0, cmd[2])
             elif cmd[0] == "ltrim":
                 k, a, b = cmd[1], cmd[2], cmd[3]
                 self.red.lists[k] = self.red.lists.get(k, [])[a:b+1]
             elif cmd[0] == "set":
                 k, v, ex, nx, px = cmd[1], cmd[2], cmd[3], cmd[4], cmd[5]
                 if nx and k in self.red.kv: continue
                 self.red.kv[k] = v
        return True

class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.lists = {}
        self.stream = []
    
    def pipeline(self): 
        return FakePipeline(self)

    async def execute(self): return True
    
    async def set(self, k, v, ex=None, nx=False, px=None):
        if nx and k in self.kv: return None
        self.kv[k] = v
        return True
    
    async def get(self, k): return self.kv.get(k)
    
    async def lpush(self, k, v):
        self.lists.setdefault(k, [])
        self.lists[k].insert(0, v)
        
    async def ltrim(self, k, a, b):
        self.lists[k] = self.lists.get(k, [])[a:b+1]
        
    async def lrange(self, k, a, b):
        # simplistic slice handling for [0, -1]
        val = self.lists.get(k, [])
        if b == -1: return list(val[a:])
        return list(val[a:b+1])
        
    async def xadd(self, stream, msg, maxlen=None, approximate=None):
        self.stream.append((stream, msg)); return "1-0"
        
    async def xgroup_create(self, *a, **k): return True
    async def xreadgroup(self, *a, **k): return []
    async def xack(self, *a, **k): return True

def test_dq_blocks():
    asyncio.run(_async_test_dq_blocks())

async def _async_test_dq_blocks():
    r = FakeRedis()
    svc = RollbackGuardV2(r)
    svc.mode = "enforce"

    sym, rg, grp = "BTCUSDT", "thin", "default"
    r.kv[svc._k_last(sym, rg, grp)] = json.dumps({
        "sid": "sid1",
        "winner": "B",
        "prev_active": "A",
        "baseline": {"prev_mean_r": 0.10, "prev_lcb_r": 0.08},
    })
    r.kv[svc._k_active(sym, rg, grp)] = "B"

    # dq missing -> blocked (fail-open for rollback = no rollback)
    ev = {"event_type":"POSITION_CLOSED","symbol":sym,"regime":rg,"ab_group":grp,"ab_arm":"B","pnl":-10.0,"risk_usd":100.0}
    await svc.process_one(ev)
    assert r.kv[svc._k_active(sym, rg, grp)] == "B"
    
    # dq bad -> blocked
    ev2 = {
        "event_type":"POSITION_CLOSED","symbol":sym,"regime":rg,"ab_group":grp,"ab_arm":"B","pnl":-10.0,"risk_usd":100.0,
        "spread_bp": 50.0, # > max 25.0 for THIN
        "book_age_ms": 100,
        "obi_age_ms": 100
    }
    await svc.process_one(ev2)
    assert r.kv[svc._k_active(sym, rg, grp)] == "B"

    # dq ok -> should process (but we need many trades to trigger decision, so just check it doesn't crash)
    ev3 = {
        "event_type":"POSITION_CLOSED","symbol":sym,"regime":rg,"ab_group":grp,"ab_arm":"B","pnl":-10.0,"risk_usd":100.0,
        "spread_bp": 10.0,
        "book_age_ms": 100,
        "obi_age_ms": 100
    }
    await svc.process_one(ev3)
    # Still B because min_trades not reached
    assert r.kv[svc._k_active(sym, rg, grp)] == "B"

def test_telegram_notify():
    asyncio.run(_async_test_telegram_notify())

async def _async_test_telegram_notify():
    r = FakeRedis()
    svc = RollbackGuardV2(r)
    svc.mode = "suggest" # suggest mode

    sym, rg, grp = "BTCUSDT", "thin", "default"
    sid = "sid2"
    
    # 1. Setup Active=B, Last=B (prev=A), Baseline good
    r.kv[svc._k_last(sym, rg, grp)] = json.dumps({
        "sid": sid,
        "winner": "B",
        "prev_active": "A",
        "baseline": {"prev_mean_r": 1.0, "prev_lcb_r": 0.8},
    })
    r.kv[svc._k_active(sym, rg, grp)] = "B"
    
    # 2. Pre-fill with many bad trades (loss) to trigger regression
    k = svc._k_post(sym, rg, grp, sid)
    # Using THIN config: min_trades=30. We fill 30 bad trades (-1 R)
    for _ in range(35):
        r.lists.setdefault(k, []).insert(0, "-1.0")
        
    # 3. Process one more bad trade
    ev = {
        "event_type":"POSITION_CLOSED","symbol":sym,"regime":rg,"ab_group":grp,"ab_arm":"B","pnl":-100.0,"risk_usd":100.0,
        "spread_bp": 10.0, "book_age_ms": 10, "obi_age_ms": 10
    }
    await svc.process_one(ev)
    
    # 4. Check if notification sent to notify:telegram
    tg_msgs = [m for s, m in r.stream if s == "notify:telegram"]
    assert len(tg_msgs) >= 1
    assert "Rollback Suggestion" in tg_msgs[-1]["text"]
    assert "BTCUSDT" in tg_msgs[-1]["text"]
