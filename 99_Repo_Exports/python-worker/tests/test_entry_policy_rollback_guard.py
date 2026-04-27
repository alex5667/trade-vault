import asyncio
import json
import pytest
from services.entry_policy_rollback_guard_v1 import EntryPolicyRollbackGuardV1

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
    
    async def get(self, k): 
        return self.kv.get(k)
        
    async def set(self, k, v, ex=None, nx=False, px=None):
        if nx and k in self.kv: return None
        self.kv[k] = v
        return True
        
    async def lrange(self, k, a, b):
        # simplistic slice handling for [0, -1]
        val = self.lists.get(k, [])
        if b == -1: return list(val[a:])
        return list(val[a:b+1])

def test_rollback_enforce():
    asyncio.run(_async_test_rollback_enforce())

async def _async_test_rollback_enforce():
    r = FakeRedis()
    svc = EntryPolicyRollbackGuardV1(r)
    svc.mode = "enforce"
    svc.min_trades = 5
    svc.window_n = 10
    svc.min_delta_mean_r = -0.02
    svc.min_delta_lcb_r = -0.02

    sym, rg, grp = "BTCUSDT", "thin", "default"
    last_applied_key = svc._k_last_applied(sym, rg, grp)
    active_key = svc._k_active(sym, rg, grp)

    # applied winner=B, prev_active=A, baseline prev_mean=+0.10R
    r.kv[last_applied_key] = json.dumps({
        "sid": "sid1",
        "winner": "B",
        "prev_active": "A",
        "ts_ms": 1,
        "baseline": {"prev_mean_r": 0.10, "prev_lcb_r": 0.08},
    })
    r.kv[active_key] = "B"

    # feed 5 bad closes for arm B (mean ~ -0.10R)
    for _ in range(5):
        payload = {
            "event_type": "POSITION_CLOSED",
            "symbol": sym,
            "regime": rg,
            "ab_group": grp,
            "ab_arm": "B",
            "pnl": -10.0,
            "risk_usd": 100.0,
        }
        await svc.process_one(payload)

    assert r.kv[active_key] == "A"
