from __future__ import annotations

import json

from services.smt_entry_policy_service import EntryPolicyService
from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.hh = {}
        self.xadds = []
    async def get(self, k):
        return self.kv.get(k)
    async def hgetall(self, k):
        return self.hh.get(k, {})
    async def xadd(self, stream, msg, maxlen=0, approximate=True):
        self.xadds.append((stream, msg))
        return "1-0"
    async def xgroup_create(self, *a, **kw):
        return None
    async def xreadgroup(self, *a, **kw):
        return []
    async def xack(self, *a, **kw):
        return 1


import asyncio


def test_entry_policy_allow_happy_path(monkeypatch):
    async def _test():
        svc = EntryPolicyService()
        fr = FakeRedis()
        svc.r = fr

        # bundle state
        fr.hh["smt:bundle:v1:b1"] = {
            "decision": "continuation",
            "pick": "ETHUSDT",
            "coh": "0.80",
            "leader": "BTCUSDT",
            "leader_conf_score": "0.80",
            "news_blocked": "0",
        }
        # snap
        fr.kv["smt:snap:ETHUSDT"] = json.dumps({
            "symbol": "ETHUSDT",
            "ts_ms": 1,
            "close_px": 100.0,
            "zone_id": "W_HIGH",
            "zone_side": "MID",
            "zone_dist_bp": 8.0,
            "zone_ok": 1,
            "regime": "range",
            "abs_lvl_th_unstable": 0,
            "of_strong": 1,
            "of_dir": "LONG",
            "of_confirm_score": 1.0,
            "obi_stable_sec": 0.0,
            "iceberg_strict": 0,
            "zone_type": "LEVEL",
            "zone_src": "weekly",
            "zone_px_lo": 101.0,
            "zone_px_hi": 101.0,
            "strong_gate_have": 2,
            "strong_gate_need": 2,
            "strong_gate_scn": "continuation",
        })

        now = get_ny_time_millis()
        cand = {
            "type": "entry_candidate",
            "ts_ms": str(now),
            "symbol": "ETHUSDT",
            "side": "LONG",
            "bundle": "b1",
            "zone_id": "W_HIGH",
            "setup_ts_ms": str(now - 100),
            "payload": json.dumps({"setup": {"bundle": "b1"}}),
        }
        await svc.process_one(cand)
        # should emit entry + audit
        streams = [s for s, _ in fr.xadds]
        if RS.TRADE_ENTRY not in streams:
            print(f"DEBUG: streams found: {streams}")
        assert RS.TRADE_ENTRY in streams
        assert RS.ENTRY_AUDIT in streams

    asyncio.run(_test())


def test_entry_policy_denies_thin_without_extra(monkeypatch):
    async def _test():
        svc = EntryPolicyService()
        fr = FakeRedis()
        svc.r = fr

        fr.hh["smt:bundle:v1:b1"] = {"decision": "continuation", "pick": "ETHUSDT", "coh": "0.80", "leader": "BTCUSDT", "leader_conf_score": "0.80", "news_blocked": "0"}
        fr.kv["smt:snap:ETHUSDT"] = json.dumps({
            "symbol": "ETHUSDT",
            "ts_ms": 1,
            "close_px": 100.0,
            "zone_id": "W_HIGH",
            "zone_side": "MID",
            "zone_dist_bp": 8.0,
            "zone_ok": 1,
            "regime": "thin",
            "abs_lvl_th_unstable": 0,
            "of_strong": 1,
            "of_dir": "LONG",
            "of_confirm_score": 1.0,
            "obi_stable_sec": 0.0,
            "iceberg_strict": 0,
        })

        now = get_ny_time_millis()
        cand = {"type": "entry_candidate", "ts_ms": str(now), "symbol": "ETHUSDT", "side": "LONG", "bundle": "b1", "zone_id": "W_HIGH", "setup_ts_ms": str(now - 100), "payload": "{}"}
        await svc.process_one(cand)
        # should NOT emit trade entry, but audit
        streams = [s for s, _ in fr.xadds]
        assert RS.TRADE_ENTRY not in streams
        assert RS.ENTRY_AUDIT in streams

    asyncio.run(_test())
