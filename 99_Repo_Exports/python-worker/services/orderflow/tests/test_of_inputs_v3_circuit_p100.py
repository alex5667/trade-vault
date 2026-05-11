import json

import pytest

from services.orderflow.of_inputs_v3_circuit import (
    record_downgrade_and_maybe_trip,
    refresh_disabled_state,
)


class FakePipe:
    def __init__(self, r):
        self.r = r
        self.ops = []

    def get(self, key):
        self.ops.append(("get", key))
        return self

    def pttl(self, key):
        self.ops.append(("pttl", key))
        return self

    def incr(self, key):
        self.ops.append(("incr", key))
        return self

    def set(self, key, val, px=None):
        self.ops.append(("set", key, val, px))
        return self

    def zadd(self, key, mapping):
        self.ops.append(("zadd", key, mapping))
        return self

    def zremrangebyscore(self, key, min_s, max_s):
        self.ops.append(("zrem", key, min_s, max_s))
        return self

    def zcount(self, key, min_s, max_s):
        self.ops.append(("zcount", key, min_s, max_s))
        return self

    async def execute(self):
        out = []
        for op in self.ops:
            name = op[0]
            if name == "get":
                out.append(await self.r.get(op[1]))
            elif name == "pttl":
                out.append(await self.r.pttl(op[1]))
            elif name == "incr":
                out.append(await self.r.incr(op[1]))
            elif name == "set":
                out.append(await self.r.set(op[1], op[2], px=op[3]))
            elif name == "zadd":
                out.append(await self.r.zadd(op[1], op[2]))
            elif name == "zrem":
                out.append(await self.r.zremrangebyscore(op[1], op[2], op[3]))
            elif name == "zcount":
                out.append(await self.r.zcount(op[1], op[2], op[3]))
            else:
                raise RuntimeError(op)
        self.ops = []
        return out


class FakeAsyncRedis:
    def __init__(self):
        self._kv = {}
        self._exp = {}  # key -> exp_at_ms
        self._ints = {}
        self._z = {}  # key -> {member: score}
        self._now_ms = 0

    def set_now_ms(self, now_ms: int):
        self._now_ms = int(now_ms)

    def pipeline(self, transaction=False):
        return FakePipe(self)

    async def get(self, key):
        self._gc()
        return self._kv.get(key)

    async def pttl(self, key):
        self._gc()
        if key not in self._kv:
            return -2
        exp = self._exp.get(key)
        if exp is None:
            return -1
        return max(-2, int(exp) - int(self._now_ms))

    async def set(self, key, val, px=None):
        self._kv[key] = val
        if px is not None:
            self._exp[key] = int(self._now_ms) + int(px)
        return True

    async def incr(self, key):
        v = int(self._ints.get(key, 0)) + 1
        self._ints[key] = v
        return v

    async def zadd(self, key, mapping):
        z = self._z.setdefault(key, {})
        for m, s in mapping.items():
            z[str(m)] = int(s)
        return True

    async def zremrangebyscore(self, key, min_s, max_s):
        z = self._z.get(key, {})
        rm = [m for m, s in z.items() if int(min_s) <= int(s) <= int(max_s)]
        for m in rm:
            z.pop(m, None)
        return len(rm)

    async def zcount(self, key, min_s, max_s):
        z = self._z.get(key, {})
        return sum(1 for s in z.values() if int(min_s) <= int(s) <= int(max_s))

    async def mget(self, keys):
        self._gc()
        return [self._kv.get(k) for k in keys]

    def _gc(self):
        # expire keys
        expired = [k for k, exp in self._exp.items() if int(exp) <= int(self._now_ms)]
        for k in expired:
            self._kv.pop(k, None)
            self._exp.pop(k, None)


class DummyRuntime:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.of_inputs_v3_disabled_until_ms = 0
        self.of_inputs_v3_disabled_reason = ""
        self.of_inputs_v3_cb_last_refresh_ts_ms = 0


@pytest.mark.asyncio
async def test_record_and_trip_sets_cfg_and_auto_apply_keys():
    r = FakeAsyncRedis()
    sym = "BTCUSDT"
    now = 1_000_000
    r.set_now_ms(now)

    # 1st / 2nd do not trip
    res1 = await record_downgrade_and_maybe_trip(
        r, sym=sym, now_ms=now, downgrade_reason="book_stale",
        window_ms=60_000, max_downgrades_in_window=3, disable_ms=300_000,
        block_auto_apply=True, auto_apply_reason="of_inputs_v3",
    )
    assert res1["tripped"] == 0

    res2 = await record_downgrade_and_maybe_trip(
        r, sym=sym, now_ms=now + 10_000, downgrade_reason="book_stale",
        window_ms=60_000, max_downgrades_in_window=3, disable_ms=300_000,
        block_auto_apply=True, auto_apply_reason="of_inputs_v3",
    )
    assert res2["tripped"] == 0

    # 3rd trips
    res3 = await record_downgrade_and_maybe_trip(
        r, sym=sym, now_ms=now + 20_000, downgrade_reason="book_stale",
        window_ms=60_000, max_downgrades_in_window=3, disable_ms=300_000,
        block_auto_apply=True, auto_apply_reason="of_inputs_v3",
    )
    assert res3["tripped"] == 1
    assert res3["disabled_until_ms"] == (now + 20_000 + 300_000)
    assert res3["hard_until_ms"] == (now + 20_000 + 300_000)
    assert res3["cooldown_ms"] == 0

    disable_key = f"cfg:of_inputs:v3_disabled:{sym}"
    val = await r.get(disable_key)
    meta = json.loads(val)  # type: ignore
    assert meta["until_ms"] == res3["disabled_until_ms"]
    assert meta["hard_until_ms"] == res3["hard_until_ms"]
    assert meta["cooldown_ms"] == 0
    assert meta["reason"] == "book_stale"

    gk = "cfg:of_inputs_v3:auto_apply_block_global:of_inputs_v3"
    sk = f"cfg:of_inputs_v3:auto_apply_block:{sym}:of_inputs_v3"
    assert await r.get(gk) is not None
    assert await r.get(sk) is not None


@pytest.mark.asyncio
async def test_refresh_disabled_state_reads_until_ms_from_value():
    r = FakeAsyncRedis()
    sym = "ETHUSDT"
    rt = DummyRuntime(sym)

    now = 2_000_000
    r.set_now_ms(now)

    disable_key = f"cfg:of_inputs:v3_disabled:{sym}"
    payload = {"until_ms": now + 50_000, "reason": "missing_lob_fields"}
    await r.set(disable_key, json.dumps(payload), px=50_000)

    disabled, until_ms, reason = await refresh_disabled_state(r, rt, now_ms=now, refresh_every_ms=10_000)
    assert disabled is True
    assert until_ms == now + 50_000
    assert reason == "missing_lob_fields"
    assert rt.of_inputs_v3_disabled_hard_until_ms == until_ms  # type: ignore
    assert rt.of_inputs_v3_disabled_phase in ("hard", "")  # type: ignore

    # Cache hit
    r.set_now_ms(now + 1_000)
    disabled2, until2, reason2 = await refresh_disabled_state(r, rt, now_ms=now + 1_000, refresh_every_ms=10_000)
    assert disabled2 is True
    assert until2 == until_ms
    assert reason2 == reason


@pytest.mark.asyncio
async def test_refresh_disabled_state_derives_until_ms_from_ttl_when_value_has_no_until():
    r = FakeAsyncRedis()
    sym = "BNBUSDT"
    rt = DummyRuntime(sym)

    now = 3_000_000
    r.set_now_ms(now)

    disable_key = f"cfg:of_inputs:v3_disabled:{sym}"
    await r.set(disable_key, "1", px=100_000)

    disabled, until_ms, reason = await refresh_disabled_state(r, rt, now_ms=now, refresh_every_ms=10_000)
    assert disabled is True
    assert until_ms == now + 100_000
    assert reason in ("cfg_ttl", "cfg")
    assert rt.of_inputs_v3_disabled_hard_until_ms == until_ms  # type: ignore


@pytest.mark.asyncio
async def test_refresh_disabled_state_sets_phase_hard_then_cooldown():
    """Phase tracking: hard → cooldown → expired (P108 hysteresis)."""
    r = FakeAsyncRedis()
    sym = "SOLUSDT"
    rt = DummyRuntime(sym)

    now = 4_000_000
    r.set_now_ms(now)

    disable_key = f"cfg:of_inputs:v3_disabled:{sym}"
    payload = {
        "until_ms": now + 300_000 + 60_000,  # full period = disable_ms + cooldown_ms
        "hard_until_ms": now + 300_000,       # hard-disable period = disable_ms
        "cooldown_ms": 60_000,
        "reason": "book_stale",
    }
    await r.set(disable_key, json.dumps(payload), px=360_000)

    # --- phase: hard ---
    disabled, until_ms, reason = await refresh_disabled_state(r, rt, now_ms=now, refresh_every_ms=1)
    assert disabled is True
    assert until_ms == payload["until_ms"]
    assert reason == "book_stale"
    assert rt.of_inputs_v3_disabled_phase == "hard"  # type: ignore

    # --- phase: cooldown (now > hard_until_ms, but < until_ms) ---
    r.set_now_ms(now + 300_001)
    disabled2, _, _ = await refresh_disabled_state(r, rt, now_ms=now + 300_001, refresh_every_ms=1)
    assert disabled2 is True
    assert rt.of_inputs_v3_disabled_phase == "cooldown"  # type: ignore

    # --- phase: expired (now > until_ms) ---
    r.set_now_ms(now + 360_001)
    disabled3, _, _ = await refresh_disabled_state(r, rt, now_ms=now + 360_001, refresh_every_ms=1)
    assert disabled3 is False
