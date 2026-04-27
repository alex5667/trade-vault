import os

import sys
from pathlib import Path

# Add python-worker to path
sys.path.insert(0, str(Path(__file__).parent.parent / "python-worker"))

from infra.redis_repo import (
    RedisTradeRepository,
    PROFILE_ALIAS_KEY,
    PROFILE_CANON_KEY,
    TRADES_CLOSED_STREAM_NAME,
)
from domain.models import TradeClosed


class FakeRedis:
    def __init__(self):
        self.hashes = {}
        self.sets = {}
        self.streams = {}
        self.lists = {}
        self.zsets = {}
        self.kv = {}

    # basic ops
    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def smembers(self, key):
        return set(self.sets.get(key, set()))

    def sadd(self, key, value):
        self.sets.setdefault(key, set()).add(str(value))

    def srem(self, key, value):
        self.sets.setdefault(key, set()).discard(str(value))

    def sscan_iter(self, key):
        for v in list(self.sets.get(key, set())):
            yield v

    def register_script(self, _lua: str):
        # Emulate Lua behavior for unit tests.
        def _run(*, keys, args):
            order_key, open_set, stream, list1, list2, dedupe_key, zset1, zset2 = keys
            stream_maxlen = int(args[0])
            dedupe_ttl = int(args[1])
            legacy_enabled = int(args[2])
            zsets_enabled = int(args[3])
            zscore = int(args[4])
            hpair_count = int(args[5])
            idx = 6

            # HSET
            h = self.hashes.setdefault(order_key, {})
            for _ in range(hpair_count):
                k = args[idx]; v = args[idx + 1]
                h[str(k)] = str(v)
                idx += 2

            # SREM
            oid_srem = str(args[idx]); idx += 1
            self.srem(open_set, oid_srem)

            # Dedupe
            if dedupe_key in self.kv:
                return 0
            self.kv[dedupe_key] = "1"

            # XADD
            spair_count = int(args[idx]); idx += 1
            ev = {}
            for _ in range(spair_count):
                k = args[idx]; v = args[idx + 1]
                ev[str(k)] = str(v)
                idx += 2
            self.streams.setdefault(stream, []).append(ev)
            if len(self.streams[stream]) > stream_maxlen:
                self.streams[stream] = self.streams[stream][-stream_maxlen:]

            # RPUSH
            oid1 = str(args[idx]); idx += 1
            oid2 = str(args[idx]); idx += 1
            if legacy_enabled == 1:
                self.lists.setdefault(list1, []).append(oid1)
                self.lists.setdefault(list2, []).append(oid2)

            # ZADD
            if zsets_enabled == 1:
                self.zsets.setdefault(zset1, {})[oid1] = zscore
                self.zsets.setdefault(zset2, {})[oid2] = zscore

            return 1
        return _run


def _make_closed():
    return TradeClosed(
        order_id="o1",
        sid="s1",
        strategy="strat",
        source="src",
        symbol="BTCUSDT",
        tf="60",
        direction="long",
        entry_ts_ms=1,
        exit_ts_ms=1700000000000,
        entry_price=100.0,
        exit_price=110.0,
        lot=1.0,
        notional_usd=100.0,
        pnl_net=1.0,
        pnl_gross=1.2,
        fees=0.2,
        pnl_pct=1.0,
        trailing_profile="rocket_v1",
        duration_ms=1000,
        r_multiple=1.0,
    )


def test_save_closed_atomic_dedupe_profile_health_zsets_and_hash_always_updated(monkeypatch):
    r = FakeRedis()
    repo = RedisTradeRepository(r)

    # open index presence
    r.sadd("orders:open", "o1")

    closed = _make_closed()
    closed._health_snapshot = {"health_l2_stale_ratio_tick": "0.123"}

    repo.save_closed(closed)
    repo.save_closed(closed)  # retry

    # open set cleanup always
    assert "o1" not in r.smembers("orders:open")

    # stream deduped
    assert len(r.streams.get(TRADES_CLOSED_STREAM_NAME, [])) == 1
    ev = r.streams[TRADES_CLOSED_STREAM_NAME][0]

    # profile keys in stream
    assert ev.get(PROFILE_CANON_KEY) == "rocket_v1"
    assert ev.get(PROFILE_ALIAS_KEY) == "rocket_v1"

    # health merged into stream
    assert ev.get("health_l2_stale_ratio_tick") == "0.123"

    # order hash written (always)
    h = r.hgetall("order:o1")
    assert h.get("status") == "closed"
    assert h.get(PROFILE_CANON_KEY) == "rocket_v1"
    assert h.get(PROFILE_ALIAS_KEY) == "rocket_v1"
    assert h.get("direction") == "LONG"  # normalized

    # zsets created
    # Keys depend on canon_*; in this test we don't verify exact key strings,
    # only that at least one zset got the member.
    assert any(("o1" in members) for members in r.zsets.values())


def test_compact_stream_payload_feature_flag(monkeypatch):
    r = FakeRedis()
    repo = RedisTradeRepository(r)

    # enable compact mode
    monkeypatch.setenv("TRADES_CLOSED_STREAM_COMPACT", "1")
    monkeypatch.setenv("REDIS_CLOSED_ZSETS_ENABLED", "0")
    monkeypatch.setenv("REDIS_LEGACY_CLOSED_LISTS_ENABLED", "0")

    closed = _make_closed()
    repo.save_closed(closed)

    ev = r.streams[TRADES_CLOSED_STREAM_NAME][0]
    # compact mode should keep pnl_net but may drop pnl_gross
    assert "pnl_net" in ev
    assert "pnl_gross" not in ev