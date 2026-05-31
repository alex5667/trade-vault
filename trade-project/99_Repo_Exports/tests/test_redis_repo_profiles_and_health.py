import json
import sys
from pathlib import Path

# Add python-worker to path
sys.path.insert(0, str(Path(__file__).parent.parent / "python-worker"))

from infra.redis_repo import RedisTradeRepository, CANON_PROFILE_FIELD, ALIAS_PROFILE_FIELD, TRADES_CLOSED_STREAM_NAME
from domain.models import TradeClosed


class FakePipeline:
    def __init__(self, redis_instance):
        self.redis = redis_instance
        self.commands = []

    def hset(self, key, mapping=None, **kwargs):
        self.commands.append(('hset', key, mapping or {}))
        return self

    def xadd(self, name, data, maxlen=None, approximate=None):
        self.commands.append(('xadd', name, data))
        return self

    def rpush(self, key, value):
        self.commands.append(('rpush', key, value))
        return self

    def srem(self, key, value):
        self.commands.append(('srem', key, value))
        return self

    def set(self, key, value, ex=None):
        self.commands.append(('set', key, value, ex))
        return self

    def execute(self):
        for cmd in self.commands:
            if cmd[0] == 'hset':
                _, key, mapping = cmd
                self.redis.hset(key, mapping)
            elif cmd[0] == 'xadd':
                _, name, data = cmd
                self.redis.xadd(name, data)
            elif cmd[0] == 'rpush':
                _, key, value = cmd
                self.redis.rpush(key, value)
            elif cmd[0] == 'srem':
                _, key, value = cmd
                self.redis.srem(key, value)
            elif cmd[0] == 'set':
                _, key, value, ex = cmd
                self.redis.set(key, value, ex=ex)
        return True


class FakeRedis:
    def __init__(self):
        self.hashes = {}
        self.sets = {}
        self.streams = {}
        self.lists = {}
        self.kv = {}

    def pipeline(self, transaction=False):
        return FakePipeline(self)

    def hset(self, key, mapping=None, **kwargs):
        m = mapping or {}
        self.hashes.setdefault(key, {}).update(m)

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def sadd(self, key, value):
        self.sets.setdefault(key, set()).add(value)

    def srem(self, key, value):
        self.sets.setdefault(key, set()).discard(value)

    def smembers(self, key):
        return set(self.sets.get(key, set()))

    def rpush(self, key, value):
        self.lists.setdefault(key, []).append(value)

    def xadd(self, name, data, maxlen=None, approximate=None):
        self.streams.setdefault(name, []).append(dict(data))

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.kv:
            return None
        self.kv[key] = value
        return True

    def get(self, key):
        return self.kv.get(key)


def test_save_open_and_save_closed_write_profile_aliases_and_merge_health_snapshot():
    r = FakeRedis()
    repo = RedisTradeRepository(r)

    closed = TradeClosed(
        order_id="o1",
        sid="s1",
        strategy="strat",
        source="src",
        symbol="BTCUSDT",
        tf="60",
        entry_ts_ms=1,
        exit_ts_ms=2,
        entry_price=100.0,
        exit_price=110.0,
        lot=1.0,
        notional_usd=100.0,
        pnl_net=10.0,
        pnl_gross=10.0,
        fees=0.0,
        pnl_pct=1.0,
        trailing_profile="rocket_v1",
        duration_ms=1000,
    )

    # Attach snapshot as TradeMonitorService would.
    closed._health_snapshot = {
        "health_l2_stale_ratio_tick": "0.123",
        "health_avg_l2_age_ms": "45.6",
    }

    repo.save_closed(closed)

    # Hash must contain both profile keys.
    h = r.hgetall("order:o1")
    assert h.get(CANON_PROFILE_FIELD) == "rocket_v1"
    assert h.get(ALIAS_PROFILE_FIELD) == "rocket_v1"

    # Stream must contain both profile keys and health fields.
    assert TRADES_CLOSED_STREAM_NAME in r.streams
    assert len(r.streams[TRADES_CLOSED_STREAM_NAME]) == 1
    ev = r.streams[TRADES_CLOSED_STREAM_NAME][0]
    assert ev.get(CANON_PROFILE_FIELD) == "rocket_v1"
    assert ev.get(ALIAS_PROFILE_FIELD) == "rocket_v1"
    assert ev.get("health_l2_stale_ratio_tick") == "0.123"
    assert ev.get("health_avg_l2_age_ms") == "45.6"


def test_save_closed_is_deduped_for_stream_and_lists_but_updates_hash():
    r = FakeRedis()
    repo = RedisTradeRepository(r)

    closed = TradeClosed(
        order_id="o2",
        sid="s2",
        strategy="strat",
        source="src",
        symbol="BTCUSDT",
        tf="60",
        entry_ts_ms=1,
        exit_ts_ms=2,
        entry_price=100.0,
        exit_price=110.0,
        lot=1.0,
        notional_usd=100.0,
        pnl_net=1.0,
        pnl_gross=1.0,
        fees=0.0,
        pnl_pct=1.0,
        trailing_profile="rocket_v1",
        duration_ms=1000,
    )

    repo.save_closed(closed)
    repo.save_closed(closed)  # retry

    # stream published only once (dedupe)
    assert len(r.streams.get(TRADES_CLOSED_STREAM_NAME, [])) == 1

    # hash updated (still present)
    h = r.hgetall("order:o2")
    assert h.get("status") == "closed"
    assert h.get(CANON_PROFILE_FIELD) == "rocket_v1"
