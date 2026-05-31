from __future__ import annotations

import json
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest

# Добавляем пути для импорта
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "python-worker"))


# -----------------------------
# Minimal fakes (no external deps)
# -----------------------------
class FakePipeline:
    def __init__(self, r: "FakeRedis", transaction: bool):
        self._r = r
        self._transaction = transaction
        self._ops: List[Tuple[str, tuple, dict]] = []

    def hset(self, *a, **kw):
        self._ops.append(("hset", a, kw))
        return self

    def sadd(self, *a, **kw):
        self._ops.append(("sadd", a, kw))
        return self

    def srem(self, *a, **kw):
        self._ops.append(("srem", a, kw))
        return self

    def rpush(self, *a, **kw):
        self._ops.append(("rpush", a, kw))
        return self

    def xadd(self, *a, **kw):
        self._ops.append(("xadd", a, kw))
        return self

    def set(self, *a, **kw):
        self._ops.append(("set", a, kw))
        return self

    def execute(self):
        # "transaction" here means apply-or-nothing in our fake
        if self._transaction:
            snapshot = self._r._snapshot()
            try:
                for name, a, kw in self._ops:
                    getattr(self._r, name)(*a, **kw)
                return True
            except Exception:
                self._r._restore(snapshot)
                raise
        else:
            for name, a, kw in self._ops:
                getattr(self._r, name)(*a, **kw)
            return True


class FakeRedis:
    """
    In-memory fake Redis with enough commands for repo tests.
    Supports bytes mode (decode_responses=False simulation).
    """
    def __init__(self, *, return_bytes: bool = False):
        self.return_bytes = return_bytes
        self.hashes: Dict[str, Dict[str, str]] = {}
        self.sets: Dict[str, set] = {}
        self.lists: Dict[str, List[str]] = {}
        self.kv: Dict[str, str] = {}
        self.streams: Dict[str, List[Dict[str, str]]] = {}

    def _b(self, s: str):
        return s.encode("utf-8") if self.return_bytes else s

    def _snapshot(self):
        import copy
        return copy.deepcopy((self.hashes, self.sets, self.lists, self.kv, self.streams))

    def _restore(self, snap):
        self.hashes, self.sets, self.lists, self.kv, self.streams = snap

    def pipeline(self, transaction: bool = False):
        return FakePipeline(self, transaction=transaction)

    def hset(self, key: str, mapping: Dict[str, str]):
        h = self.hashes.setdefault(key, {})
        for k, v in mapping.items():
            h[str(k)] = str(v)
        return True

    def hgetall(self, key: str):
        h = self.hashes.get(key, {})
        if not self.return_bytes:
            return dict(h)
        return {self._b(k): self._b(v) for k, v in h.items()}

    def sadd(self, key: str, value: str):
        s = self.sets.setdefault(key, set())
        s.add(self._b(value) if self.return_bytes else value)
        return True

    def srem(self, key: str, value: str):
        s = self.sets.setdefault(key, set())
        if self.return_bytes:
            s.discard(self._b(value))
        else:
            s.discard(value)
        return True

    def smembers(self, key: str):
        return set(self.sets.get(key, set()))

    def sscan(self, key: str, cursor: int = 0, count: int = 10):
        # simplistic scan: return all in one batch
        items = list(self.sets.get(key, set()))
        return 0, items

    def rpush(self, key: str, value: str):
        self.lists.setdefault(key, []).append(value)
        return True

    def xadd(self, stream: str, data: Dict[str, str], maxlen: int, approximate: bool = True):
        self.streams.setdefault(stream, []).append(dict(data))
        # trim
        if len(self.streams[stream]) > maxlen:
            self.streams[stream] = self.streams[stream][-maxlen:]
        return "1-0"

    def set(self, key: str, value: str, nx: bool = False, ex: Optional[int] = None):
        if nx and key in self.kv:
            return False
        self.kv[key] = str(value)
        return True

    def get(self, key: str):
        v = self.kv.get(key)
        if v is None:
            return None
        return self._b(v) if self.return_bytes else v

    def delete(self, key: str):
        self.kv.pop(key, None)
        return True


# -----------------------------
# Minimal domain stubs for tests
# -----------------------------
class Side:
    LONG = "long"
    SHORT = "short"


@dataclass
class PositionState:
    id: str
    sid: str
    strategy: str
    source: str
    symbol: str
    tf: str
    direction: Any
    entry_price: float
    entry_ts_ms: int
    lot: float
    remaining_qty: float
    sl: float
    tp_levels: List[float]
    trail_profile: str = ""
    entry_tag: str = ""
    baseline_mode: str = "tp_sl"
    baseline_horizon_ms: int = 0
    baseline_sl: float = 0.0
    baseline_tp1: float = 0.0
    baseline_tp2: float = 0.0
    baseline_tp3: float = 0.0


@dataclass
class TradeClosed:
    order_id: str
    strategy: str
    source: str
    symbol: str
    tf: str
    exit_ts_ms: int
    exit_price: float
    entry_price: float
    lot: float
    notional_usd: float
    pnl_net: float
    pnl_gross: float
    fees: float
    pnl_pct: float
    pnl_if_fixed_exit: float
    tp_hits: int
    tp1_hit: bool
    tp2_hit: bool
    tp3_hit: bool
    tp_before_sl: bool
    close_reason: str
    close_reason_raw: str = ""
    close_reason_detail: str = ""
    baseline_exit_reason: str = ""
    baseline_exit_ts_ms: int = 0
    baseline_exit_price: float = 0.0
    entry_tag: str = ""
    trailing_profile: str = ""
    trailing_min_lock_r: float = 0.0
    min_lock_price: float = 0.0
    trailing_active: bool = False
    trailing_started: bool = False
    trailing_moves: int = 0
    duration_ms: int = 0
    mfe_pnl: float = 0.0
    mae_pnl: float = 0.0
    giveback: float = 0.0
    missed_profit: float = 0.0
    one_r_money: float = 0.0
    r_multiple: float = 0.0
    max_favorable_price: float = 0.0
    max_favorable_ts: int = 0
    schema_version: int = 1


# -----------------------------
# repo import
# -----------------------------
try:
    from infra.redis_repo import RedisTradeRepository, TRADES_CLOSED_STREAM_NAME
except ImportError as e:
    pytest.skip(f"Cannot import redis_repo: {e}", allow_module_level=True)


# canon_* stubs used by repo; if your project defines them elsewhere, these tests should
# import real ones. For now, provide minimal compatibility by monkeypatching.
def canon_strategy(x): return str(x).lower()
def canon_symbol(x): return str(x).upper()
def canon_tf(x): return str(x)
def canon_source(x): return str(x).lower()


@pytest.fixture(autouse=True)
def _patch_canon(monkeypatch):
    import infra.redis_repo as rr
    monkeypatch.setattr(rr, "canon_strategy", canon_strategy, raising=False)
    monkeypatch.setattr(rr, "canon_symbol", canon_symbol, raising=False)
    monkeypatch.setattr(rr, "canon_tf", canon_tf, raising=False)
    monkeypatch.setattr(rr, "canon_source", canon_source, raising=False)


def test_load_open_positions_decodes_bytes_without_b_prefix():
    """Проверяем, что load_open_positions корректно декодирует bytes без артефакта b'...'"""
    r = FakeRedis(return_bytes=True)
    repo = RedisTradeRepository(r)

    pos = PositionState(
        id="o1",
        sid="s1",
        strategy="strat",
        source="src",
        symbol="ETHUSDT",
        tf="1m",
        direction=Side.LONG,
        entry_price=100.0,
        entry_ts_ms=1700000000000,
        lot=1.0,
        remaining_qty=1.0,
        sl=90.0,
        tp_levels=[110.0, 120.0, 130.0],
        trail_profile="tp1",
    )
    repo.save_open(pos)

    out = repo.load_open_positions(limit=10)
    assert len(out) == 1
    h = out[0]
    # важно: не должно быть "b'open'"
    assert h["status"] == "open"
    assert h["symbol"] == "ETHUSDT"
    # tp_levels должен быть валидным JSON-строкой
    assert json.loads(h["tp_levels"]) == [110.0, 120.0, 130.0]


def test_save_open_writes_profile_both_keys_and_entry_time_aliases():
    """Проверяем, что save_open пишет trail_profile и trailing_profile, а также entry_ts_ms и entry_time"""
    r = FakeRedis()
    repo = RedisTradeRepository(r)

    pos = PositionState(
        id="o2",
        sid="s2",
        strategy="strat",
        source="src",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        entry_price=200.0,
        entry_ts_ms=1700000000001,
        lot=2.0,
        remaining_qty=2.0,
        sl=180.0,
        tp_levels=[220.0],
        trail_profile="after_tp1",
    )
    repo.save_open(pos)

    h = r.hashes["order:o2"]
    assert h["trail_profile"] == "after_tp1"
    assert h["trailing_profile"] == "after_tp1"  # alias
    assert h["entry_ts_ms"] == str(pos.entry_ts_ms)
    assert h["entry_time"] == str(pos.entry_ts_ms)  # alias


def test_save_closed_idempotent_done_key_prevents_duplicate_stream_and_lists():
    """Проверяем, что save_closed идемпотентен и не создаёт дубли в stream/lists"""
    r = FakeRedis()
    repo = RedisTradeRepository(r)

    # open index to be removed
    r.sadd("orders:open", "o3")
    r.hset("order:o3", {"status": "open"})

    closed = TradeClosed(
        order_id="o3",
        strategy="strat",
        source="src",
        symbol="ETHUSDT",
        tf="1m",
        exit_ts_ms=1700000001000,
        exit_price=105.0,
        entry_price=100.0,
        lot=1.0,
        notional_usd=100.0,
        pnl_net=5.0,
        pnl_gross=5.5,
        fees=0.5,
        pnl_pct=0.05,
        pnl_if_fixed_exit=4.0,
        tp_hits=1,
        tp1_hit=True,
        tp2_hit=False,
        tp3_hit=False,
        tp_before_sl=True,
        close_reason="tp1",
        trailing_profile="tp1",
    )

    repo.save_closed(closed)
    repo.save_closed(closed)  # повтор

    # stream должен быть 1 событие
    assert len(r.streams.get(TRADES_CLOSED_STREAM_NAME, [])) == 1
    # legacy list тоже 1 раз
    k1 = "closed:strat:ETHUSDT:1m"
    k2 = "closed:strat:ETHUSDT:1m:src"
    assert r.lists.get(k1, []) == ["o3"]
    assert r.lists.get(k2, []) == ["o3"]
    # open index очищен
    assert "o3" not in r.sets.get("orders:open", set())
    # профили должны быть в обоих ключах в hash
    h = r.hashes["order:o3"]
    assert h["trail_profile"] == "tp1"
    assert h["trailing_profile"] == "tp1"


def test_save_closed_health_snapshot_param_merged_to_stream():
    """Проверяем, что health_snapshot параметр корректно добавляется в stream"""
    r = FakeRedis()
    repo = RedisTradeRepository(r)

    r.sadd("orders:open", "o4")
    r.hset("order:o4", {"status": "open"})

    closed = TradeClosed(
        order_id="o4",
        strategy="strat",
        source="src",
        symbol="BTCUSDT",
        tf="1m",
        exit_ts_ms=1700000002000,
        exit_price=205.0,
        entry_price=200.0,
        lot=1.0,
        notional_usd=200.0,
        pnl_net=5.0,
        pnl_gross=5.0,
        fees=0.0,
        pnl_pct=0.025,
        pnl_if_fixed_exit=5.0,
        tp_hits=0,
        tp1_hit=False,
        tp2_hit=False,
        tp3_hit=False,
        tp_before_sl=False,
        close_reason="manual",
        trailing_profile="",
    )
    
    # FIX #9: health передаётся как параметр (простое и явное решение)
    health_snapshot = {"health_l2_stale_ratio_tick": "0.1", "health_dlq_rate": "0.02"}
    repo.save_closed(closed, health_snapshot=health_snapshot)

    ev = r.streams[TRADES_CLOSED_STREAM_NAME][0]
    assert ev["health_l2_stale_ratio_tick"] == "0.1"
    assert ev["health_dlq_rate"] == "0.02"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

