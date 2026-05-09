import threading


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.lock = threading.Lock()

    def get(self, k):
        with self.lock:
            return self.kv.get(k)

    def set(self, k, v, ex=None, nx=False, xx=False):
        with self.lock:
            if nx and k in self.kv:
                return None
            if xx and k not in self.kv:
                return None
            self.kv[k] = v
            return True

    def delete(self, k):
        with self.lock:
            self.kv.pop(k, None)
        return 1

    def hset(self, key, mapping=None, **kwargs):
        with self.lock:
            if key not in self.kv:
                self.kv[key] = {}
            if mapping:
                self.kv[key].update(mapping)
            return len(mapping or {})

    def srem(self, key, *values):
        with self.lock:
            if key not in self.kv:
                self.kv[key] = set()
            if isinstance(self.kv[key], set):
                return len([v for v in values if v in self.kv[key]])
            return 0

    def sadd(self, key, *values):
        with self.lock:
            if key not in self.kv:
                self.kv[key] = set()
            if isinstance(self.kv[key], set):
                self.kv[key].update(values)
            return len(values)

    def zadd(self, key, mapping, **kwargs):
        with self.lock:
            if key not in self.kv:
                self.kv[key] = {}
            if isinstance(self.kv[key], dict):
                self.kv[key].update(mapping)
            return len(mapping)

    def xadd(self, key, fields, maxlen=None, **kwargs):
        with self.lock:
            if key not in self.kv:
                self.kv[key] = []
            if isinstance(self.kv[key], list):
                self.kv[key].append(fields)
            return "fake_id"

    def expire(self, key, time):
        return True


def test_save_closed_sets_sid_done_key(monkeypatch):
    from infra.redis_repo import RedisTradeRepository

    r = FakeRedis()
    repo = RedisTradeRepository(r)

    from dataclasses import dataclass

    @dataclass
    class Closed:
        order_id: str = "oid1"
        sid: str = "sid1"
        symbol: str = "ETHUSDT"
        exit_ts_ms: int = 1000000
        exit_price: float = 110.0
        entry_price: float = 100.0
        lot: float = 1.0
        notional_usd: float = 100.0
        pnl_net: float = 10.0
        pnl_gross: float = 10.0
        fees: float = 0.0
        pnl_pct: float = 0.1
        pnl_if_fixed_exit: float = 10.0
        tp_hits: int = 1
        tp1_hit: bool = True
        tp2_hit: bool = False
        tp3_hit: bool = False
        tp_before_sl: int = 1
        close_reason_raw: str = "TP1"
        close_reason: str = "TP1"
        close_reason_detail: str = ""
        baseline_exit_reason: str = ""
        baseline_exit_ts_ms: int = 1000000
        baseline_exit_price: float = 110.0
        entry_tag: str = ""
        trailing_profile: str = ""
        trail_profile: str = ""
        trailing_min_lock_r: float = 0.0
        trailing_active: bool = False
        trailing_started: bool = False
        trailing_moves: int = 0
        duration_ms: int = 1000000
        mfe_pnl: float = 15.0
        mae_pnl: float = -5.0
        giveback: float = 5.0
        missed_profit: float = 5.0
        one_r_money: float = 10.0
        r_multiple: float = 1.0
        max_favorable_price: float = 115.0
        max_favorable_ts: int = 1000000
        schema_version: int = 1
        strategy: str = "test"
        source: str = "test"
        tf: str = "1m"
        direction: str = "LONG"
        entry_regime: str = ""
        entry_ts_ms: int = 0

    # Чтобы тест был устойчивым: выставим TTL env
    monkeypatch.setenv("CLOSED_SID_DONE_TTL_DAYS", "7")

    # Тут вызывается ваша реальная логика save_closed(); если она требует доп. полей,
    # добавьте их в Closed (минимально).
    repo.save_closed(Closed(), health_snapshot={})

    assert r.get("closed_sid_done:sid1") is not None
