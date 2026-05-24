"""Guards for PEdgeThresholdCalibrator-required flat fields on trades:closed.

Until 2026-05-23 `kind` was missing from `redis_repo.save_closed` output, which
made every calibrator bin collapse to `kind="*"` and starved the 4D
(symbol×regime×kind×direction) buckets. `ml_prob` only resolved through the deep
`signal_payload.indicators.of_confirm.evidence.ml_decision.p_edge` path, so any
signal that bypassed `MlConfirmGate` (iceberg/delta_spike/...) was skipped with
`ml_prob_missing`.
"""

import threading
from dataclasses import dataclass


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.streams = {}  # stream -> list of fields dicts
        self.lock = threading.Lock()

    def get(self, k):
        with self.lock:
            return self.kv.get(k)

    def set(self, k, v, ex=None, nx=False):
        with self.lock:
            if nx and k in self.kv:
                return None
            self.kv[k] = v
            return True

    def delete(self, k):
        with self.lock:
            self.kv.pop(k, None)
        return 1

    def xadd(self, stream, fields, maxlen=None, approximate=True):
        with self.lock:
            self.streams.setdefault(stream, []).append(fields)
        return "0-0"

    def hset(self, *a, **kw):
        return 1

    def expire(self, *a, **kw):
        return True

    def lpush(self, *a, **kw):
        return 1

    def ltrim(self, *a, **kw):
        return True

    def pipeline(self, transaction=False):
        return _FakePipe(self)


class _FakePipe:
    def __init__(self, r):
        self.r = r
        self.ops = []

    def xadd(self, stream, fields, maxlen=None, approximate=True):
        self.ops.append((stream, fields))
        return self

    def hset(self, *a, **kw):
        return self

    def expire(self, *a, **kw):
        return self

    def lpush(self, *a, **kw):
        return self

    def rpush(self, *a, **kw):
        return self

    def ltrim(self, *a, **kw):
        return self

    def sadd(self, *a, **kw):
        return self

    def srem(self, *a, **kw):
        return self

    def zadd(self, *a, **kw):
        return self

    def zrem(self, *a, **kw):
        return self

    def set(self, *a, **kw):
        return self

    def delete(self, *a, **kw):
        return self

    def hset(self, *a, **kw):
        return self

    def hdel(self, *a, **kw):
        return self

    def __getattr__(self, name):
        # Be permissive: any redis pipeline method we did not bother to mock
        # becomes a no-op chainable returning self.
        def _noop(*a, **kw):
            return self
        return _noop

    def execute(self):
        for stream, fields in self.ops:
            self.r.xadd(stream, fields)
        return [None] * len(self.ops)


@dataclass
class _Closed:
    order_id: str = "oid-iceberg-eth-1"
    sid: str = "iceberg:ETHUSDT:1779557817806:S"
    symbol: str = "ETHUSDT"
    exit_ts_ms: int = 1779560486384
    exit_price: float = 2083.43
    entry_price: float = 2061.97
    lot: float = 0.2424
    notional_usd: float = 499.8
    pnl_net: float = -5.50
    pnl_gross: float = -5.00
    fees: float = 0.5023
    pnl_pct: float = -1.0
    pnl_if_fixed_exit: float = -5.50
    tp_hits: int = 0
    tp1_hit: bool = False
    tp2_hit: bool = False
    tp3_hit: bool = False
    tp_before_sl: int = 0
    close_reason_raw: str = "TRAIL_SL"
    close_reason: str = "TRAIL_SL"
    close_reason_detail: str = ""
    baseline_exit_reason: str = ""
    baseline_exit_ts_ms: int = 0
    baseline_exit_price: float = 0.0
    entry_tag: str = "iceberg"
    trailing_profile: str = ""
    trail_profile: str = ""
    trailing_min_lock_r: float = 0.0
    trailing_active: bool = False
    trailing_started: bool = True
    trailing_moves: int = 0
    duration_ms: int = 2665577
    mfe_pnl: float = 0.38
    mae_pnl: float = -5.07
    giveback: float = 5.38
    missed_profit: float = 0.0
    one_r_money: float = 5.0
    r_multiple: float = -1.10
    max_favorable_price: float = 2058.0
    max_favorable_ts: int = 0
    schema_version: int = 1
    strategy: str = "cryptoorderflow"
    source: str = "CryptoOrderFlow"
    tf: str = "tick"
    direction: str = "SHORT"
    entry_regime: str = "range"
    entry_ts_ms: int = 1779557820807
    signal_payload: dict | str = ""


def _last_trades_closed(r, stream_name: str) -> dict:
    msgs = r.streams.get(stream_name, [])
    assert msgs, f"expected XADD into {stream_name}, got nothing"
    return msgs[-1]


def test_kind_extracted_from_entry_tag(monkeypatch):
    """When `entry_tag` is set, `kind` MUST be populated for the 4D calibrator bin."""
    from infra.redis_repo import RedisTradeRepository, TRADES_CLOSED_STREAM_NAME

    r = FakeRedis()
    repo = RedisTradeRepository(r)
    repo.save_closed(_Closed(), health_snapshot={})

    fields = _last_trades_closed(r, TRADES_CLOSED_STREAM_NAME)
    assert fields.get("kind") == "iceberg", (
        f"kind missing/wrong — calibrator would collapse to wildcard. fields={list(fields.keys())[:20]}"
    )


def test_kind_falls_back_to_sid_prefix_when_no_tag(monkeypatch):
    from infra.redis_repo import RedisTradeRepository, TRADES_CLOSED_STREAM_NAME

    r = FakeRedis()
    repo = RedisTradeRepository(r)
    c = _Closed(entry_tag="", strategy="", sid="delta_spike:SOLUSDT:1779000000000:L")
    repo.save_closed(c, health_snapshot={})

    fields = _last_trades_closed(r, TRADES_CLOSED_STREAM_NAME)
    assert fields.get("kind") == "delta_spike"


def test_ml_prob_fallback_top_level_p_edge_cal():
    """Signals that bypass MlConfirmGate carry p_edge at top of signal_payload.

    Without this fallback, calibrator skips them with `ml_prob_missing`.
    """
    from infra.redis_repo import RedisTradeRepository, TRADES_CLOSED_STREAM_NAME

    r = FakeRedis()
    repo = RedisTradeRepository(r)
    c = _Closed()
    c.signal_payload = {"p_edge_cal": 0.642}  # no nested ml_decision
    repo.save_closed(c, health_snapshot={})

    fields = _last_trades_closed(r, TRADES_CLOSED_STREAM_NAME)
    assert "ml_prob" in fields, "ml_prob fallback missed top-level p_edge_cal"
    assert float(fields["ml_prob"]) == 0.642


def test_ml_prob_deep_path_still_wins():
    """When deep `ml_decision.p_edge` is present, it must be preferred."""
    from infra.redis_repo import RedisTradeRepository, TRADES_CLOSED_STREAM_NAME

    r = FakeRedis()
    repo = RedisTradeRepository(r)
    c = _Closed()
    c.signal_payload = {
        "p_edge_cal": 0.50,
        "indicators": {
            "of_confirm": {
                "evidence": {"ml_decision": {"p_edge": 0.81}},
            },
        },
    }
    repo.save_closed(c, health_snapshot={})

    fields = _last_trades_closed(r, TRADES_CLOSED_STREAM_NAME)
    assert float(fields["ml_prob"]) == 0.81


def test_result_derived_from_r_multiple():
    from infra.redis_repo import RedisTradeRepository, TRADES_CLOSED_STREAM_NAME

    r = FakeRedis()
    repo = RedisTradeRepository(r)

    for r_mult, expected in ((1.5, "WIN"), (-1.1, "LOSS"), (0.0, "BE")):
        c = _Closed(r_multiple=r_mult)
        repo.save_closed(c, health_snapshot={})
        fields = _last_trades_closed(r, TRADES_CLOSED_STREAM_NAME)
        assert fields["result"] == expected, f"r_mult={r_mult} → expected {expected}"


def test_market_regime_falls_back_to_entry_regime():
    from infra.redis_repo import RedisTradeRepository, TRADES_CLOSED_STREAM_NAME

    r = FakeRedis()
    repo = RedisTradeRepository(r)
    repo.save_closed(_Closed(entry_regime="trending_bear"), health_snapshot={})

    fields = _last_trades_closed(r, TRADES_CLOSED_STREAM_NAME)
    assert fields["market_regime"] == "trending_bear"


def test_all_four_calibrator_axes_present_for_4d_bucket():
    """Smoke test of the full 4D-bucket key: (symbol, market_regime, kind, side)."""
    from infra.redis_repo import RedisTradeRepository, TRADES_CLOSED_STREAM_NAME

    r = FakeRedis()
    repo = RedisTradeRepository(r)
    c = _Closed()
    c.signal_payload = {"ml_prob": 0.71}
    repo.save_closed(c, health_snapshot={})

    fields = _last_trades_closed(r, TRADES_CLOSED_STREAM_NAME)
    assert fields["symbol"] == "ETHUSDT"
    assert fields["market_regime"] == "range"
    assert fields["kind"] == "iceberg"
    # calibrator reads `side` first, then falls back to `direction`
    assert fields.get("side") == "SHORT" or fields.get("direction") == "SHORT"
    assert "ml_prob" in fields and float(fields["ml_prob"]) == 0.71
    assert fields["result"] == "LOSS"
    assert float(fields["r_multiple"]) == -1.10
