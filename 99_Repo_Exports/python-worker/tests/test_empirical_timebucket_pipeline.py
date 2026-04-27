from __future__ import annotations

import json
import pytest

from services.stats_aggregator import _write_timebucket_buffers, canon_regime
from signals.empirical_levels import RedisEmpiricalStatsProvider


class FakePipe:
    def __init__(self, r: "FakeRedis"):
        self.r = r
        self.ops = []

    def hincrby(self, key, field, amount):
        self.ops.append(("hincrby", key, field, int(amount)))
        return self

    def expire(self, key, ttl):
        self.ops.append(("expire", key, int(ttl)))
        return self

    def lpush(self, key, val):
        self.ops.append(("lpush", key, val))
        return self

    def ltrim(self, key, start, end):
        self.ops.append(("ltrim", key, int(start), int(end)))
        return self

    def execute(self):
        for op in self.ops:
            if op[0] == "hincrby":
                _, key, field, amt = op
                h = self.r.hashes.setdefault(key, {})
                h[field] = int(h.get(field, 0)) + int(amt)
            elif op[0] == "expire":
                # TTL ignored in fake
                pass
            elif op[0] == "lpush":
                _, key, val = op
                self.r.lists.setdefault(key, [])
                self.r.lists[key].insert(0, val)
            elif op[0] == "ltrim":
                _, key, start, end = op
                xs = self.r.lists.get(key, [])
                self.r.lists[key] = xs[start : end + 1]
        self.ops = []
        return True


class FakeRedis:
    def __init__(self):
        self.lists = {}
        self.hashes = {}

    def pipeline(self, transaction=False):
        return FakePipe(self)

    def lrange(self, key, start, end):
        xs = self.lists.get(key, [])
        if end == -1:
            return xs[start:]
        return xs[start : end + 1]

    def hgetall(self, key):
        # provider expects bytes-or-str; we can return bytes to emulate redis-py
        h = self.hashes.get(key, {})
        out = {}
        for k, v in h.items():
            kk = k.encode() if isinstance(k, str) else k
            vv = str(v).encode() if not isinstance(v, (bytes, bytearray)) else v
            out[kk] = vv
        return out


def test_stats_aggregator_writer_emits_timebucket_lists(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("EMP_TIME_SNAPSHOT_WRITE", "1")
    monkeypatch.setenv("EMP_TIME_BUCKETS_MINUTES", "1,2,3")
    monkeypatch.setenv("EMP_TIME_SNAPSHOT_BUF_MAX", "300")
    monkeypatch.setenv("EMP_TIME_SNAPSHOT_BUF_TTL_SEC", "0")

    r = FakeRedis()

    # TradeClosed carries JSON dicts in money
    trade_closed = {
        "entry_price": 100.0,
        "lot": 1.0,
        "notional_usd": 100.0,
        "duration_ms": 180000,
        "mfe_pnl_t": json.dumps({60000: 1.0, 120000: 2.0, 180000: 3.0}),
        "mae_pnl_t": json.dumps({60000: 0.5, 120000: 1.0, 180000: 1.5}),
    }

    _write_timebucket_buffers(
        r,
        strategy="breakout",
        symbol="BTCUSDT",
        tf="1m",
        regime_key="na",
        trade_closed=trade_closed,
    )

    # bps = pnl/notional*10000 => 1.0/100*10000=100 bps
    assert r.lists["statsbuf:breakout:BTCUSDT:1m:na:mfe_bps_t60000"][0] == "100.0"
    assert r.lists["statsbuf:breakout:BTCUSDT:1m:na:mae_bps_t60000"][0] == "50.0"

    # survival counters were incremented
    surv = r.hashes["statscnt:breakout:BTCUSDT:1m:na:survival"]
    assert int(surv["total"]) == 1
    assert int(surv["alive_t60000"]) == 1
    assert int(surv["alive_t120000"]) == 1
    assert int(surv["alive_t180000"]) == 1


def test_provider_reads_strict_bucket_by_median_ttd(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("EMP_TIME_SNAPSHOTS_READ", "1")
    monkeypatch.setenv("EMP_TTD_FAST_IF_REGIME", "0")
    monkeypatch.setenv("EMP_TIME_BUCKETS_MINUTES", "1,2,3")
    monkeypatch.setenv("EMP_SURVIVE_MIN", "0")

    r = FakeRedis()
    # median(TTD)=120000 => choose bucket 120000
    r.lists["statsbuf:breakout:BTCUSDT:1m:na:ttd_ms"] = ["120000", "120000", "60000", "180000", "240000"]

    # enough samples (>=5) to pass provider thresholds
    r.lists["statsbuf:breakout:BTCUSDT:1m:na:mfe_bps_t120000"] = ["10", "20", "30", "40", "50"]
    r.lists["statsbuf:breakout:BTCUSDT:1m:na:mae_bps_t120000"] = ["1", "2", "3", "4", "5"]

    provider = RedisEmpiricalStatsProvider(r, tf="1m", buf_max=300, use_regime_dim=False)
    st = provider.get_level_stats("BTCUSDT", "breakout", "na", samples=0)
    assert st is not None
    assert st.mfe_tp1_bps_q60 == 30.0
    assert st.mae_to_tp1_bps_q80 == 4.0


def test_survival_gate_blocks(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("EMP_TIME_SNAPSHOTS_READ", "1")
    monkeypatch.setenv("EMP_TIME_BUCKETS_MINUTES", "1,2,3")
    monkeypatch.setenv("EMP_TTD_FAST_IF_REGIME", "0")
    monkeypatch.setenv("EMP_SURVIVE_MIN", "0.8")

    r = FakeRedis()
    r.lists["statsbuf:breakout:BTCUSDT:1m:na:ttd_ms"] = ["120000"] * 10
    r.lists["statsbuf:breakout:BTCUSDT:1m:na:mfe_bps_t120000"] = ["10", "20", "30", "40", "50"]
    r.lists["statsbuf:breakout:BTCUSDT:1m:na:mae_bps_t120000"] = ["1", "2", "3", "4", "5"]
    # survival: 6/10 < 0.8 => block
    r.hashes["statscnt:breakout:BTCUSDT:1m:na:survival"] = {"total": 10, "alive_t120000": 6}

    provider = RedisEmpiricalStatsProvider(r, tf="1m", buf_max=300, use_regime_dim=False)
    st = provider.get_level_stats("BTCUSDT", "breakout", "na", samples=0)
    assert st is None
