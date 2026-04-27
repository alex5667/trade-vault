from __future__ import annotations

import json
import pytest

from services.stats_aggregator import _write_timebucket_buffers


class _Pipe:
    def __init__(self, r: "FakeRedis") -> None:
        self.r = r
        self.ops = []

    def hincrby(self, key: str, field: str, amt: int):
        self.ops.append(("hincrby", key, field, int(amt)))
        return self

    def expire(self, key: str, ttl: int):
        self.ops.append(("expire", key, int(ttl)))
        return self

    def lpush(self, key: str, val: str):
        self.ops.append(("lpush", key, str(val)))
        return self

    def ltrim(self, key: str, start: int, end: int):
        self.ops.append(("ltrim", key, int(start), int(end)))
        return self

    def execute(self):
        for op in self.ops:
            if op[0] == "hincrby":
                _, k, f, a = op
                self.r.hincrby(k, f, a)
            elif op[0] == "expire":
                # ttl ignored in fake (not needed for correctness tests)
                pass
            elif op[0] == "lpush":
                _, k, v = op
                self.r.lpush(k, v)
            elif op[0] == "ltrim":
                _, k, s, e = op
                self.r.ltrim(k, s, e)
        self.ops = []
        return True


class FakeRedis:
    def __init__(self) -> None:
        self._lists = {}
        self._hashes = {}

    def pipeline(self, transaction: bool = False):
        return _Pipe(self)

    def lpush(self, key: str, val: str) -> None:
        self._lists.setdefault(key, [])
        self._lists[key].insert(0, val)

    def ltrim(self, key: str, start: int, end: int) -> None:
        xs = self._lists.get(key, [])
        if not xs:
            return
        self._lists[key] = xs[start : end + 1]

    def lrange(self, key: str, start: int, end: int):
        xs = self._lists.get(key, [])
        if end == -1:
            return xs[start:]
        return xs[start : end + 1]

    def hincrby(self, key: str, field: str, amt: int) -> None:
        h = self._hashes.setdefault(key, {})
        cur = int(h.get(field, 0) or 0)
        h[field] = str(cur + int(amt))

    def hgetall(self, key: str):
        return dict(self._hashes.get(key, {}))


def test_write_timebucket_buffers_writes_lists_and_survival(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("EMP_TIME_SNAPSHOTS_WRITE", "1")
    monkeypatch.setenv("EMP_TIME_BUCKETS_MINUTES", "1,2,3")
    monkeypatch.setenv("EMP_TIME_SNAPSHOT_BUF_MAX", "50")
    monkeypatch.setenv("EMP_TIME_SNAPSHOT_BUF_TTL_SEC", "0")

    r = FakeRedis()

    # entry=100, lot=2 => notional=200
    # pnl 10 => bps = 10/200*10000 = 500
    trade_closed = {
        "entry_price": 100.0,
        "lot": 2.0,
        "duration_ms": 180_000,  # 3 minutes
        "mfe_pnl_t": json.dumps({"60000": 10.0, "120000": 12.0, "180000": 14.0}),
        "mae_pnl_t": json.dumps({"60000": 6.0, "120000": 8.0, "180000": 10.0}),
    }

    _write_timebucket_buffers(
        r,
        strategy="breakout",
        symbol="BTCUSDT",
        tf="1m",
        regime_key="na",
        trade_closed=trade_closed,
    )

    k_mfe_60 = "statsbuf:breakout:BTCUSDT:1m:na:mfe_bps_t60000"
    k_mae_60 = "statsbuf:breakout:BTCUSDT:1m:na:mae_bps_t60000"
    assert r.lrange(k_mfe_60, 0, -1)[0] == "500.0"
    # mae 6 => 6/200*10000 = 300
    assert r.lrange(k_mae_60, 0, -1)[0] == "300.0"

    surv = r.hgetall("statscnt:breakout:BTCUSDT:1m:na:survival")
    assert surv["total"] == "1"
    assert surv["alive_t60000"] == "1"
    assert surv["alive_t120000"] == "1"
    assert surv["alive_t180000"] == "1"
