from __future__ import annotations

import pytest

from signals.empirical_levels import read_empirical_level_stats


class FakeRedis:
    def __init__(self) -> None:
        self.lists = {}
        self.hashes = {}

    def lrange(self, key: str, start: int, end: int):
        xs = self.lists.get(key, [])
        if end == -1:
            return xs[start:]
        return xs[start : end + 1]

    def hgetall(self, key: str):
        return dict(self.hashes.get(key, {}))


def _k(kind: str, sym: str, tf: str, rg: str, metric: str) -> str:
    return f"statsbuf:{kind}:{sym}:{tf}:{rg}:{metric}"


def test_timebucket_reader_uses_bucket_lists(monkeypatch: pytest.MonkeyPatch):
    # Enable time-bucket read and define buckets (1,2,3 minutes).
    monkeypatch.setenv("EMP_TIME_SNAPSHOT_READ", "1")
    monkeypatch.setenv("EMP_TIME_BUCKETS_MINUTES", "1,2,3")
    monkeypatch.setenv("EMP_TTD_FAST_IF_REGIME", "0")  # force median

    r = FakeRedis()
    kind, sym, tf, rg = "breakout", "BTCUSDT", "1m", "na"

    # median TTD = 120000ms => bucket ceil => 120000ms
    r.lists[_k(kind, sym, tf, rg, "ttd_ms")] = [b"60000", b"120000", b"120000", b"180000", b"240000"]

    # bucketed lists (the ones we expect to be used)
    r.lists[_k(kind, sym, tf, rg, "mfe_bps_t120000")] = [b"100", b"200", b"300", b"400", b"500"]
    r.lists[_k(kind, sym, tf, rg, "mae_bps_t120000")] = [b"10", b"20", b"30", b"40", b"50"]

    # legacy lists (should be ignored if bucket lists exist)
    r.lists[_k(kind, sym, tf, rg, "mfe_bps")] = [b"999", b"999", b"999", b"999", b"999"]
    r.lists[_k(kind, sym, tf, rg, "mae_bps")] = [b"888", b"888", b"888", b"888", b"888"]

    st = read_empirical_level_stats(
        r,
        symbol=sym,
        kind=kind,
        regime=rg,
        tf=tf,
        use_regime_dim=False,
        buf_max=300,
        samples=0,
    )
    assert st is not None
    # nearest-rank q=0.60 on 5 points => ceil(3)-1=2 => third element in sorted list
    assert st.mfe_tp1_bps_q60 == 300.0
    assert st.mae_to_tp1_bps_q80 == 40.0  # q=0.80 => ceil(4)-1=3 => 4th element
    assert st.ttd_tp1_ms_median == 120000


def test_timebucket_reader_falls_back_to_legacy(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("EMP_TIME_SNAPSHOT_READ", "1")
    monkeypatch.setenv("EMP_TIME_BUCKETS_MINUTES", "1,2,3")
    monkeypatch.setenv("EMP_TTD_FAST_IF_REGIME", "0")

    r = FakeRedis()
    kind, sym, tf, rg = "breakout", "BTCUSDT", "1m", "na"

    r.lists[_k(kind, sym, tf, rg, "ttd_ms")] = [b"60000", b"120000", b"120000", b"180000", b"240000"]
    # bucket lists missing -> must use legacy
    r.lists[_k(kind, sym, tf, rg, "mfe_bps")] = [b"1", b"2", b"3", b"4", b"5"]
    r.lists[_k(kind, sym, tf, rg, "mae_bps")] = [b"10", b"20", b"30", b"40", b"50"]

    st = read_empirical_level_stats(
        r,
        symbol=sym,
        kind=kind,
        regime=rg,
        tf=tf,
        use_regime_dim=False,
        buf_max=300,
        samples=0,
    )
    assert st is not None
    assert st.mfe_tp1_bps_q60 == 3.0
    assert st.mae_to_tp1_bps_q80 == 40.0


def test_survival_gate_blocks_when_below_threshold(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("EMP_TIME_SNAPSHOT_READ", "1")
    monkeypatch.setenv("EMP_TIME_BUCKETS_MINUTES", "1,2,3")
    monkeypatch.setenv("EMP_TTD_FAST_IF_REGIME", "0")
    monkeypatch.setenv("EMP_SURVIVE_MIN", "0.8")

    r = FakeRedis()
    kind, sym, tf, rg = "breakout", "BTCUSDT", "1m", "na"

    r.lists[_k(kind, sym, tf, rg, "ttd_ms")] = [b"120000", b"120000", b"120000", b"120000", b"120000"]
    r.lists[_k(kind, sym, tf, rg, "mfe_bps_t120000")] = [b"100", b"200", b"300", b"400", b"500"]
    r.lists[_k(kind, sym, tf, rg, "mae_bps_t120000")] = [b"10", b"20", b"30", b"40", b"50"]

    # total=10, alive=6 => 0.6 < 0.8 => should return None (caller falls back to RR/ATR levels)
    r.hashes[f"statscnt:{kind}:{sym}:{tf}:{rg}:survival"] = {
        b"total": b"10",
        b"alive_t120000": b"6",
    }

    st = read_empirical_level_stats(
        r,
        symbol=sym,
        kind=kind,
        regime=rg,
        tf=tf,
        use_regime_dim=False,
        buf_max=300,
        samples=0,
    )
    assert st is None
