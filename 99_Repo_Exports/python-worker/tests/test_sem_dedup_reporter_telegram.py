import pytest

from monitoring.sem_dedup_reporter import SemDedupReporter, SemDedupPolicy, TelegramSink


class FakeLogger:
    def __init__(self) -> None:
        self.exc = []
    def exception(self, msg: str) -> None:
        self.exc.append(msg)


class FakeTG(TelegramSink):
    def __init__(self) -> None:
        self.sent = []
    def send(self, text: str) -> bool:
        self.sent.append(text)
        return True


class FakeEmitter:
    def __init__(self, snapshots):
        self._snaps = list(snapshots)
        self.i = 0
    def get_sem_stats_snapshot(self):
        s = self._snaps[min(self.i, len(self._snaps) - 1)]
        self.i += 1
        return s


def test_over_suppression_sends_with_explanations():
    # prev -> cur interval deltas: hits=80, writes=20 => ratio=0.8 (over)
    prev = {
        "enabled": True, "bucket_ms": 1000, "level_decimals": 2, "ttl_ms": 15000,
        "hits": {"BTCUSDT|breakout": 0}, "writes": {"BTCUSDT|breakout": 0},
    }
    cur = {
        "enabled": True, "bucket_ms": 1000, "level_decimals": 2, "ttl_ms": 15000,
        "hits": {"BTCUSDT|breakout": 80}, "writes": {"BTCUSDT|breakout": 20},
    }
    em = FakeEmitter([prev, cur])
    tg = FakeTG()
    log = FakeLogger()
    pol = SemDedupPolicy(min_events=10, over_ratio=0.6, under_ratio=0.05, hits_spike_per_min=999999, top_n=3)
    rep = SemDedupReporter(emitter=em, tg=tg, logger=log, policy=pol)

    # init prev
    assert rep.run_once(now_ms=1000000) is None
    msg = rep.run_once(now_ms=1000000 + 60_000)  # 1 min
    assert msg is not None
    assert "ПЕРЕЖИМ" in msg
    assert "Рекомендации (ослабить dedup)" in msg
    assert "Аналитика по sem_dedup_hits_total" in msg

    assert rep.maybe_send() is False  # no new delta now (FakeEmitter repeats last snap)


def test_under_suppression_sends_with_explanations():
    # hits=1, writes=99 => ratio=0.01 (under)
    prev = {
        "enabled": True, "bucket_ms": 1000, "level_decimals": 2, "ttl_ms": 15000,
        "hits": {"ETHUSDT|absorption": 0}, "writes": {"ETHUSDT|absorption": 0},
    }
    cur = {
        "enabled": True, "bucket_ms": 1000, "level_decimals": 2, "ttl_ms": 15000,
        "hits": {"ETHUSDT|absorption": 1}, "writes": {"ETHUSDT|absorption": 99},
    }
    em = FakeEmitter([prev, cur])
    tg = FakeTG()
    log = FakeLogger()
    pol = SemDedupPolicy(min_events=10, over_ratio=0.9, under_ratio=0.05, hits_spike_per_min=999999, top_n=3)
    rep = SemDedupReporter(emitter=em, tg=tg, logger=log, policy=pol)

    assert rep.run_once(now_ms=1000000) is None
    msg = rep.run_once(now_ms=1000000 + 60_000)
    assert msg is not None
    assert "НЕДОЖИМ" in msg
    assert "Рекомендации (усилить dedup)" in msg


def test_hits_spike_triggers_even_if_ratio_not_over():
    # hits=300, writes=700 => ratio=0.3 (not over), but hits/min spike triggers
    prev = {
        "enabled": True, "bucket_ms": 1000, "level_decimals": 2, "ttl_ms": 15000,
        "hits": {"BTCUSDT|breakout": 0}, "writes": {"BTCUSDT|breakout": 0},
    }
    cur = {
        "enabled": True, "bucket_ms": 1000, "level_decimals": 2, "ttl_ms": 15000,
        "hits": {"BTCUSDT|breakout": 300}, "writes": {"BTCUSDT|breakout": 700},
    }
    em = FakeEmitter([prev, cur])
    tg = FakeTG()
    log = FakeLogger()
    pol = SemDedupPolicy(min_events=10, over_ratio=0.9, under_ratio=0.01, hits_spike_per_min=200, top_n=3)
    rep = SemDedupReporter(emitter=em, tg=tg, logger=log, policy=pol)

    assert rep.run_once(now_ms=1000000) is None
    msg = rep.run_once(now_ms=1000000 + 60_000)
    assert msg is not None
    assert "СПАЙК" in msg
    assert "hits_rate" in msg
