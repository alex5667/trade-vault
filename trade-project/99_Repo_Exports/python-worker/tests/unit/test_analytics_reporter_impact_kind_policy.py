import json

import pytest

from services.telegram.analytics_reporter import AnalyticsReporter


class FakeTG:
    def __init__(self):
        self.msgs = []
    def send_text(self, text: str) -> bool:
        self.msgs.append(text)
        return True

def test_per_kind_thresholds_used(monkeypatch):
    monkeypatch.setenv("ANALYTICS_TG_ENABLE", "1")
    monkeypatch.setenv("ANALYTICS_TG_INTERVAL_S", "1")
    monkeypatch.setenv("SEM_DEDUP_ALERT_MIN_EVENTS", "10")
    monkeypatch.setenv("SEM_DEDUP_ALERT_HIGH", "0.60")
    monkeypatch.setenv("SEM_DEDUP_ALERT_LOW", "0.05")
    monkeypatch.setenv("SEM_DEDUP_ALERT_HIGH_BY_KIND", "breakout=0.55,extreme=0.75")
    monkeypatch.setenv("SEM_DEDUP_ALERT_COOLDOWN_S", "0")

    tg = FakeTG()
    r = AnalyticsReporter(tg=tg)

    # breakout ratio=0.58 should trigger overtight because high_thr=0.55 for breakout
    for _ in range(58):
        r.record_sem_dedup(symbol="BTCUSDT", kind="breakout", hit=True)
    for _ in range(42):
        r.record_sem_dedup(symbol="BTCUSDT", kind="breakout", hit=False)

    r.maybe_flush(now_ms=10_000_000)
    assert tg.msgs
    obj = json.loads(tg.msgs[-1])
    assert obj["type"] == "sem_dedup_analytics"
    assert obj["offenders"]["overtight"]
    assert obj["offenders"]["overtight"][0]["high_thr"] == pytest.approx(0.55)

def test_impact_bad_when_hits_up_dups_not_down(monkeypatch):
    monkeypatch.setenv("ANALYTICS_TG_ENABLE", "1")
    monkeypatch.setenv("ANALYTICS_TG_INTERVAL_S", "1")
    monkeypatch.setenv("SEM_DEDUP_ALERT_MIN_EVENTS", "10")
    monkeypatch.setenv("SEM_DEDUP_ALERT_COOLDOWN_S", "0")
    monkeypatch.setenv("SEM_DEDUP_IMPACT_ENABLE", "1")
    monkeypatch.setenv("SEM_DEDUP_IMPACT_MIN_EVENTS", "10")
    monkeypatch.setenv("SEM_DEDUP_IMPACT_HITS_GROWTH_PCT", "0.20")
    monkeypatch.setenv("SEM_DEDUP_IMPACT_DUP_DROP_PCT_MIN", "0.10")

    tg = FakeTG()
    r = AnalyticsReporter(tg=tg)

    # Window 1: hits=50, dups=50
    for _ in range(50):
        r.record_sem_dedup(symbol="BTCUSDT", kind="breakout", hit=True)
        r.record_downstream_dup(symbol="BTCUSDT", kind="breakout")
    for _ in range(50):
        r.record_sem_dedup(symbol="BTCUSDT", kind="breakout", hit=False)
    r.maybe_flush(now_ms=10_000_000)

    # Window 2: hits grow to 70 (+40%), but dups stay 50 (no drop) => impact bad
    for _ in range(70):
        r.record_sem_dedup(symbol="BTCUSDT", kind="breakout", hit=True)
    for _ in range(30):
        r.record_sem_dedup(symbol="BTCUSDT", kind="breakout", hit=False)
    for _ in range(50):
        r.record_downstream_dup(symbol="BTCUSDT", kind="breakout")
    r.maybe_flush(now_ms=10_000_000 + 2000)

    obj = json.loads(tg.msgs[-1])
    assert obj["impact"]["status"] == "bad"
    assert obj["impact"]["pairs"]
    p = obj["impact"]["pairs"][0]
    assert p["symbol"] == "BTCUSDT"
    assert p["kind"] == "breakout"
