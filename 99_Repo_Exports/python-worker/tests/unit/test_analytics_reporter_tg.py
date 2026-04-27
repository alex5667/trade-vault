import pytest

from services.telegram.analytics_reporter import AnalyticsReporter

class FakeTG:
    def __init__(self):
        self.msgs = []
    def send_text(self, text: str) -> bool:
        self.msgs.append(text)
        return True

def test_reporter_sends_overtight(monkeypatch):
    monkeypatch.setenv("ANALYTICS_TG_ENABLE", "1")
    monkeypatch.setenv("ANALYTICS_TG_INTERVAL_S", "1")
    monkeypatch.setenv("SEM_DEDUP_ALERT_MIN_EVENTS", "10")
    monkeypatch.setenv("SEM_DEDUP_ALERT_HIGH", "0.60")
    monkeypatch.setenv("SEM_DEDUP_ALERT_LOW", "0.05")
    monkeypatch.setenv("SEM_DEDUP_ALERT_COOLDOWN_S", "0")

    tg = FakeTG()
    r = AnalyticsReporter(tg=tg)
    # 9 hits, 1 write => 0.9 (пережимаете)
    for _ in range(9):
        r.record_sem_dedup(symbol="BTCUSDT", kind="breakout", hit=True)
    for _ in range(1):
        r.record_sem_dedup(symbol="BTCUSDT", kind="breakout", hit=False)
    r.maybe_flush(now_ms=10_000_000)  # force
    assert tg.msgs
    assert "Пережимаете" in tg.msgs[-1]

def test_reporter_sends_undertight(monkeypatch):
    monkeypatch.setenv("ANALYTICS_TG_ENABLE", "1")
    monkeypatch.setenv("ANALYTICS_TG_INTERVAL_S", "1")
    monkeypatch.setenv("SEM_DEDUP_ALERT_MIN_EVENTS", "10")
    monkeypatch.setenv("SEM_DEDUP_ALERT_HIGH", "0.60")
    monkeypatch.setenv("SEM_DEDUP_ALERT_LOW", "0.05")
    monkeypatch.setenv("SEM_DEDUP_ALERT_COOLDOWN_S", "0")

    tg = FakeTG()
    r = AnalyticsReporter(tg=tg)
    # 0 hits, 10 writes => 0.0 (недожимаете)
    for _ in range(10):
        r.record_sem_dedup(symbol="BTCUSDT", kind="breakout", hit=False)
    r.maybe_flush(now_ms=10_000_000)
    assert tg.msgs
    assert "Недожимаете" in tg.msgs[-1]
