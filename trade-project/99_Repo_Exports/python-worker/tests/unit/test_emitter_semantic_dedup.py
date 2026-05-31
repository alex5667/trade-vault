
from handlers.emitter.unified_signal_emitter import UnifiedSignalEmitter


class FakeOutbox:
    def __init__(self):
        self.published = []
    def publish(self, payload):
        self.published.append(payload)
        return "1"

class FakeAnalytics:
    def __init__(self):
        self.hits = 0
        self.writes = 0
    def record_sem_dedup(self, *, symbol, kind, hit):
        if hit:
            self.hits += 1
        else:
            self.writes += 1
    def record_soft_reasons(self, *, symbol, kind, payload):
        return
    def maybe_flush(self, *, now_ms=None):
        return

def test_semantic_dedup_blocks_same_bucket(monkeypatch):
    monkeypatch.setenv("OUTBOX_SEM_DEDUP", "1")
    monkeypatch.setenv("OUTBOX_SEM_DEDUP_BUCKET_MS", "1000")
    monkeypatch.setenv("OUTBOX_SEM_DEDUP_TTL_MS", "15000")
    monkeypatch.setenv("OUTBOX_SEM_DEDUP_INCLUDE_VENUE_TF", "1")
    monkeypatch.setenv("OUTBOX_SEM_DEDUP_LEVEL_FOR_KINDS", "breakout")

    outbox = FakeOutbox()
    a = FakeAnalytics()
    em = UnifiedSignalEmitter(outbox=outbox, outbox_labels=outbox, logger=type("L", (), {"exception": lambda *x, **y: None})(), analytics=a)

    p1 = {"symbol":"BTCUSDT","kind":"breakout","ts":1700000000123,"level_price":42000.12,"venue":"binance","timeframe":"1m","signal_id":"s1"}
    p2 = {"symbol":"BTCUSDT","kind":"breakout","ts":1700000000788,"level_price":42000.12,"venue":"binance","timeframe":"1m","signal_id":"s2"}

    ok1 = em.emit(p1, labels=None, dedup=False)
    ok2 = em.emit(p2, labels=None, dedup=False)

    assert ok1 is True
    assert ok2 is False  # same bucket => semantic block
    assert len(outbox.published) == 1
    assert a.writes >= 1
    assert a.hits >= 1
