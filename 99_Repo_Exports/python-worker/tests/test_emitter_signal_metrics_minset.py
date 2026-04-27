from common.metrics2 import InMemoryMetrics
from handlers.emitter.unified_signal_emitter import UnifiedSignalEmitter


class FakeOutbox:
    def __init__(self):
        self.messages = []

    def publish(self, message):
        self.messages.append(message)
        return True


class FakeLogger:
    def exception(self, msg: str) -> None:
        return


def test_emitter_counts_sent_and_label_driven_protective():
    m = InMemoryMetrics()
    outbox = FakeOutbox()
    em = UnifiedSignalEmitter(outbox=outbox, logger=FakeLogger(), metrics=m)

    payload = {"kind": "breakout", "symbol": "BTCUSDT", "ts": 123, "signal_id": "s1"}
    ok = em.emit(payload, labels={"touch_suppressed": 1}, dedup=False)
    assert ok is True

    # signals_sent
    assert any(n == "signals_sent" for (n, v, t) in m.counters)
    # touch_suppressed_total
    assert any(n == "touch_suppressed_total" for (n, v, t) in m.counters)