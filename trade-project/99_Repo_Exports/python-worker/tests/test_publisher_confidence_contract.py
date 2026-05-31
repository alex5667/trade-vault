from __future__ import annotations

from types import SimpleNamespace


def test_signal_publisher_never_sets_confidence():
    # Import inside test so repo users can relocate modules without breaking collection.
    from core.signal_publisher import SignalPublisher

    class Outbox:
        def publish(self, payload):
            return True

    class Logger:
        def exception(self, msg):
            pass

    pub = SignalPublisher(outbox=Outbox(), logger=Logger())

    ctx = SimpleNamespace(symbol="BTCUSDT", ts=123, price=100.0)
    res = SimpleNamespace(kind="breakout", side=1, raw_score=2.0, final_score=1.2, signal_id="sid", reasons=[], parts={})

    payload = pub.build_payload(ctx=ctx, result=res)
    assert "confidence" not in payload, "Publisher must not write confidence (pipeline is the only writer)"


def test_signal_publisher_publish_ok():
    from core.signal_publisher import SignalPublisher

    class Outbox:
        def __init__(self):
            self.items = []

        def publish(self, payload):
            self.items.append(payload)
            return True

    class Logger:
        def exception(self, msg):
            pass

    outbox = Outbox()
    pub = SignalPublisher(outbox=outbox, logger=Logger())
    ok = pub.publish({"kind": "x", "final_score": 1.0})
    assert ok is True
    assert len(outbox.items) == 1
