from __future__ import annotations

from handlers.emitter.unified_signal_emitter import UnifiedSignalEmitter
from handlers.emitter.label_schema import sys_labels
from handlers.pipeline.quality_flags import QualityFlag


class FakeOutbox:
    def __init__(self) -> None:
        self.published: list[dict] = []

    def publish(self, payload: dict) -> None:
        self.published.append(payload)


class FakeLogger:
    def exception(self, *_a, **_k) -> None:
        pass




def test_label_update_goes_to_labels_outbox():
    ob_sig = FakeOutbox()
    ob_lbl = FakeOutbox()
    em = UnifiedSignalEmitter(outbox=ob_sig, outbox_labels=ob_lbl, logger=FakeLogger())
    ok = em.emit({"kind": "label_update", "symbol": "BTCUSDT", "ts": 1, "label": "x"}, labels=sys_labels(label_event=1))
    assert ok is True
    assert len(ob_sig.published) == 0
    assert len(ob_lbl.published) == 1
    assert isinstance(ob_lbl.published[0].get("labels"), dict)
