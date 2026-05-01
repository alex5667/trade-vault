from __future__ import annotations

from types import SimpleNamespace

from services.candidate_emit_pipeline_v2 import CandidateFrame, OutboxWriter
from common.contracts.tradeable_contracts import assert_outbox_sidecar_meta


class FakeEmitter:
    def __init__(self):
        self.calls = []

    def emit(self, payload, labels=None, dedup=True, meta=None):
        self.calls.append({"payload": payload, "meta": meta})
        return True


def test_outboxwriter_meta_extra_goes_to_payload_meta_namespace(monkeypatch):
    em = FakeEmitter()
    handler = SimpleNamespace(_emitter=em)
    ctx = SimpleNamespace()

    f = CandidateFrame(
        handler=handler,
        ctx=ctx,
        cand=SimpleNamespace(),
        kind_str="k",
        kind_key="k",
        side_int=1,
        ctx_symbol="BTCUSDT",
        ctx_ts=1,
        ctx_price=1.0,
    )

    w = OutboxWriter()
    payload = {"sid": "sid1", "signal_id": "sid1", "kind": "k", "side": "LONG", "symbol": "BTCUSDT", "ts": 1, "price": 1.0}
    meta_extra = {"parts_full": {"x": [1, 2, 3]}, "big": {"a": "b"}}

    ok = w.emit(f, payload, meta_extra=meta_extra)
    assert ok is True
    assert len(em.calls) == 1

    meta = em.calls[0]["meta"] or {}
    assert_outbox_sidecar_meta(meta, where="outboxwriter.meta")

    pm = meta.get("payload_meta")
    assert isinstance(pm, dict)
    assert pm.get("parts_full") == {"x": [1, 2, 3]}
    assert pm.get("big") == {"a": "b"}
