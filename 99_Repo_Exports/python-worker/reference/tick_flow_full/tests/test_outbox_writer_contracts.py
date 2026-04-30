def test_outbox_writer_puts_meta_extra_under_payload_meta(monkeypatch):
    import handlers.crypto_orderflow.pipeline.candidate_emit_pipeline_v2 as mod

    class Emitter:
        def __init__(self):
            self.calls = []
        def emit(self, payload, labels=None, dedup=False, meta=None):
            self.calls.append((payload, meta))
            return True

    em = Emitter()
    handler = type("H", (), {"_emitter": em})()
    ctx = type("CTX", (), {})()

    # build_trace_sidecar_meta должен дать базовый sidecar meta
    monkeypatch.setattr(
        mod
        "build_trace_sidecar_meta"
        lambda ctx, sid: {"schema": "decision_trace_sidecar:v1", "trace_id": "T-1"}
    )

    f = mod.CandidateFrame(
        handler=handler
        ctx=ctx
        cand=object()
        kind_str="k"
        kind_key="k"
        side_int=1
        ctx_symbol="BTCUSDT"
        ctx_ts=0
        ctx_price=1.0
    )

    payload = {"sid": "S-1", "signal_id": "S-1"}
    meta_extra = {"parts_full": {"x": [1, 2, 3]}, "schema": "MUST_NOT_OVERRIDE"}

    w = mod.OutboxWriter()
    ok = w.emit(f, payload, meta_extra=meta_extra)
    assert ok is True
    assert len(em.calls) == 1

    sent_payload, meta = em.calls[0]
    assert sent_payload["sid"] == "S-1"

    # schema/trace_id должны остаться сверху (sidecar), meta_extra — только внутри payload_meta
    assert meta["schema"] == "decision_trace_sidecar:v1"
    assert meta["trace_id"] == "T-1"
    assert meta["payload_meta"]["parts_full"] == {"x": [1, 2, 3]}
    assert meta["payload_meta"]["schema"] == "MUST_NOT_OVERRIDE"
