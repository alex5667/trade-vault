import json

import pytest

from handlers.crypto_orderflow.pipeline.candidate_emit_pipeline_v2 import (
    CandidateEmitPipelineV2,
    CandidateFrame,
)


class FakeRedis:
    def __init__(self):
        self.calls = []

    def xadd(self, stream, fields, maxlen=None, approximate=True):
        self.calls.append((stream, dict(fields), maxlen, approximate))
        return "0-1"


class DummyCandidate:
    def __init__(self, sid: str):
        self.signal_id = sid
        self.tp_levels = []


class DummyCtx:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.trace_id = "trace-1"


class DummyExtractor:
    def __init__(self, frames):
        self._frames = frames

    def extract(self, ctx):
        return list(self._frames)


class DummyGates:
    def regime(self, f):
        return False, "VETO_REGIME"


class DummyWriter:
    def emit(self, payload, payload_meta=None):
        raise AssertionError("tradeable emit must not run on veto")


@pytest.fixture
def pipeline(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_DIAG_STREAM", "diag:entry_policy")

    r = FakeRedis()

    # Minimal handler: only redis is needed for diagnostic stream.
    h = type("H", (), {"redis": r})()

    cand = DummyCandidate("sid-1")
    ctx = DummyCtx("BTCUSDT")

    frame = CandidateFrame(
        cand=cand,
        features={},
        kind_key="orderflow",
        kind_str="orderflow",
        ctx_symbol=ctx.symbol,
        ctx=ctx,
    )
    # Skip levels attachment (not relevant for this unit test).
    frame.memo["levels_ensured"] = True

    p = CandidateEmitPipelineV2.__new__(CandidateEmitPipelineV2)
    p.h = h
    p.extractor = DummyExtractor([frame])
    p.gates = DummyGates()
    p.scoring = None
    p.conf_gates = None
    p.builder = None
    p.writer = DummyWriter()
    p.obs = None
    p.news_enricher = None

    return p, r, ctx


def test_veto_goes_to_diag_stream_not_tradeable(pipeline):
    p, r, ctx = pipeline

    ok = p.emit(ctx=ctx)
    assert ok is False

    assert len(r.calls) == 1
    stream, fields, *_ = r.calls[0]
    assert stream == "diag:entry_policy"

    payload = json.loads(fields["data"])
    assert payload["sid"] == "sid-1"
    assert payload["reason_code"] == "VETO_REGIME"
    assert payload["name"] == "regime_gate"
