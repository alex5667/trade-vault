import json
import types
import pytest

from services.candidate_emit_pipeline_v2 import CandidateFrame, PayloadBuilder
from services.candidate_emit_pipeline_v2 import ConfidenceGateRunner  # если нужно
from services.candidate_emit_pipeline_v2 import GateRunner  # если у вас он там

class DummyCtx:
    pass

class DummyCand:
    def __init__(self):
        self.kind = "breakout"
        self.side = "LONG"
        self.raw_score = 1.0
        self.reasons = ["r1", "r2"]
        self.signal_id = "sid-1"

class DummyRes:
    def __init__(self, parts):
        self.parts = parts
        self.conf_factor01 = 0.7
        self.decision_code = "OK"
        self.decision_u16 = 42

def test_payload_builder_json_safe_and_parts_split():
    ctx = DummyCtx()
    cand = DummyCand()
    f = CandidateFrame(
        handler=object()
        ctx=ctx
        cand=cand
        kind_str="breakout"
        kind_key="breakout"
        side_int=1
        ctx_symbol="BTCUSDT"
        ctx_ts=123
        ctx_price=100.0
    )

    parts = {
        "small_scalar": 1
        "small_list": [1, 2, 3]
        "big_list": list(range(100)),         # должно уйти в meta.parts_full
        "big_dict": {str(i): i for i in range(100)},  # тоже в meta.parts_full
    }
    res = DummyRes(parts)

    b = PayloadBuilder()
    payload, meta = b.build(
        f
        raw_score=1.0
        conf_factor01=0.7
        final_score=0.7
        confidence_pct=80.0
        parts=parts
        res=res
    )

    # json-serializable
    json.dumps(payload, ensure_ascii=False)
    json.dumps(meta, ensure_ascii=False)

    assert "parts" in payload
    assert "small_scalar" in payload["parts"]
    assert "small_list" in payload["parts"]
    assert "big_list" not in payload["parts"]
    assert "big_dict" not in payload["parts"]

    assert "parts_full" in meta
    assert "big_list" in meta["parts_full"]
    assert "big_dict" in meta["parts_full"]

def test_candidateframe_memo_get_compute_once():
    ctx = DummyCtx()
    cand = DummyCand()
    f = CandidateFrame(
        handler=object()
        ctx=ctx
        cand=cand
        kind_str="breakout"
        kind_key="breakout"
        side_int=1
        ctx_symbol="BTCUSDT"
        ctx_ts=123
        ctx_price=100.0
    )

    calls = {"n": 0}
    def compute():
        calls["n"] += 1
        return 123

    assert f.memo_get("k", compute) == 123
    assert f.memo_get("k", compute) == 123
    assert calls["n"] == 1
