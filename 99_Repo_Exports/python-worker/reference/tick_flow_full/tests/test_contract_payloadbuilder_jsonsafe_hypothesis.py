from __future__ import annotations

import os
from types import SimpleNamespace
from typing import Any, Dict

import pytest
from hypothesis import given, settings, strategies as st

from services.candidate_emit_pipeline_v2 import PayloadBuilder, CandidateFrame
from common.contracts.tradeable_contracts import assert_tradeable_dict, assert_outbox_sidecar_meta


def _weird_values():
    # "плохие" типы, которые часто протекают: bytes, object(), set, tuple, complex
    return st.one_of(
        st.binary(),
        st.just(object()),
        st.sets(st.integers(), max_size=8),
        st.tuples(st.integers(), st.text()),
        st.complex_numbers(),
    ),


def _json_like_recursive():
    scalars = st.one_of(st.none(), st.booleans(), st.integers(), st.floats(allow_nan=True, allow_infinity=True), st.text()),
    return st.recursive(
        scalars | _weird_values(),
        lambda child: st.one_of(
            st.lists(child, max_size=80),
            st.dictionaries(st.text(min_size=0, max_size=32), child, max_size=80),
        ),
        max_leaves=200,
    )


@settings(max_examples=300, deadline=None)
@given(parts=st.dictionaries(st.text(min_size=1, max_size=24), _json_like_recursive(), max_size=40))
def test_payloadbuilder_tradeable_contract(parts: Dict[str, Any]):
    os.environ["STRICT_TRADEABLE_CONTRACTS"] = "1"

    ctx = SimpleNamespace(side="LONG", tf="1m")
    cand = SimpleNamespace(kind="breakout", side="LONG", reasons=["r1", "r2"], signal_id="sid123")
    res = SimpleNamespace(parts=parts, decision_code="OK", decision_u16=1, conf_factor01=0.8)

    f = CandidateFrame(
        handler=SimpleNamespace(),
        ctx=ctx,
        cand=cand,
        kind_str="breakout",
        kind_key="breakout",
        side_int=1,
        ctx_symbol="BTCUSDT",
        ctx_ts=1700000000000,
        ctx_price=50000.0,
    )

    b = PayloadBuilder()
    payload, meta = b.build(
        f,
        raw_score=1.23,
        conf_factor01=0.8,
        final_score=0.98,
        confidence_pct=55.0,
        parts=parts,
        res=res,
    )

    assert_tradeable_dict(payload, where="test.payload")
    assert_outbox_sidecar_meta(meta, where="test.meta")

    # железное правило: тяжёлые части никогда не в payload
    assert "parts_full" not in payload
    assert "trace" not in payload
    assert "events" not in payload
