from __future__ import annotations

from types import SimpleNamespace

from services.outbox.envelope_builder import build_outbox_envelope
from common.contracts.tradeable_contracts import assert_tradeable_dict


def test_build_outbox_envelope_trace_is_summary_only():
    # Фейковый trace (достаточно интерфейса, который ваш builder использует через ctx)
    ctx = SimpleNamespace(trace_id="tid1")
    env = build_outbox_envelope(
        sid="sid1",
        ctx=ctx,
        kind="breakout",
        symbol="BTCUSDT",
        notify_payload={"a": 1},
        meta={"x": 1},
        trace=None,  # именно так у вас сейчас "safe by design"
    )

    assert_tradeable_dict(env, where="envelope")

    # В tradeable env разрешены только trace_id + trace_summary + meta.trace_meta_key
    assert "trace_id" in env
    assert "trace_summary" in env
    assert "trace" not in env
    assert "events" not in env

    m = env.get("meta") or {}
    assert isinstance(m, dict)
    assert "trace_meta_key" in m
