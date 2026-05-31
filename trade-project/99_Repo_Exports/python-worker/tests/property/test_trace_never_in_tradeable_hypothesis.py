from hypothesis import given, settings
from hypothesis import strategies as st

from services.outbox.envelope_builder import build_outbox_envelope


@settings(max_examples=200, deadline=250)
@given(meta=st.dictionaries(st.text(max_size=30), st.text(max_size=200), max_size=20))
def test_tradeable_envelope_never_contains_trace_events(meta):
    # attacker/regression: someone passes trace/events inside meta by mistake
    meta2 = dict(meta)
    meta2["trace"] = {"events": [{"type": "gate", "name": "regime"}]}
    meta2["events"] = [{"type": "gate"}]
    meta2["parts_full"] = {"x": "y" * 5000}

    env = build_outbox_envelope(
        sid="S",
        ctx=None,
        kind="k",
        symbol="BTCUSDT",
        notify_payload={"text": "hi"},
        meta=meta2,
        trace=None,
    )

    # Strict invariant:
    # tradeable envelope must not carry full trace or heavy blobs
    assert "trace" not in env
    assert "events" not in env
    m = env.get("meta", {})
    assert "trace" not in m
    assert "events" not in m
