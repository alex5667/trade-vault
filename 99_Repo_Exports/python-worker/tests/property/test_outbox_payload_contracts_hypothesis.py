
from hypothesis import given, settings
from hypothesis import strategies as st

from common.outbox_contract import validate_outbox_envelope
from services.outbox.envelope_builder import build_outbox_envelope


def json_scalar():
    # allow floats but exclude NaN/Inf at generation time
    finite_float = st.floats(allow_nan=False, allow_infinity=False, width=32)
    return st.one_of(st.none(), st.booleans(), st.integers(), finite_float, st.text(max_size=200))


def json_value(max_depth=4):
    return st.recursive(
        json_scalar(),
        lambda children: st.one_of(
            st.lists(children, max_size=20),
            st.dictionaries(st.text(max_size=50), children, max_size=20),
        ),
        max_leaves=100,
    )


@st.composite
def targets_payload(draw):
    # notify/signal_stream/audit payloads are dict-like
    d = draw(st.dictionaries(st.text(max_size=40), json_value(3), max_size=25))
    # also inject occasionally huge string to test budget trimming downstream
    if draw(st.booleans()):
        d["huge"] = "x" * 5000
    return d


@settings(max_examples=200, deadline=250)
@given(
    sid=st.text(min_size=1, max_size=64),
    kind=st.text(min_size=0, max_size=32),
    symbol=st.text(min_size=0, max_size=32),
    notify=st.one_of(st.none(), targets_payload()),
    stream_payload=st.one_of(st.none(), targets_payload()),
    audit_payload=st.one_of(st.none(), targets_payload()),
    meta=st.one_of(st.none(), st.dictionaries(st.text(max_size=40), json_value(2), max_size=20)),
)
def test_build_outbox_envelope_is_trade_safe_and_has_fingerprint(
    sid, kind, symbol, notify, stream_payload, audit_payload, meta
):
    # minimal streams
    signal_stream = "signals:test" if stream_payload is not None else None
    audit_stream = "audit:test" if audit_payload is not None else None

    env = build_outbox_envelope(
        sid=str(sid),
        ctx=None,
        kind=str(kind),
        symbol=symbol,
        notify_payload=notify,
        signal_stream=signal_stream,
        signal_stream_payload=stream_payload,
        audit_stream=audit_stream,
        audit_payload=audit_payload,
        meta=meta,
        trace=None,
    )

    # 1) must be json-safe + contract-safe
    validate_outbox_envelope(env)

    # 2) must have mutation guard fields if enabled by code (payload_sha1/payload_bytes)
    m = env.get("meta") if isinstance(env, dict) else None
    assert isinstance(m, dict)
    assert "payload_sha1" in m
    assert "payload_bytes" in m
    assert isinstance(m["payload_sha1"], str) and len(m["payload_sha1"]) >= 8
    assert isinstance(m["payload_bytes"], int) and m["payload_bytes"] > 0
