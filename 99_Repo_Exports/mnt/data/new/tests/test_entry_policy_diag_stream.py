import json

from services.outbox.envelope_builder import (
    build_entry_policy_diag_event,
    emit_entry_policy_diag_best_effort,
)


class FakeRedis:
    def __init__(self):
        self.calls = []

    def xadd(self, stream, fields, maxlen=None, approximate=True):
        self.calls.append((stream, dict(fields), maxlen, approximate))
        return "0-1"


def test_entry_policy_diag_emits_json_record():
    r = FakeRedis()
    ev = build_entry_policy_diag_event(
        sid="sid1",
        trace_id="trace1",
        kind="orderflow",
        symbol="BTCUSDT",
        stage="gates",
        name="regime_gate",
        reason_code="VETO_REGIME",
        metrics={"p": 0.123},
        extra={"note": "test"},
    )
    ok = emit_entry_policy_diag_best_effort(r, ev, stream="diag:entry_policy", maxlen=123)
    assert ok is True

    assert len(r.calls) == 1
    stream, fields, maxlen, approximate = r.calls[0]
    assert stream == "diag:entry_policy"
    assert maxlen == 123
    assert approximate is True

    assert fields.get("sid") == "sid1"
    raw = fields.get("data")
    assert isinstance(raw, str)
    parsed = json.loads(raw)

    # Minimal schema sanity
    assert parsed["sid"] == "sid1"
    assert parsed["trace_id"] == "trace1"
    assert parsed["reason_code"] == "VETO_REGIME"

    # Diagnostics stream is not a tradeable envelope.
    assert "targets" not in parsed
    assert "payload" not in parsed
