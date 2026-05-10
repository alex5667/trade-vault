import json

from common.decision_trace import DecisionTrace, emit_trace_event
from core.redis_keys import RedisStreams as RS


class FakeRedis:
    def __init__(self):
        self.xadds = []
    def xadd(self, stream, fields, maxlen=None, approximate=None):
        self.xadds.append((stream, dict(fields)))


def test_emit_trace_event_writes_diag_only():
    r = FakeRedis()
    t = DecisionTrace(trace_id="t1", sid="s1", symbol="BTCUSDT", stage="gate")
    ok = emit_trace_event(r, trace=t, stage="gate", outcome="veto", stream=RS.SIGNAL_DIAG, maxlen=1000)
    assert ok is True
    assert len(r.xadds) == 1
    stream, fields = r.xadds[0]
    assert stream == RS.SIGNAL_DIAG
    assert fields["type"] == "diag"
    assert fields["trace_id"] == "t1"
    assert fields["sid"] == "s1"
    # data must be json
    json.loads(fields["data"])

