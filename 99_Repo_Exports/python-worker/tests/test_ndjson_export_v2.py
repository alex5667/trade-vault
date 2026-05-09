import json
from pathlib import Path

import tools.export_of_inputs_ndjson_v2 as ex
from core.redis_keys import RedisStreams as RS


class _FakeRedis:
    def __init__(self, pages):
        self.pages = pages
        self.calls = 0

    def xrange(self, stream, min, max, count):
        if self.calls >= len(self.pages):
            return []
        p = self.pages[self.calls]
        self.calls += 1
        return p


def test_iter_stream_payloads_reads_payload_bytes(tmp_path: Path):
    rows = [
        (b"1-0", {b"payload": b'{"v":1,"symbol":"BTCUSDT","ts_ms":1}'}),
        (b"2-0", {b"payload": b'{"v":1,"symbol":"BTCUSDT","ts_ms":2}'}),
    ]
    r = _FakeRedis([rows, []])
    out = list(ex.iter_stream_payloads(r=r, stream=RS.OF_INPUTS, field="payload", start_id="0-0", end_id="+", batch=100))
    assert out[0][0] == "1-0"
    assert '"symbol":"BTCUSDT"' in out[0][1]


def test_export_of_inputs_resume_writes_state(tmp_path: Path, monkeypatch):
    rows = [
        (b"5-0", {b"payload": b'{"v":1,"symbol":"BTCUSDT","ts_ms":5}'}),
        (b"6-0", {b"payload": b'{"v":1,"symbol":"BTCUSDT","ts_ms":6}'}),
    ]
    r = _FakeRedis([rows, []])

    class _RedisMod:
        class Redis:
            @staticmethod
            def from_url(*_a, **_k):
                return r

    monkeypatch.setitem(ex.sys.modules, "redis", _RedisMod())

    out = tmp_path / "x.ndjson"
    state = tmp_path / "x.state"
    st = ex.export_of_inputs(
        redis_url="redis://x",
        stream=RS.OF_INPUTS,
        field="payload",
        out_path=out,
        state_file=state,
        resume=True,
        start_id="0-0",
        end_id="+",
        batch=100,
        max_records=0,
        validate=True,
        quiet=True,
    )
    assert st.written == 2
    assert state.read_text().strip() == "6-0"
    lines = out.read_text().strip().splitlines()
    assert json.loads(lines[0])["ts_ms"] == 5
