import io
import json


from tools.export_of_inputs_ndjson import iter_stream_payloads


class _RedisStub:
    def __init__(self, items):
        self._items = items
        self.calls = []

    def xrange(self, stream, min, max, count):
        # record call for assertions
        self.calls.append((stream, min, max, count))
        # emulate pagination: return first `count` matching items with id >= min
        out = []
        # handle exclusive min: "(<id>"
        excl = False
        min_id = min
        if isinstance(min, str) and min.startswith("("):
            excl = True
            min_id = min[1:]
        for rid, kv in self._items:
            if min_id != "-":
                if excl:
                    if rid <= min_id:
                        continue
                else:
                    if rid < min_id:
                        continue
            out.append((rid, kv))
            if len(out) >= count:
                break
        return out


def test_iter_stream_payloads_reads_in_order_and_validates():
    items = [
        ("1-0", {"payload": json.dumps({"v": 1, "ts_ms": 1})}),
        ("2-0", {"payload": "{bad json"}),
        ("3-0", {"payload": json.dumps({"v": 1, "ts_ms": 3})}),
    ]
    r = _RedisStub(items)
    buf = io.StringIO()
    got = list(
        iter_stream_payloads(
            r=r,
            stream="signals:of:inputs",
            field="payload",
            start_id="-",
            end_id="+",
            batch=10,
            max_records=0,
            validate_json=True,
            stderr=buf,
        )
    )
    # invalid json skipped
    assert [rid for rid, _ in got] == ["1-0", "3-0"]
    assert "invalid JSON" in buf.getvalue()


def test_iter_stream_payloads_paginates_and_exclusive_cursor():
    items = [
        ("1-0", {"payload": json.dumps({"v": 1})}),
        ("2-0", {"payload": json.dumps({"v": 1})}),
        ("3-0", {"payload": json.dumps({"v": 1})}),
    ]
    r = _RedisStub(items)
    got = list(
        iter_stream_payloads(
            r=r,
            stream="signals:of:inputs",
            field="payload",
            start_id="-",
            end_id="+",
            batch=2,
            max_records=0,
            validate_json=True,
        )
    )
    assert len(got) == 3
    # ensure second call uses exclusive min "(2-0" or "(something"
    assert any(call[1].startswith("(") for call in r.calls[1:])

