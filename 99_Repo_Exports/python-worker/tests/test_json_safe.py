# python-worker/tests/test_json_safe.py
import json
import datetime as dt
from common.json_safe import to_json_safe


def _assert_jsonable(x):
    json.dumps(x, ensure_ascii=False)


def test_to_json_safe_scalars_and_weird_types():
    obj = {
        "a": 1,
        "b": 1.25,
        "c": float("inf"),
        "d": float("nan"),
        "e": dt.datetime(2026, 1, 2, 3, 4, 5),
        "f": b"\xffhello",
        "g": set([1, 2, 3]),
        "h": ("x", 1),
        "i": object(),
    }
    out = to_json_safe(obj)
    _assert_jsonable(out)
    assert out["c"] is None
    assert out["d"] is None
    assert isinstance(out["e"], str)
    assert isinstance(out["f"], str)
    assert isinstance(out["g"], list)
    assert isinstance(out["h"], list)
    assert isinstance(out["i"], str)