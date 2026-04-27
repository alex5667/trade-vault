import datetime as dt
import decimal
from common.json_safe import to_jsonable


def _assert_json_types(x):
    assert isinstance(x, (dict, list, str, int, float, bool, type(None)))
    if isinstance(x, dict):
        for k, v in x.items():
            assert isinstance(k, str)
            _assert_json_types(v)
    if isinstance(x, list):
        for v in x:
            _assert_json_types(v)


def test_to_jsonable_converts_common_types():
    obj = {
        "dt": dt.datetime(2026, 1, 1, 12, 0, 0),
        "d": dt.date(2026, 1, 1),
        "dec": decimal.Decimal("1.25"),
        "bytes": b"\x01\x02",
        "set": {1, 2, 3},
        "nested": {"x": object()},
    }
    out = to_jsonable(obj)
    _assert_json_types(out)

    assert isinstance(out["dt"], str)
    assert isinstance(out["d"], str)
    assert isinstance(out["dec"], float)
    assert isinstance(out["bytes"], str)
    assert isinstance(out["set"], list)
    assert isinstance(out["nested"]["x"], str)
