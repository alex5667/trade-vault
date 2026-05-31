from common.json_fast import dumps1


def test_dumps1_compact_no_spaces():
    s = dumps1({"a": 1, "b": "тест"})
    # must be one-line compact
    assert " " not in s
    assert "\n" not in s
    assert s.startswith("{") and s.endswith("}")
    # unicode preserved (ensure_ascii=False)
    assert "тест" in s
