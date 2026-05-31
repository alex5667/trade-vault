from __future__ import annotations

from dataclasses import dataclass


def test_filter_dataclass_kwargs_keeps_only_known_fields():
    from handlers.data_processor import _filter_dataclass_kwargs

    @dataclass
    class Ctx:
        a: int
        b: str

    out = _filter_dataclass_kwargs(Ctx, {"a": 1, "b": "x", "c": 999})
    assert out == {"a": 1, "b": "x"}


def test_filter_dataclass_kwargs_passthrough_on_non_dataclass():
    from handlers.data_processor import _filter_dataclass_kwargs

    class NotDC:
        pass

    src = {"a": 1, "b": 2}
    out = _filter_dataclass_kwargs(NotDC, src)
    assert out == src
