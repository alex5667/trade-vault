from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from handlers.data_processor import _filter_dataclass_kwargs


@dataclass
class CtxWithExtra:
    a: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)
    data_quality_flags: Optional[List[str]] = None


@dataclass
class CtxNoExtra:
    a: int = 0


def test_preserves_unknown_keys_into_extra_and_merges_existing_extra():
    kw = {"a": 1, "x": 10, "y": "zz", "extra": {"p": 7}}
    out = _filter_dataclass_kwargs(CtxWithExtra, kw)
    assert out["a"] == 1
    assert out["extra"]["p"] == 7
    assert out["extra"]["x"] == 10
    assert out["extra"]["y"] == "zz"


def test_drops_unknown_keys_if_no_extra_field():
    kw = {"a": 1, "x": 10}
    out = _filter_dataclass_kwargs(CtxNoExtra, kw)
    assert out == {"a": 1}


def test_normalizes_data_quality_flags():
    kw = {"a": 1, "data_quality_flags": ("a", "b")}
    out = _filter_dataclass_kwargs(CtxWithExtra, kw)
    assert out["data_quality_flags"] == ["a", "b"]

    kw2 = {"a": 1, "data_quality_flags": None}
    out2 = _filter_dataclass_kwargs(CtxWithExtra, kw2)
    assert out2["data_quality_flags"] == []


def test_keeps_non_dict_extra_raw():
    kw = {"a": 1, "extra": "oops", "x": 2}
    out = _filter_dataclass_kwargs(CtxWithExtra, kw)
    assert out["extra"]["_extra_raw"] == "oops"
    assert out["extra"]["x"] == 2
