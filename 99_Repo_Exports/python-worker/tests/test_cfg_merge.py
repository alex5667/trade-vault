from __future__ import annotations

from core.cfg_merge import merged_cfg


def test_merge_override_wins():
    base = {"a": 1, "b": 2}
    ov = {"b": 9, "c": 3}
    out = merged_cfg(base, ov)
    assert out["a"] == 1
    assert out["b"] == 9
    assert out["c"] == 3
