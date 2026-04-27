from __future__ import annotations

import json

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given, strategies as st

from common.feature_flags import FeatureFlagsManager


@given(st.dictionaries(keys=st.text(min_size=1, max_size=30), values=st.one_of(st.booleans(), st.integers(), st.text(), st.none())))
def test_feature_flags_json_never_crashes(d: dict):
    # Property: arbitrary JSON dict in FEATURE_FLAGS_JSON should not crash manager.
    raw = json.dumps(d, ensure_ascii=False)
    # emulate env json path by direct construction
    ff = FeatureFlagsManager(redis=None, logger=None)
    # monkeypatch-free: call internal parser through public env by setting os.environ directly
    import os
    old = os.environ.get("FEATURE_FLAGS_JSON")
    os.environ["FEATURE_FLAGS_JSON"] = raw
    try:
        s = ff.get(force_refresh=True)
        # invariants: mask is always in 0..15
        assert 0 <= s.mask() <= 15
        assert isinstance(s.revision, int)
    finally:
        if old is None:
            os.environ.pop("FEATURE_FLAGS_JSON", None)
        else:
            os.environ["FEATURE_FLAGS_JSON"] = old
