from __future__ import annotations

import json

import pytest

from common.feature_flags import FeatureFlagsManager
from common.feature_flags_test_helpers import FakeRedis


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch):
    # ensure isolation
    for k in [
        "FEATURE_FLAGS_JSON",
        "USE_UNIFIED_SCORING",
        "USE_L3_VETO_FOR_BREAKOUT",
        "ABSORPTION_REQUIRE_2OFN_CONFIRMATIONS",
        "REGIME_DETECTOR_V2",
        "FEATURE_FLAGS_REV",
        "FEATURE_FLAGS_REDIS_KEY",
        "FEATURE_FLAGS_REFRESH_MS",
        "FEATURE_FLAGS_FILE",
    ]:
        monkeypatch.delenv(k, raising=False)
    yield


def test_flags_from_env_json(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("FEATURE_FLAGS_JSON", json.dumps({
        "USE_UNIFIED_SCORING": True,
        "USE_L3_VETO_FOR_BREAKOUT": True,
        "ABSORPTION_REQUIRE_2OFN_CONFIRMATIONS": False,
        "REGIME_DETECTOR_V2": True,
        "rev": 12,
    }))
    ff = FeatureFlagsManager(redis=None, logger=None)
    s = ff.get(force_refresh=True)
    assert s.use_unified_scoring is True
    assert s.use_l3_veto_for_breakout is True
    assert s.absorption_require_2ofn_confirmations is False
    assert s.regime_detector_v2 is True
    assert s.revision == 12
    assert s.mask() == (1 << 0) | (1 << 1) | (1 << 3)  # bit2 off


def test_flags_from_per_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("USE_UNIFIED_SCORING", "1")
    monkeypatch.setenv("USE_L3_VETO_FOR_BREAKOUT", "0")
    monkeypatch.setenv("ABSORPTION_REQUIRE_2OFN_CONFIRMATIONS", "1")
    monkeypatch.setenv("REGIME_DETECTOR_V2", "1")
    monkeypatch.setenv("FEATURE_FLAGS_REV", "7")
    ff = FeatureFlagsManager(redis=None, logger=None)
    s = ff.get(force_refresh=True)
    assert s.use_unified_scoring is True
    assert s.use_l3_veto_for_breakout is False
    assert s.absorption_require_2ofn_confirmations is True
    assert s.regime_detector_v2 is True
    assert s.revision == 7


def test_flags_from_redis_over_env(monkeypatch: pytest.MonkeyPatch):
    r = FakeRedis()
    monkeypatch.setenv("FEATURE_FLAGS_REDIS_KEY", "feature_flags:json")
    monkeypatch.setenv("USE_UNIFIED_SCORING", "0")
    r.set("feature_flags:json", json.dumps({"USE_UNIFIED_SCORING": True, "rev": 99}))
    ff = FeatureFlagsManager(redis=r, logger=None)
    s = ff.get(force_refresh=True)
    assert s.use_unified_scoring is True
    assert s.revision == 99


def test_refresh_keeps_prev_on_bad_redis(monkeypatch: pytest.MonkeyPatch):
    r = FakeRedis()
    monkeypatch.setenv("FEATURE_FLAGS_REDIS_KEY", "feature_flags:json")
    r.set("feature_flags:json", json.dumps({"USE_UNIFIED_SCORING": True, "rev": 1}))
    ff = FeatureFlagsManager(redis=r, logger=None)
    s1 = ff.get(force_refresh=True)
    assert s1.use_unified_scoring is True
    r.set("feature_flags:json", "not-json")
    s2 = ff.get(force_refresh=True)  # should keep previous snapshot (fail-open)
    assert s2.use_unified_scoring is True
    assert s2.revision == 1
