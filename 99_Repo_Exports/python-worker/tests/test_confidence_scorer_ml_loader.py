"""Tests for the mtime-cached ML scorer loader in confidence_scorer.

The loader was added so `ML_SCORING_ENABLE=1` can be set in production
BEFORE the scorer_model.lgb file exists (training validation may take
days to pass). Previously, every scoring call did joblib.load() of a
non-existent file → exception spam in logs + wasted CPU.

The cache:
- Returns (None, None) silently when file is absent
- Reloads only when mtime changes
- Logs INFO on successful load, WARNING on load failure
"""
from __future__ import annotations

import os
import time

import joblib
import pytest

from confidence_calculation import confidence_scorer as cs


@pytest.fixture(autouse=True)
def _reset_cache():
    """Reset the module-level cache between tests."""
    cs._ML_MODEL_CACHE.update(path=None, mtime=0.0, model=None, features=None)
    yield
    cs._ML_MODEL_CACHE.update(path=None, mtime=0.0, model=None, features=None)


class TestLoaderMissingFile:
    def test_returns_none_when_path_missing(self, tmp_path):
        path = str(tmp_path / "scorer_model.lgb")  # doesn't exist
        model, feats = cs._load_ml_scorer(path)
        assert model is None
        assert feats is None

    def test_returns_none_when_path_empty(self):
        model, feats = cs._load_ml_scorer("")
        assert model is None
        assert feats is None

    def test_no_log_spam_on_missing_file(self, tmp_path, caplog):
        path = str(tmp_path / "scorer_model.lgb")
        # Call 5 times — must not log warnings
        for _ in range(5):
            cs._load_ml_scorer(path)
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert warnings == []


class TestLoaderHappyPath:
    def _make_model_files(self, tmp_path, n_features: int = 3):
        """Create dummy joblib files that look like a saved model+features."""
        model_path = str(tmp_path / "scorer_model.lgb")
        feats_path = str(tmp_path / "scorer_model.features")
        # Plain dicts pickle cleanly across test boundaries (unlike nested classes).
        joblib.dump({"kind": "stub_model", "version": 1}, model_path)
        joblib.dump([f"f{i}" for i in range(n_features)], feats_path)
        return model_path

    def test_loads_model_when_present(self, tmp_path):
        path = self._make_model_files(tmp_path, n_features=4)
        model, feats = cs._load_ml_scorer(path)
        assert model is not None
        assert feats == ["f0", "f1", "f2", "f3"]

    def test_cache_hit_no_reload(self, tmp_path):
        path = self._make_model_files(tmp_path)
        m1, _ = cs._load_ml_scorer(path)
        m2, _ = cs._load_ml_scorer(path)
        # Same object id → returned from cache, not reloaded
        assert m1 is m2

    def test_reloads_on_mtime_change(self, tmp_path):
        path = self._make_model_files(tmp_path)
        m1, _ = cs._load_ml_scorer(path)
        # Touch the file to bump mtime
        future = time.time() + 10
        os.utime(path, (future, future))
        m2, _ = cs._load_ml_scorer(path)
        # Different mtime → new object loaded
        assert m1 is not m2


class TestLoaderCorruptFile:
    def test_returns_none_and_warns_on_load_error(self, tmp_path, caplog):
        # Write garbage that joblib cannot deserialize
        path = str(tmp_path / "scorer_model.lgb")
        with open(path, "wb") as f:
            f.write(b"not a joblib file")
        # Pretend the .features file is also there (also garbage)
        with open(str(tmp_path / "scorer_model.features"), "wb") as f:
            f.write(b"not a joblib file")

        model, feats = cs._load_ml_scorer(path)
        assert model is None
        assert feats is None
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert any("reload failed" in r.message.lower() for r in warnings)
