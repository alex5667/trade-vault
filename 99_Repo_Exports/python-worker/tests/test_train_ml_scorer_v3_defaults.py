from __future__ import annotations

import importlib


def _reload_module():
    mod = importlib.import_module("scripts.train_ml_scorer_v3")
    return importlib.reload(mod)


def test_default_v3_embargo_falls_back_to_pit(monkeypatch):
    monkeypatch.delenv("ML_SCORER_V3_EMBARGO_MS", raising=False)
    monkeypatch.setenv("PIT_EMBARGO_MS", "3600000")
    mod = _reload_module()
    assert mod._default_v3_embargo_ms() == 3_600_000


def test_default_v3_embargo_prefers_explicit_env(monkeypatch):
    monkeypatch.setenv("ML_SCORER_V3_EMBARGO_MS", "7200000")
    monkeypatch.setenv("PIT_EMBARGO_MS", "3600000")
    mod = _reload_module()
    assert mod._default_v3_embargo_ms() == 7_200_000


def test_require_pit_embargo_coherence_enabled_by_default(monkeypatch):
    monkeypatch.delenv("ML_SCORER_V3_REQUIRE_PIT_EMBARGO_COHERENCE", raising=False)
    mod = _reload_module()
    assert mod._default_require_pit_embargo_coherence() is True


def test_require_pit_embargo_coherence_can_be_disabled(monkeypatch):
    monkeypatch.setenv("ML_SCORER_V3_REQUIRE_PIT_EMBARGO_COHERENCE", "0")
    mod = _reload_module()
    assert mod._default_require_pit_embargo_coherence() is False
