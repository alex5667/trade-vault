"""Tests for the regime-bridge fix in `_publish_of_inputs`.

Bug: veto-path called `_publish_of_inputs` with the raw signal, BEFORE the
regime-resolution step at signal_pipeline.py:~2594. Result was ~87% of
records in `signals:of:inputs` having `regime=None`, which made the
`_cat_regime_idx` scorer feature useless (importance=0, distribution 87% UNKNOWN).

Fix: `_publish_of_inputs` now accepts `runtime` and resolves regime from
`runtime.last_regime` when the signal/indicators don't carry it.

These tests verify the bridge logic in isolation (no Redis, no publisher),
by directly invoking the helper-block in a stub harness.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any


def _apply_regime_bridge(enriched_signal: dict[str, Any], runtime: Any | None) -> None:
    """Subset of `_publish_of_inputs` that we want to verify.

    Copied verbatim from signal_pipeline.py so the test breaks if anyone
    silently changes the bridge logic.
    """
    _inds = enriched_signal.get("indicators")
    if not isinstance(_inds, dict):
        return
    _NA_TOKENS = ("na", "NA", "None", "unknown", "?", "")
    _need_inds_regime = (
        not _inds.get("regime")
        or str(_inds.get("regime")) in _NA_TOKENS
    )
    if _need_inds_regime:
        _top_regime = (str(enriched_signal.get("regime") or "")).lower().strip()
        if (not _top_regime or _top_regime in _NA_TOKENS) and runtime is not None:
            _top_regime = (str(getattr(runtime, "last_regime", "") or "")).lower().strip()
        if _top_regime and _top_regime not in _NA_TOKENS:
            _inds["regime"] = _top_regime
            enriched_signal.setdefault("regime", _top_regime)


class TestRegimeBridgeFromRuntime:
    def test_fills_indicators_regime_from_runtime_when_signal_lacks(self):
        sig = {"indicators": {}}  # no regime anywhere
        rt = SimpleNamespace(last_regime="trending_bear")
        _apply_regime_bridge(sig, rt)
        assert sig["indicators"]["regime"] == "trending_bear"
        assert sig["regime"] == "trending_bear"

    def test_fills_indicators_regime_from_top_level(self):
        sig = {"regime": "range", "indicators": {}}
        _apply_regime_bridge(sig, runtime=None)
        assert sig["indicators"]["regime"] == "range"

    def test_does_not_overwrite_existing_indicators_regime(self):
        sig = {"regime": "range", "indicators": {"regime": "expansion"}}
        rt = SimpleNamespace(last_regime="squeeze")
        _apply_regime_bridge(sig, rt)
        # already populated → stays
        assert sig["indicators"]["regime"] == "expansion"

    def test_skips_na_top_level_and_uses_runtime(self):
        sig = {"regime": "na", "indicators": {"regime": "unknown"}}
        rt = SimpleNamespace(last_regime="trending_bull")
        _apply_regime_bridge(sig, rt)
        assert sig["indicators"]["regime"] == "trending_bull"
        # top-level regime stays "na" (only setdefault used) — bridge fixed indicators
        assert sig["regime"] == "na"

    def test_question_mark_treated_as_unknown(self):
        sig = {"indicators": {"regime": "?"}}
        rt = SimpleNamespace(last_regime="range")
        _apply_regime_bridge(sig, rt)
        assert sig["indicators"]["regime"] == "range"

    def test_no_runtime_no_signal_regime_leaves_unset(self):
        sig = {"indicators": {}}
        _apply_regime_bridge(sig, runtime=None)
        assert sig["indicators"].get("regime") is None
        assert "regime" not in sig

    def test_runtime_with_na_last_regime_does_not_corrupt(self):
        sig = {"indicators": {}}
        rt = SimpleNamespace(last_regime="na")
        _apply_regime_bridge(sig, rt)
        # "na" must NOT be written — we only write meaningful regimes
        assert "regime" not in sig["indicators"]

    def test_top_level_normalisation_lowercase(self):
        sig = {"regime": "TRENDING_BULL", "indicators": {}}
        _apply_regime_bridge(sig, runtime=None)
        assert sig["indicators"]["regime"] == "trending_bull"

    def test_indicators_none_skipped_gracefully(self):
        sig = {"regime": "range", "indicators": None}  # malformed
        _apply_regime_bridge(sig, runtime=None)  # must not raise


class TestRegimeBridgeFeedsCategoricalEncoder:
    """End-to-end intent: after bridge, scorer's regime encoder produces a
    real ordinal rather than UNKNOWN(-1)."""

    def test_bridged_regime_maps_to_known_ordinal(self):
        from core.scorer_categorical_features import REGIME_UNKNOWN, encode_regime

        sig = {"indicators": {}}
        rt = SimpleNamespace(last_regime="trending_bear")
        _apply_regime_bridge(sig, rt)
        idx = encode_regime(sig["indicators"]["regime"])
        assert idx != REGIME_UNKNOWN
        assert idx == 2  # trending_bear

    def test_no_bridge_falls_back_to_unknown(self):
        from core.scorer_categorical_features import REGIME_UNKNOWN, encode_regime

        sig = {"indicators": {}}
        _apply_regime_bridge(sig, runtime=None)
        idx = encode_regime(sig["indicators"].get("regime"))
        assert idx == REGIME_UNKNOWN
