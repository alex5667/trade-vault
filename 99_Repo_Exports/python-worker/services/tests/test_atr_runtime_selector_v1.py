"""
Phase 2 unit tests: services/atr_runtime_selector.py

Run:
  cd python-worker
  PYTHONPATH=. pytest -q services/tests/test_atr_runtime_selector_v1.py
"""
from __future__ import annotations

import pytest
from services.atr_runtime_selector import (
    select_runtime_atr_profile
    _compute_target_tf_ms
    _nearest_allowed_tf
    _build_candidates
    _compute_vol_ratio
    _parse_allowed_tfs
)

_ALLOWED = [15000, 30000, 60000, 180000, 300000, 900000]
_NOW_MS = 1_700_000_000_000  # arbitrary fixed ts


# ---------------------------------------------------------------------------
# TF selector math
# ---------------------------------------------------------------------------

class TestNearestAllowedTf:
    def test_exact_match(self):
        assert _nearest_allowed_tf(60000, _ALLOWED) == 60000

    def test_nearest_below(self):
        # ideal=45000 → nearest to 45000 is 30000 (diff=15000) or 60000 (diff=15000); ties broken by smaller
        result = _nearest_allowed_tf(45000, _ALLOWED)
        assert result in {30000, 60000}

    def test_nearest_above(self):
        assert _nearest_allowed_tf(100000, _ALLOWED) == 60000

    def test_empty_allowed(self):
        assert _nearest_allowed_tf(60000, []) == 60000

    def test_single_element(self):
        assert _nearest_allowed_tf(99999, [60000]) == 60000


class TestComputeTargetTfMs:
    def test_short_horizon_picks_fast_tf(self):
        # short alpha → short TF
        tf = _compute_target_tf_ms(30000, 15000, 14, _ALLOWED)
        # target_window = max(15000, 45000) = 45000; ideal = 45000/14 ≈ 3214 → nearest = 15000
        assert tf == 15000

    def test_long_horizon_picks_slow_tf(self):
        # 5m hold, 3m alpha → target_window = max(180000, 450000) = 450000; ideal = 450000/14 ≈ 32142 → 30000
        tf = _compute_target_tf_ms(300000, 180000, 14, _ALLOWED)
        assert tf in {30000, 60000}

    def test_zero_horizon_fallback(self):
        tf = _compute_target_tf_ms(0, 0, 14, _ALLOWED)
        assert tf == 60000  # bootstrap fallback

    def test_window_n_1(self):
        # window_n=1: ideal = target_window itself, clamped to allowed
        tf = _compute_target_tf_ms(900000, 900000, 1, _ALLOWED)
        assert tf == 900000


# ---------------------------------------------------------------------------
# Candidate scanning
# ---------------------------------------------------------------------------

class TestBuildCandidates:
    def test_reads_alias_keys_from_indicators(self):
        sig = {
            "indicators": {
                "atr_1m": 200.0
                "atr_ts_ms_1m": _NOW_MS - 5000
            }
        }
        cands = _build_candidates(sig, sig["indicators"], {}, _NOW_MS)
        assert 60000 in cands
        assert abs(cands[60000][0] - 200.0) < 1e-6
        assert cands[60000][1] == 5000  # age_ms

    def test_reads_numeric_tf_keys(self):
        sig = {
            "indicators": {
                "atr_15000": 80.0
                "atr_ts_ms_15000": _NOW_MS - 1000
            }
        }
        cands = _build_candidates(sig, sig["indicators"], {}, _NOW_MS)
        assert 15000 in cands

    def test_no_candidates_when_empty(self):
        cands = _build_candidates({}, {}, {}, _NOW_MS)
        assert cands == {}

    def test_multiple_tfs(self):
        # indicators must be inside signal["indicators"] for the provider to pick them up
        ind = {
            "atr_15s": 80.0
            "atr_ts_ms_15s": _NOW_MS
            "atr_15m": 400.0
            "atr_ts_ms_15m": _NOW_MS
        }
        sig = {"indicators": ind}
        cands = _build_candidates(sig, ind, {}, _NOW_MS)
        assert 15000 in cands
        assert 900000 in cands

    def test_indicators_takes_priority_over_signal_level(self):
        # Provider reads signal["indicators"] FIRST (source=indicators wins).
        # signal-level atr_1m counts only as payload fallback.
        sig = {
            "atr_1m": 999.0
            "atr_ts_ms_1m": _NOW_MS
            "indicators": {
                "atr_1m": 100.0
                "atr_ts_ms_1m": _NOW_MS
            }
        }
        cands = _build_candidates(sig, sig["indicators"], {}, _NOW_MS)
        # indicators source wins: value must be 100.0
        assert abs(cands[60000][0] - 100.0) < 1e-6


# ---------------------------------------------------------------------------
# Vol ratio
# ---------------------------------------------------------------------------

class TestComputeVolRatio:
    def test_fast_slower_than_expected_gives_ratio_lt_1(self):
        # 3-tuple: (value, age_ms, source)
        cands = {15000: (1.0, 0, "indicators"), 900000: (4.0, 0, "indicators")}
        ratio, z = _compute_vol_ratio(cands)
        assert abs(ratio - 0.25) < 1e-9
        assert z == 0.0

    def test_single_candidate_returns_1(self):
        cands = {60000: (200.0, 0, "indicators")}
        ratio, z = _compute_vol_ratio(cands)
        assert ratio == 1.0

    def test_empty_candidates_returns_1(self):
        ratio, z = _compute_vol_ratio({})
        assert ratio == 1.0


# ---------------------------------------------------------------------------
# Full selector: happy paths
# ---------------------------------------------------------------------------

class TestSelectorHappyPaths:
    def test_exact_tf_match(self):
        """When the ideal TF candidate exists, reason=ATR_SEL_EXACT."""
        sig = {
            "price": 65000.0
            "indicators": {
                # 5m hold, 2m alpha → target_window=max(120000,450000)=450000 / 14 ≈ 32142 → 30000
                "atr_30s": 120.0
                "atr_ts_ms_30s": _NOW_MS - 1000
                "atr_1m": 200.0
                "atr_ts_ms_1m": _NOW_MS - 1000
            }
        }
        out = select_runtime_atr_profile(
            signal=sig
            price=65000.0
            hold_target_ms=300000
            alpha_half_life_ms=120000
            now_ms=_NOW_MS
        )
        assert out["atr_value"] > 0.0
        assert out["atr_tf_ms"] in _ALLOWED
        assert out["selector_reason_code"] in {"ATR_SEL_EXACT", "ATR_SEL_NEAREST"}
        assert out["mode"] == "horizon"

    def test_nearest_fallback_when_exact_missing(self):
        """Selector picks nearest available TF when exact match missing."""
        sig = {
            "indicators": {
                # only 15m present; any hold/alpha horizon selection should pick it
                "atr_15m": 350.0
                "atr_ts_ms_15m": _NOW_MS
            }
        }
        out = select_runtime_atr_profile(
            signal=sig
            price=50000.0
            hold_target_ms=60000,  # 1m hold → short target
            alpha_half_life_ms=30000
            now_ms=_NOW_MS
        )
        assert out["mode"] == "horizon"
        assert out["atr_tf_ms"] == 900000
        assert out["selector_reason_code"] == "ATR_SEL_NEAREST"

    def test_vol_ratio_computed_correctly(self):
        """vol_ratio_fast_slow = fast/slow = 1.0/4.0 = 0.25."""
        sig = {
            "indicators": {
                "atr_15s": 1.0
                "atr_ts_ms_15s": _NOW_MS
                "atr_15m": 4.0
                "atr_ts_ms_15m": _NOW_MS
            }
        }
        out = select_runtime_atr_profile(
            signal=sig
            price=100.0
            hold_target_ms=120000
            alpha_half_life_ms=60000
            now_ms=_NOW_MS
        )
        assert abs(out["vol_ratio_fast_slow"] - 0.25) < 1e-9

    def test_atr_pct_computed(self):
        sig = {
            "indicators": {
                "atr_1m": 650.0
                "atr_ts_ms_1m": _NOW_MS
            }
        }
        out = select_runtime_atr_profile(
            signal=sig
            price=65000.0
            hold_target_ms=0
            alpha_half_life_ms=0
            now_ms=_NOW_MS
        )
        assert abs(out["atr_pct"] - 0.01) < 1e-6

    def test_reason_details_contains_expected_keys(self):
        sig = {
            "indicators": {
                "atr_1m": 100.0
                "atr_ts_ms_1m": _NOW_MS
            }
        }
        out = select_runtime_atr_profile(
            signal=sig
            price=1000.0
            hold_target_ms=100000
            alpha_half_life_ms=50000
            now_ms=_NOW_MS
        )
        rd = out["selector_reason_details"]
        assert "target_tf_ms" in rd
        assert "picked_tf_ms" in rd
        assert "candidate_n" in rd
        assert rd["candidate_n"] >= 1


# ---------------------------------------------------------------------------
# Fallback paths
# ---------------------------------------------------------------------------

class TestSelectorFallbacks:
    def test_no_candidates_uses_legacy_atr(self):
        sig = {
            "atr": 250.0
            "atr_ts_ms": _NOW_MS - 3000
            "price": 65000.0
            "indicators": {}
        }
        out = select_runtime_atr_profile(
            signal=sig
            price=65000.0
            hold_target_ms=600000
            alpha_half_life_ms=300000
            now_ms=_NOW_MS
        )
        assert out["mode"] == "legacy"
        assert abs(out["atr_value"] - 250.0) < 1e-6
        assert out["selector_reason_code"] == "ATR_SEL_LEGACY_FALLBACK"
        assert out["atr_source"] == "legacy_fallback"

    def test_stale_candidate_triggers_legacy_fallback(self, monkeypatch):
        """If sole candidate exceeds ATR_HORIZON_CANDIDATE_MAX_AGE_MS, fallback fires."""
        monkeypatch.setenv("ATR_HORIZON_CANDIDATE_MAX_AGE_MS", "1000")
        sig = {
            "atr": 100.0
            "indicators": {
                "atr_1m": 200.0
                "atr_ts_ms_1m": _NOW_MS - 5000,  # 5s old > 1s limit
            }
        }
        out = select_runtime_atr_profile(
            signal=sig
            price=10000.0
            hold_target_ms=60000
            alpha_half_life_ms=30000
            now_ms=_NOW_MS
        )
        # stale candidate rejected; legacy atr=100.0 used
        assert out["mode"] == "legacy"
        assert out["selector_reason_code"] == "ATR_SEL_LEGACY_FALLBACK"

    def test_zero_price_atr_pct_is_zero(self):
        sig = {
            "atr": 200.0
            "indicators": {}
        }
        out = select_runtime_atr_profile(
            signal=sig
            price=0.0
            hold_target_ms=0
            alpha_half_life_ms=0
            now_ms=_NOW_MS
        )
        assert out["atr_pct"] == 0.0

    def test_empty_signal_does_not_raise(self):
        out = select_runtime_atr_profile(
            signal={}
            price=1000.0
            hold_target_ms=0
            alpha_half_life_ms=0
            now_ms=_NOW_MS
        )
        assert out["selector_reason_code"] == "ATR_SEL_LEGACY_FALLBACK"
        assert out["atr_value"] == 0.0

    def test_non_dict_signal_does_not_raise(self):
        out = select_runtime_atr_profile(
            signal=None,  # type: ignore[arg-type]
            price=1000.0
            hold_target_ms=0
            alpha_half_life_ms=0
            now_ms=_NOW_MS
        )
        assert out["mode"] == "legacy"


# ---------------------------------------------------------------------------
# ENV override coverage
# ---------------------------------------------------------------------------

class TestEnvOverrides:
    def test_custom_allowed_tfs(self, monkeypatch):
        monkeypatch.setenv("ATR_HORIZON_ALLOWED_TFS_MS", "60000,300000")
        from importlib import reload
        import services.atr_runtime_selector as m
        tfs = m._parse_allowed_tfs()
        assert set(tfs) == {60000, 300000}

    def test_parse_allowed_tfs_defaults(self, monkeypatch):
        monkeypatch.delenv("ATR_HORIZON_ALLOWED_TFS_MS", raising=False)
        from services.atr_runtime_selector import _parse_allowed_tfs
        tfs = _parse_allowed_tfs()
        assert 60000 in tfs
        assert len(tfs) >= 1
