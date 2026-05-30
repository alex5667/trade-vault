"""Unit tests for signals/bounded_sl.py and its wiring into compute_levels.

Plan 2.4: bounded SL = max(k*ATR, p75(MAE_30d_bps)) to mitigate
microstructure-noise SL hits.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from signals.bounded_sl import (
    _reset_cache_for_tests,
    apply_bounded_sl_floor,
    resolve_mae_floor_bps,
)
from signals.risk_levels import compute_levels


@pytest.fixture(autouse=True)
def _wipe_cache():
    _reset_cache_for_tests()
    yield
    _reset_cache_for_tests()


# ---------------------------------------------------------------------------
# resolve_mae_floor_bps — pure logic, percentile/cap/k math
# ---------------------------------------------------------------------------

class TestResolveMaeFloor:
    def test_no_data_returns_zero(self):
        # No cfg override, redis returns nothing (mocked via patch).
        with patch("signals.bounded_sl._read_priors_from_redis", return_value={}):
            floor, meta = resolve_mae_floor_bps("BTCUSDT", {})
        assert floor == 0.0
        assert meta["source"] == 0.0

    def test_cfg_injection_used_first(self):
        # cfg injection takes precedence over Redis.
        cfg = {"mae_p75_bps_30d": 50.0, "mae_sample_count_30d": 200.0}
        with patch("signals.bounded_sl._read_priors_from_redis") as mock_r:
            floor, meta = resolve_mae_floor_bps("BTCUSDT", cfg)
            mock_r.assert_not_called()
        assert floor == 50.0
        assert meta["source"] == 1.0
        assert meta["sample_count"] == 200.0

    def test_min_samples_gate(self, monkeypatch):
        # Below BOUNDED_SL_MIN_SAMPLES → returns 0 even with non-zero p75.
        monkeypatch.setenv("BOUNDED_SL_MIN_SAMPLES", "100")
        priors = {"p75_mae_bps_30d": 80.0, "sample_count": 50.0}
        with patch("signals.bounded_sl._read_priors_from_redis", return_value=priors):
            floor, meta = resolve_mae_floor_bps("BTCUSDT", {})
        assert floor == 0.0
        assert meta["sample_count"] == 50.0

    def test_k_multiplier(self, monkeypatch):
        monkeypatch.setenv("BOUNDED_SL_MIN_SAMPLES", "0")
        monkeypatch.setenv("BOUNDED_SL_MAE_K", "1.5")
        monkeypatch.setenv("BOUNDED_SL_MAE_P75_CAP_BPS", "1000")
        priors = {"p75_mae_bps_30d": 40.0, "sample_count": 200.0}
        with patch("signals.bounded_sl._read_priors_from_redis", return_value=priors):
            floor, _ = resolve_mae_floor_bps("BTCUSDT", {})
        assert floor == pytest.approx(60.0)

    def test_cap_enforced(self, monkeypatch):
        monkeypatch.setenv("BOUNDED_SL_MIN_SAMPLES", "0")
        monkeypatch.setenv("BOUNDED_SL_MAE_K", "1.0")
        monkeypatch.setenv("BOUNDED_SL_MAE_P75_CAP_BPS", "100")
        priors = {"p75_mae_bps_30d": 300.0, "sample_count": 200.0}
        with patch("signals.bounded_sl._read_priors_from_redis", return_value=priors):
            floor, meta = resolve_mae_floor_bps("BTCUSDT", {})
        assert floor == 100.0  # capped
        assert meta["mae_p75_bps"] == 300.0  # raw preserved


# ---------------------------------------------------------------------------
# apply_bounded_sl_floor — env gating + apply/shadow behavior
# ---------------------------------------------------------------------------

class TestApplyBoundedSL:
    def test_disabled_passes_through(self, monkeypatch):
        monkeypatch.setenv("BOUNDED_SL_ENABLED", "0")
        new_dist, telem = apply_bounded_sl_floor(
            "BTCUSDT", 100.0, 0.10, {"mae_p75_bps_30d": 200.0, "mae_sample_count_30d": 200.0},
        )
        assert new_dist == 0.10
        assert telem["enabled"] == 0
        assert telem["applied"] == 0

    def test_shadow_mode_observes_without_applying(self, monkeypatch):
        monkeypatch.setenv("BOUNDED_SL_ENABLED", "1")
        monkeypatch.setenv("BOUNDED_SL_SHADOW", "1")
        monkeypatch.setenv("BOUNDED_SL_MAE_K", "1.0")
        monkeypatch.setenv("BOUNDED_SL_MAE_P75_CAP_BPS", "1000")
        # entry=100, stop_dist=0.10 → 10 bps. p75=50 bps → floor=0.5 in price.
        new_dist, telem = apply_bounded_sl_floor(
            "BTCUSDT", 100.0, 0.10,
            {"mae_p75_bps_30d": 50.0, "mae_sample_count_30d": 200.0},
        )
        assert new_dist == 0.10  # NOT applied
        assert telem["enabled"] == 1
        assert telem["shadow"] == 1
        assert telem["applied"] == 0
        assert telem["would_apply"] == 1
        assert telem["mae_floor_bps"] == pytest.approx(50.0)
        assert telem["delta_dist"] == pytest.approx(0.40, rel=1e-3)

    def test_enforce_mode_applies_floor(self, monkeypatch):
        monkeypatch.setenv("BOUNDED_SL_ENABLED", "1")
        monkeypatch.setenv("BOUNDED_SL_SHADOW", "0")
        monkeypatch.setenv("BOUNDED_SL_MAE_K", "1.0")
        monkeypatch.setenv("BOUNDED_SL_MAE_P75_CAP_BPS", "1000")
        new_dist, telem = apply_bounded_sl_floor(
            "BTCUSDT", 100.0, 0.10,
            {"mae_p75_bps_30d": 50.0, "mae_sample_count_30d": 200.0},
        )
        assert new_dist == pytest.approx(0.50)
        assert telem["applied"] == 1
        assert telem["final_dist"] == pytest.approx(0.50)
        assert telem["base_dist"] == 0.10

    def test_no_op_when_atr_already_wider(self, monkeypatch):
        monkeypatch.setenv("BOUNDED_SL_ENABLED", "1")
        monkeypatch.setenv("BOUNDED_SL_SHADOW", "0")
        monkeypatch.setenv("BOUNDED_SL_MAE_K", "1.0")
        # entry=100, stop_dist=2.0 (200 bps). p75=50 bps → floor=0.5.
        new_dist, telem = apply_bounded_sl_floor(
            "BTCUSDT", 100.0, 2.0,
            {"mae_p75_bps_30d": 50.0, "mae_sample_count_30d": 200.0},
        )
        assert new_dist == 2.0  # ATR-based stop already exceeds the floor
        assert telem["applied"] == 0
        assert telem["would_apply"] == 0

    def test_fail_open_on_invalid_input(self, monkeypatch):
        monkeypatch.setenv("BOUNDED_SL_ENABLED", "1")
        monkeypatch.setenv("BOUNDED_SL_SHADOW", "0")
        # entry<=0 → return original
        new_dist, telem = apply_bounded_sl_floor(
            "BTCUSDT", 0.0, 0.10, {"mae_p75_bps_30d": 50.0, "mae_sample_count_30d": 200.0},
        )
        assert new_dist == 0.10
        assert telem["applied"] == 0


# ---------------------------------------------------------------------------
# 2026-05-27 WR fix — ATR-multiple cap (BOUNDED_SL_MAX_ATR_MULT)
# ---------------------------------------------------------------------------

class TestBoundedSLAtrCap:
    """In low-vol windows ATR shrinks but p75(MAE_30d) is computed over a month.
    Without a cap the MAE floor dominates ATR-scaled SL → effective 9 ATR SL/TP
    → 0% WR. The cap caps the ratio mae_floor_bps / atr_bps."""

    def _enforce_env(self, monkeypatch):
        monkeypatch.setenv("BOUNDED_SL_ENABLED", "1")
        monkeypatch.setenv("BOUNDED_SL_SHADOW", "0")
        monkeypatch.setenv("BOUNDED_SL_MAE_K", "1.0")
        monkeypatch.setenv("BOUNDED_SL_MAE_P75_CAP_BPS", "1000")

    def test_cap_triggered_in_low_vol(self, monkeypatch):
        """ATR=5 bps, MAE floor=50 bps → ratio 10x > default cap 4.0 → triggered."""
        self._enforce_env(monkeypatch)
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_MULT", "4.0")
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_SHADOW", "0")
        # entry=100, atr=0.05 (5 bps). MAE=50 bps → floor_dist=0.50 (5x ATR).
        new_dist, telem = apply_bounded_sl_floor(
            "BTCUSDT", 100.0, 0.01, {"mae_p75_bps_30d": 50.0, "mae_sample_count_30d": 200.0},
            atr=0.05,
        )
        assert telem["atr_cap_triggered"] == 1
        assert telem["atr_cap_skipped"] == 1
        # Floor was skipped — original stop_dist preserved
        assert new_dist == 0.01
        assert telem["applied"] == 0
        assert telem["mae_floor_to_atr_mult"] == pytest.approx(10.0)

    def test_cap_not_triggered_in_normal_vol(self, monkeypatch):
        """ATR=20 bps, MAE floor=50 bps → ratio 2.5x < cap 4.0 → not triggered."""
        self._enforce_env(monkeypatch)
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_MULT", "4.0")
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_SHADOW", "0")
        # entry=100, atr=0.20 (20 bps). MAE=50 bps → ratio 2.5.
        new_dist, telem = apply_bounded_sl_floor(
            "BTCUSDT", 100.0, 0.01, {"mae_p75_bps_30d": 50.0, "mae_sample_count_30d": 200.0},
            atr=0.20,
        )
        assert telem["atr_cap_triggered"] == 0
        assert telem["atr_cap_skipped"] == 0
        # Floor applied as usual
        assert new_dist == pytest.approx(0.50)
        assert telem["applied"] == 1

    def test_cap_shadow_records_but_does_not_skip(self, monkeypatch):
        """SHADOW=1 → cap recorded in telemetry but floor still applied."""
        self._enforce_env(monkeypatch)
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_MULT", "4.0")
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_SHADOW", "1")
        new_dist, telem = apply_bounded_sl_floor(
            "BTCUSDT", 100.0, 0.01, {"mae_p75_bps_30d": 50.0, "mae_sample_count_30d": 200.0},
            atr=0.05,
        )
        assert telem["atr_cap_triggered"] == 1
        assert telem["atr_cap_shadow"] == 1
        assert telem["atr_cap_skipped"] == 0
        # Floor applied (shadow doesn't enforce skip)
        assert new_dist == pytest.approx(0.50)
        assert telem["applied"] == 1

    def test_no_cap_when_atr_not_provided(self, monkeypatch):
        """Backward compatibility: atr=None → no cap logic, same as before."""
        self._enforce_env(monkeypatch)
        new_dist, telem = apply_bounded_sl_floor(
            "BTCUSDT", 100.0, 0.01, {"mae_p75_bps_30d": 50.0, "mae_sample_count_30d": 200.0},
        )
        # No atr_cap fields populated (no skip)
        assert telem["atr_cap_triggered"] == 0
        assert "atr_bps" not in telem
        assert new_dist == pytest.approx(0.50)

    def test_no_cap_when_atr_zero(self, monkeypatch):
        """atr=0 → defensive: no cap (avoid div-by-zero)."""
        self._enforce_env(monkeypatch)
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_SHADOW", "0")
        new_dist, telem = apply_bounded_sl_floor(
            "BTCUSDT", 100.0, 0.01, {"mae_p75_bps_30d": 50.0, "mae_sample_count_30d": 200.0},
            atr=0.0,
        )
        assert telem["atr_cap_triggered"] == 0
        assert new_dist == pytest.approx(0.50)

    def test_cap_disabled_via_zero_mult(self, monkeypatch):
        """BOUNDED_SL_MAX_ATR_MULT=0 → cap disabled."""
        self._enforce_env(monkeypatch)
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_MULT", "0")
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_SHADOW", "0")
        new_dist, telem = apply_bounded_sl_floor(
            "BTCUSDT", 100.0, 0.01, {"mae_p75_bps_30d": 50.0, "mae_sample_count_30d": 200.0},
            atr=0.05,  # would normally trigger (10x ratio)
        )
        # No cap with 0 mult → floor still applied
        assert telem["atr_cap_triggered"] == 0
        assert new_dist == pytest.approx(0.50)

    def test_cap_exact_boundary(self, monkeypatch):
        """ratio == max_mult → not triggered (strict >)."""
        self._enforce_env(monkeypatch)
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_MULT", "5.0")
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_SHADOW", "0")
        # 50 bps MAE / 10 bps ATR = exactly 5.0
        new_dist, telem = apply_bounded_sl_floor(
            "BTCUSDT", 100.0, 0.01, {"mae_p75_bps_30d": 50.0, "mae_sample_count_30d": 200.0},
            atr=0.10,
        )
        assert telem["atr_cap_triggered"] == 0
        assert telem["mae_floor_to_atr_mult"] == pytest.approx(5.0)
        assert new_dist == pytest.approx(0.50)

    # ── 2026-05-29 ATR-cap CLAMP mode (replaces skip on low-ATR conditions) ──
    def test_cap_clamp_mode_clamps_floor_to_max_mult(self, monkeypatch):
        """clamp mode + shadow=0 → floor clamped at max_mult × atr_bps."""
        self._enforce_env(monkeypatch)
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_MULT", "4.0")
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_SHADOW", "0")
        monkeypatch.setenv("BOUNDED_SL_ATR_CAP_MODE", "clamp")
        # entry=100, atr=0.05 (5 bps), raw floor=50 bps → ratio 10x → cap triggered.
        # Clamp to 4.0 × 5 bps = 20 bps → floor_dist = 100 × 20/10000 = 0.20.
        new_dist, telem = apply_bounded_sl_floor(
            "BTCUSDT", 100.0, 0.01, {"mae_p75_bps_30d": 50.0, "mae_sample_count_30d": 200.0},
            atr=0.05,
        )
        assert telem["atr_cap_triggered"] == 1
        assert telem["atr_cap_mode"] == "clamp"
        assert telem["atr_cap_clamped"] == 1
        assert telem["atr_cap_clamped_floor_bps"] == pytest.approx(20.0)
        assert telem.get("atr_cap_skipped", 0) == 0
        # Clamped floor of 20 bps applies (0.20 > original stop_dist 0.01)
        assert new_dist == pytest.approx(0.20)
        assert telem["applied"] == 1

    def test_cap_clamp_mode_shadow_records_but_does_not_apply(self, monkeypatch):
        """clamp mode + shadow=1 → telemetry records clamp, behaviour = legacy floor."""
        self._enforce_env(monkeypatch)
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_MULT", "4.0")
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_SHADOW", "1")
        monkeypatch.setenv("BOUNDED_SL_ATR_CAP_MODE", "clamp")
        new_dist, telem = apply_bounded_sl_floor(
            "BTCUSDT", 100.0, 0.01, {"mae_p75_bps_30d": 50.0, "mae_sample_count_30d": 200.0},
            atr=0.05,
        )
        assert telem["atr_cap_triggered"] == 1
        assert telem["atr_cap_clamped"] == 1
        assert telem["atr_cap_clamped_floor_bps"] == pytest.approx(20.0)
        # Shadow=1: raw floor of 50 bps still applied (legacy behaviour preserved)
        assert new_dist == pytest.approx(0.50)
        assert telem["applied"] == 1

    def test_cap_skip_mode_default_legacy_behaviour(self, monkeypatch):
        """No BOUNDED_SL_ATR_CAP_MODE set → default 'skip' = legacy."""
        self._enforce_env(monkeypatch)
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_MULT", "4.0")
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_SHADOW", "0")
        monkeypatch.delenv("BOUNDED_SL_ATR_CAP_MODE", raising=False)
        new_dist, telem = apply_bounded_sl_floor(
            "BTCUSDT", 100.0, 0.01, {"mae_p75_bps_30d": 50.0, "mae_sample_count_30d": 200.0},
            atr=0.05,
        )
        assert telem["atr_cap_triggered"] == 1
        assert telem["atr_cap_mode"] == "skip"
        assert telem["atr_cap_skipped"] == 1
        # Floor skipped — original tiny stop_dist preserved.
        assert new_dist == 0.01

    def test_cap_clamp_not_triggered_passes_through(self, monkeypatch):
        """clamp mode + ratio <= max_mult → no clamp, raw floor applies."""
        self._enforce_env(monkeypatch)
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_MULT", "4.0")
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_SHADOW", "0")
        monkeypatch.setenv("BOUNDED_SL_ATR_CAP_MODE", "clamp")
        # ATR=20 bps, floor=50 bps → ratio 2.5 < 4.0 → not triggered.
        new_dist, telem = apply_bounded_sl_floor(
            "BTCUSDT", 100.0, 0.01, {"mae_p75_bps_30d": 50.0, "mae_sample_count_30d": 200.0},
            atr=0.20,
        )
        assert telem["atr_cap_triggered"] == 0
        assert telem.get("atr_cap_clamped", 0) == 0
        # Raw floor of 50 bps applies
        assert new_dist == pytest.approx(0.50)

    def test_cap_clamp_reproduces_ethusdt_incident(self, monkeypatch):
        """Repro of the 2026-05-29 ETHUSDT report (avg SL=7.51 ATR).

        ETH at ~2008, ATR=0.958 (4.77 bps), MAE floor=83.8 bps.
        Legacy 'skip' → SL collapses to 1.2 ATR = 5.7 bps (noise-hit).
        New 'clamp' → SL = 4.0 × ATR = 19.1 bps (sane minimum).
        """
        self._enforce_env(monkeypatch)
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_MULT", "4.0")
        monkeypatch.setenv("BOUNDED_SL_MAX_ATR_SHADOW", "0")
        monkeypatch.setenv("BOUNDED_SL_ATR_CAP_MODE", "clamp")
        entry = 2008.0
        atr = 0.958  # ~4.77 bps
        base_stop = atr * 1.2  # 1.149 ≈ 5.72 bps
        new_dist, telem = apply_bounded_sl_floor(
            "ETHUSDT", entry, base_stop,
            {"mae_p75_bps_30d": 83.8, "mae_sample_count_30d": 3772.0},
            atr=atr,
        )
        # ATR in bps = 0.958/2008 × 10000 = 4.77
        atr_bps = (atr / entry) * 10000.0
        assert telem["atr_cap_triggered"] == 1
        assert telem["atr_cap_clamped"] == 1
        # Clamped floor = 4.0 × 4.77 ≈ 19.1 bps
        assert telem["atr_cap_clamped_floor_bps"] == pytest.approx(4.0 * atr_bps)
        # Final dist ≈ 19.1 bps × entry / 10000 ≈ 3.83 price units = 4.0 ATR exactly
        assert new_dist / atr == pytest.approx(4.0, rel=1e-3)


# ---------------------------------------------------------------------------
# compute_levels integration — SL widened when feature ON+ENFORCE
# ---------------------------------------------------------------------------

class TestComputeLevelsBounded:
    def _cfg_atr(self, mae_p75: float, sample_count: float = 200.0) -> dict[str, Any]:
        return {
            "STOP_MODE": "ATR",
            "STOP_ATR_MULT": 0.6,
            "TP_MODE": "RR",
            "TP_RR": "1,2,3",
            # Disable adaptive hard-floor so we test the MAE floor in isolation.
            "SL_FLOOR_DEFAULT_BPS": "0",
            "spread_bps": 0.0,
            "slippage_ema_bps": 0.0,
            "mae_p75_bps_30d": mae_p75,
            "mae_sample_count_30d": sample_count,
        }

    def test_disabled_default_no_widening(self, monkeypatch):
        monkeypatch.delenv("BOUNDED_SL_ENABLED", raising=False)
        monkeypatch.setenv("SL_FLOOR_DEFAULT_BPS", "0")
        monkeypatch.setenv("SL_FLOOR_SPREAD_MULT", "0")
        monkeypatch.setenv("SL_FLOOR_SLIPPAGE_MULT", "0")
        monkeypatch.setenv("SL_FLOOR_ATR_MULT", "0")
        cfg = self._cfg_atr(mae_p75=500.0)  # huge would-be floor
        levels = compute_levels(entry=100.0, atr=1.0, side="LONG", cfg=cfg, symbol="BTCUSDT")
        # ATR stop: 0.6 * 1.0 = 0.6 → sl=99.4; floor would push to 5.0 but feature is OFF.
        assert levels["stop_dist"] == pytest.approx(0.6)
        assert levels["sl"] == pytest.approx(99.4)

    def test_enforce_widens_sl(self, monkeypatch):
        monkeypatch.setenv("BOUNDED_SL_ENABLED", "1")
        monkeypatch.setenv("BOUNDED_SL_SHADOW", "0")
        monkeypatch.setenv("BOUNDED_SL_MAE_K", "1.0")
        monkeypatch.setenv("BOUNDED_SL_MAE_P75_CAP_BPS", "1000")
        monkeypatch.setenv("SL_FLOOR_DEFAULT_BPS", "0")
        monkeypatch.setenv("SL_FLOOR_SPREAD_MULT", "0")
        monkeypatch.setenv("SL_FLOOR_SLIPPAGE_MULT", "0")
        monkeypatch.setenv("SL_FLOOR_ATR_MULT", "0")
        # mae_p75=80 bps → 100 * 80/10000 = 0.8 in price. ATR-based = 0.6.
        cfg = self._cfg_atr(mae_p75=80.0)
        levels = compute_levels(entry=100.0, atr=1.0, side="LONG", cfg=cfg, symbol="BTCUSDT")
        assert levels["stop_dist"] == pytest.approx(0.8)
        assert levels["sl"] == pytest.approx(99.2)

    def test_shadow_no_widening(self, monkeypatch):
        monkeypatch.setenv("BOUNDED_SL_ENABLED", "1")
        monkeypatch.setenv("BOUNDED_SL_SHADOW", "1")  # shadow → observe only
        monkeypatch.setenv("BOUNDED_SL_MAE_K", "1.0")
        monkeypatch.setenv("SL_FLOOR_DEFAULT_BPS", "0")
        monkeypatch.setenv("SL_FLOOR_SPREAD_MULT", "0")
        monkeypatch.setenv("SL_FLOOR_SLIPPAGE_MULT", "0")
        monkeypatch.setenv("SL_FLOOR_ATR_MULT", "0")
        cfg = self._cfg_atr(mae_p75=80.0)
        levels = compute_levels(entry=100.0, atr=1.0, side="LONG", cfg=cfg, symbol="BTCUSDT")
        # Shadow leaves stop_dist at the original 0.6.
        assert levels["stop_dist"] == pytest.approx(0.6)

    def test_enforce_no_op_when_atr_wider(self, monkeypatch):
        monkeypatch.setenv("BOUNDED_SL_ENABLED", "1")
        monkeypatch.setenv("BOUNDED_SL_SHADOW", "0")
        monkeypatch.setenv("BOUNDED_SL_MAE_K", "1.0")
        monkeypatch.setenv("SL_FLOOR_DEFAULT_BPS", "0")
        monkeypatch.setenv("SL_FLOOR_SPREAD_MULT", "0")
        monkeypatch.setenv("SL_FLOOR_SLIPPAGE_MULT", "0")
        monkeypatch.setenv("SL_FLOOR_ATR_MULT", "0")
        # mae_p75=10 bps → floor=0.1; ATR stop=0.6 wins.
        cfg = self._cfg_atr(mae_p75=10.0)
        levels = compute_levels(entry=100.0, atr=1.0, side="LONG", cfg=cfg, symbol="BTCUSDT")
        assert levels["stop_dist"] == pytest.approx(0.6)


# ---------------------------------------------------------------------------
# pit_priors_rolling_v1 — p75/p50/p90 MAE bps emission
# ---------------------------------------------------------------------------

class TestPitPriorsMaeBps:
    def test_p75_mae_bps_emitted(self):
        from orderflow_services.pit_priors_rolling_v1 import compute_rolling_priors

        now_ms = 30 * 86_400_000 + 5_000_000_000  # well after embargo
        # Build 30 trades with monotonic mae_bps so percentiles are deterministic.
        ts_close = now_ms - 7_200_000  # 2h ago (past embargo of 1h)
        trades = []
        for i in range(1, 31):  # i=1..30, mae_bps=10..300
            trades.append({
                "symbol": "BTCUSDT",
                "scenario": "default",
                "session": "us_main",
                "ts_close": str(ts_close - i * 1_000),
                "result": "LOSS" if i % 2 == 0 else "WIN",
                "r_multiple": str(-1.0 if i % 2 == 0 else 1.5),
                "mae_r": "0.5",
                "mfe_r": "1.2",
                "mae_bps": str(i * 10.0),
            })
        _, p30 = compute_rolling_priors(trades, now_ms)
        key = ("BTCUSDT", "default", "all")
        assert key in p30
        agg = p30[key]
        assert "p75_mae_bps_30d" in agg
        assert "p50_mae_bps_30d" in agg
        assert "p90_mae_bps_30d" in agg
        # mae_bps values are 10,20,...,300 — p50≈150-160, p75≈230, p90≈270.
        assert agg["p50_mae_bps_30d"] > 0.0
        assert agg["p75_mae_bps_30d"] >= agg["p50_mae_bps_30d"]
        assert agg["p90_mae_bps_30d"] >= agg["p75_mae_bps_30d"]
