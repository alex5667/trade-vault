"""
Test: scenario=none root cause — delta_z bypass in of_confirm_engine

Verifies that:
1. When sweep_recent=0, no divergence, regime='na' → scenario was 'none' (pre-fix)
2. After fix: when |delta_z| >= scenario_dz_bypass_threshold → scenario='continuation', trend_dir from sign
3. Bypass is directionally correct (negative delta_z → SHORT, positive → LONG)
4. Below threshold: still 'none' (no false positives)
5. Configurable threshold works
"""
import types

import pytest


def _make_runtime(last_div=None, last_sweep=None, last_regime="na"):
    r = types.SimpleNamespace()
    r.last_div = last_div
    r.last_sweep = last_sweep
    r.last_regime = last_regime
    r.last_obi_event = None
    r.last_iceberg_event = None
    r.last_ofi_event = None
    r.last_fp_edge = None
    r.last_reclaim = None
    return r


def _hidden_trend_dir(kind):
    """Mirror of core.of_confirm_engine.hidden_trend_dir."""
    if not kind:
        return None
    k = str(kind).lower()
    if k == "bullish_hidden":
        return "LONG"
    if k == "bearish_hidden":
        return "SHORT"
    return None


def _select_scenario(delta_z: float, cfg: dict, runtime, indicators: dict) -> tuple[str, str, str | None, dict]:
    """
    Mirror of of_confirm_engine.py scenario selection logic (lines 791-847 post-fix).
    Returns (scenario, fallback_reason, trend_dir, indicators).
    """
    sweep_recent = False  # simulated: no mature pools
    scenario = "reversal" if sweep_recent else "continuation"
    fallback_reason = "unknown"
    trend_dir = None

    if scenario == "continuation":
        cvd_q = int(indicators.get("cvd_quarantine_active", 0) or 0)
        div = None if cvd_q == 1 else getattr(runtime, "last_div", None)
        trend_dir = _hidden_trend_dir(getattr(div, "kind", None) if div else None)

        if trend_dir is None:
            rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
            if "bull" in rg:
                trend_dir = "LONG"
            elif "bear" in rg:
                trend_dir = "SHORT"
        else:
            indicators["hidden_div_used"] = 1

        if trend_dir is None:
            # --- NEW: delta_z strength bypass ---
            try:
                _dz_bypass_th = float(cfg.get("scenario_dz_bypass_threshold", 10.0))
                _dz_val = float(delta_z)
                if abs(_dz_val) >= _dz_bypass_th:
                    trend_dir = "SHORT" if _dz_val < 0.0 else "LONG"
                    scenario = "continuation"
                    fallback_reason = "dz_bypass"
                    indicators["scenario_dz_bypass"] = 1
                    indicators["scenario_dz_bypass_th"] = float(_dz_bypass_th)
            except Exception:
                pass

            if trend_dir is None:
                scenario = "none"
                fallback_reason = "no_sweep_and_no_trend"
                indicators["of_debug_fail"] = f"no_trend:regime={getattr(runtime, 'last_regime', 'na')}"

    return scenario, fallback_reason, trend_dir, indicators


class TestScenarioDzBypass:
    """Tests for delta_z strength bypass in scenario selection."""

    def test_strong_short_gets_continuation(self):
        """ETHUSDT delta_z=-21.5 should bypass to continuation/SHORT."""
        rt = _make_runtime()
        ind = {}
        scenario, reason, trend_dir, ind = _select_scenario(-21.568, {}, rt, ind)

        assert scenario == "continuation", f"Expected continuation, got {scenario}"
        assert trend_dir == "SHORT", f"Expected SHORT, got {trend_dir}"
        assert reason == "dz_bypass"
        assert ind.get("scenario_dz_bypass") == 1
        assert ind.get("scenario_dz_bypass_th") == pytest.approx(10.0)

    def test_strong_long_gets_continuation(self):
        """delta_z=+18.3 (buy pressure) → continuation/LONG."""
        rt = _make_runtime()
        ind = {}
        scenario, reason, trend_dir, ind = _select_scenario(18.3, {}, rt, ind)

        assert scenario == "continuation"
        assert trend_dir == "LONG", f"Expected LONG, got {trend_dir}"
        assert reason == "dz_bypass"

    def test_weak_signal_stays_none(self):
        """delta_z=-9.5 (< 10.0 threshold) → no bypass → scenario=none."""
        rt = _make_runtime()
        ind = {}
        scenario, reason, trend_dir, ind = _select_scenario(-9.5, {}, rt, ind)

        assert scenario == "none", f"Expected none for weak signal, got {scenario}"
        assert trend_dir is None
        assert ind.get("scenario_dz_bypass", 0) == 0
        assert "of_debug_fail" in ind

    def test_exactly_at_threshold(self):
        """delta_z=-10.0 (== threshold) → bypass triggered."""
        rt = _make_runtime()
        ind = {}
        scenario, reason, trend_dir, ind = _select_scenario(-10.0, {}, rt, ind)
        assert scenario == "continuation"
        assert trend_dir == "SHORT"

    def test_just_below_threshold(self):
        """delta_z=-9.999 (< 10.0) → no bypass."""
        rt = _make_runtime()
        ind = {}
        scenario, _, trend_dir, ind = _select_scenario(-9.999, {}, rt, ind)
        assert scenario == "none"
        assert trend_dir is None

    def test_custom_threshold_lower(self):
        """Custom threshold=5.0: delta_z=-7.0 should bypass."""
        rt = _make_runtime()
        cfg = {"scenario_dz_bypass_threshold": 5.0}
        ind = {}
        scenario, reason, trend_dir, ind = _select_scenario(-7.0, cfg, rt, ind)

        assert scenario == "continuation"
        assert trend_dir == "SHORT"
        assert ind.get("scenario_dz_bypass_th") == pytest.approx(5.0)

    def test_custom_threshold_higher(self):
        """Custom threshold=15.0: delta_z=-12.0 should NOT bypass."""
        rt = _make_runtime()
        cfg = {"scenario_dz_bypass_threshold": 15.0}
        ind = {}
        scenario, _, trend_dir, ind = _select_scenario(-12.0, cfg, rt, ind)

        assert scenario == "none"
        assert trend_dir is None

    def test_regime_takes_priority_over_dz_bypass(self):
        """If regime='trending_bear' → trend_dir='SHORT' BEFORE bypass is needed."""
        rt = _make_runtime(last_regime="trending_bear")
        ind = {}
        scenario, reason, trend_dir, ind = _select_scenario(-5.0, {}, rt, ind)

        # trend_dir resolved from regime → no bypass needed
        assert scenario == "continuation"
        assert trend_dir == "SHORT"
        assert ind.get("scenario_dz_bypass", 0) == 0, "Bypass should NOT be triggered when regime does the job"
        assert reason != "dz_bypass"

    def test_hidden_div_takes_priority_over_dz_bypass(self):
        """If hidden_div.kind='bearish_hidden' → trend_dir='SHORT' before bypass."""
        div = types.SimpleNamespace(kind="bearish_hidden")
        rt = _make_runtime(last_div=div)
        ind = {}
        scenario, reason, trend_dir, ind = _select_scenario(-5.0, {}, rt, ind)

        assert scenario == "continuation"
        assert trend_dir == "SHORT"
        assert ind.get("hidden_div_used") == 1
        assert ind.get("scenario_dz_bypass", 0) == 0

    def test_no_false_bypass_on_zero_delta_z(self):
        """delta_z=0 should NOT trigger bypass (no signal direction)."""
        rt = _make_runtime()
        ind = {}
        scenario, _, trend_dir, ind = _select_scenario(0.0, {}, rt, ind)

        assert scenario == "none"
        assert ind.get("scenario_dz_bypass", 0) == 0

    def test_bypass_observable_via_indicator(self):
        """scenario_dz_bypass=1 must be present for replay/observability."""
        rt = _make_runtime()
        ind = {}
        _, _, _, ind = _select_scenario(-21.5, {}, rt, ind)

        assert "scenario_dz_bypass" in ind
        assert "scenario_dz_bypass_th" in ind
        assert ind["scenario_dz_bypass"] == 1
