# tests/test_fill_prob_exec_pen_fallback_v1.py
from __future__ import annotations

"""
Unit-level tests for the fill_prob / exec_fill_pen *fallback* block
introduced in:
  python-worker/services/orderflow/components/tick_processor.py
  python-worker/tick_flow_full/services/orderflow/components/tick_processor.py

The block (inserted right after of_engine.build()) does:
  1. If "fill_prob_proxy" not in indicators  → compute_fill_prob_proxy() and
     populate fill_prob_proxy / eta_fill_sec / fill_prob_p_base / fill_prob_p_wait.
  2. If "exec_fill_pen" not in indicators AND fill_prob_proxy is now present
     → exec_fill_pen = w_fill * (1 - clamp01(fill_prob_proxy))

These tests exercise the same logic directly via compute_fill_prob_proxy so
they run without needing the full tick_processor import chain.
"""

import pytest

from core.fill_prob_proxy import compute_fill_prob_proxy

# ---------------------------------------------------------------------------
# Helpers to simulate the fallback block (same logic as tick_processor.py)
# ---------------------------------------------------------------------------

def _run_fallback(indicators: dict, direction: str, runtime_config: dict | None = None):
    """
    Mirrors the fallback block in tick_processor exactly.
    Returns modified indicators dict in-place and also returns it.
    """
    if runtime_config is None:
        runtime_config = {}

    # Block copied verbatim from the diff (sans `runtime` object — replaced by runtime_config dict)
    try:
        if "fill_prob_proxy" not in indicators:
            fp = compute_fill_prob_proxy(
                direction=str(direction),
                cancel_to_trade_bid=float(indicators.get("cancel_to_trade_bid", 0.0) or 0.0),
                cancel_to_trade_ask=float(indicators.get("cancel_to_trade_ask", 0.0) or 0.0),
                eta_fill_bid_sec=float(indicators.get("eta_fill_bid_sec", 0.0) or 0.0),
                eta_fill_ask_sec=float(indicators.get("eta_fill_ask_sec", 0.0) or 0.0),
                max_wait_s=float(runtime_config.get("fill_prob_max_wait_s", 2.0) or 2.0),
            )
            indicators.setdefault("fill_prob_proxy", float(fp.get("fill_prob_proxy", 0.0) or 0.0))
            indicators.setdefault("eta_fill_sec",     float(fp.get("eta_fill_sec",     0.0) or 0.0))
            indicators.setdefault("fill_prob_p_base", float(fp.get("p_base",           0.0) or 0.0))
            indicators.setdefault("fill_prob_p_wait", float(fp.get("p_wait",           0.0) or 0.0))

        if "exec_fill_pen" not in indicators and "fill_prob_proxy" in indicators:
            w_fill = float(runtime_config.get("exec_fill_pen_w", 0.20) or 0.20)
            p = float(indicators.get("fill_prob_proxy", 0.0) or 0.0)
            if p < 0.0:
                p = 0.0
            if p > 1.0:
                p = 1.0
            indicators["exec_fill_pen"] = float(w_fill * (1.0 - p))
    except Exception:
        pass

    return indicators


# ---------------------------------------------------------------------------
# Tests: fallback fires when OFConfirmEngine has NOT set the fields
# ---------------------------------------------------------------------------

class TestFallbackFiresWhenMissing:
    """Fallback must populate fill_prob_proxy / exec_fill_pen when absent."""

    def test_fill_prob_proxy_populated(self):
        """Absent fill_prob_proxy → fallback must populate it (non-None float)."""
        ind = {
            "cancel_to_trade_bid": 0.3,
            "cancel_to_trade_ask": 0.5,
            "eta_fill_bid_sec": 0.8,
            "eta_fill_ask_sec": 1.5,
        }
        _run_fallback(ind, direction="LONG")
        assert "fill_prob_proxy" in ind
        assert isinstance(ind["fill_prob_proxy"], float)
        assert 0.0 <= ind["fill_prob_proxy"] <= 1.0

    def test_exec_fill_pen_populated(self):
        """After fill_prob_proxy is set by fallback, exec_fill_pen must also appear."""
        ind = {
            "cancel_to_trade_bid": 0.2,
            "eta_fill_bid_sec": 0.5,
        }
        _run_fallback(ind, direction="LONG")
        assert "exec_fill_pen" in ind
        assert 0.0 <= ind["exec_fill_pen"] <= 1.0

    def test_auxillary_keys_populated(self):
        """Fallback must also write eta_fill_sec, fill_prob_p_base, fill_prob_p_wait."""
        ind = {"cancel_to_trade_bid": 0.1, "eta_fill_bid_sec": 1.0}
        _run_fallback(ind, direction="LONG")
        for key in ("eta_fill_sec", "fill_prob_p_base", "fill_prob_p_wait"):
            assert key in ind, f"Expected key '{key}' to be in indicators after fallback"

    def test_short_direction_uses_ask_side(self):
        """For SHORT direction: ask-side stats must be used."""
        ind = {
            "cancel_to_trade_bid": 9.9,  # very high bid cancel — must NOT affect SHORT
            "cancel_to_trade_ask": 0.1,  # low ask cancel → high prob
            "eta_fill_bid_sec": 100.0,   # very slow bid — must NOT affect SHORT
            "eta_fill_ask_sec": 0.5,
        }
        _run_fallback(ind, direction="SHORT")
        # With bid cancel=9.9 (ignored), ask cancel=0.1 → p_base is high
        assert ind["fill_prob_proxy"] > 0.5, (
            f"SHORT should use ask side with low ask-cancel rate; got {ind['fill_prob_proxy']}"
        )


# ---------------------------------------------------------------------------
# Tests: fallback does NOT override engine-populated fields
# ---------------------------------------------------------------------------

class TestFallbackDoesNotOverride:
    """Engine-populated values must survive the fallback (setdefault / presence check)."""

    def test_existing_fill_prob_proxy_not_overwritten(self):
        """
        If engine already set fill_prob_proxy, the fallback must leave it untouched,
        even when L3-lite inputs would compute a different value.
        """
        engine_value = 0.777
        ind = {
            "fill_prob_proxy": engine_value,
            "cancel_to_trade_bid": 0.0,   # would give ~1.0 if recomputed
            "eta_fill_bid_sec": 0.01,
        }
        _run_fallback(ind, direction="LONG")
        assert ind["fill_prob_proxy"] == pytest.approx(engine_value, abs=1e-9), (
            "Fallback must NOT overwrite engine-set fill_prob_proxy"
        )

    def test_existing_exec_fill_pen_not_overwritten(self):
        """If engine set exec_fill_pen, fallback must not recalculate it."""
        engine_pen = 0.123
        ind = {
            "fill_prob_proxy": 0.5,
            "exec_fill_pen": engine_pen,
        }
        _run_fallback(ind, direction="LONG")
        assert ind["exec_fill_pen"] == pytest.approx(engine_pen, abs=1e-9), (
            "Fallback must NOT overwrite engine-set exec_fill_pen"
        )

    def test_partial_override_only_missing_fields(self):
        """Engine sets fill_prob_proxy but not exec_fill_pen: only exec_fill_pen added."""
        ind = {
            "fill_prob_proxy": 0.8,  # engine set this
            # exec_fill_pen missing → fallback should add it
        }
        _run_fallback(ind, direction="LONG", runtime_config={"exec_fill_pen_w": 0.20})
        # fill_prob_proxy unchanged
        assert ind["fill_prob_proxy"] == pytest.approx(0.8, abs=1e-9)
        # exec_fill_pen = 0.20 * (1 - 0.8) = 0.04
        assert ind["exec_fill_pen"] == pytest.approx(0.20 * (1.0 - 0.8), abs=1e-9)


# ---------------------------------------------------------------------------
# Tests: exec_fill_pen formula correctness
# ---------------------------------------------------------------------------

class TestExecFillPenFormula:
    """Verify exec_fill_pen = w_fill * (1 - clamp01(fill_prob_proxy))."""

    @pytest.mark.parametrize("p, w_fill, expected", [
        (1.0, 0.20, 0.0),    # perfect fill probability → zero penalty
        (0.0, 0.20, 0.20),   # zero fill probability → full weight penalty
        (0.5, 0.20, 0.10),   # 50% probability → 10% penalty
        (0.75, 0.30, 0.075), # 75% prob, custom weight
    ])
    def test_formula_known_values(self, p, w_fill, expected):
        ind = {"fill_prob_proxy": p}
        _run_fallback(ind, direction="LONG", runtime_config={"exec_fill_pen_w": w_fill})
        assert ind["exec_fill_pen"] == pytest.approx(expected, abs=1e-9)

    def test_prob_clamped_below_zero(self):
        """If fill_prob_proxy is somehow negative, it must be clamped to 0 before formula."""
        ind = {"fill_prob_proxy": -0.5}
        _run_fallback(ind, direction="LONG", runtime_config={"exec_fill_pen_w": 0.20})
        # clamped → p=0.0 → exec_fill_pen = 0.20 * 1.0 = 0.20
        assert ind["exec_fill_pen"] == pytest.approx(0.20, abs=1e-9)

    def test_prob_clamped_above_one(self):
        """If fill_prob_proxy is somehow > 1, it must be clamped to 1 before formula."""
        ind = {"fill_prob_proxy": 1.5}
        _run_fallback(ind, direction="LONG", runtime_config={"exec_fill_pen_w": 0.20})
        # clamped → p=1.0 → exec_fill_pen = 0.20 * 0.0 = 0.0
        assert ind["exec_fill_pen"] == pytest.approx(0.0, abs=1e-9)

    def test_default_weight_is_0_20(self):
        """Without explicit exec_fill_pen_w in config, default weight = 0.20."""
        ind = {"fill_prob_proxy": 0.5}
        _run_fallback(ind, direction="LONG", runtime_config={})  # no weight set
        # 0.20 * (1 - 0.5) = 0.10
        assert ind["exec_fill_pen"] == pytest.approx(0.10, abs=1e-9)


# ---------------------------------------------------------------------------
# Tests: config override for max_wait_s / exec_fill_pen_w
# ---------------------------------------------------------------------------

class TestConfigOverrides:
    """Runtime config overrides must be respected."""

    def test_max_wait_s_override(self):
        """
        Larger max_wait_s → more time to fill → higher p_wait → higher fill_prob.
        This verifies that `runtime.config.get("fill_prob_max_wait_s", 2.0)` is passed through.
        """
        ind_short_wait = {
            "cancel_to_trade_bid": 0.2,
            "eta_fill_bid_sec": 3.0,   # eta > max_wait_s → p_wait < 1
        }
        ind_long_wait = {
            "cancel_to_trade_bid": 0.2,
            "eta_fill_bid_sec": 3.0,
        }
        _run_fallback(ind_short_wait, direction="LONG", runtime_config={"fill_prob_max_wait_s": 1.0})
        _run_fallback(ind_long_wait,  direction="LONG", runtime_config={"fill_prob_max_wait_s": 5.0})
        assert ind_long_wait["fill_prob_proxy"] > ind_short_wait["fill_prob_proxy"], (
            "Larger max_wait_s must yield higher fill_prob_proxy when eta > default max_wait"
        )

    def test_exec_fill_pen_w_override(self):
        """exec_fill_pen_w from config must scale the penalty correctly."""
        ind_low_w  = {"fill_prob_proxy": 0.5}
        ind_high_w = {"fill_prob_proxy": 0.5}
        _run_fallback(ind_low_w,  direction="LONG", runtime_config={"exec_fill_pen_w": 0.10})
        _run_fallback(ind_high_w, direction="LONG", runtime_config={"exec_fill_pen_w": 0.40})
        assert ind_high_w["exec_fill_pen"] > ind_low_w["exec_fill_pen"]
        assert ind_low_w["exec_fill_pen"]  == pytest.approx(0.10 * 0.5, abs=1e-9)
        assert ind_high_w["exec_fill_pen"] == pytest.approx(0.40 * 0.5, abs=1e-9)


# ---------------------------------------------------------------------------
# Tests: edge cases and robustness
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """Edge and boundary cases for the fallback block."""

    def test_all_zeros_input(self):
        """All-zero L3-lite inputs → fallback runs without exception, fills fields safely."""
        ind = {
            "cancel_to_trade_bid": 0.0,
            "cancel_to_trade_ask": 0.0,
            "eta_fill_bid_sec":    0.0,
            "eta_fill_ask_sec":    0.0,
        }
        _run_fallback(ind, direction="LONG")
        # With zero cancel and zero eta: p_base=1.0, p_wait=1.0 → fill_prob=1.0
        assert ind["fill_prob_proxy"] == pytest.approx(1.0, abs=1e-9)
        # exec_fill_pen = 0.20 * (1 - 1.0) = 0.0
        assert ind["exec_fill_pen"] == pytest.approx(0.0, abs=1e-9)

    def test_empty_indicators(self):
        """Empty indicators dict: fallback populates with defaults (no crash)."""
        ind: dict = {}
        _run_fallback(ind, direction="LONG")
        assert "fill_prob_proxy" in ind
        assert "exec_fill_pen"   in ind
        assert 0.0 <= ind["fill_prob_proxy"] <= 1.0

    def test_missing_l3_stats_graceful(self):
        """No L3-lite keys at all → should still run without exception and give valid result."""
        ind: dict = {}
        try:
            _run_fallback(ind, direction="SHORT")
        except Exception as e:
            pytest.fail(f"Fallback raised unexpectedly: {e}")
        assert "fill_prob_proxy" in ind
        assert isinstance(ind["fill_prob_proxy"], float)

    def test_unknown_direction_defaults_to_ask_side(self):
        """Unknown direction falls back to non-LONG branch (ask side) in compute_fill_prob_proxy."""
        ind = {
            "cancel_to_trade_bid": 9.0,  # would make bid fill_prob very low
            "cancel_to_trade_ask": 0.1,  # ask side: good
            "eta_fill_bid_sec":    100.0,
            "eta_fill_ask_sec":    0.5,
        }
        _run_fallback(ind, direction="UNKNOWN_DIR")
        # Unknown direction maps to else branch → ask side used → expect higher prob
        assert ind["fill_prob_proxy"] > 0.5, (
            f"Unknown direction should use ask side (like SHORT); got {ind['fill_prob_proxy']}"
        )

    def test_smoke_check_catches_always_zero(self):
        """
        Smoke-check regression: verify that a *broken wiring* scenario where
        cancel_to_trade and eta arrive as proper values but fill_prob_proxy
        is somehow 0.0 would be detectable (i.e., fallback would NOT produce 0.0 here).
        This is the primary motivation for the fallback:
          'smoke-check реально ловил "залипло в 0/na" как поломку проводки'.
        """
        # Good inputs that should yield fill_prob_proxy >> 0
        ind = {
            "cancel_to_trade_bid": 0.2,
            "eta_fill_bid_sec":    0.5,
        }
        _run_fallback(ind, direction="LONG")
        assert ind["fill_prob_proxy"] > 0.0, (
            "Good L3-lite inputs must produce fill_prob_proxy > 0 — "
            "if fallback yields 0.0 here the smoke-check cannot distinguish real 0 from broken wiring"
        )
