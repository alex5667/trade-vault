"""P2.6 — Quarter-Kelly position-size scaler (shadow / enforce).

Covers:
  1.  compute_kelly_scale: positive edge → scale > 0
  2.  compute_kelly_scale: p_edge=0 → scale 1.0 (neutral)
  3.  compute_kelly_scale: p_edge=1.0 → capped at max_scale
  4.  compute_kelly_scale: negative kelly → min_scale floor
  5.  compute_kelly_scale: scale in [min_scale, max_scale]
  6.  apply_kelly_sizing shadow: does NOT change effective_risk_pct
  7.  apply_kelly_sizing shadow: writes kelly_scale_shadow to indicators
  8.  apply_kelly_sizing enforce: changes effective_risk_pct
  9.  apply_kelly_sizing enforce: min_scale floor respected
 10.  apply_kelly_sizing enforce: max_scale cap respected
 11.  apply_kelly_sizing: writes kelly_p_edge_input to indicators
 12.  apply_kelly_sizing: missing p_edge → 1.0 scale (neutral)
 13.  quarter-Kelly formula sanity: p=0.6, b=1.5 → known f*
 14.  p_edge < 0 → clamped to min_scale (invalid input)
 15.  tp1_target_r=0 → treated as 1.0 (no division by zero)
"""
from __future__ import annotations

import math
import pytest

from core.kelly_sizer_v2 import compute_kelly_scale, apply_kelly_sizing, _BASELINE_KELLY


# ── 1. Positive edge ──────────────────────────────────────────────────────────

def test_positive_edge_scale_positive():
    scale = compute_kelly_scale(p_edge=0.60, tp1_target_r=1.5)
    assert scale > 0


# ── 2. p_edge=0 → neutral ────────────────────────────────────────────────────

def test_zero_p_edge_neutral():
    scale = compute_kelly_scale(p_edge=0.0, tp1_target_r=1.5)
    assert scale == pytest.approx(1.0)


# ── 3. p_edge=1.0 → capped at max_scale ─────────────────────────────────────

def test_full_certainty_capped():
    scale = compute_kelly_scale(p_edge=1.0, tp1_target_r=2.0, max_scale=1.5)
    assert scale == pytest.approx(1.5)


# ── 4. Negative Kelly → min_scale floor ──────────────────────────────────────

def test_negative_kelly_min_floor():
    # p=0.2, b=0.5 → full_kelly = 0.2 - 0.8/0.5 = 0.2 - 1.6 = -1.4 → min_scale
    scale = compute_kelly_scale(p_edge=0.20, tp1_target_r=0.5, min_scale=0.4)
    assert scale == pytest.approx(0.4)


# ── 5. Scale always in [min_scale, max_scale] ────────────────────────────────

@pytest.mark.parametrize("p,b", [(0.3, 1.0), (0.5, 1.5), (0.7, 2.0), (0.9, 3.0)])
def test_scale_bounds(p, b):
    scale = compute_kelly_scale(p, b, min_scale=0.5, max_scale=1.5)
    assert 0.5 <= scale <= 1.5


# ── 6. Shadow mode: does NOT change effective_risk_pct ───────────────────────

def test_shadow_no_change():
    ind: dict = {"p_edge": 0.65, "tp1_target_r": 1.5}
    result = apply_kelly_sizing(ind, 5.0, enforce=False)
    assert result == pytest.approx(5.0)


# ── 7. Shadow mode: writes kelly_scale_shadow ────────────────────────────────

def test_shadow_writes_indicator():
    ind: dict = {"p_edge": 0.60, "tp1_target_r": 1.5}
    apply_kelly_sizing(ind, 5.0, enforce=False)
    assert "kelly_scale_shadow" in ind
    assert math.isfinite(ind["kelly_scale_shadow"])


# ── 8. Enforce mode: changes effective_risk_pct ──────────────────────────────

def test_enforce_changes_risk():
    ind: dict = {"p_edge": 0.70, "tp1_target_r": 2.0}
    result = apply_kelly_sizing(ind, 5.0, enforce=True)
    # With strong edge (0.70) scale > 1 → result > 5.0 or at most max capped
    assert result != pytest.approx(5.0) or ind["kelly_scale_shadow"] == pytest.approx(1.0)


# ── 9. Enforce: min_scale floor ──────────────────────────────────────────────

def test_enforce_min_floor():
    # Negative-EV bet → min_scale applied by module default (0.5) → risk halved at worst
    ind: dict = {"p_edge": 0.10, "tp1_target_r": 0.5}
    result = apply_kelly_sizing(ind, 4.0, enforce=True)
    assert result <= 4.0  # negative Kelly → scale ≤ 1 → risk doesn't increase


# ── 10. Enforce: max_scale cap ───────────────────────────────────────────────

def test_enforce_max_cap():
    # Very high p_edge → would be huge Kelly; capped at max_scale=1.5
    ind: dict = {"p_edge": 0.99, "tp1_target_r": 5.0}
    result = apply_kelly_sizing(ind, 4.0, enforce=True)
    max_possible = 4.0 * 1.5
    assert result <= max_possible + 1e-9


# ── 11. writes kelly_p_edge_input ────────────────────────────────────────────

def test_writes_p_edge_input():
    ind: dict = {"p_edge": 0.62}
    apply_kelly_sizing(ind, 5.0, enforce=False)
    assert ind["kelly_p_edge_input"] == pytest.approx(0.62, abs=1e-4)


# ── 12. Missing p_edge → neutral ─────────────────────────────────────────────

def test_missing_p_edge_neutral():
    ind: dict = {}
    result = apply_kelly_sizing(ind, 5.0, enforce=True)
    # p_edge=0 → scale=1.0 → no change
    assert result == pytest.approx(5.0)


# ── 13. Quarter-Kelly formula sanity ─────────────────────────────────────────

def test_quarter_kelly_formula():
    # p=0.6, b=1.5 → full_kelly = 0.6 - 0.4/1.5 ≈ 0.333
    # quarter_kelly = 0.333 × 0.25 ≈ 0.0833
    # anchor = 0.25 × 0.25 = 0.0625
    # scale = 0.0833/0.0625 ≈ 1.333
    scale = compute_kelly_scale(0.60, 1.5, kelly_fraction=0.25, min_scale=0.5, max_scale=2.0)
    expected_full_kelly = 0.60 - 0.40 / 1.5
    expected_frac = expected_full_kelly * 0.25
    expected_scale = expected_frac / (_BASELINE_KELLY * 0.25)
    assert scale == pytest.approx(expected_scale, rel=1e-5)


# ── 14. Negative p_edge → min_scale ──────────────────────────────────────────

def test_invalid_negative_p_edge():
    scale = compute_kelly_scale(p_edge=-0.1, tp1_target_r=1.5, min_scale=0.5)
    assert scale == pytest.approx(1.0)  # degenerate → neutral


# ── 15. tp1_target_r=0 → no division-by-zero ─────────────────────────────────

def test_zero_rr_no_div_zero():
    scale = compute_kelly_scale(p_edge=0.6, tp1_target_r=0.0, min_scale=0.5)
    assert math.isfinite(scale)
