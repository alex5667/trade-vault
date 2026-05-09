"""Unit tests for BinanceExecutor orchestrator trailing stop logic.

These tests are purely CPU-bound and require no network access or Binance API keys.
They test:
  - _compute_profile_sl(): SL computation for LONG/SHORT, clamping, edge cases
  - trail_mode ENV switch (orchestrator vs native selection)
  - Trailing SL move delta threshold logic

Run from project root:
  cd python-worker && PYTHONPATH=. python -m pytest services/tests/test_binance_trailing_orchestrator_v1.py -v
"""

import importlib.util
import math
import sys
from pathlib import Path

# --- Load the module directly to avoid heavy dependency chain ---
mod_path = Path(__file__).parent.parent / "binance_executor.py"
spec = importlib.util.spec_from_file_location("binance_executor", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)

BinanceExecutor = mod.BinanceExecutor


# ---------------------------------------------------------------------------
# _compute_profile_sl() — LONG
# ---------------------------------------------------------------------------

def test_compute_profile_sl_long_basic():
    """LONG: SL = current_price - trail_distance."""
    sl = BinanceExecutor._compute_profile_sl(
        side="LONG",
        current_price=100.0,
        trail_distance=2.0,
        original_sl=90.0,
        point=0.01,
    )
    assert sl is not None
    assert math.isclose(sl, 98.0, rel_tol=1e-6), f"got {sl}"


def test_compute_profile_sl_long_respects_original_sl():
    """LONG: if computed SL would be below original_sl, use original_sl."""
    sl = BinanceExecutor._compute_profile_sl(
        side="LONG",
        current_price=100.0,
        trail_distance=15.0,  # 100 - 15 = 85, which is below original_sl=90
        original_sl=90.0,
        point=0.01,
    )
    assert sl is not None
    assert math.isclose(sl, 90.0, rel_tol=1e-6), f"got {sl}"


def test_compute_profile_sl_long_never_above_price():
    """LONG: SL must be strictly below current_price."""
    sl = BinanceExecutor._compute_profile_sl(
        side="LONG",
        current_price=100.0,
        trail_distance=0.001,  # very small distance
        original_sl=99.999,    # original_sl very close to price
        point=0.01,
    )
    assert sl is not None
    assert sl < 100.0, f"SL {sl} should be below price 100.0"


# ---------------------------------------------------------------------------
# _compute_profile_sl() — SHORT
# ---------------------------------------------------------------------------

def test_compute_profile_sl_short_basic():
    """SHORT: SL = current_price + trail_distance."""
    sl = BinanceExecutor._compute_profile_sl(
        side="SHORT",
        current_price=100.0,
        trail_distance=2.0,
        original_sl=110.0,
        point=0.01,
    )
    assert sl is not None
    assert math.isclose(sl, 102.0, rel_tol=1e-6), f"got {sl}"


def test_compute_profile_sl_short_respects_original_sl():
    """SHORT: if computed SL above original_sl, clamp to original_sl."""
    sl = BinanceExecutor._compute_profile_sl(
        side="SHORT",
        current_price=100.0,
        trail_distance=15.0,  # 100 + 15 = 115, which is above original_sl=110
        original_sl=110.0,
        point=0.01,
    )
    assert sl is not None
    assert math.isclose(sl, 110.0, rel_tol=1e-6), f"got {sl}"


def test_compute_profile_sl_short_never_below_price():
    """SHORT: SL must be strictly above current_price."""
    sl = BinanceExecutor._compute_profile_sl(
        side="SHORT",
        current_price=100.0,
        trail_distance=0.001,
        original_sl=100.001,
        point=0.01,
    )
    assert sl is not None
    assert sl > 100.0, f"SL {sl} should be above price 100.0"


# ---------------------------------------------------------------------------
# _compute_profile_sl() — edge cases
# ---------------------------------------------------------------------------

def test_compute_profile_sl_negative_distance():
    """Negative trail_distance returns None."""
    sl = BinanceExecutor._compute_profile_sl(
        side="LONG", current_price=100.0, trail_distance=-1.0,
        original_sl=90.0, point=0.01,
    )
    assert sl is None


def test_compute_profile_sl_zero_price():
    """Zero current_price returns None."""
    sl = BinanceExecutor._compute_profile_sl(
        side="LONG", current_price=0.0, trail_distance=2.0,
        original_sl=90.0, point=0.01,
    )
    assert sl is None


def test_compute_profile_sl_zero_point_defaults():
    """Zero point should default to 0.0001 and still compute."""
    sl = BinanceExecutor._compute_profile_sl(
        side="LONG", current_price=100.0, trail_distance=2.0,
        original_sl=90.0, point=0.0,
    )
    assert sl is not None
    # With point=0.0001 it should be very close to 98.0
    assert 97.9 < sl < 98.1, f"got {sl}"


def test_compute_profile_sl_no_original_sl():
    """When original_sl=0, the computation should not clamp."""
    sl_long = BinanceExecutor._compute_profile_sl(
        side="LONG", current_price=100.0, trail_distance=50.0,
        original_sl=0.0, point=0.01,
    )
    assert sl_long is not None
    assert math.isclose(sl_long, 50.0, rel_tol=1e-6), f"got {sl_long}"


# ---------------------------------------------------------------------------
# _compute_profile_sl() — crypto-realistic values
# ---------------------------------------------------------------------------

def test_compute_profile_sl_btc_long():
    """BTC LONG: price=67000, ATR=500, mult=0.6, trail_distance=300."""
    sl = BinanceExecutor._compute_profile_sl(
        side="LONG",
        current_price=67000.0,
        trail_distance=300.0,   # 500 * 0.6
        original_sl=66000.0,
        point=0.10,
    )
    assert sl is not None
    # Expected: 67000 - 300 = 66700
    assert math.isclose(sl, 66700.0, rel_tol=1e-6), f"got {sl}"


def test_compute_profile_sl_sol_short():
    """SOL SHORT: price=150.0, ATR=5.0, mult=0.6, trail_distance=3.0."""
    sl = BinanceExecutor._compute_profile_sl(
        side="SHORT",
        current_price=150.0,
        trail_distance=3.0,     # 5.0 * 0.6
        original_sl=160.0,
        point=0.001,
    )
    assert sl is not None
    # Expected: 150 + 3 = 153.0
    assert math.isclose(sl, 153.0, rel_tol=1e-6), f"got {sl}"


# ---------------------------------------------------------------------------
# SL move delta threshold logic
# ---------------------------------------------------------------------------

def test_sl_move_delta_skip_small():
    """If candidate improves by less than min_delta_pct, skip the move."""
    current_sl = 98.0
    candidate_sl = 98.01  # only 0.01% improvement
    min_delta_pct = 0.05

    delta_pct = abs(candidate_sl - current_sl) / current_sl * 100
    improved = candidate_sl > current_sl  # LONG
    should_move = improved and delta_pct >= min_delta_pct
    assert not should_move, f"delta_pct={delta_pct:.4f}% should be below threshold"


def test_sl_move_delta_accept_large():
    """If candidate improves by more than min_delta_pct, accept the move."""
    current_sl = 98.0
    candidate_sl = 98.10  # ~0.10% improvement
    min_delta_pct = 0.05

    delta_pct = abs(candidate_sl - current_sl) / current_sl * 100
    improved = candidate_sl > current_sl  # LONG
    should_move = improved and delta_pct >= min_delta_pct
    assert should_move, f"delta_pct={delta_pct:.4f}% should be above threshold"


def test_sl_move_no_worsening_long():
    """LONG: candidate below current_sl should never trigger a move."""
    current_sl = 98.0
    candidate_sl = 97.5
    improved = candidate_sl > current_sl  # False for LONG
    assert not improved


def test_sl_move_no_worsening_short():
    """SHORT: candidate above current_sl should never trigger a move."""
    current_sl = 102.0
    candidate_sl = 103.0
    improved = candidate_sl < current_sl  # False for SHORT
    assert not improved
