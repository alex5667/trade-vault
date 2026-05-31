"""
Regression tests for ML_SKIP EMA-smoothed lag check in signal_confidence.py.

Background: before this fix the check used instantaneous lag_ms against a 400ms
threshold.  The structural Redis-stream pipeline lag is 400-500ms in normal
operation, so ML was blocked on 100% of signals.  Fix: EMA smoothing + raise
default threshold to 1000ms.
"""

import importlib


def _fresh_ema():
    """Return a clean _update_lag_ema with isolated EMA state."""
    import services.signal_confidence as sc
    sc._ML_LAG_EMA.clear()
    return sc._update_lag_ema


def test_steady_state_450ms_below_1000ms_threshold():
    """Structural 400-500ms lag must not trigger ML_SKIP at 1000ms threshold."""
    ema_fn = _fresh_ema()
    ema = None
    for _ in range(20):
        ema = ema_fn("BTCUSDT", 450.0)
    assert ema is not None
    assert ema < 1000.0, f"Steady 450ms lag should be below 1000ms threshold, got {ema:.1f}"


def test_single_spike_does_not_trigger_skip():
    """One 1500ms spike on a healthy 450ms baseline must not cross 1000ms."""
    ema_fn = _fresh_ema()
    # Warm up to steady 450ms
    for _ in range(10):
        ema_fn("ETHUSDT", 450.0)
    # Single spike
    ema = ema_fn("ETHUSDT", 1500.0)
    assert ema < 1000.0, f"Single 1500ms spike should not trigger skip, ema={ema:.1f}"


def test_sustained_overload_triggers_skip():
    """Sustained 2000ms lag for ~15 ticks must eventually cross the 1000ms threshold."""
    ema_fn = _fresh_ema()
    ema = 0.0
    for _ in range(15):
        ema = ema_fn("SOLUSDT", 2000.0)
    assert ema > 1000.0, f"Sustained 2000ms lag should trigger skip, ema={ema:.1f}"


def test_recovery_after_overload():
    """After overload resolves, EMA drops back below threshold within ~30 ticks."""
    ema_fn = _fresh_ema()
    # Saturate to overload
    for _ in range(20):
        ema_fn("PEPEUSDT", 2000.0)
    # Recovery to normal
    ema = None
    for _ in range(30):
        ema = ema_fn("PEPEUSDT", 300.0)
    assert ema is not None
    assert ema < 1000.0, f"EMA should recover below threshold after 30 normal ticks, got {ema:.1f}"


def test_per_symbol_isolation():
    """High lag on one symbol must not affect EMA of another symbol."""
    ema_fn = _fresh_ema()
    # BTC steady 450ms
    for _ in range(10):
        ema_fn("BTCUSDT", 450.0)
    # SOL overloaded
    for _ in range(20):
        ema_fn("SOLUSDT", 2000.0)
    # BTC EMA should still be near 450ms
    btc_ema = ema_fn("BTCUSDT", 450.0)
    assert btc_ema < 1000.0, f"BTC EMA should be unaffected by SOL overload, got {btc_ema:.1f}"


def test_default_threshold_is_1000ms():
    """ENV default ML_LAG_THRESHOLD_MS must be 1000ms (not 400ms) after fix."""
    import os
    import services.signal_confidence as sc
    # Remove any override to test the code default
    orig = os.environ.pop("ML_LAG_THRESHOLD_MS", None)
    try:
        import importlib
        importlib.reload(sc)
        # Verify the default string in the getenv call
        import ast, inspect
        src = inspect.getsource(sc.ConfidenceScorer.score)
        assert '"1000.0"' in src or "'1000.0'" in src, (
            "Default ML_LAG_THRESHOLD_MS must be '1000.0' in signal_confidence.py"
        )
    finally:
        if orig is not None:
            os.environ["ML_LAG_THRESHOLD_MS"] = orig
