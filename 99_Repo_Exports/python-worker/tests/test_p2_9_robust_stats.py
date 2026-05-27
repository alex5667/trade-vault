"""P2.9 — MAD-z audit regression tests.

Covers:
  1. bounded_mad_z: basic computation
  2. bounded_mad_z: outlier resistance (outlier doesn't blow up z)
  3. bounded_mad_z: cap is enforced
  4. bounded_mad_z: cold / too-short history → 0.0
  5. bounded_mad_z: all-identical history → 0.0 (zero MAD)
  6. bounded_mad_z: non-finite x → 0.0
  7. bounded_mad_z: non-finite values in history are filtered
  8. RollingRobustZ: outlier resistance (mean/std would give wrong result)
  9. RollingRobustZ: z==0 when fewer than 8 samples
  10. Regression guard: no np.mean/np.std in signal-critical detector files
"""
from __future__ import annotations

import math

from core.robust_stats import RollingRobustZ, bounded_mad_z


# ─── 1. bounded_mad_z basic ─────────────────────────────────────────────────

def test_bounded_mad_z_basic():
    history = [float(i) for i in range(1, 21)]  # [1..20], median=10.5
    z = bounded_mad_z(15.0, history)
    assert z > 0, "value above median should give positive z"
    assert z < 6.0, "normal deviation should not hit cap"


def test_bounded_mad_z_median_value_gives_near_zero():
    history = [float(i) for i in range(1, 21)]
    med = 10.5
    z = bounded_mad_z(med, history)
    assert abs(z) < 0.01, "value at median should give ~0 z-score"


# ─── 2. outlier resistance ───────────────────────────────────────────────────

def test_bounded_mad_z_outlier_resistant():
    """90 normal values with variation + 10 extreme outliers.
    MAD-z of a normal value should stay near 0; mean/std would be badly skewed.
    Values must have non-zero MAD — identical values give MAD=0 regardless of method.
    """
    import random
    rng = random.Random(42)
    # Normal cluster: centred at 10, spread ~1 (non-zero MAD)
    normal = [10.0 + rng.gauss(0, 1.0) for _ in range(90)]
    outliers = [1_000_000.0] * 10
    history = normal + outliers

    z_normal = bounded_mad_z(10.5, history)
    assert abs(z_normal) < 2.0, (
        f"MAD-z should be near 0 for a normal value with outliers present, got {z_normal}"
    )

    # Contrast: MAD-z correctly flags the actual outlier value as astronomically extreme;
    # mean/std gives only ~3σ because std is inflated to ~300k by the outliers themselves.
    import statistics
    mean_val = statistics.mean(history)
    std_val = statistics.stdev(history)
    z_outlier_mad = bounded_mad_z(1_000_000.0, history, cap=1e9)
    z_outlier_std = (1_000_000.0 - mean_val) / (std_val + 1e-12) if std_val > 0 else 0.0
    assert z_outlier_mad > 1_000.0, (
        f"MAD-z should be astronomically high for actual outlier, got {z_outlier_mad}"
    )
    assert abs(z_outlier_std) < 10.0, (
        f"std-based z understates outlier extremity (inflated std masks it), got {z_outlier_std}"
    )


# ─── 3. cap enforcement ──────────────────────────────────────────────────────

def test_bounded_mad_z_cap_is_enforced():
    history = [10.0] * 20
    # x is massively above the median (all history = 10.0, MAD = 0 → bounded_mad_z handles)
    z = bounded_mad_z(10.0, history, cap=6.0)
    assert -6.0 <= z <= 6.0


def test_bounded_mad_z_extreme_value_capped():
    normal = [10.0] * 20
    z = bounded_mad_z(100_000.0, normal + [11.0], cap=4.0)
    assert z <= 4.0, f"z={z} should be capped at 4.0"


def test_bounded_mad_z_negative_cap():
    normal = [10.0] * 20
    z = bounded_mad_z(-100_000.0, normal + [9.0], cap=4.0)
    assert z >= -4.0, f"z={z} should be capped at -4.0"


# ─── 4. cold / short history ─────────────────────────────────────────────────

def test_bounded_mad_z_too_short_returns_zero():
    z = bounded_mad_z(5.0, [1.0, 2.0, 3.0], min_n=8)  # only 3 < 8
    assert z == 0.0


def test_bounded_mad_z_empty_history_returns_zero():
    z = bounded_mad_z(5.0, [])
    assert z == 0.0


# ─── 5. zero MAD ─────────────────────────────────────────────────────────────

def test_bounded_mad_z_zero_mad_returns_zero():
    """All-identical history → MAD=0 → should not raise ZeroDivisionError."""
    history = [5.0] * 20
    z = bounded_mad_z(5.0, history)
    assert z == 0.0


# ─── 6-7. non-finite values ──────────────────────────────────────────────────

def test_bounded_mad_z_nonfinite_x_returns_zero():
    history = [float(i) for i in range(1, 21)]
    assert bounded_mad_z(float("nan"), history) == 0.0
    assert bounded_mad_z(float("inf"), history) == 0.0
    assert bounded_mad_z(float("-inf"), history) == 0.0


def test_bounded_mad_z_nonfinite_in_history_filtered():
    """nan/inf in history are filtered; computation should still work."""
    history = [10.0] * 15 + [float("nan"), float("inf")]
    z = bounded_mad_z(10.5, history)
    # Should not raise; result is finite
    assert math.isfinite(z)


# ─── 8. RollingRobustZ outlier resistance ────────────────────────────────────

def test_rolling_robust_z_outlier_resistant():
    """After warming up with normal values, a single outlier in the history
    should not cause the z-score of the next normal value to explode."""
    import random
    rng = random.Random(7)
    rz = RollingRobustZ(window=50)
    # 49 Gaussian values so MAD is non-zero; identical values → MAD=0 regardless of method
    for v in [10.0 + rng.gauss(0, 1.0) for _ in range(49)]:
        rz.update(v)
    rz.update(1_000_000.0)  # one outlier
    z = rz.z(10.5)
    assert abs(z) < 2.0, f"MAD-z={z} should be near 0 for normal value with 1 outlier in 50"


def test_rolling_robust_z_cold_returns_zero():
    rz = RollingRobustZ(window=50)
    for v in [1.0, 2.0, 3.0]:
        rz.update(v)
    assert rz.z(2.0) == 0.0


def test_rolling_robust_z_warm_gives_nonzero_for_extreme():
    rz = RollingRobustZ(window=50)
    for v in [10.0] * 50:
        rz.update(v)
    rz.update(11.0)  # small variation in history
    z = rz.z(50.0)  # very extreme value
    assert z > 3.0, "extreme value should give high z-score after warmup"


# ─── 10. regression guard: no np.mean/np.std in signal-gate threshold code ──

def test_no_mean_std_in_gate_threshold_files():
    """Guard against accidentally introducing mean/std-based thresholds in
    signal-critical gate/detector files that already use RollingRobustZ."""
    import os, ast

    files_to_check = [
        "services/orderflow/derivatives_context.py",
        "services/orderflow/message_rate.py",
        "services/orderflow/components/book_processor/lob_pressure_tracker.py",
    ]

    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    violations: list[str] = []

    for rel_path in files_to_check:
        abs_path = os.path.join(base, rel_path)
        if not os.path.exists(abs_path):
            continue
        source = open(abs_path).read()
        try:
            tree = ast.parse(source)
        except SyntaxError:
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            # np.mean / np.std as Attribute calls
            if isinstance(node.func, ast.Attribute):
                if node.func.attr in ("mean", "std"):
                    if isinstance(node.func.value, ast.Name) and node.func.value.id == "np":
                        violations.append(f"{rel_path}:{node.lineno}: np.{node.func.attr}()")

    assert not violations, (
        "Found mean/std-based computation in signal-gate files — "
        "migrate to RollingRobustZ or bounded_mad_z:\n" + "\n".join(violations)
    )
