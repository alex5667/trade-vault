from __future__ import annotations

from core.cusum_drift_detector import CuSumDriftDetector, _ece_from_bins, _rebuild_ece_bins


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _feed(det: CuSumDriftDetector, *, schema: str, regime: str,
          p_hats: list[float], outcomes: list[int]) -> list[bool]:
    alarms = []
    for p, y in zip(p_hats, outcomes):
        alarms.append(det.observe(schema=schema, regime=regime, p_hat=p, outcome=y))
    return alarms


def _make_calibrated(n: int, p: float = 0.6) -> tuple[list[float], list[int]]:
    """Calibrated: p_hat=p, outcomes Bernoulli(p) via deterministic LCG."""
    import random
    rng = random.Random(42)
    ps = [p] * n
    ys = [1 if rng.random() < p else 0 for _ in range(n)]
    return ps, ys


def _warmup(det: CuSumDriftDetector, *, schema: str = "v14_of",
            regime: str = "trend", warmup: int = 100) -> None:
    """Feed exactly `warmup` calibrated samples to exit warmup, nothing more."""
    ps, ys = _make_calibrated(warmup)
    _feed(det, schema=schema, regime=regime, p_hats=ps, outcomes=ys)


# ---------------------------------------------------------------------------
# ECE helpers
# ---------------------------------------------------------------------------

def test_ece_from_bins_perfect_calibration() -> None:
    bins = [(0.65 * 10, 6.5, 10)] + [(0.0, 0.0, 0)] * 9
    ece = _ece_from_bins(bins)
    assert ece == 0.0


def test_ece_from_bins_empty() -> None:
    assert _ece_from_bins([(0.0, 0.0, 0)] * 10) == 0.0


def test_ece_from_bins_large_mismatch() -> None:
    # p_hat always 0.9 but hit_rate = 0.1 → ECE = 0.8
    bins = [(0.9 * 10, 1.0, 10)] + [(0.0, 0.0, 0)] * 9
    ece = _ece_from_bins(bins)
    assert abs(ece - 0.8) < 1e-9


def test_rebuild_ece_bins_deciles() -> None:
    window = [(0.05, 1), (0.15, 0), (0.95, 1), (0.95, 1)]
    bins = _rebuild_ece_bins(window)
    assert bins[0][2] == 1   # bucket [0.0, 0.1) → 0.05
    assert bins[1][2] == 1   # bucket [0.1, 0.2) → 0.15
    assert bins[9][2] == 2   # bucket [0.9, 1.0) → 0.95 × 2


# ---------------------------------------------------------------------------
# warmup phase
# ---------------------------------------------------------------------------

def test_no_alarm_during_warmup() -> None:
    det = CuSumDriftDetector(warmup=20, delta=0.0, threshold=0.0)
    alarms = _feed(det, schema="v14", regime="trend",
                   p_hats=[0.9] * 19, outcomes=[0] * 19)
    assert not any(alarms), "No alarm before warmup completes"


def test_warmup_freezes_baseline() -> None:
    det = CuSumDriftDetector(warmup=50, delta=0.0, threshold=999.0)
    # p=0.7, alternating y=1/0 → brier alternates 0.09 and 0.49 → mean ≈ 0.29
    ps = [0.7] * 50
    ys = [1, 0] * 25
    _feed(det, schema="v14", regime="trend", p_hats=ps, outcomes=ys)
    b = det.baseline_brier("v14", "trend")
    assert b > 0.0, "baseline should be set after warmup"
    assert abs(b - 0.29) < 0.05, f"expected ~0.29, got {b}"


def test_ph_stays_zero_in_warmup() -> None:
    det = CuSumDriftDetector(warmup=30)
    _feed(det, schema="x", regime="range", p_hats=[0.5] * 20, outcomes=[0] * 20)
    assert det.current_ph("x", "range") == 0.0


# ---------------------------------------------------------------------------
# PH detection on injected drift
# ---------------------------------------------------------------------------

def test_alarm_fires_on_sustained_drift() -> None:
    det = CuSumDriftDetector(warmup=50, delta=0.001, threshold=0.10,
                             cooldown_observations=0)
    # Warmup with ~calibrated data
    ps_warm = [0.65] * 50
    ys_warm = [1, 0, 1, 1, 0] * 10
    _feed(det, schema="s", regime="trend", p_hats=ps_warm, outcomes=ys_warm)

    # Inject systematic drift: p_hat=0.65 but all losses → Brier = 0.65²=0.4225
    # vs baseline ~0.2275 → each step adds ~0.20 to PH
    alarms = []
    for _ in range(20):
        alarms.append(det.observe(schema="s", regime="trend", p_hat=0.65, outcome=0))
    assert any(alarms), "Alarm must fire on sustained drift"


def test_false_alarm_rate_low_under_calibrated_noise() -> None:
    """Under calibrated noise, alarm rate must be well below 1% of observations."""
    import random
    rng = random.Random(42)
    det = CuSumDriftDetector(warmup=100, delta=0.02, threshold=1.0,
                             cooldown_observations=100)
    _warmup(det, schema="v", regime="trend", warmup=100)
    alarms = []
    for _ in range(500):
        p = 0.60 + rng.gauss(0.0, 0.05)
        p = max(0.01, min(0.99, p))
        y = 1 if rng.random() < p else 0
        alarms.append(det.observe(schema="v", regime="trend", p_hat=p, outcome=y))
    alarm_rate = sum(alarms) / len(alarms)
    assert alarm_rate < 0.01, f"False alarm rate too high: {alarm_rate:.3f}"


def test_alarm_resets_ph_score() -> None:
    det = CuSumDriftDetector(warmup=30, delta=0.0, threshold=0.05,
                             cooldown_observations=0)
    # warmup
    _feed(det, schema="a", regime="r", p_hats=[0.5] * 30, outcomes=[1, 0] * 15)
    # drive to alarm
    for _ in range(50):
        det.observe(schema="a", regime="r", p_hat=0.9, outcome=0)
    assert det.n_alarms("a", "r") >= 1
    # PH should be reset (0) after alarm
    # (it may have already started accumulating again, so ≤ threshold)
    assert det.current_ph("a", "r") < det.threshold


# ---------------------------------------------------------------------------
# cooldown
# ---------------------------------------------------------------------------

def test_cooldown_suppresses_repeated_alarms() -> None:
    det = CuSumDriftDetector(warmup=20, delta=0.0, threshold=0.05,
                             cooldown_observations=50)
    _feed(det, schema="b", regime="r", p_hats=[0.5] * 20, outcomes=[1, 0] * 10)
    alarms = []
    for _ in range(100):
        alarms.append(det.observe(schema="b", regime="r", p_hat=0.9, outcome=0))
    alarm_count = sum(alarms)
    assert alarm_count <= 2, f"Cooldown should limit alarms, got {alarm_count}"


# ---------------------------------------------------------------------------
# multi-key isolation
# ---------------------------------------------------------------------------

def test_different_schema_regime_isolated() -> None:
    det = CuSumDriftDetector(warmup=30, delta=0.001, threshold=0.10,
                             cooldown_observations=0)
    _warmup(det, schema="A", regime="trend", warmup=30)
    _warmup(det, schema="B", regime="range", warmup=30)

    # Record B/range alarm count before drift injection
    alarms_b_before = det.n_alarms("B", "range")

    # Drift only in (A, trend)
    for _ in range(30):
        det.observe(schema="A", regime="trend", p_hat=0.9, outcome=0)

    assert det.n_alarms("A", "trend") >= 1
    # B/range must not be touched by A/trend observations
    assert det.n_alarms("B", "range") == alarms_b_before


def test_different_regimes_same_schema_isolated() -> None:
    det = CuSumDriftDetector(warmup=30, delta=0.001, threshold=0.10,
                             cooldown_observations=0)
    _warmup(det, schema="s", regime="trend", warmup=30)
    _warmup(det, schema="s", regime="range", warmup=30)

    alarms_range_before = det.n_alarms("s", "range")

    for _ in range(30):
        det.observe(schema="s", regime="trend", p_hat=0.9, outcome=0)

    assert det.n_alarms("s", "trend") >= 1
    assert det.n_alarms("s", "range") == alarms_range_before


# ---------------------------------------------------------------------------
# ECE integration
# ---------------------------------------------------------------------------

def test_ece_rises_with_miscalibration() -> None:
    det = CuSumDriftDetector(warmup=10, ece_window_size=100)
    # Calibrated: p≈0.3, win_rate≈0.3
    for i in range(50):
        det.observe(schema="s", regime="r", p_hat=0.3, outcome=1 if i % 3 == 0 else 0)
    ece_cal = det.current_ece("s", "r")

    # Inject: p=0.9 but always lose
    det2 = CuSumDriftDetector(warmup=10, ece_window_size=100)
    for _ in range(50):
        det2.observe(schema="s", regime="r", p_hat=0.9, outcome=0)
    ece_mis = det2.current_ece("s", "r")

    assert ece_mis > ece_cal, f"miscalibrated ECE {ece_mis:.3f} should exceed calibrated {ece_cal:.3f}"


def test_ece_near_zero_perfect_calibration() -> None:
    det = CuSumDriftDetector(warmup=10, ece_window_size=200)
    # p=0.5, hit_rate=0.5 → ECE≈0
    for i in range(200):
        det.observe(schema="s", regime="r", p_hat=0.5, outcome=i % 2)
    ece = det.current_ece("s", "r")
    assert ece < 0.05, f"ECE should be near 0 for perfectly calibrated data, got {ece:.4f}"


# ---------------------------------------------------------------------------
# snapshot / load_state
# ---------------------------------------------------------------------------

def test_snapshot_roundtrip() -> None:
    det = CuSumDriftDetector(warmup=30)
    _warmup(det, schema="v14", regime="trend", warmup=30)
    snap = det.snapshot()
    assert len(snap) >= 1
    row = next(r for r in snap if r["schema"] == "v14" and r["regime"] == "trend")
    assert row["warmup_done"] is True
    assert row["baseline_brier"] > 0.0


def test_load_state_restores_baseline() -> None:
    det = CuSumDriftDetector(warmup=100)  # long warmup
    # Inject a pre-computed baseline directly
    det.load_state([
        {"schema": "v14", "regime": "trend", "baseline_brier": 0.25,
         "n_alarms": 2, "n_observed": 500},
    ])
    assert det.baseline_brier("v14", "trend") == 0.25
    assert det.n_alarms("v14", "trend") == 2
    assert det.n_observed("v14", "trend") == 500


def test_load_state_tolerates_malformed_rows() -> None:
    det = CuSumDriftDetector()
    det.load_state([
        None,
        {},
        {"schema": "v14"},  # missing regime
        {"schema": "v14", "regime": "trend", "baseline_brier": -1.0},  # cold
        {"schema": "v14", "regime": "range", "baseline_brier": 0.20},  # OK
    ])
    assert det.baseline_brier("v14", "range") == 0.20
    assert det.baseline_brier("v14", "trend") == -1.0


def test_load_state_cold_baseline_skipped() -> None:
    det = CuSumDriftDetector()
    det.load_state([{"schema": "s", "regime": "r", "baseline_brier": -1.0}])
    assert det.baseline_brier("s", "r") == -1.0


# ---------------------------------------------------------------------------
# edge cases
# ---------------------------------------------------------------------------

def test_observe_invalid_inputs_ignored() -> None:
    det = CuSumDriftDetector(warmup=5)
    assert det.observe(schema="s", regime="r", p_hat=float("nan"), outcome=1) is False
    assert det.observe(schema="s", regime="r", p_hat="bad", outcome=1) is False  # type: ignore[arg-type]
    assert det.n_observed("s", "r") == 0


def test_ph_zero_before_any_data() -> None:
    det = CuSumDriftDetector()
    assert det.current_ph("x", "y") == 0.0
    assert det.current_ece("x", "y") == 0.0
    assert det.baseline_brier("x", "y") == -1.0
    assert det.n_observed("x", "y") == 0
    assert det.n_alarms("x", "y") == 0


def test_empty_snapshot() -> None:
    det = CuSumDriftDetector()
    assert det.snapshot() == []


def test_p_hat_clamped_to_01() -> None:
    det = CuSumDriftDetector(warmup=5)
    # These should not raise; p_hat is clamped
    det.observe(schema="s", regime="r", p_hat=1.5, outcome=1)
    det.observe(schema="s", regime="r", p_hat=-0.3, outcome=0)
    assert det.n_observed("s", "r") == 2
