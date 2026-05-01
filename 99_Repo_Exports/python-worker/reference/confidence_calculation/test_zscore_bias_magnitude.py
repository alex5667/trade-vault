import math

import pytest


def _import_detector():
    # Support multiple repo layouts
    try:
        from core.crypto_orderflow_detectors import DeltaSpikeDetector
        return DeltaSpikeDetector
    except Exception:
        from crypto_orderflow_detectors import DeltaSpikeDetector
        return DeltaSpikeDetector


DeltaSpikeDetector = _import_detector()


def _std_eff(values):
    """
    Mirror the detector's std_eff logic:
    std_eff = max(std_dev(values), std_floor) where std_floor = max(1e-6, 0.10 * mean_abs(values))
    """
    n = len(values)
    assert n > 0
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / n
    std_dev = math.sqrt(var) if var > 0 else 0.0
    mean_abs = sum(abs(v) for v in values) / n
    std_floor = max(1e-6, 0.10 * mean_abs)
    return mean, max(std_dev, std_floor)


def test_self_inclusion_bias_is_in_10_20_percent_typical_case():
    """
    Deterministic "typical" case that produces ~13.4% bias:
      prev window: N=30 samples alternating +1/-1 => mean=0, std=1 (std_floor=0.1 inactive)
      outlier: +3

    We quantify the bias if current value were incorrectly included into stats before computing z:
      bias_fraction = 1 - |z_incl| / |z_prev|
    Expect: 10%..20%
    """
    N = 30
    prev = [1.0 if i % 2 == 0 else -1.0 for i in range(N)]
    x = 3.0

    mean_prev, std_prev = _std_eff(prev)
    z_prev = (x - mean_prev) / std_prev

    incl = prev + [x]
    mean_incl, std_incl = _std_eff(incl)
    z_incl = (x - mean_incl) / std_incl

    assert z_prev == pytest.approx(3.0, abs=1e-12)  # sanity: std=1, mean=0
    assert abs(z_incl) < abs(z_prev)                # self-inclusion must reduce |z|

    bias_fraction = 1.0 - (abs(z_incl) / abs(z_prev))
    bias_pct = bias_fraction * 100.0

    # Bias in this deterministic configuration should land inside 10..20%
    assert 10.0 <= bias_pct <= 20.0

    # And it is ~13.4% (stable number) — keep it as a tight regression check
    assert bias_pct == pytest.approx(13.39745962155614, rel=1e-12, abs=1e-12)


def test_detector_emits_unbiased_z_on_same_case():
    """
    Confirms the detector outputs z computed on the previous window (unbiased),
    not the self-included version.
    """
    det = DeltaSpikeDetector(window=64, z_threshold=2.0, min_abs_volume=0.0)

    # Fill with N=30 alternating +1/-1
    for i in range(30):
        # is_sell=(i%2==1) => is_buyer_maker=True if i%2==1 else False
        tick = {"qty": 1.0, "is_buyer_maker": (i % 2 == 1), "ts_ms": 1000 + i}
        det.push(tick)

    prev = list(det.values)
    assert len(prev) >= 10

    # Outlier +3 (BUY) => is_buyer_maker=False
    ev = det.push({"qty": 3.0, "is_buyer_maker": False, "ts_ms": 999999})
    assert ev is not None

    mean_prev, std_prev = _std_eff(prev)
    z_expected = (3.0 - mean_prev) / std_prev
    assert ev["z"] == pytest.approx(z_expected, rel=1e-12, abs=1e-12)

    # Self-included z would be smaller; ensure we didn't accidentally return it
    mean_incl, std_incl = _std_eff(prev + [3.0])
    z_incl = (3.0 - mean_incl) / std_incl
    assert abs(ev["z"]) > abs(z_incl)
