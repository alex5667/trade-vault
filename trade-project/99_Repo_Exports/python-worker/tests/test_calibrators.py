from __future__ import annotations

import json
import random

from core.book_rate_calibrator import BookRateCalibrator
from core.delta_notional_calibrator import DeltaNotionalCalibrator


def test_book_rate_calibrator_ready_and_persist_roundtrip():
    c = BookRateCalibrator(min_samples=30)
    # regime=na, simulate 100ms snapshots => 10Hz, but add jitter up to 50Hz
    for _ in range(60):
        c.update(regime="range", book_rate_hz=10.0 + random.random() * 40.0)
    th = c.thresholds(regime="range", default_min_hz=5.0, default_warn_hz=10.0)
    assert th.n >= 30
    assert th.src.startswith("calib")
    assert th.min_hz > 0
    raw = json.dumps(c.dump_regime_state(symbol="TEST", regime="range", updated_ts_ms=1000))
    c2 = BookRateCalibrator(min_samples=30)
    c2.load_regime_state(json.loads(raw))
    th2 = c2.thresholds(regime="range", default_min_hz=5.0, default_warn_hz=10.0)
    assert abs(th2.min_hz - th.min_hz) < 1e-6


def test_delta_notional_calibrator_tiers():
    c = DeltaNotionalCalibrator(min_samples=50)
    # synthetic: lognormal-ish notional
    xs = []
    for _ in range(200):
        v = max(1.0, random.random()) * 1_000_000.0
        xs.append(v)
        c.update(regime="trend", delta_notional_usd=v)
    th = c.thresholds(regime="trend", default_tier0_usd=1.0, default_tier1_usd=2.0, default_tier2_usd=3.0)
    assert th.src.startswith("calib")
    assert th.tier0_usd > 0
    assert th.tier1_usd >= th.tier0_usd
    assert th.tier2_usd >= th.tier1_usd
