from __future__ import annotations

import pytest

from handlers.crypto_orderflow.core.crypto_orderflow_calibration import (
    ConfidenceCalibratorCfg,
    RollingPercentileCalibrator,
)


def test_rolling_percentile_calibrator_seeded_history_golden_percentile():
    """
    Golden test (deterministic):
      fixed abs_history -> fixed percentile.

    IMPORTANT:
      - мы не трогаем "боевую" реализацию RollingPercentileCalibrator,
        тест использует test-only seed_history_for_tests().
      - это ловит регрессы в wire-ABI (confidence_pct API) и в логике привязки пайплайна.
    """
    cfg = ConfidenceCalibratorCfg()
    cal = RollingPercentileCalibrator(cfg)
    assert hasattr(cal, "seed_history_for_tests")
    assert hasattr(cal, "confidence_pct")

    # abs_history = [1,2,3,4]
    cal.seed_history_for_tests(kind="breakout", symbol="BTCUSDT", abs_scores=[1, 2, 3, 4])

    # pct = 100 * (count <= abs(value)) / n
    assert cal.confidence_pct(kind="breakout", symbol="BTCUSDT", final_score=0.5, ts_ms=0) == pytest.approx(0.0)
    assert cal.confidence_pct(kind="breakout", symbol="BTCUSDT", final_score=1.0, ts_ms=0) == pytest.approx(25.0)
    assert cal.confidence_pct(kind="breakout", symbol="BTCUSDT", final_score=2.0, ts_ms=0) == pytest.approx(50.0)
    assert cal.confidence_pct(kind="breakout", symbol="BTCUSDT", final_score=3.0, ts_ms=0) == pytest.approx(75.0)
    assert cal.confidence_pct(kind="breakout", symbol="BTCUSDT", final_score=4.0, ts_ms=0) == pytest.approx(100.0)


def test_seed_is_isolated_by_symbol_and_kind():
    cfg = ConfidenceCalibratorCfg()
    cal = RollingPercentileCalibrator(cfg)
    cal.seed_history_for_tests(kind="breakout", symbol="BTCUSDT", abs_scores=[10])
    cal.seed_history_for_tests(kind="absorption", symbol="BTCUSDT", abs_scores=[1, 2])

    assert cal.confidence_pct(kind="breakout", symbol="BTCUSDT", final_score=10.0, ts_ms=0) == pytest.approx(100.0)
    assert cal.confidence_pct(kind="absorption", symbol="BTCUSDT", final_score=1.0, ts_ms=0) == pytest.approx(50.0)
