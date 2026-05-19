from __future__ import annotations

from dataclasses import dataclass

import pytest

from handlers.crypto_orderflow.core.crypto_orderflow_calibration import (
    ConfidenceCalibratorCfg,
    RollingPercentileCalibrator,
)
from signals.unified_pipeline import UnifiedSignalPipeline


class _Dummy:
    pass


class _Publisher:
    def __init__(self):
        self.last_payload = None

    def build_payload(self, ctx=None, scoring_result=None, result=None):
        # минимальный "wire" payload; pipeline вызывает build_payload(ctx=..., result=...)
        sr = result if result is not None else scoring_result
        return {
            "kind": "breakout",
            "symbol": getattr(ctx, "symbol", ""),
            "ts": getattr(ctx, "ts", 0),
            "final_score": getattr(sr, "final_score", 0.0),
        }

    def publish(self, payload):
        self.last_payload = payload


@dataclass
class _ScoringResult:
    final_score: float
    should_emit: bool = True


class _ScoringEngine:
    def __init__(self, final_score: float):
        self._fs = final_score
    def score(self, ctx):
        return _ScoringResult(final_score=self._fs)


def test_unified_pipeline_uses_same_confidence_calibrator_scale():
    cal = RollingPercentileCalibrator(ConfidenceCalibratorCfg(min_history=4))
    cal.seed_history_for_tests(kind="breakout", symbol="BTCUSDT", abs_scores=[1, 2, 3, 4])

    publisher = _Publisher()
    pipe = UnifiedSignalPipeline(
        scoring_engine=_ScoringEngine(final_score=3.0),
        regime_service=_Dummy(),
        golden_logic=_Dummy(),
        exec_filters=_Dummy(),
        publisher=publisher,
        calibrator=None,
        confidence_calibrator=cal,
        confidence_cap_pct=95.0,
    )

    ctx = _Dummy()
    ctx.symbol = "BTCUSDT"
    ctx.ts = 1700000000000

    # process() должен проставить confidence в payload (0..100)
    pipe.process(ctx)
    assert publisher.last_payload is not None
    assert publisher.last_payload["confidence"] == pytest.approx(75.0)
