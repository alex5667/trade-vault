from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import os
import math


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else (hi if x > hi else x)


def _isfinite(x: float) -> bool:
    return bool(math.isfinite(x))


@dataclass(frozen=True)
class ScoreResult:
    final_score: float
    conf_factor01: float
    confidence_pct: float
    parts: dict[str, float]


class ScoreModel:
    """
    Единая ось:
      final_score = raw_score * conf_factor01
      confidence_pct = calibration(|final_score|) в [0..100]
    """

    def __init__(self) -> None:
        # чем больше scale, тем "быстрее" confidence выходит к 100
        self.scale = float(os.getenv("CONF_CALIB_SCALE", "3.5"))
        self.max_pct = float(os.getenv("CONF_CALIB_MAX_PCT", "98.0"))
        self.min_pct = float(os.getenv("CONF_CALIB_MIN_PCT", "1.0"))

    def score(self, *, raw_score: float, conf_factor01: float) -> ScoreResult:
        rs = float(raw_score) if _isfinite(float(raw_score)) else 0.0
        cf = float(conf_factor01) if _isfinite(float(conf_factor01)) else 0.0
        cf = _clamp(cf, 0.0, 1.0)
        final = rs * cf

        # простая калибровка: logistic по модулю final_score
        x = abs(final) * max(self.scale, 1e-9)
        # sigmoid
        pct = 100.0 * (1.0 / (1.0 + math.exp(-x)))
        pct = _clamp(pct, self.min_pct, self.max_pct) if cf > 0 else 0.0

        return ScoreResult(final_score=float(final), conf_factor01=cf, confidence_pct=float(pct), parts={"x": float(x)})
