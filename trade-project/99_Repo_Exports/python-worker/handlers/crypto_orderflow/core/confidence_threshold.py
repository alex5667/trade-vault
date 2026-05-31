from __future__ import annotations

"""
Confidence threshold filter — per-symbol and per-cluster gates.

Design:
    - Static layer: per-symbol ENV overrides (MIN_CONF_BTCUSDT etc.)
    - Dynamic layer (opt-in): ConfidenceThresholdCalibrator reads reliability_calibrator
      Redis curves and inverts them to dynamic per-cluster min_conf thresholds.
    - Both checks (confidence_pct AND conf_factor) must pass.
    - Fail-closed on missing data (0.0 is rejected).

ENV Configuration:
    MIN_CONF_DEFAULT:         default min confidence score (e.g. 50)
    MIN_CONF_BTCUSDT:         BTC-specific threshold (e.g. 75)
    MIN_CONF_FACTOR_DEFAULT:  default min confidence factor 0-1 (e.g. 0.45)
    MIN_CONF_FACTOR_BTCUSDT:  BTC-specific factor (e.g. 0.55)
    CONF_CAL_ENFORCE:         1 → enable calibrated thresholds (default 0 = shadow)

Usage (static only):
    f = ConfidenceThresholdFilter.from_env()
    r = f.evaluate(confidence_pct=72.0, conf_factor=0.48, symbol="BTCUSDT")

Usage (with calibrator):
    from core.confidence_threshold_calibrator import ConfidenceThresholdCalibrator
    cal = ConfidenceThresholdCalibrator(redis_client=sync_redis, enforce=False)
    f = ConfidenceThresholdFilter.from_env(calibrator=cal)
    r = f.evaluate(confidence_pct=72.0, conf_factor=0.48, symbol="BTCUSDT",
                   kind="breakout", regime="trend", session="us", venue="binance", tf="5m")
"""

import math
import os
from dataclasses import dataclass
from typing import Any
import contextlib


@dataclass
class ConfidenceThresholdConfig:
    """Static (ENV-driven) configuration for the filter."""

    min_conf_default: float = 70.0          # Absolute confidence (0-100)
    min_conf_factor_default: float = 0.45   # Confidence factor (0-1)

    min_conf_by_symbol: dict[str, float] = None     # type: ignore
    min_conf_factor_by_symbol: dict[str, float] = None  # type: ignore

    def __post_init__(self) -> None:
        if self.min_conf_by_symbol is None:
            self.min_conf_by_symbol = {}
        if self.min_conf_factor_by_symbol is None:
            self.min_conf_factor_by_symbol = {}

    @classmethod
    def from_env(cls) -> ConfidenceThresholdConfig:
        min_conf_default = float(os.getenv("MIN_CONF_DEFAULT", "50.0"))
        min_conf_factor_default = float(os.getenv("MIN_CONF_FACTOR_DEFAULT", "0.45"))

        min_conf_by_symbol: dict[str, float] = {}
        for key, value in os.environ.items():
            if key.startswith("MIN_CONF_") and not key.startswith("MIN_CONF_FACTOR_") and key != "MIN_CONF_DEFAULT":
                symbol = key.replace("MIN_CONF_", "")
                with contextlib.suppress(ValueError, TypeError):
                    min_conf_by_symbol[symbol] = float(value)

        min_conf_factor_by_symbol: dict[str, float] = {}
        for key, value in os.environ.items():
            if key.startswith("MIN_CONF_FACTOR_") and key != "MIN_CONF_FACTOR_DEFAULT":
                symbol = key.replace("MIN_CONF_FACTOR_", "")
                with contextlib.suppress(ValueError, TypeError):
                    min_conf_factor_by_symbol[symbol] = float(value)

        return cls(
            min_conf_default=min_conf_default,
            min_conf_factor_default=min_conf_factor_default,
            min_conf_by_symbol=min_conf_by_symbol,
            min_conf_factor_by_symbol=min_conf_factor_by_symbol,
        )


@dataclass
class ConfidenceThresholdResult:
    """Evaluation result from ConfidenceThresholdFilter."""

    passed: bool

    confidence_pct: float       # actual confidence (0-100)
    conf_factor: float          # actual factor (0-1)

    min_conf_threshold: float   # threshold applied (0-100)
    min_conf_factor_threshold: float  # factor threshold applied (0-1)

    conf_pct_passed: bool
    conf_factor_passed: bool

    symbol: str
    calibrated: bool = False    # True if min_conf_threshold came from calibrator
    veto_reason: str | None = None


class ConfidenceThresholdFilter:
    """
    Confidence threshold gate supporting both static (ENV) and dynamic
    (calibrator-driven) thresholds.

    Static layer: per-symbol ENV overrides (always active).
    Dynamic layer: ConfidenceThresholdCalibrator reads reliability_calibrator
      Redis curves → inverts → per-cluster min_conf (active when calibrator.enforce=True).

    Priority: calibrated > symbol-override > default.
    """

    def __init__(
        self,
        config: ConfidenceThresholdConfig,
        calibrator: Any = None,
    ) -> None:
        self.config = config
        self._calibrator = calibrator  # optional ConfidenceThresholdCalibrator

    @classmethod
    def from_env(cls, calibrator: Any = None) -> ConfidenceThresholdFilter:
        return cls(ConfidenceThresholdConfig.from_env(), calibrator=calibrator)

    # ── threshold resolution ───────────────────────────────────────────────────

    def _get_min_conf_pct(
        self,
        symbol: str,
        *,
        kind: str = "na",
        venue: str = "na",
        session: str = "na",
        tf: str = "na",
        regime: str = "na",
    ) -> tuple[float, bool]:
        """
        Returns (threshold, calibrated_flag).
        calibrated_flag=True when the calibrator provided a committed value.
        """
        if self._calibrator is not None and getattr(self._calibrator, "enforce", False):
            try:
                dyn = self._calibrator.min_conf_for(
                    symbol=symbol, kind=kind, venue=venue,
                    session=session, tf=tf, regime=regime,
                )
                if dyn > 0.0:
                    return dyn, True
            except Exception:
                pass

        # Static fallback: per-symbol ENV override → default
        return self.config.min_conf_by_symbol.get(symbol, self.config.min_conf_default), False

    def _get_min_conf_factor(self, symbol: str) -> float:
        return self.config.min_conf_factor_by_symbol.get(
            symbol, self.config.min_conf_factor_default,
        )

    # ── public evaluate ────────────────────────────────────────────────────────

    def evaluate(
        self,
        confidence_pct: float | None,
        conf_factor: float | None,
        symbol: str,
        *,
        kind: str = "na",
        venue: str = "na",
        session: str = "na",
        tf: str = "na",
        regime: str = "na",
    ) -> ConfidenceThresholdResult:
        """
        Evaluate signal against confidence thresholds.

        Args:
            confidence_pct: Signal confidence (0-100), None → fail-closed.
            conf_factor:    Confidence factor (0-1), None → fail-closed.
            symbol:         Trading symbol (e.g. "BTCUSDT").
            kind:           Signal kind for cluster-aware calibration.
            venue/session/tf/regime: Additional cluster dims for calibration.

        Returns:
            ConfidenceThresholdResult — passed=True only when BOTH checks pass.
        """
        min_conf_pct, calibrated = self._get_min_conf_pct(
            symbol, kind=kind, venue=venue, session=session, tf=tf, regime=regime,
        )
        min_conf_factor = self._get_min_conf_factor(symbol)

        conf_pct = _safe_float(confidence_pct, 0.0)
        conf_fac = _safe_float(conf_factor, 0.0)

        conf_pct_passed = conf_pct >= min_conf_pct
        conf_factor_passed = conf_fac >= min_conf_factor
        passed = conf_pct_passed and conf_factor_passed

        veto_reason: str | None = None
        if not passed:
            failures = []
            if not conf_pct_passed:
                src = "cal" if calibrated else "env"
                failures.append(f"confidence={conf_pct:.1f} < min={min_conf_pct:.1f}[{src}]")
            if not conf_factor_passed:
                failures.append(f"conf_factor={conf_fac:.3f} < min={min_conf_factor:.3f}")
            veto_reason = "; ".join(failures)

        return ConfidenceThresholdResult(
            passed=passed,
            confidence_pct=conf_pct,
            conf_factor=conf_fac,
            min_conf_threshold=min_conf_pct,
            min_conf_factor_threshold=min_conf_factor,
            conf_pct_passed=conf_pct_passed,
            conf_factor_passed=conf_factor_passed,
            symbol=symbol,
            calibrated=calibrated,
            veto_reason=veto_reason,
        )


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        f = float(val) if val is not None else default
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


# Backward-compat alias for callers that imported safe_float from this module
safe_float = _safe_float
