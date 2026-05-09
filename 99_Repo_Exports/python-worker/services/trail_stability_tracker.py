from __future__ import annotations

"""
Trail Stability Tracker — monitors calibration parameter stability over multiple runs.

On each calibration run, appends a snapshot to a Redis list per symbol × regime.
Computes coefficient of variation (CV) of callback_atr_mult and trend of confidence
over the recorded history to determine if params are stable enough for enforce.

Key pattern: trail:stability:{symbol}:{regime}  (Redis list of JSON snapshots)
Fail-open: never raises.
"""
import json
import math
import os
from dataclasses import asdict, dataclass
from typing import Any

from common.log import setup_logger
from utils.time_utils import get_ny_time_millis

logger = setup_logger("TrailStabilityTracker")

EPS = 1e-9


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class StabilityConfig:
    enabled: bool
    min_runs: int           # minimum runs before declaring stable
    max_cv_pct: float       # maximum coefficient of variation (%) for callback
    max_history: int        # max snapshots to keep per bucket
    key_prefix: str
    ttl_sec: int

    @classmethod
    def from_env(cls) -> StabilityConfig:
        return cls(
            enabled=_env_bool("TRAIL_STABILITY_ENABLED", True),
            min_runs=int(os.getenv("TRAIL_STABILITY_MIN_RUNS", "6") or 6),
            max_cv_pct=float(os.getenv("TRAIL_STABILITY_MAX_CV_PCT", "15") or 15),
            max_history=int(os.getenv("TRAIL_STABILITY_MAX_HISTORY", "30") or 30),
            key_prefix=os.getenv("TRAIL_STABILITY_KEY_PREFIX", "trail:stability") or "trail:stability",
            ttl_sec=int(os.getenv("TRAIL_STABILITY_TTL_SEC", str(14 * 86400)) or 14 * 86400),
        )


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class RunSnapshot:
    """One calibration run snapshot."""
    run_ts_ms: int
    callback_atr_mult: float
    activate_offset_bps: float
    min_profit_lock_r: float
    confidence: float
    n_total: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> RunSnapshot:
        return cls(
            run_ts_ms=int(d.get("run_ts_ms", 0)),
            callback_atr_mult=float(d.get("callback_atr_mult", 0)),
            activate_offset_bps=float(d.get("activate_offset_bps", 0)),
            min_profit_lock_r=float(d.get("min_profit_lock_r", 0)),
            confidence=float(d.get("confidence", 0)),
            n_total=int(d.get("n_total", 0)),
        )


@dataclass
class StabilityReport:
    """Stability assessment for one symbol × regime."""
    symbol: str
    regime: str
    n_runs: int
    callback_cv_pct: float        # coefficient of variation %
    conf_trend: str               # "rising" | "falling" | "flat"
    is_stable: bool
    min_callback: float
    max_callback: float
    latest_confidence: float
    first_run_ts_ms: int
    latest_run_ts_ms: int
    days_observed: float          # calendar days between first and latest run

    def to_redis_mapping(self) -> dict[str, str]:
        return {k: str(v) for k, v in asdict(self).items()}


# ---------------------------------------------------------------------------
# Pure computation
# ---------------------------------------------------------------------------

def _cv_pct(values: list[float]) -> float:
    """Coefficient of variation in %, 0 if insufficient data."""
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    if abs(mean) < EPS:
        return 0.0
    var = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
    std = math.sqrt(var)
    return (std / abs(mean)) * 100.0


def _linear_trend(values: list[float]) -> str:
    """
    Simple linear regression trend: 'rising', 'falling', or 'flat'.
    values are ordered chronologically (oldest first).
    """
    n = len(values)
    if n < 3:
        return "flat"

    # Simple OLS: y = a + b*x, check sign of b
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n

    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    den = sum((i - x_mean) ** 2 for i in range(n))

    if abs(den) < EPS:
        return "flat"

    slope = num / den
    # Normalize slope by y_mean to get relative trend
    if abs(y_mean) < EPS:
        return "flat"

    rel_slope = slope / abs(y_mean)

    if rel_slope > 0.02:   # > 2% per run → rising
        return "rising"
    elif rel_slope < -0.02:  # < -2% per run → falling
        return "falling"
    return "flat"


def compute_stability(
    snapshots: list[RunSnapshot],
    min_runs: int,
    max_cv_pct: float,
    symbol: str,
    regime: str,
) -> StabilityReport:
    """Pure computation of stability from snapshots."""
    n = len(snapshots)

    if n == 0:
        return StabilityReport(
            symbol=symbol, regime=regime, n_runs=0,
            callback_cv_pct=0.0, conf_trend="flat", is_stable=False,
            min_callback=0.0, max_callback=0.0, latest_confidence=0.0,
            first_run_ts_ms=0, latest_run_ts_ms=0, days_observed=0.0,
        )

    # Sort by timestamp (oldest first)
    sorted_snaps = sorted(snapshots, key=lambda s: s.run_ts_ms)

    callbacks = [s.callback_atr_mult for s in sorted_snaps]
    confidences = [s.confidence for s in sorted_snaps]

    cv = _cv_pct(callbacks)
    trend = _linear_trend(confidences)

    first_ts = sorted_snaps[0].run_ts_ms
    latest_ts = sorted_snaps[-1].run_ts_ms
    days = (latest_ts - first_ts) / (86400 * 1000) if latest_ts > first_ts else 0.0

    is_stable = (
        n >= min_runs
        and cv <= max_cv_pct
        and trend != "falling"
    )

    return StabilityReport(
        symbol=symbol,
        regime=regime,
        n_runs=n,
        callback_cv_pct=round(cv, 2),
        conf_trend=trend,
        is_stable=is_stable,
        min_callback=round(min(callbacks), 6),
        max_callback=round(max(callbacks), 6),
        latest_confidence=round(sorted_snaps[-1].confidence, 4),
        first_run_ts_ms=first_ts,
        latest_run_ts_ms=latest_ts,
        days_observed=round(days, 2),
    )


# ---------------------------------------------------------------------------
# Tracker engine (with Redis I/O)
# ---------------------------------------------------------------------------

class TrailStabilityTracker:
    """
    Appends calibration snapshots to Redis lists, computes stability per bucket.
    """

    def __init__(self, redis_client: Any, *, cfg: StabilityConfig | None = None):
        self.redis = redis_client
        self.cfg = cfg or StabilityConfig.from_env()

    def record_and_assess(
        self,
        calibrated_params: list[Any],
    ) -> list[StabilityReport]:
        """
        Record a calibration run and assess stability.

        Args:
            calibrated_params: list of CalibratedTrailParams from calibrator.

        Returns:
            List of StabilityReport, one per symbol × regime.
        """
        if not self.cfg.enabled:
            logger.info("Stability tracker disabled (TRAIL_STABILITY_ENABLED=0)")
            return []

        results: list[StabilityReport] = []

        for p in calibrated_params:
            symbol = getattr(p, "symbol", "")
            regime = getattr(p, "regime", "na")
            if not symbol:
                continue

            snapshot = RunSnapshot(
                run_ts_ms=get_ny_time_millis(),
                callback_atr_mult=getattr(p, "callback_atr_mult", 0.0),
                activate_offset_bps=getattr(p, "activate_offset_bps", 0.0),
                min_profit_lock_r=getattr(p, "min_profit_lock_r", 0.0),
                confidence=getattr(p, "confidence", 0.0),
                n_total=getattr(p, "n_total", 0),
            )

            # Append snapshot to Redis list
            self._append_snapshot(symbol, regime, snapshot)

            # Read all snapshots and assess
            history = self._read_history(symbol, regime)
            report = compute_stability(
                history,
                min_runs=self.cfg.min_runs,
                max_cv_pct=self.cfg.max_cv_pct,
                symbol=symbol,
                regime=regime,
            )
            results.append(report)

        logger.info("Stability assessment complete: %d buckets", len(results))
        return results

    def _list_key(self, symbol: str, regime: str) -> str:
        return f"{self.cfg.key_prefix}:{symbol}:{regime}"

    def _append_snapshot(self, symbol: str, regime: str, snap: RunSnapshot) -> None:
        """Append snapshot to Redis list and trim to max_history."""
        if self.redis is None:
            return
        key = self._list_key(symbol, regime)
        try:
            self.redis.rpush(key, json.dumps(snap.to_dict(), separators=(",", ":")))
            self.redis.ltrim(key, -self.cfg.max_history, -1)
            if self.cfg.ttl_sec > 0:
                self.redis.expire(key, self.cfg.ttl_sec)
        except Exception as e:
            logger.error("Failed to append snapshot %s: %s", key, e)

    def _read_history(self, symbol: str, regime: str) -> list[RunSnapshot]:
        """Read all snapshots from Redis list."""
        if self.redis is None:
            return []
        key = self._list_key(symbol, regime)
        try:
            raw_list = self.redis.lrange(key, 0, -1)
            snapshots = []
            for raw in raw_list:
                try:
                    d = json.loads(raw)
                    snapshots.append(RunSnapshot.from_dict(d))
                except Exception:
                    continue
            return snapshots
        except Exception as e:
            logger.error("Failed to read history %s: %s", key, e)
            return []

    # ------------------------------------------------------------------
    # Telegram formatting
    # ------------------------------------------------------------------

    @staticmethod
    def format_telegram_report(reports: list[StabilityReport]) -> str:
        """Format stability assessment for Telegram."""
        if not reports:
            return ""

        lines = ["📏 <b>Stability Assessment</b>\n"]

        for r in sorted(reports, key=lambda x: x.symbol):
            if r.is_stable:
                emoji = "✅"
                status = "stable"
            elif r.n_runs < 6:
                emoji = "🔄"
                status = f"collecting ({r.n_runs}/6 runs)"
            else:
                emoji = "⚠️"
                status = "unstable"

            lines.append(
                f"  {r.symbol}: {status} {emoji} | "
                f"CV={r.callback_cv_pct:.1f}% conf={r.conf_trend} "
                f"n={r.n_runs} ({r.days_observed:.1f}d)"
            )

        # Summary
        stable = sum(1 for r in reports if r.is_stable)
        unstable = sum(1 for r in reports if not r.is_stable and r.n_runs >= 6)
        collecting = sum(1 for r in reports if r.n_runs < 6)

        verdict = "🟢 READY" if stable == len(reports) and stable > 0 else "🟡 NOT READY"
        lines.append(
            f"\n{verdict}: {stable}✅ stable, {collecting}🔄 collecting, {unstable}⚠️ unstable"
        )

        return "\n".join(lines)
