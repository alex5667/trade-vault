from __future__ import annotations

"""
Trail Calibrator — computes optimal trailing params per symbol × regime.

Reads post-analysis data from trail:analysis:{symbol}:{regime} (written by TrailPostAnalyzer)
and computes calibrated trailing parameters:
  - callback_atr_mult: optimal ATR multiplier for callback
  - activate_offset_bps: optimal activation offset
  - min_profit_lock_r: minimum locked profit in R

Writes to: trail:calib:{symbol}:{regime}
Mode: shadow (log only) or enforce (executor reads and uses).

Fail-open: never raises.
"""
import os
from dataclasses import asdict, dataclass
from typing import Any

from common.log import setup_logger
from utils.time_utils import get_ny_time_millis

logger = setup_logger("TrailCalibrator")

EPS = 1e-9


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _sf(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(str(v).strip())
    except Exception:
        return default


def _si(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(float(str(v).strip()))
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TrailCalibratorConfig:
    enabled: bool
    mode: str               # "shadow" | "enforce"
    safety_mult: float      # multiplier for P75(MAE) → callback
    max_change_pct: float   # max ±% change from current
    min_confidence: float   # minimum confidence to calibrate
    min_n: int              # minimum trades in bucket
    ttl_sec: int            # Redis TTL
    key_prefix: str
    analysis_key_prefix: str

    @classmethod
    def from_env(cls) -> TrailCalibratorConfig:
        return cls(
            enabled=_env_bool("TRAIL_CALIB_ENABLED", True),
            mode=os.getenv("TRAIL_CALIB_MODE", "shadow") or "shadow",
            safety_mult=float(os.getenv("TRAIL_CALIB_SAFETY_MULT", "1.2") or 1.2),
            max_change_pct=float(os.getenv("TRAIL_CALIB_MAX_CHANGE_PCT", "30") or 30),
            min_confidence=float(os.getenv("TRAIL_CALIB_MIN_CONFIDENCE", "0.55") or 0.55),
            min_n=_si(os.getenv("TRAIL_ANALYZER_MIN_N", "50"), 50),
            ttl_sec=_si(os.getenv("TRAIL_CALIB_TTL_SEC", str(48 * 3600)), 48 * 3600),
            key_prefix=os.getenv("TRAIL_CALIB_KEY_PREFIX", "trail:calib") or "trail:calib",
            analysis_key_prefix=os.getenv("TRAIL_ANALYZER_KEY_PREFIX", "trail:analysis") or "trail:analysis",
        )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

@dataclass
class CalibratedTrailParams:
    """Calibrated trailing parameters for one symbol × regime."""
    symbol: str
    regime: str
    callback_atr_mult: float    # optimal ATR multiplier for callback
    activate_offset_bps: float  # optimal activation offset BPS
    min_profit_lock_r: float    # minimum locked profit in R
    confidence: float
    mode: str                   # "shadow" | "enforce"
    n_total: int
    computed_at_ms: int
    previous_callback_atr_mult: float  # for delta tracking

    def to_redis_mapping(self) -> dict[str, str]:
        d: dict[str, str] = {}
        for k, v in asdict(self).items():
            d[k] = str(v)
        return d


# ---------------------------------------------------------------------------
# Calibrator engine
# ---------------------------------------------------------------------------

class TrailCalibrator:
    """
    Reads trail:analysis:{symbol}:{regime} hashes,
    computes optimal trailing params,
    writes to trail:calib:{symbol}:{regime}.
    """

    # Default ATR in BPS for major symbols (fallback)
    DEFAULT_ATR_BPS: dict[str, float] = {
        "BTCUSDT": 30.0,
        "ETHUSDT": 45.0,
        "SOLUSDT": 80.0,
        "XRPUSDT": 60.0,
        "BNBUSDT": 40.0,
        "DOGEUSDT": 100.0,
    }
    DEFAULT_ATR_BPS_FALLBACK = 50.0

    def __init__(self, redis_client: Any, *, cfg: TrailCalibratorConfig | None = None):
        self.redis = redis_client
        self.cfg = cfg or TrailCalibratorConfig.from_env()

    def run(self, symbols: list[str] | None = None) -> list[CalibratedTrailParams]:
        """Main entry point. Returns list of calibrated params."""
        if not self.cfg.enabled:
            logger.info("Trail calibrator disabled (TRAIL_CALIB_ENABLED=0)")
            return []

        if self.redis is None:
            return []

        # Find all analysis keys
        analysis_keys = self._scan_analysis_keys(symbols)
        if not analysis_keys:
            logger.info("No trail:analysis:* keys found — post-analyzer must run first")
            return []

        results: list[CalibratedTrailParams] = []
        for key in analysis_keys:
            params = self._calibrate_from_analysis(key)
            if params:
                self._write_to_redis(params)
                results.append(params)

        logger.info(
            "Trail calibration complete: %d params written (mode=%s)",
            len(results), self.cfg.mode,
        )
        return results

    def _scan_analysis_keys(self, symbols: list[str] | None = None) -> list[str]:
        """Scan Redis for trail:analysis:* keys."""
        try:
            pattern = f"{self.cfg.analysis_key_prefix}:*"
            keys = []
            cursor = 0
            while True:
                cursor, batch = self.redis.scan(cursor=cursor, match=pattern, count=10000)
                keys.extend(batch)
                if cursor == 0:
                    break

            if symbols:
                syms = {s.upper() for s in symbols}
                keys = [k for k in keys if any(f":{s}:" in k for s in syms)]

            return sorted(keys)
        except Exception as e:
            logger.error("Failed to scan analysis keys: %s", e)
            return []

    def _calibrate_from_analysis(self, analysis_key: str) -> CalibratedTrailParams | None:
        """Compute calibrated params from one analysis bucket."""
        try:
            h = self.redis.hgetall(analysis_key)
            if not h:
                return None

            # Parse analysis data
            symbol = (h.get("symbol", ""))
            regime = (h.get("regime", "na"))
            n_total = _si(h.get("n_total"), 0)
            confidence = _sf(h.get("confidence"), 0.0)
            optimal_callback_bps = _sf(h.get("optimal_callback_bps"), 0.0)
            mfe_p25_r = _sf(h.get("mfe_p25_r"), 0.0)
            trail_hit_rate = _sf(h.get("trail_hit_rate"), 0.0)

            if not symbol or n_total < self.cfg.min_n:
                return None
            if confidence < self.cfg.min_confidence:
                logger.debug(
                    "Skipping %s:%s — confidence %.3f < min %.3f",
                    symbol, regime, confidence, self.cfg.min_confidence,
                )
                return None

            # Compute callback_atr_mult
            atr_bps = self.DEFAULT_ATR_BPS.get(symbol, self.DEFAULT_ATR_BPS_FALLBACK)
            if optimal_callback_bps > EPS and atr_bps > EPS:
                callback_atr_mult = optimal_callback_bps / atr_bps
            else:
                callback_atr_mult = 1.0  # default

            # Clamp to reasonable range
            callback_atr_mult = max(0.3, min(3.0, callback_atr_mult))

            # Compute activate_offset_bps
            # Use P25(MFE) × 0.3 as activation threshold
            activate_offset_bps = mfe_p25_r * atr_bps * 0.3
            activate_offset_bps = max(2.0, min(50.0, activate_offset_bps))

            # Compute min_profit_lock_r
            # Lock at least P25(MFE) × 0.2
            min_profit_lock_r = mfe_p25_r * 0.2
            min_profit_lock_r = max(0.05, min(0.5, min_profit_lock_r))

            # Hysteresis: check previous value and limit change
            prev_callback = self._read_previous_callback(symbol, regime)
            if prev_callback > EPS and self.cfg.max_change_pct > 0:
                max_delta = prev_callback * self.cfg.max_change_pct / 100.0
                callback_atr_mult = max(
                    prev_callback - max_delta,
                    min(prev_callback + max_delta, callback_atr_mult),
                )

            return CalibratedTrailParams(
                symbol=symbol,
                regime=regime,
                callback_atr_mult=round(callback_atr_mult, 6),
                activate_offset_bps=round(activate_offset_bps, 4),
                min_profit_lock_r=round(min_profit_lock_r, 6),
                confidence=round(confidence, 4),
                mode=self.cfg.mode,
                n_total=n_total,
                computed_at_ms=get_ny_time_millis(),
                previous_callback_atr_mult=round(prev_callback, 6),
            )

        except Exception as e:
            logger.error("Failed to calibrate from %s: %s", analysis_key, e)
            return None

    def _read_previous_callback(self, symbol: str, regime: str) -> float:
        """Read previous calibrated callback_atr_mult for hysteresis."""
        key = f"{self.cfg.key_prefix}:{symbol}:{regime}"
        try:
            val = self.redis.hget(key, "callback_atr_mult")
            if val:
                return _sf(val)
        except Exception:
            pass
        return 0.0

    def _write_to_redis(self, params: CalibratedTrailParams) -> None:
        """Write calibrated params to Redis."""
        key = f"{self.cfg.key_prefix}:{params.symbol}:{params.regime}"
        try:
            pipe = self.redis.pipeline(transaction=False)
            pipe.hset(key, mapping=params.to_redis_mapping())
            if self.cfg.ttl_sec > 0:
                pipe.expire(key, self.cfg.ttl_sec)
            pipe.execute()
            logger.info(
                "Wrote %s: callback=%.3f, offset=%.1fbps, lock=%.3fR (mode=%s, conf=%.3f, prev=%.3f)",
                key, params.callback_atr_mult, params.activate_offset_bps,
                params.min_profit_lock_r, params.mode, params.confidence,
                params.previous_callback_atr_mult,
            )
        except Exception as e:
            logger.error("Failed to write %s: %s", key, e)

    # ------------------------------------------------------------------
    # Telegram formatting
    # ------------------------------------------------------------------

    @staticmethod
    def format_telegram_report(params_list: list[CalibratedTrailParams]) -> str:
        """Format calibrated params for Telegram."""
        if not params_list:
            return "🔧 <b>Trail Calibrator</b>\n\nNo calibrations performed."

        lines = [f"🔧 <b>Trail Calibrator</b> (mode={params_list[0].mode})\n"]

        by_sym: dict[str, list[CalibratedTrailParams]] = {}
        for p in params_list:
            by_sym.setdefault(p.symbol, []).append(p)

        for sym in sorted(by_sym.keys()):
            lines.append(f"\n<b>{sym}</b>:")
            for p in sorted(by_sym[sym], key=lambda x: x.regime):
                delta = ""
                if p.previous_callback_atr_mult > EPS:
                    pct = ((p.callback_atr_mult - p.previous_callback_atr_mult) / p.previous_callback_atr_mult) * 100
                    delta = f" (Δ{pct:+.1f}%)"
                lines.append(
                    f"  {p.regime}: cb={p.callback_atr_mult:.3f}×ATR{delta}, "
                    f"offset={p.activate_offset_bps:.1f}bps, lock={p.min_profit_lock_r:.3f}R | "
                    f"conf={p.confidence:.2f} n={p.n_total}"
                )

        return "\n".join(lines)
