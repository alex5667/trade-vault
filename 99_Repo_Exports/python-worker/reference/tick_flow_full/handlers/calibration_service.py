# calibration_service.py
from __future__ import annotations
"""
Calibration functionality extracted from base_orderflow_handler.py
"""

from utils.time_utils import get_ny_time_millis

import json
import math
import os
import time
from typing import Optional, Dict, Any, TYPE_CHECKING, Callable, Tuple

# from common.log import setup_logger
def setup_logger(name):
    import logging
    return logging.getLogger(name)

if TYPE_CHECKING:
    from contexts import CoreSignalContext


class CalibrationService:
    """
    Service for metric calibration and local calibration.
    """

    # default thresholds when cfg is missing (metric-specific)
    # mode: "abs" | "gt" | "lt"
    _DEFAULT_METRIC_DEFAULTS: Dict[str, Tuple[str, float]] = {
        "deltaSpike_z": ("abs", 2.0),
        "obi": ("abs", 0.8),                 # if OBI is normalized [-1..1]
        "absorption_score": ("gt", 0.15),    # tune per your scale
        "liquidity_score": ("gt", 0.15),     # tune per your scale
        "atr_quantile": ("gt", 0.90),        # extreme vol regime
    }

    # Stable contract keys (always present in ctx.calibrated[metric])
    _CAL_KEYS = ("value", "mode", "is_extreme", "threshold", "quantile", "q90", "q95", "q98")

    def __init__(
        self,
        symbol: str,
        local_calibration: Any = None,
        redis_client: Any = None,
        config_manager: Any = None,
        *,
        quantile_fn: Optional[Callable[[Any, float], float]] = None,
        cfg_cache_ttl_ms: Optional[int] = None,
    ):
        self.symbol = symbol
        self.local_calibration = local_calibration
        self.redis = redis_client
        self.config_manager = config_manager
        self.logger = setup_logger(f"CalibrationService:{symbol}")
        self._quantile_fn = quantile_fn

        # cache for cfg lookups (hot path)
        # key -> (cfg | None, expires_at_ms)
        self._cfg_cache: Dict[Tuple[str, str, str, str], Tuple[Any, int]] = {}
        if cfg_cache_ttl_ms is None:
            cfg_cache_ttl_ms = int(os.getenv("CALIBRATION_CFG_CACHE_TTL_MS", "300000"))  # 5m
        self._cfg_cache_ttl_ms = int(cfg_cache_ttl_ms)

        # warn throttling (avoid log spam)
        self._last_quantile_warn_ms = 0

    def invalidate_cfg_cache(self) -> None:
        """Invalidate metric cfg cache (call on reload of calibration/config)."""
        self._cfg_cache.clear()

    def _get_metric_cfg(self, sym: str, session: str, regime: str, metric_name: str) -> Any:
        """Small TTL cache for local_calibration.get_metric_cfg(). Caches None too (with TTL)."""
        if self.local_calibration is None:
            return None

        key = (sym, session, regime, metric_name)
        ttl = int(self._cfg_cache_ttl_ms)
        if ttl <= 0:
            return self.local_calibration.get_metric_cfg(sym, session, regime, metric_name)

        now_ms = get_ny_time_millis()
        hit = self._cfg_cache.get(key)
        if hit is not None:
            cfg, exp_ms = hit
            if now_ms < int(exp_ms):
                return cfg

        cfg = self.local_calibration.get_metric_cfg(sym, session, regime, metric_name)
        self._cfg_cache[key] = (cfg, now_ms + ttl)
        return cfg

    def _cal_payload(
        self,
        *,
        value: Any,
        mode: str,
        is_extreme: bool,
        threshold: float,
        quantile: Optional[float],
        q90: Any,
        q95: Any,
        q98: Any,
    ) -> Dict[str, Any]:
        """Stable calibrated payload (always the same keys)."""
        out = {
            "value": value,
            "mode": mode,
            "is_extreme": bool(is_extreme),
            "threshold": float(threshold),
            "quantile": quantile,
            "q90": q90,
            "q95": q95,
            "q98": q98,
        }
        # hard guarantee of stable keys (defensive)
        for k in self._CAL_KEYS:
            out.setdefault(k, None)
        return out

    def _apply_metric_calibration(
        self,
        ctx: "CoreSignalContext",
        metric_name: str,
        *,
        default_extreme_z: float = 2.0,
    ) -> None:
        """
        Calibrate a single metric using local calibration data.
        Falls back to defaults if no local calibration available.
        """

        # Ensure dicts exist
        if getattr(ctx, "metrics", None) is None:
            ctx.metrics = {}
        if getattr(ctx, "calibrated", None) is None:
            ctx.calibrated = {}

        raw_value = ctx.metrics.get(metric_name)
        if raw_value is None:
            return

        # Flags/bools are NOT numeric-calibrated
        if isinstance(raw_value, bool):
            ctx.calibrated[metric_name] = self._cal_payload(
                value=bool(raw_value),
                mode="flag",
                is_extreme=bool(raw_value),
                threshold=1.0,
                quantile=None,
                q90=None, q95=None, q98=None,
            )
            return

        # Try numeric conversion
        try:
            v = float(raw_value)
        except (TypeError, ValueError):
            return
        if not math.isfinite(v):
            return

        # Get calibration config for this metric
        sym = str(getattr(ctx, "symbol", self.symbol) or self.symbol)
        session = str(getattr(ctx, "session", None) or "mixed")
        regime = str(getattr(ctx, "regime_label", None) or "mixed")

        cfg = self._get_metric_cfg(sym, session, regime, metric_name)

        # Decide compare mode/threshold
        if cfg:
            mode = str(getattr(cfg, "compare", None) or getattr(cfg, "mode", None) or "abs").lower()
            try:
                threshold = float(getattr(cfg, "threshold", default_extreme_z))
            except (TypeError, ValueError):
                threshold = default_extreme_z
            if not math.isfinite(threshold):
                threshold = default_extreme_z
        else:
            mode, threshold = self._DEFAULT_METRIC_DEFAULTS.get(metric_name, ("abs", default_extreme_z))

        if mode not in ("abs", "gt", "lt"):
            mode = "abs"
        if mode == "abs" and threshold < 0:
            threshold = abs(threshold)

        # Quantile (optional, only if we have a callable + points)
        quantile = None
        cdf_points = getattr(cfg, "cdf_points", None) if cfg else None
        if cfg and cdf_points is not None:
            qfn = self._quantile_fn
            if qfn is None and hasattr(self.local_calibration, "eval_local_quantile"):
                qfn = getattr(self.local_calibration, "eval_local_quantile")
            if qfn is not None:
                try:
                    quantile = float(qfn(cdf_points, v))
                    if quantile != quantile:  # NaN
                        quantile = None
                    elif quantile < 0.0:
                        quantile = 0.0
                    elif quantile > 1.0:
                        quantile = 1.0
                except Exception:
                    quantile = None
            else:
                now_ms = get_ny_time_millis()
                if now_ms - self._last_quantile_warn_ms > 60_000:
                    self._last_quantile_warn_ms = now_ms
                    self.logger.warning(
                        "Quantile function missing; metric=%s will be calibrated without quantile",
                        metric_name,
                    )

        # Extreme logic
        if mode == "gt":
            is_extreme = v >= threshold
        elif mode == "lt":
            is_extreme = v <= threshold
        else:
            is_extreme = abs(v) >= threshold

        # Store calibrated results
        ctx.calibrated[metric_name] = self._cal_payload(
            value=v,
            mode=mode,
            is_extreme=is_extreme,
            threshold=threshold,
            quantile=quantile,
            q90=getattr(cfg, "q90", None) if cfg else None,
            q95=getattr(cfg, "q95", None) if cfg else None,
            q98=getattr(cfg, "q98", None) if cfg else None,
        )

    def _apply_local_calibration(self, ctx: "CoreSignalContext") -> None:
        """
        Apply local calibration to key metrics.
        Falls back to defaults if no local calibration available.
        """

        # Numeric metrics
        metrics_to_calibrate = [
            "deltaSpike_z",
            "obi",
            "absorption_score",
            "liquidity_score",
            "atr_quantile",
        ]

        for metric_name in metrics_to_calibrate:
            self._apply_metric_calibration(ctx, metric_name)

        # Flags (store, but don't numeric-calibrate)
        for flag_name in ["weak_progress"]:
            self._apply_metric_calibration(ctx, flag_name)

    def calibrate_context(self, ctx: "CoreSignalContext") -> None:
        """Apply all calibration to context."""
        self._apply_local_calibration(ctx)

    def get_calibrated_trailing_params(self) -> Dict[str, Any]:
        """
        Читает откалиброванные параметры из Redis symbol_specs.
        Возвращает параметры для трейлинга или fallback на значения из конфига.
        """
        def _fallback() -> Dict[str, Any]:
            return {
                "stop_atr_mult": 2.0,
                "rr_levels": [2.0, 3.0, 5.0],
            }

        # config_manager is not required for Redis trailing (keep it optional)
        if not self.redis:
            self.logger.debug("Redis or config_manager not available, using defaults for %s", self.symbol)
            return _fallback()

        def _parse_rr_levels(x: Any) -> list[float]:
            if x is None:
                return []
            if isinstance(x, str):
                s = x.replace(";", ",").replace(":", ",").replace("|", ",")
                out: list[float] = []
                for p in (p.strip() for p in s.split(",")):
                    if not p:
                        continue
                    try:
                        out.append(float(p))
                    except ValueError:
                        continue
                return out
            if isinstance(x, (list, tuple)):
                out: list[float] = []
                for v in x:
                    try:
                        out.append(float(v))
                    except (TypeError, ValueError):
                        continue
                return out
            return []

        try:
            spec_key = f"symbol_specs:{self.symbol}"
            spec_data = self.redis.get(spec_key)

            if not spec_data:
                self.logger.debug("No symbol specs found in Redis for %s, using config defaults", self.symbol)
                return _fallback()

            if isinstance(spec_data, (bytes, bytearray)):
                spec_data = spec_data.decode("utf-8", errors="replace")
            elif isinstance(spec_data, str):
                # Already decoded by Redis decode_responses=True
                pass

            spec = json.loads(spec_data)
            if not isinstance(spec, dict):
                return _fallback()

            # staleness guard (optional)
            max_age_ms = int(os.getenv("SYMBOL_SPECS_MAX_AGE_MS", "86400000"))  # 24h
            now_ms = get_ny_time_millis()
            ts_ms = int(spec.get("ts_ms") or spec.get("updated_ts_ms") or spec.get("updated_at_ms") or 0)
            # allow seconds timestamps
            if 0 < ts_ms < 10**12:
                ts_ms *= 1000
            age_ms = max(0, now_ms - ts_ms) if ts_ms > 0 else 0
            if ts_ms > 0 and age_ms > max_age_ms:
                self.logger.warning(
                    "symbol_specs stale for %s: age_ms=%d > max_age_ms=%d (ts_ms=%d)",
                    self.symbol, age_ms, max_age_ms, ts_ms
                )
                return _fallback()

            trailing = (spec or {}).get("trailing", {}) if isinstance(spec, dict) else {}
            if not isinstance(trailing, dict):
                trailing = {}

            try:
                stop_atr_mult = float(trailing.get("stop_atr_mult", 2.0))
            except (TypeError, ValueError):
                stop_atr_mult = 2.0
            if stop_atr_mult <= 0:
                stop_atr_mult = 2.0

            rr_levels = _parse_rr_levels(trailing.get("rr_levels"))
            if not rr_levels:
                rr_levels = [2.0, 3.0, 5.0]

            # normalize: unique, sorted, sane bounds
            rr_levels = sorted({float(x) for x in rr_levels if x and float(x) > 0.0})
            if not rr_levels:
                rr_levels = [2.0, 3.0, 5.0]

            return {
                "stop_atr_mult": stop_atr_mult,
                "rr_levels": rr_levels,
            }
        except Exception as e:
            self.logger.warning("Failed to get calibrated trailing params: %s", e)
            return _fallback()
