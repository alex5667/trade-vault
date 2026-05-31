from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Any

from services.ev_giveback_stats import GivebackEmaConfig, read_giveback_ema
from utils.time_utils import get_ny_time_millis


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _canon_regime(v: Any) -> str:
    if v is None:
        return "na"
    if isinstance(v, str):
        s = v.strip().lower()
        return s if s else "na"
    s = str(getattr(v, "name", None) or getattr(v, "value", None) or v).strip().lower()
    return s if s else "na"


def _safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        f = float(x)
        if not math.isfinite(f):
            return None
        return f
    except Exception:
        return None


def _parse_csv_set(s: str) -> set:
    out = set()
    for part in (s or "").split(","):
        p = part.strip().lower()
        if p:
            out.add(p)
    return out


@dataclass(frozen=True)
class TrailingConditionConfig:
    """
    Enables trailing AFTER TP1 only if:
      momentum_ok OR giveback_risk_high

    Momentum inputs are read from ctx using multiple fallbacks (fail-open).
    giveback_risk is read from Redis EMA written by StatsAggregator.
    """

    enabled: bool
    kinds_allow: set
    require_alignment: bool

    z_thr: float
    obi_thr: float
    require_obi_sustained: bool

    use_giveback_ema: bool
    giveback_bps_min: float
    giveback_min_samples: int

    @classmethod
    def from_env(cls) -> TrailingConditionConfig:
        enabled = _env_bool("TRAIL_COND_ENABLED", True)
        kinds_allow = _parse_csv_set(os.getenv("TRAIL_COND_KINDS", "breakout,extreme,obi_spike,absorption") or "")
        if not kinds_allow:
            kinds_allow = {"breakout", "extreme", "obi_spike", "absorption"}
        require_alignment = _env_bool("TRAIL_COND_REQUIRE_ALIGNMENT", True)
        try:
            z_thr = float(os.getenv("TRAIL_COND_Z_THR", "2.0") or "2.0")
        except Exception:
            z_thr = 2.0
        try:
            obi_thr = float(os.getenv("TRAIL_COND_OBI_THR", "0.10") or "0.10")
        except Exception:
            obi_thr = 0.10
        require_obi_sustained = _env_bool("TRAIL_COND_REQUIRE_OBI_SUSTAINED", False)
        use_giveback_ema = _env_bool("TRAIL_COND_USE_GIVEBACK_EMA", True)
        try:
            giveback_bps_min = float(os.getenv("TRAIL_COND_GIVEBACK_BPS_MIN", "20") or "20")
        except Exception:
            giveback_bps_min = 20.0
        try:
            giveback_min_samples = int(os.getenv("TRAIL_COND_GIVEBACK_MIN_SAMPLES", "30") or "30")
        except Exception:
            giveback_min_samples = 30

        return cls(
            enabled=bool(enabled),
            kinds_allow=set(kinds_allow),
            require_alignment=bool(require_alignment),
            z_thr=float(max(0.0, z_thr)),
            obi_thr=float(max(0.0, obi_thr)),
            require_obi_sustained=bool(require_obi_sustained),
            use_giveback_ema=bool(use_giveback_ema),
            giveback_bps_min=float(max(0.0, giveback_bps_min)),
            giveback_min_samples=int(max(0, giveback_min_samples)),
        )


@dataclass(frozen=True)
class TrailingDecision:
    enabled: bool
    reason: str
    momentum_ok: bool
    giveback_risk_ok: bool
    giveback_ema_bps: float
    giveback_samples: int


class TrailingConditionEvaluator:
    def __init__(self, redis_client: Any, *, cfg: TrailingConditionConfig | None = None):
        self.redis = redis_client
        self.cfg = cfg or TrailingConditionConfig.from_env()
        self.gb_cfg = GivebackEmaConfig.from_env()

    def _read_z(self, ctx: Any) -> float | None:
        # Multiple possible names across detectors/pipelines.
        for name in ("z_delta", "delta_z", "z", "raw_score", "zscore"):
            v = _safe_float(getattr(ctx, name, None))
            if v is not None:
                return v
        # Nested objects (ctx.of.*) if present.
        of = getattr(ctx, "of", None)
        if of is not None:
            for name in ("z_delta", "delta_z", "z"):
                v = _safe_float(getattr(of, name, None))
                if v is not None:
                    return v
        return None

    def _read_obi(self, ctx: Any) -> float | None:
        for name in ("obi_avg", "obi_20", "obi", "obi_value"):
            v = _safe_float(getattr(ctx, name, None))
            if v is not None:
                return v
        of = getattr(ctx, "of", None)
        if of is not None:
            for name in ("obi_avg", "obi_20", "obi"):
                v = _safe_float(getattr(of, name, None))
                if v is not None:
                    return v
        return None

    def _read_obi_sustained(self, ctx: Any) -> bool | None:
        for name in ("obi_sustained", "obi_is_sustained", "obi_stable"):
            v = getattr(ctx, name, None)
            if isinstance(v, bool):
                return v
        return None

    def evaluate(
        self,
        ctx: Any,
        *,
        side: str,
        symbol: str,
        kind: str,
        tf: str,
        regime: str,
    ) -> TrailingDecision:
        # Default is "enabled" if config disabled -> preserves legacy behavior.
        if not self.cfg.enabled:
            return TrailingDecision(True, "TRAIL_COND_DISABLED", False, False, 0.0, 0)

        kd = (kind or "").strip().lower()
        if kd and self.cfg.kinds_allow and kd not in self.cfg.kinds_allow:
            # For kinds not in allowlist -> do not trail (prefer fixed TP2/TP3).
            return TrailingDecision(False, f"KIND_BLOCK:{kd}", False, False, 0.0, 0)

        s = (side or "").strip().upper()
        is_long = s in {"LONG", "BUY"}
        is_short = s in {"SHORT", "SELL"}

        z = self._read_z(ctx)
        obi = self._read_obi(ctx)
        obi_sust = self._read_obi_sustained(ctx)

        # ---- momentum gate ----
        momentum_ok = False
        parts: list[str] = []

        if z is not None and abs(float(z)) >= float(self.cfg.z_thr) and self.cfg.z_thr > 0:
            momentum_ok = True
            parts.append(f"z={float(z):.3f}>=thr{self.cfg.z_thr:.3f}")

        if obi is not None and abs(float(obi)) >= float(self.cfg.obi_thr) and self.cfg.obi_thr > 0:
            # alignment: LONG expects +OBI, SHORT expects -OBI
            if self.cfg.require_alignment and (is_long or is_short):
                if is_long and float(obi) < float(self.cfg.obi_thr):
                    parts.append(f"obi_misalign_long={float(obi):.3f}")
                elif is_short and float(obi) > -float(self.cfg.obi_thr):
                    parts.append(f"obi_misalign_short={float(obi):.3f}")
                else:
                    momentum_ok = True
                    parts.append(f"obi={float(obi):.3f} ok")
            else:
                momentum_ok = True
                parts.append(f"obi={float(obi):.3f} ok")

        if self.cfg.require_obi_sustained:
            if obi_sust is True:
                momentum_ok = True
                parts.append("obi_sustained=1")
            else:
                parts.append("obi_sustained=0")

        # ---- giveback risk (EMA) ----
        gb_ema = 0.0
        gb_samples = 0
        giveback_ok = False
        if self.cfg.use_giveback_ema and self.redis is not None:
            st = read_giveback_ema(
                self.redis,
                cfg=self.gb_cfg,
                kind=kd,
                symbol=symbol,
                tf=(tf or "1m"),
                regime=_canon_regime(regime),
            )
            if st:
                gb_samples = int(st.get("samples") or 0)
                gb_ema = float(st.get("ema_giveback_bps") or 0.0)
                if gb_samples >= int(self.cfg.giveback_min_samples) and gb_ema >= float(self.cfg.giveback_bps_min):
                    giveback_ok = True

        enabled = bool(momentum_ok or giveback_ok)

        if enabled:
            reason = "OR(" + ",".join(parts) + (f",gb_ema={gb_ema:.1f}bps@{gb_samples}" if giveback_ok else "") + ")"
        else:
            reason = "NO_MOMENTUM_NO_GIVEBACK"

        return TrailingDecision(enabled, reason, momentum_ok, giveback_ok, float(gb_ema), int(gb_samples))

    # ------------------------------------------------------------------
    # Step 4: Calibrated params reader (trail:calib:{symbol}:{regime})
    # ------------------------------------------------------------------

    def get_calibrated_params(
        self,
        symbol: str,
        regime: str,
        *,
        max_stale_ms: int = 172_800_000,  # 48h default
    ) -> dict[str, Any] | None:
        """
        Read calibrated trailing params from trail:calib:{symbol}:{regime}.

        Returns dict with:
          - callback_atr_mult: float
          - activate_offset_bps: float
          - min_profit_lock_r: float
          - mode: str ('shadow'|'enforce')
          - confidence: float
        or None if not available / stale / mode != enforce.

        Fail-open: never raises, returns None on any error.
        """
        if self.redis is None:
            return None

        calib_prefix = os.getenv("TRAIL_CALIB_KEY_PREFIX", "trail:calib") or "trail:calib"
        rg = _canon_regime(regime)
        key = f"{calib_prefix}:{symbol.upper()}:{rg}"

        try:
            h = self.redis.hgetall(key)
            if not h:
                return None

            mode = (h.get("mode") or "shadow")
            if mode != "enforce":
                return None  # shadow mode — don't use calibrated params

            # Staleness check
            computed_ms = int(float(h.get("computed_at_ms") or "0"))
            if computed_ms > 0:
                age_ms = get_ny_time_millis() - computed_ms
                if age_ms > max_stale_ms:
                    return None  # stale calibration

            callback_atr_mult = _safe_float(h.get("callback_atr_mult"))
            if callback_atr_mult is None or callback_atr_mult <= 0:
                return None

            return {
                "callback_atr_mult": float(callback_atr_mult),
                "activate_offset_bps": _safe_float(h.get("activate_offset_bps") or 5.0),
                "min_profit_lock_r": _safe_float(h.get("min_profit_lock_r") or 0.1),
                "mode": mode,
                "confidence": _safe_float(h.get("confidence") or 0.0),
                "n_total": int(float(h.get("n_total") or "0")),
            }
        except Exception:
            return None
