from __future__ import annotations

import os
import math
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional, Set, Tuple, Literal

from handlers.crypto_orderflow.utils.log_sampler import sampled_warning

logger = logging.getLogger(__name__)

# =============================================================================
# Quality gates for CryptoOrderFlow
#
# Goals:
#  A1) Regime/session gating:
#      - forbid kinds in regimes where they are statistically bad
#  A1) Liquidity gating:
#      - avoid bad fills/noisy market: spread too wide, depth too low,
#        burst_flip_ratio too high, ATR regime out of bounds
#  A2) Consistency gate:
#      - require agreement between key signals for each kind
#  A3) Data-quality / staleness hard veto:
#      - non-epoch timestamps, large lag, out-of-order stream, quarantine flags,
#        ATR staleness (if you provide atr_ts_ms in ctx)
#
# Design constraints:
#  - FAIL-OPEN by default on missing optional metrics (configurable strict mode).
#  - Deterministic decisions for unit tests and structured logging.
# =============================================================================


def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return float(default)


def _env_str(name: str, default: str = "") -> str:
    """Get string from ENV (strips whitespace)."""
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _parse_csv_set(v: str) -> Set[str]:
    out: Set[str] = set()
    for x in (v or "").split(","):
        s = x.strip().lower()
        if s:
            out.add(s)
    return out


def _norm_symbol(sym: str) -> str:
    return (sym or "").strip().upper().replace("/", "").replace("-", "")


def _sym_env_pick(prefix: str, symbol: str, default: float) -> float:
    # Example: LIQ_MAX_SPREAD_BPS_BTCUSDT -> per-symbol override
    s = _norm_symbol(symbol)
    return _env_float(f"{prefix}_{s}", default)


@dataclass(frozen=True)
class QualityGateDecision:
    apply: bool
    veto: bool
    reason_code: str
    notes: str = ""


def _safe_int(x: Any) -> Optional[int]:
    try:
        i = int(x)
        return i
    except Exception:
        return None


def _get_flags(ctx: Any) -> Set[str]:
    """
    Normalize ctx.data_quality_flags -> set[str] lowercase.
    This list is already used in your crypto pipeline (e.g. 'missing_htf', 'l3_missing', 'stale_l2').
    """
    flags = getattr(ctx, "data_quality_flags", None)
    if not isinstance(flags, list):
        return set()
    out: Set[str] = set()
    for x in flags:
        try:
            s = str(x).strip().lower()
            if s:
                out.add(s)
        except Exception:
            pass
    return out


def _flags_intersect(flags: Set[str], veto: Set[str]) -> Set[str]:
    if not flags or not veto:
        return set()
    return set(flags.intersection(veto))


def _sym_base(symbol: str) -> str:
    """BTCUSDT -> BTC; ETHUSDT -> ETH; fallback: symbol upper."""
    s = (symbol or "").strip().upper()
    if s.endswith("USDT") and len(s) > 4:
        return s[:-4]
    return s


def _safe_float(x: Any) -> Optional[float]:
    try:
        f = float(x)
        return f if math.isfinite(f) else None
    except Exception:
        return None


def _get_attr_any(obj: Any, names: Tuple[str, ...]) -> Any:
    """Try multiple attribute names (compat across refactors)."""
    for n in names:
        try:
            if hasattr(obj, n):
                return getattr(obj, n)
        except Exception:
            continue
    return None


def _get_metric(ctx: Any, names: Tuple[str, ...], *, allow_of: bool = True) -> Any:
    """
    Read metric from ctx or ctx.of (raw orderflow), trying several names.
    """
    v = _get_attr_any(ctx, names)
    if v is not None:
        return v
    if allow_of:
        of = getattr(ctx, "of", None)
        if of is not None:
            return _get_attr_any(of, names)
    return None


def _is_epoch_ms(ts: int) -> bool:
    """
    Epoch ms sanity range:
      2001-09-09 .. 2286-11-20 roughly
    """
    try:
        t = int(ts)
    except Exception:
        return False
    return 1_000_000_000_000 <= t <= 10_000_000_000_000


def _pick_float(
    prefix: str,
    *,
    symbol: str,
    kind: str,
    regime: str,
    default: float,
) -> float:
    """
    Hierarchical override resolver via ENV.
    Prefix examples:
      QUALITY_SPREAD_MAX_BPS
      QUALITY_DEPTH_MIN

    Lookup order (most specific -> least):
      {prefix}__{SYM}__{KIND}__{REGIME}
      {prefix}__{SYM}__{KIND}
      {prefix}__{KIND}__{REGIME}
      {prefix}__{KIND}
      {prefix}_DEFAULT
    """
    sym = _norm_symbol(symbol)
    k = (kind or "").strip().upper()
    r = (regime or "").strip().upper()
    keys = [
        f"{prefix}__{sym}__{k}__{r}",
        f"{prefix}__{sym}__{k}",
        f"{prefix}__{k}__{r}",
        f"{prefix}__{k}",
        f"{prefix}_DEFAULT",
    ]
    for name in keys:
        if os.getenv(name) is not None:
            return _env_float(name, default)
    return float(default)


def _pick_allow_set(prefix: str, *, kind: str) -> Set[str]:
    """
    Parse allow-lists like:
      QUALITY_ALLOW_REGIMES__BREAKOUT=trending_bull,trending_bear,expansion
      QUALITY_ALLOW_SESSIONS__ABSORPTION=us_main,european
    """
    k = (kind or "").strip().upper()
    name = f"{prefix}__{k}"
    if os.getenv(name) is None:
        return set()
    return _parse_csv_set(_env_str(name, ""))


@dataclass(frozen=True)
class DataQualityGateDecision:
    apply: bool
    veto: bool
    reason_code: str
    notes: str = ""
    updated_last_ts_ms: Optional[int] = None


# =============================================================================
# A3) Data-quality / staleness hard gate
# =============================================================================
@dataclass
class DataQualityGate:
    enabled: bool
    require_epoch_ts: bool
    max_event_lag_ms: int
    max_future_skew_ms: int
    out_of_order_tolerance_ms: int
    quarantine_veto: bool
    atr_stale_max_ms: int
    strict_missing_atr_ts: bool
    veto_on_quality_flags: Set[str]
    touch_stale_veto: bool
    touch_stale_apply_kinds: Set[str]

    @classmethod
    def from_env(cls) -> "DataQualityGate":
        return cls(
            enabled=_env_bool("DATA_QUALITY_GATE_ENABLED", True),
            require_epoch_ts=_env_bool("DATA_REQUIRE_EPOCH_TS", True),
            max_event_lag_ms=int(_env_float("DATA_MAX_EVENT_LAG_MS", 2500.0)),
            max_future_skew_ms=int(_env_float("DATA_MAX_FUTURE_SKEW_MS", 200.0)),
            out_of_order_tolerance_ms=int(_env_float("DATA_OUT_OF_ORDER_TOL_MS", 1000.0)),
            quarantine_veto=_env_bool("DATA_QUARANTINE_VETO", True),
            atr_stale_max_ms=int(_env_float("DATA_ATR_STALE_MAX_MS", 60_000.0)),
            strict_missing_atr_ts=_env_bool("DATA_STRICT_MISSING_ATR_TS", False),
            # Optional: turn existing ctx.data_quality_flags into a hard veto.
            # Example:
            #   DATA_VETO_FLAGS=stale_l2,l3_missing
            veto_on_quality_flags=_parse_csv_set(_env_str("DATA_VETO_FLAGS", "")),
            # Optional: treat stale touch snapshot as data-quality hard veto (for touch-sensitive kinds).
            touch_stale_veto=_env_bool("DATA_TOUCH_STALE_VETO", False),
            touch_stale_apply_kinds=_parse_csv_set(_env_str("DATA_TOUCH_STALE_APPLY_KINDS", "")),
        )

    def evaluate(
        self,
        *,
        ctx: Any,
        symbol: str,
        kind: str,
        now_ms: int,
        last_ts_ms: Optional[int],
    ) -> DataQualityGateDecision:
        if not self.enabled:
            return DataQualityGateDecision(apply=False, veto=False, reason_code="OK", updated_last_ts_ms=last_ts_ms)

        # Optional: convert existing pipeline quality flags to a hard veto.
        # This is useful when you already mark broken contexts (stale_l2, l3_missing).
        try:
            flags = _get_flags(ctx)
            bad = _flags_intersect(flags, self.veto_on_quality_flags)
            if bad:
                return DataQualityGateDecision(
                    apply=True, veto=True, reason_code="VETO_DATA_FLAGS",
                    notes=f"flags={sorted(list(bad))}",
                    updated_last_ts_ms=last_ts_ms,
                )
        except Exception:
            pass

        # Try to obtain event ts (epoch ms).
        ts = _get_metric(ctx, ("ts_event_ms", "ts", "ts_ms"), allow_of=True)
        ts_i = None
        try:
            ts_i = int(ts) if ts is not None else None
        except Exception:
            ts_i = None

        if ts_i is None:
            # If we cannot even read ts, we fail-open (avoid breaking pipeline).
            sampled_warning(logger, "DATA_QUALITY_MISSING_TS", "⚠️ %s: missing ts_event_ms for kind=%s, fail-open", symbol, kind)
            return DataQualityGateDecision(
                apply=True, veto=False, reason_code="OK",
                notes="missing_ts_fail_open",
                updated_last_ts_ms=last_ts_ms,
            )

        if self.require_epoch_ts and not _is_epoch_ms(ts_i):
            return DataQualityGateDecision(
                apply=True, veto=True, reason_code="VETO_NON_EPOCH_TS",
                notes=f"ts={ts_i}",
                updated_last_ts_ms=last_ts_ms,
            )

        # Lag and future skew checks
        lag = int(now_ms) - int(ts_i)
        if lag > int(self.max_event_lag_ms):
            return DataQualityGateDecision(
                apply=True, veto=True, reason_code="VETO_EVENT_LAG",
                notes=f"lag_ms={lag} max={self.max_event_lag_ms} ts={ts_i} now={now_ms}",
                updated_last_ts_ms=last_ts_ms,
            )
        if int(ts_i) > int(now_ms) + int(self.max_future_skew_ms):
            return DataQualityGateDecision(
                apply=True, veto=True, reason_code="VETO_FUTURE_TS",
                notes=f"ts={ts_i} now={now_ms} skew_max={self.max_future_skew_ms}",
                updated_last_ts_ms=last_ts_ms,
            )

        # Out-of-order detection (best-effort, per handler instance)
        updated_last = last_ts_ms
        if last_ts_ms is not None and int(ts_i) + int(self.out_of_order_tolerance_ms) < int(last_ts_ms):
            # We DO NOT update last_ts here to avoid moving the watermark backwards.
            return DataQualityGateDecision(
                apply=True, veto=True, reason_code="VETO_OUT_OF_ORDER",
                notes=f"ts={ts_i} last={last_ts_ms} tol={self.out_of_order_tolerance_ms}",
                updated_last_ts_ms=last_ts_ms,
            )
        updated_last = int(ts_i) if updated_last is None else max(int(updated_last), int(ts_i))

        # Quarantine / bad-time flags (multiple possible attribute names)
        if self.quarantine_veto:
            qflag = _get_metric(
                ctx,
                (
                    "time_quarantine_active",
                    "bad_time_quarantined",
                    "is_time_quarantined",
                    "time_is_quarantined",
                    "bad_time_freeze_active",
                ),
                allow_of=True,
            )
            if bool(qflag):
                return DataQualityGateDecision(
                    apply=True, veto=True, reason_code="VETO_TIME_QUARANTINE",
                    notes=f"flag={qflag}",
                    updated_last_ts_ms=updated_last,
                )

        # Optional: touch staleness veto (you have ctx.touch_is_stale).
        # This is intentionally off by default because touch snapshots might be unavailable early in warmup.
        if self.touch_stale_veto:
            kind_l = (kind or "").strip().lower()
            if (not self.touch_stale_apply_kinds) or (kind_l in self.touch_stale_apply_kinds):
                tis = getattr(ctx, "touch_is_stale", None)
                if tis is True:
                    return DataQualityGateDecision(
                        apply=True, veto=True, reason_code="VETO_TOUCH_STALE",
                        notes="touch_is_stale=True",
                        updated_last_ts_ms=updated_last,
                    )

        # ATR staleness (only if atr timestamp is available)
        # NOTE: ATR timestamp can live on ctx or on raw OrderflowContext (ctx.of)
        atr_ts = (
            getattr(ctx, "atr_ts_ms", None)
            or (getattr(getattr(ctx, "of", None), "atr_ts_ms", None) if getattr(ctx, "of", None) is not None else None)
            or _get_metric(ctx, ("atr_updated_ms", "atr_last_update_ms"), allow_of=True)
        )
        atr_ts_i = None
        try:
            atr_ts_i = int(atr_ts) if atr_ts is not None else None
        except Exception:
            atr_ts_i = None

        if atr_ts_i is None:
            if self.strict_missing_atr_ts:
                return DataQualityGateDecision(
                    apply=True, veto=True, reason_code="VETO_MISSING_ATR_TS",
                    notes="atr_ts_ms not provided",
                    updated_last_ts_ms=updated_last,
                )
            sampled_warning(logger, "DATA_QUALITY_MISSING_ATR_TS", "⚠️ %s: missing atr_ts_ms for kind=%s, fail-open", symbol, kind)
            return DataQualityGateDecision(
                apply=True, veto=False, reason_code="OK",
                notes="missing_atr_ts_fail_open",
                updated_last_ts_ms=updated_last,
            )

        atr_age = int(now_ms) - int(atr_ts_i)
        if atr_age > int(self.atr_stale_max_ms):
            return DataQualityGateDecision(
                apply=True, veto=True, reason_code="VETO_ATR_STALE",
                notes=f"atr_age_ms={atr_age} max={self.atr_stale_max_ms} atr_ts={atr_ts_i}",
                updated_last_ts_ms=updated_last,
            )

        return DataQualityGateDecision(apply=True, veto=False, reason_code="OK", updated_last_ts_ms=updated_last)


# =============================================================================
# A1) Regime gating
# =============================================================================
@dataclass
class RegimeGate:
    enabled: bool
    apply_kinds: Set[str]
    deny_breakout_regimes: Set[str]
    deny_absorption_regimes: Set[str]
    deny_extreme_regimes: Set[str]
    deny_obi_spike_regimes: Set[str]
    require_regime_present: bool

    @classmethod
    def from_env(cls) -> "RegimeGate":
        return cls(
            enabled=_env_bool("REGIME_GATE_ENABLED", True),
            apply_kinds=_parse_csv_set(_env_str("REGIME_APPLY_KINDS", "breakout,absorption,extreme,obi_spike")),
            # Default policy: breakout forbidden in range/squeeze; absorption forbidden in trending/expansion
            deny_breakout_regimes=_parse_csv_set(_env_str("REGIME_DENY_BREAKOUT", "range,squeeze")),
            deny_absorption_regimes=_parse_csv_set(_env_str("REGIME_DENY_ABSORPTION", "trending_bull,trending_bear,expansion")),
            deny_extreme_regimes=_parse_csv_set(_env_str("REGIME_DENY_EXTREME", "")),
            deny_obi_spike_regimes=_parse_csv_set(_env_str("REGIME_DENY_OBI_SPIKE", "")),
            require_regime_present=_env_bool("REGIME_REQUIRE_PRESENT", False),
        )

    def evaluate(self, *, ctx: Any, symbol: str, kind: str, side: str) -> QualityGateDecision:
        if not self.enabled:
            return QualityGateDecision(False, False, "OK")
        kind_l = (kind or "").strip().lower()
        if self.apply_kinds and kind_l not in self.apply_kinds:
            return QualityGateDecision(False, False, "OK")

        # regime может быть либо в ctx.of.regime (строка), либо в ctx.regime (объект), либо отсутствовать
        of = getattr(ctx, "of", None)
        regime = (
            getattr(ctx, "regime", None)
            or (getattr(of, "regime", None) if of is not None else None)
        )
        if regime is None:
            if self.require_regime_present:
                return QualityGateDecision(True, True, "VETO_MISSING_REGIME", "regime missing")
            return QualityGateDecision(True, False, "OK", "missing_regime_fail_open")

        if not isinstance(regime, str):
            # If regime is an object (RegimeInfo), try .name/.label
            regime = str(getattr(regime, "name", None) or getattr(regime, "label", None) or regime)
        r = str(regime).strip().lower()

        deny = set()
        if kind_l == "breakout":
            deny = self.deny_breakout_regimes
        elif kind_l == "absorption":
            deny = self.deny_absorption_regimes
        elif kind_l == "extreme":
            deny = self.deny_extreme_regimes
        elif kind_l == "obi_spike":
            deny = self.deny_obi_spike_regimes

        if deny and r in deny:
            return QualityGateDecision(True, True, "VETO_REGIME", f"kind={kind_l} regime={r} denied={sorted(list(deny))}")
        return QualityGateDecision(True, False, "OK")


# =============================================================================
# A1) Liquidity gating
# =============================================================================
@dataclass
class LiquidityGate:
    enabled: bool
    apply_kinds: Set[str]
    # Spread
    max_spread_bps_default: float
    # Depth
    min_depth5_default: float
    # Burst instability
    max_burst_flip_ratio_default: float

    @classmethod
    def from_env(cls) -> "LiquidityGate":
        return cls(
            enabled=_env_bool("LIQ_GATE_ENABLED", True),
            apply_kinds=_parse_csv_set(_env_str("LIQ_APPLY_KINDS", "breakout,absorption,extreme,obi_spike")),
            max_spread_bps_default=_env_float("LIQ_MAX_SPREAD_BPS", 15.0),
            # IMPORTANT:
            # depth_* units depend on your upstream (qty/usd-normalized/etc).
            # default=0 disables depth veto unless you explicitly set it.
            min_depth5_default=_env_float("LIQ_MIN_DEPTH_5", 0.0),
            max_burst_flip_ratio_default=_env_float("LIQ_MAX_BURST_FLIP_RATIO", 0.80),
        )

    def evaluate(self, *, ctx: Any, symbol: str, kind: str, side: str) -> QualityGateDecision:
        if not self.enabled:
            return QualityGateDecision(False, False, "OK")
        kind_l = (kind or "").strip().lower()
        if self.apply_kinds and kind_l not in self.apply_kinds:
            return QualityGateDecision(False, False, "OK")

        of = getattr(ctx, "of", None)
        spread = _get_metric(ctx, ("spread_bps",), allow_of=True)
        depth_bid_5 = _get_metric(ctx, ("depth_bid_5",), allow_of=True)
        depth_ask_5 = _get_metric(ctx, ("depth_ask_5",), allow_of=True)
        burst_flip = _get_metric(ctx, ("burst_flip_ratio",), allow_of=True)

        # per-symbol overrides
        max_spread = _sym_env_pick("LIQ_MAX_SPREAD_BPS", symbol, self.max_spread_bps_default)
        min_depth = _sym_env_pick("LIQ_MIN_DEPTH_5", symbol, self.min_depth5_default)
        max_flip = _sym_env_pick("LIQ_MAX_BURST_FLIP_RATIO", symbol, self.max_burst_flip_ratio_default)

        try:
            if spread is not None and math.isfinite(float(spread)) and float(spread) > float(max_spread):
                return QualityGateDecision(True, True, "VETO_SPREAD", f"spread_bps={float(spread):.3f} > {float(max_spread):.3f}")
        except Exception:
            pass

        try:
            if float(min_depth) > 0.0:
                db = float(depth_bid_5) if depth_bid_5 is not None else float("nan")
                da = float(depth_ask_5) if depth_ask_5 is not None else float("nan")
                if math.isfinite(db) and math.isfinite(da):
                    dmin = min(db, da)
                    if dmin < float(min_depth):
                        return QualityGateDecision(True, True, "VETO_DEPTH", f"min(depth_bid_5,depth_ask_5)={dmin:.3f} < {float(min_depth):.3f}")
        except Exception:
            pass

        try:
            if burst_flip is not None and math.isfinite(float(burst_flip)) and float(burst_flip) > float(max_flip):
                return QualityGateDecision(True, True, "VETO_BURST_FLIP", f"burst_flip_ratio={float(burst_flip):.3f} > {float(max_flip):.3f}")
        except Exception:
            pass

        return QualityGateDecision(True, False, "OK")


# =============================================================================
# A1) Regime/session gating + liquidity gating
# =============================================================================
@dataclass
class RegimeSessionLiquidityGate:
    enabled: bool
    strict_missing_metrics: bool
    apply_kinds: Set[str]

    @classmethod
    def from_env(cls) -> "RegimeSessionLiquidityGate":
        return cls(
            enabled=_env_bool("QUALITY_GATE_ENABLED", True),
            strict_missing_metrics=_env_bool("QUALITY_STRICT_MISSING_METRICS", False),
            apply_kinds=_parse_csv_set(_env_str("QUALITY_APPLY_KINDS", "")),
        )

    def evaluate(self, *, ctx: Any, symbol: str, kind: str, side: str) -> QualityGateDecision:
        if not self.enabled:
            return QualityGateDecision(apply=False, veto=False, reason_code="OK", notes="quality_gate_disabled")

        kind_l = (kind or "").strip().lower()
        if self.apply_kinds and kind_l not in self.apply_kinds:
            return QualityGateDecision(apply=False, veto=False, reason_code="OK", notes="kind_not_applicable")

        # regime/session sources:
        # - ctx.of.regime (string) is available in OrderflowContext
        # - ctx.session (string) exists in SignalContext
        regime = _get_metric(ctx, ("regime",), allow_of=True)
        regime_s = (str(regime) if regime is not None else "").strip().lower()
        session = _get_metric(ctx, ("session",), allow_of=False)
        session_s = (str(session) if session is not None else "").strip().lower()

        # Regime allow-list per kind
        allow_regimes = _pick_allow_set("QUALITY_ALLOW_REGIMES", kind=kind)
        if allow_regimes and regime_s and regime_s not in allow_regimes:
            return QualityGateDecision(
                apply=True, veto=True, reason_code="VETO_REGIME_NOT_ALLOWED",
                notes=f"regime={regime_s} allow={sorted(list(allow_regimes))}",
            )

        # Session allow-list per kind (optional)
        allow_sessions = _pick_allow_set("QUALITY_ALLOW_SESSIONS", kind=kind)
        if allow_sessions and session_s and session_s not in allow_sessions:
            return QualityGateDecision(
                apply=True, veto=True, reason_code="VETO_SESSION_NOT_ALLOWED",
                notes=f"session={session_s} allow={sorted(list(allow_sessions))}",
            )

        # Liquidity metrics (spread/depth/burst flip)
        spread_bps = _safe_float(_get_metric(ctx, ("spread_bps",), allow_of=True))
        depth_bid_5 = _safe_float(_get_metric(ctx, ("depth_bid_5",), allow_of=True))
        depth_ask_5 = _safe_float(_get_metric(ctx, ("depth_ask_5",), allow_of=True))
        burst_flip_ratio = _safe_float(_get_metric(ctx, ("burst_flip_ratio",), allow_of=True))

        # Volatility regime metrics (optional)
        daily_atr_bps = _safe_float(_get_metric(ctx, ("daily_atr_bps",), allow_of=True))
        atr_q_14 = _safe_float(_get_metric(ctx, ("atr_q_14",), allow_of=True))

        # Thresholds (with hierarchy overrides by symbol/kind/regime)
        sp_max = _pick_float("QUALITY_SPREAD_MAX_BPS", symbol=symbol, kind=kind, regime=regime_s, default=8.0)
        depth_min = _pick_float("QUALITY_DEPTH_MIN", symbol=symbol, kind=kind, regime=regime_s, default=0.0)
        flip_max = _pick_float("QUALITY_BURST_FLIP_MAX", symbol=symbol, kind=kind, regime=regime_s, default=10_000.0)

        # daily_atr_bps bounds
        atr_bps_min = _pick_float("QUALITY_DAILY_ATR_BPS_MIN", symbol=symbol, kind=kind, regime=regime_s, default=0.0)
        atr_bps_max = _pick_float("QUALITY_DAILY_ATR_BPS_MAX", symbol=symbol, kind=kind, regime=regime_s, default=10_000.0)

        # atr_q_14 bounds (quantile style, usually 0..1)
        q_min = _pick_float("QUALITY_ATR_Q14_MIN", symbol=symbol, kind=kind, regime=regime_s, default=-1.0)
        q_max = _pick_float("QUALITY_ATR_Q14_MAX", symbol=symbol, kind=kind, regime=regime_s, default=2.0)

        # Apply checks with fail-open/strict-missing control
        if spread_bps is None:
            if self.strict_missing_metrics:
                return QualityGateDecision(apply=True, veto=True, reason_code="VETO_MISSING_SPREAD", notes="spread_bps missing")
        else:
            if spread_bps > sp_max:
                return QualityGateDecision(
                    apply=True, veto=True, reason_code="VETO_SPREAD_TOO_WIDE",
                    notes=f"spread_bps={spread_bps:.2f} > max={sp_max:.2f} regime={regime_s}",
                )

        if depth_min > 0.0:
            if depth_bid_5 is None or depth_ask_5 is None:
                if self.strict_missing_metrics:
                    return QualityGateDecision(
                        apply=True, veto=True, reason_code="VETO_MISSING_DEPTH",
                        notes="depth_bid_5/depth_ask_5 missing",
                    )
            else:
                dmin = min(depth_bid_5, depth_ask_5)
                if dmin < depth_min:
                    return QualityGateDecision(
                        apply=True, veto=True, reason_code="VETO_DEPTH_TOO_LOW",
                        notes=f"depth_min_side={dmin:.2f} < min={depth_min:.2f}",
                    )

        if burst_flip_ratio is None:
            if self.strict_missing_metrics:
                return QualityGateDecision(apply=True, veto=True, reason_code="VETO_MISSING_BURST_FLIP", notes="burst_flip_ratio missing")
        else:
            if burst_flip_ratio > flip_max:
                return QualityGateDecision(
                    apply=True, veto=True, reason_code="VETO_BURST_FLIP_HIGH",
                    notes=f"burst_flip_ratio={burst_flip_ratio:.3f} > max={flip_max:.3f}",
                )

        if daily_atr_bps is not None:
            if daily_atr_bps < atr_bps_min or daily_atr_bps > atr_bps_max:
                return QualityGateDecision(
                    apply=True, veto=True, reason_code="VETO_DAILY_ATR_BPS_OUT_OF_RANGE",
                    notes=f"daily_atr_bps={daily_atr_bps:.1f} notin [{atr_bps_min:.1f},{atr_bps_max:.1f}]",
                )
        elif self.strict_missing_metrics and (atr_bps_min > 0.0 or atr_bps_max < 10_000.0):
            return QualityGateDecision(apply=True, veto=True, reason_code="VETO_MISSING_DAILY_ATR_BPS", notes="daily_atr_bps missing")

        if atr_q_14 is not None:
            if atr_q_14 < q_min or atr_q_14 > q_max:
                return QualityGateDecision(
                    apply=True, veto=True, reason_code="VETO_ATR_Q14_OUT_OF_RANGE",
                    notes=f"atr_q_14={atr_q_14:.3f} notin [{q_min:.3f},{q_max:.3f}]",
                )
        elif self.strict_missing_metrics and (q_min > -1.0 or q_max < 2.0):
            return QualityGateDecision(apply=True, veto=True, reason_code="VETO_MISSING_ATR_Q14", notes="atr_q_14 missing")

        return QualityGateDecision(apply=True, veto=False, reason_code="OK")


# =============================================================================
# A2) Consistency gate (feature agreement rules)
# =============================================================================
@dataclass
class SignalConsistencyGate:
    enabled: bool
    strict_missing_metrics: bool
    apply_kinds: Set[str]

    # breakout rules
    breakout_min_z: float
    breakout_min_obi: float
    breakout_require_obi: bool
    breakout_require_obi20: bool
    breakout_min_microshift_bps: float

    # Touch-based confirmation (your real refill/depletion signals):
    # - use touch_* from services.touch_level_tracker (already attached to ctx)
    # - for breakout we normally want "depletion" on the hit side (ask for LONG, bid for SHORT)
    breakout_require_touch_fresh: bool
    breakout_require_touch_tag: bool
    breakout_touch_tag_required: str  # usually 'depletion' for breakout
    breakout_min_touch_rho: float
    breakout_min_touch_traded_w: float

    absorption_require_touch_fresh: bool
    absorption_touch_tag_required: str  # usually 'refill' for absorption
    absorption_min_touch_rho: float
    absorption_min_touch_traded_w: float

    # absorption rules
    absorption_min_z: float
    absorption_require_weak_progress: bool

    # obi_spike rules
    obi_spike_require_sustained: bool

    # extreme rules (optional L3 metric)
    extreme_max_cancel_to_trade: float

    @classmethod
    def from_env(cls) -> "SignalConsistencyGate":
        enabled = _env_bool("CONSISTENCY_GATE_ENABLED", True)
        strict_missing = _env_bool("CONSISTENCY_STRICT_MISSING_METRICS", False)
        apply_kinds = _parse_csv_set(_env_str("CONSISTENCY_APPLY_KINDS", ""))  # empty => all


        extreme_max_cancel_to_trade = _env_float("EXTREME_L3_MAX_CANCEL_TO_TRADE", 1e9)

        return cls(
            enabled=enabled,
            strict_missing_metrics=strict_missing,
            apply_kinds=apply_kinds,

            breakout_min_z=_env_float("CONS_BREAKOUT_MIN_Z", 2.0),
            breakout_min_obi=_env_float("CONS_BREAKOUT_MIN_OBI", 0.0),
            breakout_require_obi=_env_bool("BREAKOUT_REQUIRE_OBI", True),
            breakout_require_obi20=_env_bool("BREAKOUT_REQUIRE_OBI20", True),
            breakout_min_microshift_bps=_env_float("BREAKOUT_MIN_MICROPRICE_SHIFT_BPS", 0.0),

            # Breakout touch confirmation:
            # - use touch_* from services.touch_level_tracker (already attached to ctx)
            # - for breakout we normally want "depletion" on the hit side (ask for LONG, bid for SHORT)
            breakout_require_touch_fresh=_env_bool("CONS_BREAKOUT_REQUIRE_TOUCH_FRESH", True),
            breakout_require_touch_tag=_env_bool("CONS_BREAKOUT_REQUIRE_TOUCH_TAG", True),
            breakout_touch_tag_required=_env_str("CONS_BREAKOUT_TOUCH_TAG_REQUIRED", "depletion").strip().lower(),
            breakout_min_touch_rho=_env_float("CONS_BREAKOUT_MIN_TOUCH_RHO", 0.10),
            breakout_min_touch_traded_w=_env_float("CONS_BREAKOUT_MIN_TOUCH_TRADED_W", 0.0),

            absorption_min_z=_env_float("CONS_ABSORPTION_MIN_Z", 2.0),
            absorption_require_weak_progress=_env_bool("CONS_ABSORPTION_REQUIRE_WEAK_PROGRESS", True),

            # Absorption touch confirmation:
            # - for absorption (meanrev) we normally want "refill" on support/resistance side
            absorption_require_touch_fresh=_env_bool("CONS_ABSORPTION_REQUIRE_TOUCH_FRESH", True),
            absorption_touch_tag_required=_env_str("CONS_ABSORPTION_TOUCH_TAG_REQUIRED", "refill").strip().lower(),
            absorption_min_touch_rho=_env_float("CONS_ABSORPTION_MIN_TOUCH_RHO", 0.10),
            absorption_min_touch_traded_w=_env_float("CONS_ABSORPTION_MIN_TOUCH_TRADED_W", 0.0),

            obi_spike_require_sustained=_env_bool("CONS_OBI_SPIKE_REQUIRE_SUSTAINED", True),
            extreme_max_cancel_to_trade=extreme_max_cancel_to_trade,
        )

    def _touch_pick(self, ctx: Any, *, kind_l: str, side: str) -> Tuple[Optional[bool], str, Optional[float], Optional[float]]:
        """
        Map ctx.touch_* fields into a single "hit side" view:
          - breakout/extreme/obi_spike:
              LONG hits ask side; SHORT hits bid side
          - absorption (meanrev):
              LONG often buys support -> bid side; SHORT often sells resistance -> ask side
        Returns:
          (touch_is_stale, tag, rho, traded_w)
        """
        s = (side or "").strip().upper()
        tis = getattr(ctx, "touch_is_stale", None)

        if kind_l in {"breakout", "extreme", "obi_spike"}:
            hit = "ask" if s == "LONG" else "bid"
        elif kind_l == "absorption":
            hit = "bid" if s == "LONG" else "ask"
        else:
            hit = "ask" if s == "LONG" else "bid"

        if hit == "ask":
            tag = str(getattr(ctx, "touch_ask_tag", "none") or "none").strip().lower()
            rho = _safe_float(getattr(ctx, "touch_ask_rho", None))
            traded_w = _safe_float(getattr(ctx, "touch_ask_traded_w", None))
            return tis, tag, rho, traded_w
        else:
            tag = str(getattr(ctx, "touch_bid_tag", "none") or "none").strip().lower()
            rho = _safe_float(getattr(ctx, "touch_bid_rho", None))
            traded_w = _safe_float(getattr(ctx, "touch_bid_traded_w", None))
            return tis, tag, rho, traded_w

    def evaluate(self, *, ctx: Any, symbol: str, kind: str, side: str) -> QualityGateDecision:
        if not self.enabled:
            return QualityGateDecision(apply=False, veto=False, reason_code="OK", notes="consistency_disabled")

        kind_l = (kind or "").strip().lower()
        if self.apply_kinds and kind_l not in self.apply_kinds:
            return QualityGateDecision(apply=False, veto=False, reason_code="OK", notes="kind_not_applicable")

        # Core features (names are stable in OrderflowContext)
        z_delta = _safe_float(_get_metric(ctx, ("z_delta",), allow_of=True))
        obi = _safe_float(_get_metric(ctx, ("obi",), allow_of=True))
        obi_20 = _safe_float(_get_metric(ctx, ("obi_20",), allow_of=True))
        microshift = _safe_float(_get_metric(ctx, ("microprice_shift_bps_20",), allow_of=True))
        weak_progress = _get_metric(ctx, ("weak_progress",), allow_of=True)

        # Touch snapshot (real refill/depletion state in your pipeline)
        touch_is_stale, touch_tag, touch_rho, touch_traded_w = self._touch_pick(ctx, kind_l=kind_l, side=str(side))

        # Optional L3 metric for extreme
        cancel_to_trade = _safe_float(_get_metric(ctx, ("cancel_to_trade", "l3_cancel_to_trade", "cancel_to_trade_ratio"), allow_of=True))

        # Per-symbol z/obi thresholds often exist as BTC_DELTA_Z_THRESHOLD / BTC_OBI_THRESHOLD.
        base = _sym_base(symbol)
        sym_z = os.getenv(f"{base}_DELTA_Z_THRESHOLD")
        sym_obi = os.getenv(f"{base}_OBI_THRESHOLD")
        z_thr = _safe_float(sym_z) if sym_z is not None else None
        obi_thr = _safe_float(sym_obi) if sym_obi is not None else None

        # -------------------------------------------------------------------------
        # breakout: z_delta > thr AND (obi, obi_20) agree AND microshift > min
        # -------------------------------------------------------------------------
        if kind_l == "breakout":
            zmin = float(z_thr) if z_thr is not None else float(self.breakout_min_z)
            if z_delta is None:
                if self.strict_missing_metrics:
                    return QualityGateDecision(True, True, "VETO_MISSING_Z_DELTA", "breakout requires z_delta")
            else:
                if z_delta < zmin:
                    return QualityGateDecision(True, True, "VETO_BREAKOUT_Z_TOO_LOW", f"z_delta={z_delta:.3f} < {zmin:.3f}")

            if self.breakout_require_obi:
                if obi is None:
                    if self.strict_missing_metrics:
                        return QualityGateDecision(True, True, "VETO_MISSING_OBI", "breakout requires obi")
                else:
                    min_obi = float(obi_thr) if obi_thr is not None else float(self.breakout_min_obi)
                    if abs(obi) < min_obi:
                        return QualityGateDecision(True, True, "VETO_BREAKOUT_OBI_TOO_WEAK", f"obi={obi:.3f} < {min_obi:.3f}")

            if self.breakout_require_obi20:
                if obi_20 is None:
                    if self.strict_missing_metrics:
                        return QualityGateDecision(True, True, "VETO_MISSING_OBI20", "breakout requires obi_20")
                else:
                    # For consistency we require obi_20 to have same sign as obi (if both exist)
                    if obi is not None and (obi_20 * obi) < 0:
                        return QualityGateDecision(True, True, "VETO_BREAKOUT_OBI_SIGN_MISMATCH", f"obi={obi:.3f} obi_20={obi_20:.3f}")

            if self.breakout_min_microshift_bps > 0.0:
                if microshift is None:
                    if self.strict_missing_metrics:
                        return QualityGateDecision(True, True, "VETO_MISSING_MICROSHIFT", "breakout requires microprice_shift_bps_20")
                else:
                    if microshift < float(self.breakout_min_microshift_bps):
                        return QualityGateDecision(
                            True, True, "VETO_BREAKOUT_MICROSHIFT_TOO_LOW",
                            f"microshift={microshift:.3f} < {self.breakout_min_microshift_bps:.3f}",
                        )

            # Touch confirmation:
            #  - require fresh touch snapshot (unless disabled)
            #  - require tag=depletion on hit side (ask for LONG / bid for SHORT), unless disabled
            if self.breakout_require_touch_fresh:
                if touch_is_stale is True:
                    return QualityGateDecision(True, True, "VETO_BREAKOUT_TOUCH_STALE", "touch_is_stale=True")
                if touch_is_stale is None and self.strict_missing_metrics:
                    return QualityGateDecision(True, True, "VETO_MISSING_TOUCH_STALE", "touch_is_stale missing")

            if self.breakout_require_touch_tag:
                req = str(self.breakout_touch_tag_required or "depletion").strip().lower()
                if not touch_tag:
                    if self.strict_missing_metrics:
                        return QualityGateDecision(True, True, "VETO_MISSING_TOUCH_TAG", "touch_tag missing")
                else:
                    if touch_tag != req:
                        return QualityGateDecision(
                            True, True, "VETO_BREAKOUT_TOUCH_TAG_MISMATCH",
                            f"touch_tag={touch_tag} required={req}",
                        )

            if touch_rho is None:
                if self.strict_missing_metrics and self.breakout_min_touch_rho > 0:
                    return QualityGateDecision(True, True, "VETO_MISSING_TOUCH_RHO", "touch_rho missing")
            else:
                if touch_rho < float(self.breakout_min_touch_rho):
                    return QualityGateDecision(
                        True, True, "VETO_BREAKOUT_TOUCH_RHO_LOW",
                        f"touch_rho={touch_rho:.3f} < {self.breakout_min_touch_rho:.3f}",
                    )

            if touch_traded_w is None:
                if self.strict_missing_metrics and self.breakout_min_touch_traded_w > 0:
                    return QualityGateDecision(True, True, "VETO_MISSING_TOUCH_TRADED_W", "touch_traded_w missing")
            else:
                if touch_traded_w < float(self.breakout_min_touch_traded_w):
                    return QualityGateDecision(
                        True, True, "VETO_BREAKOUT_TOUCH_TRADED_W_LOW",
                        f"touch_traded_w={touch_traded_w:.3f} < {self.breakout_min_touch_traded_w:.3f}",
                    )

            return QualityGateDecision(True, False, "OK")

        # -------------------------------------------------------------------------
        # absorption: z_delta > thr AND weak_progress==True AND refill_score>=min
        # -------------------------------------------------------------------------
        if kind_l == "absorption":
            zmin = float(z_thr) if z_thr is not None else float(self.absorption_min_z)
            if z_delta is None:
                if self.strict_missing_metrics:
                    return QualityGateDecision(True, True, "VETO_MISSING_Z_DELTA", "absorption requires z_delta")
            else:
                if z_delta < zmin:
                    return QualityGateDecision(True, True, "VETO_ABSORPTION_Z_TOO_LOW", f"z_delta={z_delta:.3f} < {zmin:.3f}")

            if self.absorption_require_weak_progress:
                if weak_progress is None:
                    if self.strict_missing_metrics:
                        return QualityGateDecision(True, True, "VETO_MISSING_WEAK_PROGRESS", "absorption requires weak_progress")
                else:
                    if bool(weak_progress) is not True:
                        return QualityGateDecision(True, True, "VETO_ABSORPTION_NO_WEAK_PROGRESS", "weak_progress=False")

            # Touch confirmation for absorption (meanrev):
            #  - require fresh snapshot
            #  - require tag=refill on support/resistance side (bid for LONG / ask for SHORT)
            if self.absorption_require_touch_fresh:
                if touch_is_stale is True:
                    return QualityGateDecision(True, True, "VETO_ABSORPTION_TOUCH_STALE", "touch_is_stale=True")
                if touch_is_stale is None and self.strict_missing_metrics:
                    return QualityGateDecision(True, True, "VETO_MISSING_TOUCH_STALE", "touch_is_stale missing")

            req = str(self.absorption_touch_tag_required or "refill").strip().lower()
            if touch_tag != req:
                return QualityGateDecision(
                    True, True, "VETO_ABSORPTION_TOUCH_TAG_MISMATCH",
                    f"touch_tag={touch_tag} required={req}",
                )

            if touch_rho is None:
                if self.strict_missing_metrics and self.absorption_min_touch_rho > 0:
                    return QualityGateDecision(True, True, "VETO_MISSING_TOUCH_RHO", "touch_rho missing")
            else:
                if touch_rho < float(self.absorption_min_touch_rho):
                    return QualityGateDecision(
                        True, True, "VETO_ABSORPTION_TOUCH_RHO_LOW",
                        f"touch_rho={touch_rho:.3f} < {self.absorption_min_touch_rho:.3f}",
                    )

            if touch_traded_w is None:
                if self.strict_missing_metrics and self.absorption_min_touch_traded_w > 0:
                    return QualityGateDecision(True, True, "VETO_MISSING_TOUCH_TRADED_W", "touch_traded_w missing")
            else:
                if touch_traded_w < float(self.absorption_min_touch_traded_w):
                    return QualityGateDecision(
                        True, True, "VETO_ABSORPTION_TOUCH_TRADED_W_LOW",
                        f"touch_traded_w={touch_traded_w:.3f} < {self.absorption_min_touch_traded_w:.3f}",
                    )

            return QualityGateDecision(True, False, "OK")

        # -------------------------------------------------------------------------
        # obi_spike: require sustained imbalance (obi_sustained=True) if enabled
        # -------------------------------------------------------------------------
        if kind_l == "obi_spike":
            if self.obi_spike_require_sustained:
                sustained = _get_metric(ctx, ("obi_sustained",), allow_of=True)
                if sustained is None:
                    if self.strict_missing_metrics:
                        return QualityGateDecision(True, True, "VETO_MISSING_OBI_SUSTAINED", "obi_spike requires obi_sustained")
                else:
                    if bool(sustained) is not True:
                        return QualityGateDecision(True, True, "VETO_OBI_SPIKE_NOT_SUSTAINED", "obi_sustained=False")
            return QualityGateDecision(True, False, "OK")

        # -------------------------------------------------------------------------
        # extreme: avoid if cancel_to_trade is pathological (if metric exists)
        # -------------------------------------------------------------------------
        if kind_l == "extreme":
            if cancel_to_trade is not None and cancel_to_trade > float(self.extreme_max_cancel_to_trade):
                return QualityGateDecision(
                    True, True, "VETO_EXTREME_CANCEL_TO_TRADE_HIGH",
                    f"cancel_to_trade={cancel_to_trade:.3f} > {self.extreme_max_cancel_to_trade:.3f}",
                )
            return QualityGateDecision(True, False, "OK")

        # Default: not applicable => pass
        return QualityGateDecision(True, False, "OK", notes="no_kind_specific_rules")
