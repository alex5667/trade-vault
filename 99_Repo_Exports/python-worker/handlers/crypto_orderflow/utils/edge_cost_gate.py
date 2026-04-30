"""
EdgeCostGate: execution-cost gate.

This patch hard-fixes timestamp/session/EMA usage to avoid regressions:
  - everywhere we normalize timestamps via domain.time_utils.normalize_ts_ms()
  - if timestamp is invalid / skewed -> we DO NOT use EMA (only default / half-spread)
  - strictness is controlled by env and can be toggled from docker-compose

Additionally:
  - EMA slippage key v2 includes symbol×venue×session×tf×kind (with v1 fallback).
  - optional "gate profile" controls how aggressive we are without cutting too many signals by default.
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import functools

@functools.lru_cache(maxsize=1024)
def _cached_getenv(k, d=None): return os.getenv(k, d)
import time
import math
from dataclasses import dataclass
from typing import Any, Literal, Optional, Set, Tuple

from common.decision_trace import Span, trace_gate

# ---------------------------------------------------------------------
# Prometheus metrics for observability
# ---------------------------------------------------------------------
try:
    from prometheus_client import Counter, Gauge
    edge_time_skew_ms = Gauge(
        "edge_time_skew_ms", 
        "Absolute time skew between signal event time and local wall-clock (ms)"
        ["symbol", "reason_code"]
    )
    edge_bad_time_total = Counter(
        "edge_bad_time_total"
        "Total count of signals rejected or penalized due to large time skew"
        ["symbol", "reason"]
    )

    # Execution-health overlay telemetry (bounded labels).
    edge_exec_health_tighten_total = Counter(
        "edge_exec_health_tighten_total"
        "Total count of EdgeCostGate tighten events due to execution-health rollups"
        ["symbol"]
    )
    edge_exec_health_veto_total = Counter(
        "edge_exec_health_veto_total"
        "Total count of EdgeCostGate veto events due to execution-health rollups"
        ["symbol", "reason_code"]
    )
except ImportError:
    Counter = Gauge = None
    edge_time_skew_ms = edge_bad_time_total = edge_exec_health_tighten_total = edge_exec_health_veto_total = None

# ---------------------------------------------------------------------
# IMPORTANT: timestamp normalization MUST be consistent across pipeline.
# Use ONE canonical timestamp normalizer across the whole pipeline.
# TradeMonitor already uses domain.time_utils.normalize_ts_ms(); gates must do the same
# to avoid regressions when some component suddenly receives seconds / bad clocks / minutes-of-day.
# ---------------------------------------------------------------------
try:
    from domain.time_utils import normalize_ts_ms, session_from_ts_ms
    from domain.gate_profile import strict_enabled
except Exception:  # pragma: no cover (tests may import without full deps)
    normalize_ts_ms = None  # type: ignore
    strict_enabled = None  # type: ignore
    session_from_ts_ms = None  # type: ignore

# -----------------------------------------------------------------------------
# Canonical time/session helpers
#
# IMPORTANT (regression-proofing):
# - We MUST NOT keep a local implementation of session_from_ts_ms() here because:
#     * it silently overrides imported implementations
#     * it can drift from the canonical trading-session rules
# - The source of truth is domain/time_utils.py::session_from_ts_ms
#
# To preserve backward compatibility for any code that imported
# `session_from_ts_ms` from THIS module historically, we re-export the canonical
# function by aliasing it (not re-implementing it).
# -----------------------------------------------------------------------------
session_from_ts_ms = session_from_ts_ms  # re-export canonical implementation

_EPOCH_MS_MIN = 1_000_000_000_000  # 10^12, ~2001-09-09 in ms. Anything below is suspicious.


@functools.lru_cache(maxsize=1024)
def _env_str(name: str, default: str) -> str:
    try:
        v = _cached_getenv(name, default)
        return str(v) if v is not None else str(default)
    except Exception:
        return str(default)

# ---------------------------------------------------------------------
# IMPORTANT: timestamp normalization MUST be consistent across pipeline.
# We intentionally reuse domain.time_utils.normalize_ts_ms() everywhere
# to avoid regressions when some ctx.ts/ctx.ts_ms accidentally comes
# as seconds or non-epoch (minutes-of-day, etc.).
# ---------------------------------------------------------------------
# This patch hardens the pipeline against:
#   - ts_ms <= 0 (missing/invalid) => session="na", NO EMA usage (only default/half-spread)
#   - seconds timestamps (10 digits) => safely normalize to ms
#   - non-epoch / too-small timestamps => treat as invalid and skip EMA (fail-open)
# ---------------------------------------------------------------------------
from domain.time_utils import normalize_ts_ms

# Centralized key + EMA writer/reader utils for execution-cost statistics.
# We keep this in services/ to allow StatsAggregator to write using identical key format.
from services.execution_cost_ema import (
    session_from_ts_ms
)

# Single source of truth for epoch-ms normalization (already used in TradeMonitor)
from domain.time_utils import normalize_ts_ms

# -----------------------------------------------------------------------------
# EdgeCostGate v2: adds EV-based gate ("ev") in addition to legacy move gates.
#
# Legacy:
#   expected_move_bps >= K * (fees_bps + slippage_bps)
#
# New (EV):
#   EV_bps = p_hit_tp1 * tp1_bps - (1 - p_hit_tp1) * stop_bps
#   Require:
#     - p_hit_tp1 >= EDGE_EV_P_MIN
#     - EV_bps    >= K * (fees_bps + slippage_bps)
#
# Where p_hit_tp1 is attached to ctx from online stats (EMA / rolling)
# see services/ev_tp1_stats.py and stats_aggregator.py patch.
# -----------------------------------------------------------------------------
ExpectedMoveMode = Literal["tp1", "rr", "atr", "ev"]


@functools.lru_cache(maxsize=1024)
def _env_float(name: str, default: float) -> float:
    """Безопасное извлечение float из ENV."""
    try:
        return float(_cached_getenv(name, str(default)) or default)
    except Exception:
        return float(default)




def _safe_float(x, default=0.0) -> float:
    """Безопасное извлечение float."""
    try:
        f = float(x)
        return f if math.isfinite(f) else default
    except Exception:
        return float(default)

def _env_int(name: str, default: int) -> int:
    """Безопасное извлечение int из ENV."""
    try:
        return int(float(_cached_getenv(name, str(default)) or default))
    except Exception:
        return int(default)


def _norm_symbol(sym: str) -> str:
    """Нормализация символа: UPPER, без /, без -."""
    return (sym or "").strip().upper().replace("/", "").replace("-", "")


def _parse_csv_set(v: str) -> Set[str]:
    """Парсинг CSV строки в множество lowercase значений."""
    out: Set[str] = set()
    for x in (v or "").split(","):
        s = x.strip().lower()
        if s:
            out.add(s)
    return out

def _clamp01(x: float) -> float:
    try:
        xx = float(x)
    except Exception:
        return float("nan")
    if not math.isfinite(xx):
        return float("nan")
    return 0.0 if xx < 0.0 else (1.0 if xx > 1.0 else xx)


def _first_float(x: Any) -> Optional[float]:
    """
    Превращает scalar/list/tuple/строку в первый float.
    Возвращает None если нельзя.
    """
    if x is None:
        return None
    if isinstance(x, (int, float)):
        f = float(x)
        return f if math.isfinite(f) else None
    if isinstance(x, (list, tuple)):
        if not x:
            return None
        return _first_float(x[0])
    # allow "1,1.5,2.5"
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        head = s.split(",")[0].strip()
        try:
            f = float(head)
            return f if math.isfinite(f) else None
        except Exception:
            return None
    try:
        f = float(x)
        return f if math.isfinite(f) else None
    except Exception:
        return None





@functools.lru_cache(maxsize=1024)
def _env_bool(name: str, default: bool) -> bool:
    v = (_cached_getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _canon_tf(v: Any) -> str:
    s = str(v or "").strip().lower()
    return s if s else "na"


def _normalize_ts_ms_for_costs(ts_ms: Any) -> int:
    """
    Единый нормализатор timestamp для execution-cost логики (EMA/session).

    Жёстко фиксируем (и тестируем):
      - ts_ms <= 0 -> 0 (invalid)
      - seconds -> ms (через normalize_ts_ms)
      - non-epoch -> 0
    """
    try:
        raw = int(float(ts_ms or 0))
    except Exception:
        raw = 0
    try:
        t = int(normalize_ts_ms(raw) or 0)
    except Exception:
        t = raw
    if t <= 0:
        return 0
    # normalize_ts_ms already handles seconds/non-epoch policy
    return int(t)


def _normalize_ctx_ts_ms(ctx: Any, ts_ms: Optional[int]) -> int:
    """
    Единственная точка нормализации ts_ms для EMA/session логики.
    Политика (жёстко фиксируем и тестируем):
      - normalize_ts_ms(...) как базовая нормализация
      - если после нормализации timestamp выглядит как seconds (10-digit) => *1000
      - если ts_ms <= 0 или слишком мал (явно не epoch) => 0 (invalid)
    """
    try:
        # Use ts_ms parameter if provided, otherwise try ctx.ts_ms, ctx.ts, etc.
        raw = ts_ms
        if raw is None:
            raw = getattr(ctx, "ts_ms", None) or getattr(ctx, "ts", None) or getattr(ctx, "timestamp", None)
        raw = int(raw or 0)
    except Exception:
        raw = 0
    try:
        t = int(normalize_ts_ms(raw) or 0)
    except Exception:
        t = raw
    if t <= 0:
        return 0
    # If someone accidentally passes seconds, normalize to ms.
    # 1e12 ~ 2001-09-09 in ms; 1e9 ~ 2001-09-09 in seconds.
    if t < 1_000_000_000_000:
        if t >= 1_000_000_000:
            return int(t * 1000)
        # too small => not epoch -> treat invalid to avoid wrong sessions/keys
        return 0
    return int(t)


def _extract_spread_bps_from_ctx(ctx: Any) -> float:
    """
    Best-effort spread bps extraction (fail-open).
    Supports:
      - ctx.spread_bps
      - ctx.ask/ctx.bid and aliases: a/b, best_ask/best_bid, l1_ask/l1_bid
      - ctx.of.spread_bps / ctx.of.ask / ctx.of.bid  (OF snapshot fallback)
    """
    if ctx is None:
        return 0.0
    try:
        v = getattr(ctx, "spread_bps", None)
        if v is not None:
            x = float(v)
            if math.isfinite(x) and x > 0:
                return float(x)
    except Exception:
        pass

    def _get_from(obj: Any, *names: str) -> Optional[float]:
        for n in names:
            try:
                vv = getattr(obj, n, None)
                if vv is None:
                    continue
                f = float(vv)
                if math.isfinite(f) and f > 0:
                    return f
            except Exception:
                continue
        return None

    def _compute_spread_from_ask_bid(obj: Any) -> float:
        ask = _get_from(obj, "ask", "best_ask", "l1_ask", "a")
        bid = _get_from(obj, "bid", "best_bid", "l1_bid", "b")
        if ask is None or bid is None or ask <= bid:
            return 0.0
        mid = (ask + bid) / 2.0
        if mid <= 0:
            return 0.0
        return float((ask - bid) / mid * 10_000.0)

    # Try ctx directly first
    sp_ctx = _compute_spread_from_ask_bid(ctx)
    if sp_ctx > 0:
        return sp_ctx

    # Fallback: ctx.of (OrderFlow snapshot — where spread lives in our pipeline)
    of = getattr(ctx, "of", None)
    if of is not None:
        try:
            v = getattr(of, "spread_bps", None)
            if v is not None:
                x = float(v)
                if math.isfinite(x) and x > 0:
                    return float(x)
        except Exception:
            pass
        sp_of = _compute_spread_from_ask_bid(of)
        if sp_of > 0:
            return sp_of

    return 0.0


def estimate_slippage_bps(
    ctx: Any
    *
    redis_client: Any
    symbol: str
    venue: str
    ts_ms: Any
    kind: Optional[str] = None
    tf: Optional[str] = None
    default_bps: float = 5.0
    use_spread_half: bool = True
) -> float:
    """
    slippage_bps = max(default_bps, half_spread_bps, EMA(realized_slippage_bps@dims))
    Dims (v2 key, backward-compatible):
      symbol × venue × session × tf × kind

    Timestamp rules (hard-fixed to avoid regressions):
      - normalize_ts_ms() is the ONLY normalizer (seconds->ms, strings->int, invalid->0)
      - if ts invalid/skewed      -> skip EMA (only default/half-spread) OR veto (policy=veto)
      - if ts in seconds          -> normalize_ts_ms() fixes it (no local эвристик)
    """
    # ---------------------------------------------------------------------
    # STRICT TS POLICY (fail-open by default):
    #
    # EDGE_TS_BAD_POLICY:
    #   - "correct_skip_ema" (default): if ts BAD -> correct to now for audit fields
    #                                  but SKIP EMA and use only base slippage.
    #                                  if ts OK  -> USE EMA (normal behavior).
    #   - "skip_ema"                : ALWAYS skip EMA (only default/half-spread).
    #   - "veto"                    : if ts BAD -> return huge slippage (forces gate veto).
    #
    # EDGE_TS_MAX_SKEW_MS:
    #   - absolute skew threshold vs wall-clock now (ms). If exceeded => ts BAD.
    #
    # EDGE_DISABLE_EMA:
    #   - global kill-switch to never read EMA (keeps base behavior).
    # ---------------------------------------------------------------------
    policy = (_cached_getenv("EDGE_TS_BAD_POLICY", "correct_skip_ema") or "").strip().lower()

    try:
        max_skew_ms = int(float(_cached_getenv("EDGE_TS_MAX_SKEW_MS", "21600000")))  # 6h
    except Exception:
        max_skew_ms = 21600000
    try:
        veto_bps = float(_cached_getenv("EDGE_TS_BAD_VETO_BPS", "1000000"))
    except Exception:
        veto_bps = 1_000_000.0

    disable_ema = str(_cached_getenv("EDGE_DISABLE_EMA", "0")).strip() in {"1", "true", "yes", "on"}

    now_ms = get_ny_time_millis()
    # normalize with the shared normalizer (handles seconds->ms, bad strings, NaN, etc.)
    tsm = int(normalize_ts_ms(ts_ms))

    ts_invalid = False
    ts_reason = ""
    ts_corrected = False

    if tsm <= 0:
        ts_invalid = True
        ts_reason = "ts<=0"
        if edge_bad_time_total:
            edge_bad_time_total.labels(symbol=symbol, reason="ts_zero").inc()
    else:
        skew = abs(int(tsm) - int(now_ms))
        # record raw skew for observability
        if edge_time_skew_ms:
            edge_time_skew_ms.labels(symbol=symbol, reason_code="gate_check").set(float(skew))

        if max_skew_ms > 0 and skew > max_skew_ms:
            ts_invalid = True
            ts_reason = f"skew_ms={skew}"
            if edge_bad_time_total:
                edge_bad_time_total.labels(symbol=symbol, reason="skew_exceeded").inc()

    # Apply policy (only affects BAD ts)
    if ts_invalid and policy.startswith("correct"):
        # Correct to now ONLY for audit stability / downstream debug fields.
        # IMPORTANT: correction does NOT mean we trust ts; EMA must still be skipped in BAD case.
        tsm = now_ms
        ts_corrected = True

    # Attach lightweight audit flags to ctx (safe: dynamic attrs; no protocol break)
    try:
        setattr(ctx, "_ts_ms_norm", int(tsm))
        setattr(ctx, "_ts_invalid", bool(ts_invalid))
        setattr(ctx, "_ts_corrected", bool(ts_corrected))
        setattr(ctx, "_ts_reason", str(ts_reason))
        setattr(ctx, "_ts_policy", str(policy))
        setattr(ctx, "_ts_skew_ms", int(abs(int(ts_ms or 0) - int(now_ms))) if ts_ms else 0)
    except Exception:
        pass

    # Compute base (default vs spread/2). This part must NEVER depend on EMA.
    base = float(default_bps)
    if use_spread_half:
        try:
            # NOTE: имя функции в этом модуле — _extract_spread_bps_from_ctx()
            sp = float(_extract_spread_bps_from_ctx(ctx))
            if sp > 0 and math.isfinite(sp):
                base = max(base, float(sp) * 0.5)
        except Exception:
            pass

    # If ts invalid => do NOT use EMA (avoid poisoning keys). Optionally force veto.
    if ts_invalid:
        if policy == "veto":
            return float(max(base, veto_bps))
        return float(base)

    # Valid ts from here.
    if disable_ema or policy == "skip_ema":
        return float(base)

    # only here we may use EMA (ts is valid and profile allows)
    sess = str(getattr(ctx, "session", None) or session_from_ts_ms(int(tsm)) or "na")
    tfv = _canon_tf(tf or getattr(ctx, "tf", None) or getattr(ctx, "timeframe", None) or "na")
    knd = (
        kind
        or getattr(ctx, "kind", None)
        or getattr(ctx, "signal_kind", None)
        or getattr(ctx, "strategy", None)
        or "na"
    )
    knd = str(knd or "na").strip().lower() or "na"

    # Key = symbol×venue×session×tf×kind (backward-compatible fallback below)
    ema = _load_slippage_ema_bps(
        redis_client
        symbol=str(symbol or "").upper()
        venue=str(venue or "na").lower()
        session=str(sess or "na").lower()
        tf=str(tfv or "na").lower()
        kind=str(knd or "na").lower()
    )
    if ema is not None and math.isfinite(float(ema)) and float(ema) > 0:
        return float(max(base, float(ema)))
    return float(base)


def _normalize_ts_ms_fail_open(ts_ms: Any) -> int:
    """
    Unified timestamp normalizer for gates.
    Policy:
      - invalid / non-epoch -> 0  (forces session="na" and skips EMA)
      - epoch seconds       -> convert to ms
      - epoch ms            -> normalize_ts_ms(...)
    """
    try:
        raw = int(float(ts_ms or 0))
    except Exception:
        return 0
    if raw <= 0:
        return 0

    # Guard against "minutes-of-day" and other non-epoch small ints.
    # Anything below ~2001-09-09 in seconds is not a plausible epoch timestamp.
    if raw < 1_000_000_000:
        return 0

    # Seconds -> ms (common accidental regression).
    if raw < 1_000_000_000_000:
        raw = raw * 1000

    try:
        if normalize_ts_ms is not None:
            return int(normalize_ts_ms(int(raw)))
        return int(raw)
    except Exception:
        return int(raw)


def _load_slippage_ema_bps(
    redis_client: Any, *
    symbol: str, venue: str, session: str, tf: str, kind: str
) -> Optional[float]:
    """
    Reads EMA slippage from Redis.
    New key includes kind, but we keep backward compatibility:
      1) slipema:{symbol}:{venue}:{session}:{tf}:{kind}
      2) slipema:{symbol}:{venue}:{session}:{tf}:na
      3) (legacy) slipema:{symbol}:{venue}:{session}:{tf}
    """
    if redis_client is None:
        return None
    try:
        symbol_u = str(symbol or "").upper()
        venue_l = str(venue or "na").lower()
        sess_l  = str(session or "na").lower()
        tf_l    = str(tf or "na").lower()
        kind_l  = str(kind or "na").lower()

        keys = [
            f"slipema:{symbol_u}:{venue_l}:{sess_l}:{tf_l}:{kind_l}"
            f"slipema:{symbol_u}:{venue_l}:{sess_l}:{tf_l}:na"
            f"slipema:{symbol_u}:{venue_l}:{sess_l}:{tf_l}"
        ]

        min_n = int(float(_cached_getenv("EDGE_SLIP_EMA_MIN_SAMPLES", "20")))

        for key in keys:
            d = redis_client.hgetall(key) or {}
            # tolerate bytes/str
            def _g(name: str) -> str:
                v = d.get(name) or d.get(name.encode("utf-8"))
                if v is None:
                    return ""
                return v.decode("utf-8", errors="ignore") if isinstance(v, (bytes, bytearray)) else str(v)

            n = int(float(_g("samples") or "0"))
            if n < max(1, min_n):
                continue
            ema = float(_g("ema_slip_bps") or _g("ema_slippage_bps") or "0")
            if ema > 0 and math.isfinite(ema):
                return float(ema)
    except Exception:
        return None
    return None


def _hget_ema(redis_client: Any, key: str, *, min_n: int) -> Optional[float]:
    """
    Expected hash fields (best-effort, fail-open):
      - samples / n
      - ema_bps / ema_slippage_bps / ema
    """
    try:
        d = redis_client.hgetall(key) or {}
    except Exception:
        return None

    # Redis may return bytes.
    def _b2s(x: Any) -> str:
        if isinstance(x, bytes):
            return x.decode("utf-8", errors="ignore")
        return str(x)

    try:
        # Normalize keys to strings.
        dd: Dict[str, str] = {}
        for k, v in dict(d).items():
            dd[_b2s(k)] = _b2s(v)
        n = int(float(dd.get("samples") or dd.get("n") or 0))
        if n < int(min_n):
            return None
        ema = float(dd.get("ema_bps") or dd.get("ema_slippage_bps") or dd.get("ema") or 0.0)
        if ema > 0 and math.isfinite(ema):
            return float(ema)
    except Exception:
        return None
    return None


def _load_drift_active(
    redis_client: Any
    *
    symbol: str
    venue: str
    session: str
    tf: str
    kind: str
) -> Tuple[float, float, str]:
    """
    Read active drift factor (temporary tightening) from Redis.

    Keys:
      - v2: drift:active:v2:{symbol}:{venue}:{session}:{tf}:{kind}
      - v1: drift:active:v1:{symbol}:{venue}:{session}:{tf}

    Hash fields:
      factor, score, feature

    Fail-open:
      - missing/invalid -> (1.0, nan, "")
    """
    if redis_client is None:
        return 1.0, float("nan"), ""

    include_kind = (_cached_getenv("FEATURE_DRIFT_INCLUDE_KIND", "0") or "").strip().lower() in {"1", "true", "yes", "on"}

    def _b2s(x: Any) -> str:
        if isinstance(x, bytes):
            return x.decode("utf-8", errors="ignore")
        return str(x)

    def _read(key: str) -> Optional[Tuple[float, float, str]]:
        try:
            d = redis_client.hgetall(key) or {}
        except Exception:
            return None
        dd: Dict[str, str] = {}
        try:
            for k, v in dict(d).items():
                dd[_b2s(k)] = _b2s(v)
        except Exception:
            return None
        try:
            f = float(dd.get("factor") or 1.0)
            s = float(dd.get("score") or float("nan"))
            feat = str(dd.get("feature") or "")
            if not math.isfinite(f) or f <= 0:
                return None
            return float(f), float(s), str(feat)
        except Exception:
            return None

    sym = str(symbol or "").upper()
    ven = str(venue or "na").lower()
    sess = str(session or "na").lower()
    tfv = str(tf or "na").lower()
    knd = str(kind or "na").lower()

    if include_kind:
        k2 = f"drift:active:v2:{sym}:{ven}:{sess}:{tfv}:{knd}"
        r2 = _read(k2)
        if r2 is not None:
            return r2
    k1 = f"drift:active:v1:{sym}:{ven}:{sess}:{tfv}"
    r1 = _read(k1)
    if r1 is not None:
        return r1
    return 1.0, float("nan"), ""



# -----------------------------------------------------------------------------
# Execution-health overlay (TCA rollups)
#
# Phase G (P6): make EdgeCostGate aware of execution degradation:
#   - IS_p95 (implementation shortfall) too high
#   - permanent impact too high
#   - realized spread too low (adverse selection proxy)
#
# We read these from Redis rollups produced by post-trade TCA worker.
#
# IMPORTANT:
#   - Fail-open by default: missing redis / missing keys -> no veto, no tighten.
#   - No high-cardinality labels: these values go to ctx/audit, and optionally to
#     Prometheus counters with bounded symbol allowlist elsewhere.
# -----------------------------------------------------------------------------

def _parse_csv_ints(v: str, *, default: Tuple[int, ...]) -> Tuple[int, ...]:
    out = []
    for x in (v or "").split(","):
        s = x.strip()
        if not s:
            continue
        try:
            out.append(int(float(s)))
        except Exception:
            continue
    return tuple(out) if out else tuple(default)


def _redis_get_float_best_effort(r: Any, key: str) -> Optional[float]:
    """
    Reads a float value from Redis using GET (preferred) or HGETALL fallback.

    We keep this extremely defensive:
      - bytes/str supported
      - empty / NaN -> None
    """
    if r is None:
        return None
    try:
        if hasattr(r, "get"):
            v = r.get(key)
            if v is None:
                return None
            if isinstance(v, (bytes, bytearray)):
                v = v.decode("utf-8", errors="ignore")
            f = float(v)
            return float(f) if math.isfinite(f) else None
    except Exception:
        pass
    # Some deployments store rollups as hashes (rare). Try to read common fields.
    try:
        if hasattr(r, "hgetall"):
            d = r.hgetall(key) or {}
            def _b2s(x: Any) -> str:
                if isinstance(x, (bytes, bytearray)):
                    return x.decode("utf-8", errors="ignore")
                return str(x)
            dd = { _b2s(k): _b2s(v) for k, v in dict(d).items() }
            for fld in ("value", "p95", "p50", "val"):
                if fld in dd:
                    f = float(dd[fld])
                    if math.isfinite(f):
                        return float(f)
    except Exception:
        pass
    return None


def _tca_key_candidates(
    *
    metric: str
    symbol: str
    venue: str
    session: str
    tf: str
    kind: str
    side: str
    delta_sec: Optional[int] = None
) -> Tuple[str, ...]:
    """
    Generates a bounded list of candidate Redis keys for a given TCA rollup metric.

    Canonical format (recommended by our P1 TCA worker):
      - is_p95:
          tca:is_p95_bps:<sym>:<venue>:<session>:<tf>:<kind>:<side>
      - realized_spread (p50) and perm_impact (p95) include delta_sec:
          tca:realized_spread_p50_bps:<d>:<sym>:<venue>:<session>:<tf>:<kind>:<side>
          tca:perm_impact_p95_bps:<d>:<sym>:<venue>:<session>:<tf>:<kind>:<side>

    Fallbacks (best-effort, fail-open):
      - tf -> all
      - kind -> all
      - session -> all
      - side -> all
    """
    sym = str(symbol or "").upper()
    ven = str(venue or "na").lower()
    sess = str(session or "na").lower()
    tfv = str(tf or "all").lower()
    knd = str(kind or "all").lower()
    sde = str(side or "all").lower()

    def _mk(ses: str, tf_: str, k: str, sd: str) -> str:
        if delta_sec is None:
            return f"tca:{metric}:{sym}:{ven}:{ses}:{tf_}:{k}:{sd}"
        return f"tca:{metric}:{int(delta_sec)}:{sym}:{ven}:{ses}:{tf_}:{k}:{sd}"

    keys = [
        _mk(sess, tfv, knd, sde)
        _mk(sess, "all", knd, sde)
        _mk(sess, tfv, "all", sde)
        _mk(sess, "all", "all", sde)
        _mk("all", "all", "all", sde)
        _mk(sess, "all", "all", "all")
        _mk("all", "all", "all", "all")
    ]
    return tuple(dict.fromkeys(keys))


def _load_exec_health_rollups(
    redis_client: Any
    *
    symbol: str
    venue: str
    session: str
    tf: str
    kind: str
    side: str
    delta_list: Tuple[int, ...]
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[int]]:
    """
    Reads:
      - is_p95_bps (no delta)
      - perm_impact_p95_bps (max across deltas)
      - realized_spread_p50_bps (min across deltas)  # more negative = worse adverse selection
    Returns: (is_p95, perm_impact_p95_max, realized_p50_min, chosen_delta_for_perm)
    """
    is_p95 = None
    for k in _tca_key_candidates(
        metric="is_p95_bps"
        symbol=symbol, venue=venue, session=session, tf=tf, kind=kind, side=side
    ):
        is_p95 = _redis_get_float_best_effort(redis_client, k)
        if is_p95 is not None:
            break

    perm_max = None
    perm_delta = None
    for d in delta_list:
        val = None
        for k in _tca_key_candidates(
            metric="perm_impact_p95_bps"
            delta_sec=int(d)
            symbol=symbol, venue=venue, session=session, tf=tf, kind=kind, side=side
        ):
            val = _redis_get_float_best_effort(redis_client, k)
            if val is not None:
                break
        if val is None:
            continue
        if perm_max is None or float(val) > float(perm_max):
            perm_max = float(val)
            perm_delta = int(d)

    real_min = None
    for d in delta_list:
        val = None
        for k in _tca_key_candidates(
            metric="realized_spread_p50_bps"
            delta_sec=int(d)
            symbol=symbol, venue=venue, session=session, tf=tf, kind=kind, side=side
        ):
            val = _redis_get_float_best_effort(redis_client, k)
            if val is not None:
                break
        if val is None:
            continue
        if real_min is None or float(val) < float(real_min):
            real_min = float(val)

    return is_p95, perm_max, real_min, perm_delta


@dataclass(frozen=True)
class EdgeCostGateDecision:
    """
    Decision object для детерминированных unit tests + structured logging.
    
    Содержит полную информацию о решении gate:
    - apply: применялся ли gate
    - veto: заблокирован ли сигнал
    - reason_code: код причины (для метрик/логов)
    - expected_move_bps: ожидаемое движение цены (bps)
    - threshold_bps: минимальное требуемое движение (bps)
    - fees_bps: комиссия (bps)
    - slippage_bps: проскальзывание (bps)
    - k: применённый коэффициент
    - mode: метод оценки edge (tp1/rr/atr)
    - notes: дополнительные заметки
    """
    apply: bool
    veto: bool
    reason_code: str
    expected_move_bps: float
    threshold_bps: float
    fees_bps: float
    slippage_bps: float
    k: float
    mode: ExpectedMoveMode
    notes: str = ""

    # --- EV-mode diagnostics (backward-compatible defaults) ---
    # p_hit_tp1: online probability estimate (EMA/rolling) in [0..1]
    p_hit_tp1: float = float("nan")
    p_min: float = float("nan")
    # tp1_bps/stop_bps are computed from ctx levels (tp1/sl vs entry)
    tp1_bps: float = float("nan")
    stop_bps: float = float("nan")
    # EV itself (in bps). In EV mode we also mirror it into expected_move_bps
    ev_bps: float = float("nan")
    # stats sample count & source marker (for logs)
    stats_n: int = 0
    stats_src: str = ""

    # --- Feature drift alarm (optional, backward-compatible) ---
    # factor>1 means we temporarily tightened execution gates due to distribution shift
    drift_factor: float = 1.0
    drift_score: float = float("nan")
    drift_feature: str = ""

    # --- Compatibility fields (Telemetery/Ingestor) ---
    buffer_bps: float = 0.0
    total_costs_bps: float = 0.0
    edge_source: str = "none"

    # --- Execution-health overlay (TCA rollups) ---
    # Filled when EdgeCostGate consults post-trade TCA rollups from Redis.
    exec_is_p95_bps: float = float("nan")
    exec_perm_impact_p95_bps: float = float("nan")
    exec_realized_spread_p50_bps: float = float("nan")
    exec_perm_impact_delta_sec: int = 0
    exec_health_mode: str = ""
    exec_health_tighten_add_bps: float = 0.0

    # --- Compatibility Properties ----
    @property
    def passed(self) -> bool:
        return not self.veto

    @property
    def veto_reason(self) -> Optional[str]:
        return self.reason_code if self.veto else None

    @property
    def cost_multiplier(self) -> float:
        return float(self.k)

    @property
    def expected_edge_bps(self) -> float:
        return float(self.expected_move_bps)

    @property
    def required_edge_bps(self) -> float:
        return float(self.threshold_bps)

    @property
    def edge_ratio(self) -> float:
        req = float(self.threshold_bps)
        exp = float(self.expected_move_bps)
        if req > 0:
             return exp / req
        return float("inf") if exp > 0 else 0.0


@dataclass
class EdgeCostGate:
    """
    Step 2 anti-churn gate:

      expected_move_bps >= K * (fees_bps + slippage_bps)

    Practical notes:
      - Gate должен выполняться *перед emit* сигнала для снижения churn.
      - Точность зависит от наличия в ctx:
          ctx.entry_price + ctx.tp1_price  (лучший вариант)
        ИЛИ ctx.entry_price + ctx.sl_price + ctx.tp_rr
        ИЛИ ctx.entry_price + ctx.atr + ctx.tp_atr_mult (or tp_atr_mults[0])
      - Если levels отсутствуют, по умолчанию fail-open (EDGE_COST_STRICT_MISSING_LEVELS=0).
    """

    # ---------------------------------------------------------------------
    # Единственный источник истины для reason_code по Cost/Edge гейту.
    # Это те коды, которые должны попадать в:
    #   - veto metrics
    #   - аналитические агрегаты
    #   - debug logs
    #
    # Требование P0: убрать "двойные истины" и разношерстные reason_code.
    # ---------------------------------------------------------------------
    REASON_OK = "edge_cost:ok"
    REASON_SKIP = "edge_cost:skip"  # gate disabled / kind not applicable
    REASON_BELOW_K = "edge_cost:below_k"
    REASON_MISSING_LEVELS = "edge_cost:missing_levels"
    # EV-mode specific
    REASON_EV_MISSING_INPUTS = "edge_cost:ev:missing_inputs"
    REASON_EV_INSUFFICIENT_STATS = "edge_cost:ev:insufficient_stats"
    REASON_EV_PROB = "edge_cost:ev:prob"
    REASON_EV_BELOW_K = "edge_cost:ev:below_k"

    def passes(self, *, ctx: Any, kind: str, symbol: str) -> bool:
        """
        Bool-адаптер для hot-path:
          True  -> сигнал проходит gate
          False -> veto
        IMPORTANT: не дублировать формулы. Единственная логика в evaluate().
        """
        return not self.evaluate(ctx=ctx, kind=kind, symbol=symbol).veto

    enabled: bool
    mode: ExpectedMoveMode
    strict_missing_levels: bool
    apply_kinds: Set[str]

    # K multiplier
    k_default: float
    k_by_symbol: Dict[str, float]

    # costs in bps
    fees_bps_default: float
    slippage_bps_default: float
    slippage_use_spread_half: bool

    # hard floor: минимально допустимый expected_move_bps (anti-churn)
    min_expected_move_bps_default: float
    min_expected_move_bps_by_symbol: Dict[str, float]

    # NEW: optional redis client for EMA slippage model
    redis: Any = None

    # --- EV mode knobs ---
    # Cold-start guard: require at least N samples in tp1 stats to trust p_hit_tp1.
    ev_min_trades: int = 40
    # If True: missing/insufficient stats => veto (fail-closed). Else: fail-open.
    ev_strict_missing_stats: bool = False
    # Dynamic K: adjust K based on volatility (ATR)
    ev_dynamic_k_enabled: bool = False
    ev_dynamic_k_atr_mult: float = 0.0  # K = K_base * (1 + atr_mult * normalized_atr)
    # Minimal probability to allow trades (cuts low-probability noise aggressively)
    ev_p_min: float = 0.55
    # Per-kind probability thresholds (e.g., breakout needs higher p than absorption)
    ev_p_min_by_kind: Dict[str, float] = None  # type: ignore

    # Buffer bps: доп. запас на плохие филлы (P1-9)
    buffer_base_bps: float = 0.0
    buffer_atr_mult: float = 0.0
    buffer_spread_mult: float = 0.0
    buffer_max_bps: float = 25.0

    @classmethod
    def from_env(cls) -> "EdgeCostGate":
        """Создание gate из переменных окружения."""
        enabled = _env_bool("EDGE_COST_GATE_ENABLED", False)
        mode = (_cached_getenv("EDGE_EXPECTED_MOVE_MODE", "tp1") or "tp1").strip().lower()
        if mode not in {"tp1", "rr", "atr", "ev"}:
            mode = "tp1"

        strict_missing_levels = _env_bool("EDGE_COST_STRICT_MISSING_LEVELS", False)
        apply_kinds = _parse_csv_set(_cached_getenv("EDGE_COST_APPLY_KINDS", "") or "")

        k_default = _env_float("EDGE_COST_K", 4.0)

        # Symbol-specific K overrides:
        # EDGE_COST_K_BTCUSDT, EDGE_COST_K_ETHUSDT, ...
        k_by_symbol: Dict[str, float] = {}
        for key, val in os.environ.items():
            if key.startswith("EDGE_COST_K_") and key != "EDGE_COST_K":
                sym = _norm_symbol(key[len("EDGE_COST_K_"):])
                try:
                    k_by_symbol[sym] = float(val)
                except Exception:
                    continue

        # If EDGE_FEES_BPS_DEFAULT is not set, derive from CRYPTO_COMMISSION_RATE.
        # Example: 0.0004 => 4 bps one-way => 8 bps round-trip.
        if _cached_getenv("EDGE_FEES_BPS_DEFAULT") is None and _cached_getenv("CRYPTO_COMMISSION_RATE") is not None:
            try:
                one_way_bps = float(_cached_getenv("CRYPTO_COMMISSION_RATE", "0")) * 10_000.0
                fees_bps_default = max(0.0, 2.0 * one_way_bps)
            except Exception:
                fees_bps_default = 4.0
        else:
            fees_bps_default = _env_float("EDGE_FEES_BPS_DEFAULT", 4.0)

        slippage_bps_default = _env_float("EDGE_SLIPPAGE_BPS_DEFAULT", 4.0)
        slippage_use_spread_half = _env_bool("EDGE_SLIPPAGE_USE_SPREAD_HALF", True)

        # Minimal expected move floor:
        # EDGE_MIN_EXPECTED_MOVE_BPS=20
        # EDGE_MIN_EXPECTED_MOVE_BPS_BTCUSDT=25
        min_expected_move_bps_default = _env_float("EDGE_MIN_EXPECTED_MOVE_BPS", 0.0)
        min_expected_move_bps_by_symbol: Dict[str, float] = {}
        for key, val in os.environ.items():
            if key.startswith("EDGE_MIN_EXPECTED_MOVE_BPS_") and key != "EDGE_MIN_EXPECTED_MOVE_BPS":
                sym = _norm_symbol(key[len("EDGE_MIN_EXPECTED_MOVE_BPS_"):])
                try:
                    min_expected_move_bps_by_symbol[sym] = float(val)
                except Exception:
                    continue

        # EV-mode parameters (defaults chosen to be practical for crypto)
        # Recommended starting point:
        #   EDGE_EXPECTED_MOVE_MODE=ev
        #   EDGE_EV_P_MIN=0.55
        #   EDGE_EV_MIN_TRADES=40
        #   EDGE_EV_STRICT_MISSING_STATS=0   (fail-open until stats warm up)
        #   EDGE_EV_P_MIN_BREAKOUT=0.58  (per-kind override)
        #   EDGE_EV_P_MIN_ABSORPTION=0.52
        #   EDGE_EV_DYNAMIC_K_ENABLED=1
        #   EDGE_EV_DYNAMIC_K_ATR_MULT=0.5
        ev_p_min = _env_float("EDGE_EV_P_MIN", 0.55)
        ev_min_trades = _env_int("EDGE_EV_MIN_TRADES", 40)
        ev_strict_missing_stats = _env_bool("EDGE_EV_STRICT_MISSING_STATS", False)
        
        # Per-kind p_min configuration
        ev_p_min_by_kind = {}
        for key, val in os.environ.items():
            if key.startswith("EDGE_EV_P_MIN_") and key != "EDGE_EV_P_MIN":
                kind = key[len("EDGE_EV_P_MIN_"):].lower()
                try:
                    ev_p_min_by_kind[kind] = float(val)
                except Exception:
                    continue
        
        # Dynamic K based on volatility
        ev_dynamic_k_enabled = _env_bool("EDGE_EV_DYNAMIC_K_ENABLED", False)
        ev_dynamic_k_atr_mult = _env_float("EDGE_EV_DYNAMIC_K_ATR_MULT", 0.5)

        # Buffer bps config (P1-9)
        buffer_base_bps = _env_float("EDGE_BUFFER_BASE_BPS", 2.0)
        buffer_atr_mult = _env_float("EDGE_BUFFER_ATR_MULT", 0.05)
        buffer_spread_mult = _env_float("EDGE_BUFFER_SPREAD_MULT", 0.1)
        buffer_max_bps = _env_float("EDGE_BUFFER_MAX_BPS", 25.0)

        return cls(
            enabled=enabled
            mode=mode,  # type: ignore[arg-type]
            strict_missing_levels=strict_missing_levels
            apply_kinds=apply_kinds
            k_default=k_default
            k_by_symbol=k_by_symbol
            fees_bps_default=fees_bps_default
            slippage_bps_default=slippage_bps_default
            slippage_use_spread_half=slippage_use_spread_half
            min_expected_move_bps_default=min_expected_move_bps_default
            min_expected_move_bps_by_symbol=min_expected_move_bps_by_symbol
            ev_min_trades=int(ev_min_trades)
            ev_strict_missing_stats=bool(ev_strict_missing_stats)
            ev_dynamic_k_enabled=bool(ev_dynamic_k_enabled)
            ev_dynamic_k_atr_mult=float(ev_dynamic_k_atr_mult)
            ev_p_min=float(ev_p_min)
            ev_p_min_by_kind=ev_p_min_by_kind
            buffer_base_bps=float(buffer_base_bps)
            buffer_atr_mult=float(buffer_atr_mult)
            buffer_spread_mult=float(buffer_spread_mult)
            buffer_max_bps=float(buffer_max_bps)
        )

    def _k_for(self, symbol: str) -> float:
        """Получение K коэффициента для символа (с fallback на default)."""
        s = _norm_symbol(symbol)
        return float(self.k_by_symbol.get(s, self.k_default))

    def _p_min_for_kind(self, kind: str) -> float:
        """
        Получение минимального порога вероятности для kind.
        
        Позволяет настраивать разные пороги для разных типов сигналов:
        - breakout может требовать выше p (0.58)
        - absorption может быть мягче (0.52)
        """
        k = (kind or "").strip().lower()
        return float(self.ev_p_min_by_kind.get(k, self.ev_p_min))

    def _get_buffer_bps(self, ctx: Any, symbol: str) -> float:
        """
        Динамический буфер для компенсации проскальзывания при исполнении (fills).
        Использует ATR как прокси для волатильности и Spread как прокси для ликвидности.
        """
        # 1) ATR contribution
        # Try to get daily_atr_bps (normalized)
        atr_bps = _safe_float(getattr(ctx, "daily_atr_bps", None))
        if atr_bps is None or atr_bps <= 0:
            # Fallback to absolute ATR if available
            atr = (
                getattr(ctx, "atr", None)
                or getattr(ctx, "atr14", None)
                or getattr(ctx, "atr_1m", None)
            )
            # Find entry price
            of = getattr(ctx, "of", None)
            entry = (
                getattr(ctx, "entry_price", None)
                or getattr(ctx, "entry", None)
                or (getattr(of, "price", None) if of is not None else None)
                or getattr(ctx, "price", None)
            )
            if atr is not None and entry is not None and float(entry) > 0:
                atr_bps = (float(atr) / float(entry)) * 10_000.0
            else:
                atr_bps = 0.0

        # 2) Spread contribution
        spread_bps = 0.0
        try:
            sp = _extract_spread_bps_from_ctx(ctx)
            if sp > 0 and math.isfinite(sp):
                spread_bps = float(sp)
        except Exception:
            pass

        # 3) Formula: Base + Mult1*ATR_bps + Mult2*Spread_bps
        buf = float(self.buffer_base_bps)
        buf += float(atr_bps) * float(self.buffer_atr_mult)
        buf += float(spread_bps) * float(self.buffer_spread_mult)

        # 4) Clamp to [0, max]
        return float(min(float(self.buffer_max_bps), max(0.0, buf)))

    def _dynamic_k(self, k_base: float, ctx: Any) -> float:
        """
        Динамический K на основе волатильности (ATR).
        
        Логика:
          - Высокая волатильность => выше K (более строгий порог)
          - Низкая волатильность => ниже K (менее строгий)
        
        Formula:
          K_dynamic = K_base * (1 + atr_mult * normalized_atr)
          
        где normalized_atr = (atr - typical_atr) / typical_atr
        
        Например, если ATR выше обычного на 50% и atr_mult=0.5:
          K_dynamic = K_base * (1 + 0.5 * 0.5) = K_base * 1.25
        """
        if not self.ev_dynamic_k_enabled:
            return float(k_base)
        
        # Extract ATR from ctx
        of = getattr(ctx, "of", None)
        atr = (
            getattr(ctx, "atr", None)
            or getattr(ctx, "atr14", None)
            or getattr(ctx, "atr_1m", None)
            or (getattr(of, "atr", None) if of is not None else None)
        )
        
        if atr is None:
            return float(k_base)
        
        try:
            atr_f = float(atr)
        except Exception:
            return float(k_base)
        
        if not math.isfinite(atr_f) or atr_f <= 0.0:
            return float(k_base)
        
        # Normalize ATR (simplified: assume typical_atr is stored or estimated)
        # For crypto: typical ATR ~ 0.5-2% of price
        # We'll use a simple heuristic: higher ATR => higher K
        # normalized_atr = (atr_f - 1.0) / 1.0  # if typical is 1.0
        # For now, use direct scaling: K *= (1 + mult * min(atr_f, 5.0))
        
        # Cap ATR contribution to avoid extreme K
        atr_capped = min(float(atr_f), 5.0)
        k_mult = 1.0 + float(self.ev_dynamic_k_atr_mult) * (atr_capped / 2.0)
        
        return float(k_base) * float(k_mult)

    def _min_move_for(self, symbol: str) -> float:
        sym = _norm_symbol(symbol)
        return float(self.min_expected_move_bps_by_symbol.get(sym, self.min_expected_move_bps_default))

    def _costs_bps(self, ctx: Any, *, kind: str, symbol: str, tf: Optional[str] = None) -> Tuple[float, float]:
        """
        Оценка fees_bps / slippage_bps.
        ---------------------------------------------------------------------
        """
        fees_bps = float(self.fees_bps_default)
        redis_client = getattr(self, "redis", None) or getattr(ctx, "redis", None)

        # IMPORTANT (anti-regression):
        #  - НЕ приводим ts к int() здесь. В реальном потоке ts может быть строкой "1700.."
        #    float-строкой "1700..0", None, "nan", и т.д.
        #  - ЕДИНЫЙ нормализатор normalize_ts_ms() живёт внутри estimate_slippage_bps().
        #  - Поэтому сюда передаём "сырой" ts (Any), а не локальные эвристики.
        raw_ts = (
            getattr(ctx, "ts_ms", None)
            or getattr(ctx, "ts", None)
            or getattr(ctx, "timestamp", None)
            or 0
        )

        slippage_bps = estimate_slippage_bps(
            ctx
            redis_client=redis_client
            symbol=str(symbol or getattr(ctx, "symbol", "") or "")
            venue=str(getattr(ctx, "venue", "") or "na")
            ts_ms=raw_ts
            # allow ctx.tf to participate in v2 key without depending on caller
            tf=str(getattr(ctx, "tf", None) or getattr(ctx, "timeframe", None) or "na")
            default_bps=self.slippage_bps_default
            use_spread_half=self.slippage_use_spread_half
        )

        # ------------------------------------------------------------------
        # Drift tightening toggle:
        #  - default profile: off unless EDGE_DRIFT_TIGHTEN=1
        #  - strict profile : on by default
        # ------------------------------------------------------------------
        strict = strict_enabled() if strict_enabled is not None else False
        tighten = _env_bool("EDGE_DRIFT_TIGHTEN", strict)

        # Inflate slippage under drift (cuts a lot of trades).
        drift_factor = 1.0
        drift_score = float("nan")
        drift_feat = ""
        if tighten:
            try:
                # IMPORTANT (anti-regression): use the SAME normalized ts as estimate_slippage_bps().
                # Do NOT re-normalize here - it breaks string/float-string ts handling.
                tsm = getattr(ctx, "_ts_ms_norm", None)
                if tsm and tsm > 0:
                    sess = str(getattr(ctx, "session", None) or session_from_ts_ms(int(tsm)) or "na")
                    tfv = _canon_tf(getattr(ctx, "tf", None) or getattr(ctx, "timeframe", None) or "na")
                    drift_factor, drift_score, drift_feat = _load_drift_active(
                        redis_client
                        symbol=str(symbol or "")
                        venue=str(getattr(ctx, "venue", "") or "na")
                        session=str(sess or "na")
                        tf=str(tfv or "na")
                        kind=str(kind or "na")
                    )
                    if not math.isfinite(float(drift_factor)) or float(drift_factor) <= 0:
                        drift_factor = 1.0
            except Exception:
                drift_factor = 1.0

            if float(drift_factor) > 1.0:
                try:
                    cap = float(_cached_getenv("EDGE_DRIFT_SLIPPAGE_CAP_MULT", "3.0"))
                except Exception:
                    cap = 3.0
                mult = float(min(cap, max(1.0, float(drift_factor))))
                slippage_bps = float(slippage_bps) * mult

        try:
            setattr(ctx, "_drift_factor", float(drift_factor))
            setattr(ctx, "_drift_score", float(drift_score))
            setattr(ctx, "_drift_feature", str(drift_feat))
            setattr(ctx, "_edge_drift_tighten", bool(tighten))
        except Exception:
            pass

        return fees_bps, slippage_bps

    @staticmethod
    def _bps(a: float, b: float) -> float:
        """
        Абсолютное движение от a->b в basis points относительно a.
        """
        if a is None or b is None:
            return float("nan")
        aa = float(a)
        bb = float(b)
        if aa <= 0.0 or not math.isfinite(aa) or not math.isfinite(bb):
            return float("nan")
        return abs(bb - aa) / aa * 10_000.0

    def _ev_bps(self, ctx: Any) -> Tuple[float, float, float, float, int, str]:
        """
        Compute EV in bps using:
          EV_bps = p_hit_tp1 * tp1_bps - (1 - p_hit_tp1) * stop_bps

        Inputs required on ctx:
          entry_price (or entry/price/of.price)
          tp1_price   (or tp1)
          sl_price    (or sl)
          tp1_hit_prob (0..1) + tp1_hit_n (sample count) + tp1_hit_src (optional)

        Returns:
          (ev_bps, tp1_bps, stop_bps, p, n, src)
        """
        of = getattr(ctx, "of", None)
        entry = (
            getattr(ctx, "entry_price", None)
            or getattr(ctx, "entry", None)
            or getattr(ctx, "price", None)
            or (getattr(of, "price", None) if of is not None else None)
        )
        tp1 = getattr(ctx, "tp1_price", None) or getattr(ctx, "tp1", None)
        sl = getattr(ctx, "sl_price", None) or getattr(ctx, "sl", None)

        # p_hit_tp1 is attached by services/ev_tp1_stats.attach_tp1_hit_prob_to_ctx
        p = getattr(ctx, "tp1_hit_prob", None)
        if p is None:
            p = getattr(ctx, "tp1_hit_prob_ema", None)  # allow legacy naming
        n = getattr(ctx, "tp1_hit_n", None)
        src = str(getattr(ctx, "tp1_hit_src", "") or "")

        if entry is None or tp1 is None or sl is None or p is None or n is None:
            return float("nan"), float("nan"), float("nan"), float("nan"), 0, src

        try:
            entry_f = float(entry)
            tp1_f = float(tp1)
            sl_f = float(sl)
        except Exception:
            return float("nan"), float("nan"), float("nan"), float("nan"), 0, src

        p01 = _clamp01(float(p))
        try:
            nn = int(n)
        except Exception:
            nn = 0
        if not math.isfinite(p01):
            return float("nan"), float("nan"), float("nan"), float("nan"), nn, src

        tp1_bps = self._bps(entry_f, tp1_f)
        stop_bps = self._bps(entry_f, sl_f)
        if not math.isfinite(tp1_bps) or not math.isfinite(stop_bps):
            return float("nan"), float("nan"), float("nan"), p01, nn, src

        ev = (p01 * float(tp1_bps)) - ((1.0 - p01) * float(stop_bps))
        return float(ev), float(tp1_bps), float(stop_bps), float(p01), int(nn), src

    def _expected_move_bps(self, ctx: Any, mode: ExpectedMoveMode) -> float:
        """
        Оценка expected_move_bps:
          tp1: |tp1-entry| / entry
          rr : |entry-sl| * rr / entry
          atr: atr * mult / entry
        """
        of = getattr(ctx, "of", None)
        entry = (
            getattr(ctx, "entry_price", None)
            or getattr(ctx, "entry", None)
            or (getattr(of, "price", None) if of is not None else None)
            or getattr(ctx, "price", None)
        )

        if mode == "tp1":
            tp1 = getattr(ctx, "tp1_price", None) or getattr(ctx, "tp1", None)
            if tp1 is None:
                # allow list-like tp_levels
                tps = getattr(ctx, "tp_levels", None)
                if isinstance(tps, (list, tuple)) and len(tps) > 0:
                    tp1 = tps[0]
            return self._bps(float(entry), float(tp1)) if entry is not None and tp1 is not None else float("nan")

        if mode == "rr":
            sl = getattr(ctx, "sl_price", None) or getattr(ctx, "sl", None)
            rr = getattr(ctx, "tp_rr", None)
            if rr is None:
                rr = getattr(ctx, "rr_list", None)
            if rr is None:
                rr = getattr(ctx, "rr", None)
            rr_f = _first_float(rr)
            if rr_f is None:
                rr_f = _env_float("EDGE_TP_RR_FALLBACK", 1.0)
            if entry is None or sl is None:
                return float("nan")
            risk_bps = self._bps(float(entry), float(sl))
            if not math.isfinite(risk_bps):
                return float("nan")
            try:
                return float(risk_bps) * float(rr_f)
            except Exception:
                return float("nan")

        # atr mode
        atr = (
            getattr(ctx, "atr", None)
            or getattr(ctx, "atr14", None)
            or getattr(ctx, "atr_1m", None)
            or (getattr(of, "atr", None) if of is not None else None)
        )
        mult = getattr(ctx, "tp1_atr_mult", None)
        if mult is None:
            # if you have tp_atr_mults, pick first
            try:
                ms = getattr(ctx, "tp_atr_mults", None)
                if isinstance(ms, (list, tuple)) and len(ms) > 0:
                    mult = ms[0]
            except Exception:
                pass
        if mult is None:
            mult = _env_float("EDGE_TP1_ATR_MULT_FALLBACK", 1.0)

        if entry is None or atr is None or mult is None:
            return float("nan")

        try:
            move = float(atr) * float(mult)
            return self._bps(float(entry), float(entry) + move)
        except Exception:
            return float("nan")

    def evaluate(self, *, ctx: Any, kind: str, symbol: str) -> EdgeCostGateDecision:
        """
        Возвращает детерминированный decision object.
        Без логирования внутри -> проще тестирование & переиспользование.
        """
        # Тайминг всего gate (включая cost model + EV/move вычисления)
        # Важно: не меняет семантику gate, только диагностика.
        _span = Span()
        _span.__enter__()
        k = self._k_for(symbol)
        kind_l = (kind or "").strip().lower()
        apply = self.enabled and (not self.apply_kinds or kind_l in self.apply_kinds)
        if not apply:
            d = EdgeCostGateDecision(
                apply=False
                veto=False
                reason_code=self.REASON_SKIP
                expected_move_bps=0.0
                threshold_bps=0.0
                fees_bps=0.0
                slippage_bps=0.0
                k=float(k)
                mode=self.mode
                notes="gate_disabled_or_kind_not_applicable"
                drift_factor=1.0
                drift_score=0.0
                drift_feature=""
            )
            trace_gate(
                ctx
                stage="gates"
                name="edge_cost_gate"
                passed=True
                veto=False
                reason_code=str(d.reason_code)
                metrics={"apply": False, "k": float(d.k), "mode": str(d.mode)}
            )
            return d

        # ------------------------------------------------------------------
        # Costs model hardening:
        # - Fees: legacy (unchanged)
        # - Slippage: measured model with strict ts normalization and fail-open rules
        #
        # If your _costs_bps already returns a "base" slippage (default/half-spread)
        # we keep that AND take max(base, model) to avoid silently loosening the gate.
        # ------------------------------------------------------------------
        fees_bps, slip_bps = self._costs_bps(ctx, kind=str(kind or ""), symbol=str(symbol or ""), tf=getattr(ctx, "tf", None) or getattr(ctx, "timeframe", None))

        # ------------------------------------------------------------------
        # Execution-health overlay (TCA rollups).
        #
        # Motivation:
        #   Execution quality can degrade in ways NOT visible in the current
        #   spread/depth snapshot (toxic flow, venue issues, stress). Post-trade
        #   TCA rollups let us detect this and react deterministically.
        #
        # Modes (EDGE_EXEC_HEALTH_MODE / EXEC_HEALTH_MODE):
        #   - off      : disabled
        #   - monitor  : annotate ctx only
        #   - tighten  : inflate slippage and optionally K (strict profile)
        #   - veto     : hard veto when execution is clearly degraded
        #   - auto     : map from GATE_PROFILE (default->monitor, strict->tighten, hard->veto)
        #
        # Fail-open:
        #   - missing redis / missing keys -> no veto, no tighten.
        # ------------------------------------------------------------------
        exec_mode_raw = (_cached_getenv("EDGE_EXEC_HEALTH_MODE") or _cached_getenv("EXEC_HEALTH_MODE") or "off").strip().lower()
        if exec_mode_raw in {"", "auto"}:
            prof = (_cached_getenv("GATE_PROFILE", "default") or "default").strip().lower()
            if prof == "strict":
                exec_mode = "tighten"
            elif prof == "hard":
                exec_mode = "veto"
            else:
                exec_mode = "monitor"
        else:
            exec_mode = exec_mode_raw

        # thresholds: 0 == disabled
        exec_is_thr = _env_float("EXEC_MAX_IS_P95_BPS", 0.0)
        exec_perm_thr = _env_float("EXEC_MAX_PERM_IMPACT_P95_BPS", 0.0)
        # realized spread p50: if below this (more negative) -> adverse selection risk
        exec_real_min = _env_float("EXEC_MIN_REALIZED_SPREAD_P50_BPS", -999.0)

        exec_add_mult = _env_float("EDGE_EXEC_HEALTH_TIGHTEN_ADD_MULT", 1.0)
        exec_add_cap = _env_float("EDGE_EXEC_HEALTH_TIGHTEN_ADD_CAP_BPS", 8.0)
        exec_k_mult = _env_float("EDGE_EXEC_HEALTH_TIGHTEN_K_MULT", 1.0)

        delta_list = _parse_csv_ints(_cached_getenv("EXEC_TCA_DELTA_SEC_LIST", "1,5"), default=(1, 5))

        exec_is_p95 = None
        exec_perm_p95 = None
        exec_real_p50 = None
        exec_perm_delta = None
        exec_flags: list = []

        if exec_mode not in {"off", "0", "false"}:
            r_exec = getattr(self, "redis", None) or getattr(ctx, "redis", None)
            if r_exec is not None:
                ven = str(getattr(ctx, "venue", None) or getattr(ctx, "source", None) or "na")
                tsm = getattr(ctx, "_ts_ms_norm", None)
                try:
                    tsm_i = int(tsm or 0)
                except Exception:
                    tsm_i = 0
                sess = str(getattr(ctx, "session", None) or (session_from_ts_ms(tsm_i) if tsm_i > 0 else "na") or "na")
                tfv = str(getattr(ctx, "tf", None) or getattr(ctx, "timeframe", None) or "all") or "all"
                knd = str(kind_l or getattr(ctx, "kind", None) or "all") or "all"

                side_raw = getattr(ctx, "side", None) or getattr(ctx, "direction", None) or getattr(ctx, "dir", None) or ""
                side_s = str(side_raw).strip().lower()
                if side_s in {"1", "long", "buy", "bull"}:
                    side_s = "long"
                elif side_s in {"-1", "short", "sell", "bear"}:
                    side_s = "short"
                elif side_s in {"", "na", "none"}:
                    side_s = "all"

                exec_is_p95, exec_perm_p95, exec_real_p50, exec_perm_delta = _load_exec_health_rollups(
                    r_exec
                    symbol=str(symbol or getattr(ctx, "symbol", "") or "")
                    venue=ven
                    session=sess
                    tf=str(tfv)
                    kind=str(knd)
                    side=str(side_s)
                    delta_list=delta_list
                )

                # surface for audit/debug
                try:
                    setattr(ctx, "exec_is_p95_bps", float(exec_is_p95) if exec_is_p95 is not None else float("nan"))
                    setattr(ctx, "exec_perm_impact_p95_bps", float(exec_perm_p95) if exec_perm_p95 is not None else float("nan"))
                    setattr(ctx, "exec_realized_spread_p50_bps", float(exec_real_p50) if exec_real_p50 is not None else float("nan"))
                    setattr(ctx, "exec_perm_impact_delta_sec", int(exec_perm_delta or 0))
                    setattr(ctx, "exec_health_mode", str(exec_mode))
                except Exception:
                    pass

                is_bad = exec_is_thr > 0 and exec_is_p95 is not None and float(exec_is_p95) > float(exec_is_thr)
                perm_bad = exec_perm_thr > 0 and exec_perm_p95 is not None and float(exec_perm_p95) > float(exec_perm_thr)
                adv_bad = exec_real_min > -900 and exec_real_p50 is not None and float(exec_real_p50) < float(exec_real_min)

                if is_bad:
                    exec_flags.append("is_p95_high")
                if perm_bad:
                    exec_flags.append("perm_impact_high")
                if adv_bad:
                    exec_flags.append("adverse_sel")

                # tighten: inflate slippage by excess over threshold (bounded by cap)
                if exec_mode in {"tighten", "veto"} and exec_flags:
                    d1 = max(0.0, float(exec_is_p95) - float(exec_is_thr)) if is_bad else 0.0
                    d2 = max(0.0, float(exec_perm_p95) - float(exec_perm_thr)) if perm_bad else 0.0
                    d3 = max(0.0, float(exec_real_min) - float(exec_real_p50)) if adv_bad else 0.0
                    add = float(exec_add_mult) * float(max(d1, d2, d3))
                    add = float(min(float(exec_add_cap), max(0.0, add)))
                    if add > 0.0:
                        slip_bps = float(slip_bps) + float(add)
                        try:
                            setattr(ctx, "exec_health_tighten_add_bps", float(add))
                            if edge_exec_health_tighten_total:
                                edge_exec_health_tighten_total.labels(symbol=str(symbol or "").upper() or "NA").inc()
                        except Exception:
                            pass
                    if exec_k_mult and float(exec_k_mult) > 1.0:
                        try:
                            setattr(ctx, "exec_health_tighten_k", float(exec_k_mult))
                        except Exception:
                            pass

                # hard veto: only when both IS and perm_impact exceed thresholds
                if exec_mode == "veto" and exec_flags:
                    veto_on_adverse = _env_bool("EDGE_EXEC_HEALTH_VETO_ON_ADVERSE", False)
                    veto_reason = ""
                    if is_bad and perm_bad:
                        veto_reason = "VETO_IMPL_SHORTFALL_P95"
                    elif veto_on_adverse and adv_bad and (is_bad or perm_bad):
                        veto_reason = "VETO_ADVERSE_SELECTION"

                    if veto_reason:
                        note = f"exec_health veto flags={','.join(exec_flags)} is_p95={exec_is_p95} perm_p95={exec_perm_p95} real_p50={exec_real_p50}"
                        d = EdgeCostGateDecision(
                            apply=True
                            veto=True
                            reason_code=str(veto_reason)
                            expected_move_bps=0.0
                            threshold_bps=0.0
                            fees_bps=float(fees_bps)
                            slippage_bps=float(slip_bps)
                            k=float(k)
                            mode=self.mode
                            notes=note
                            exec_is_p95_bps=float(exec_is_p95) if exec_is_p95 is not None else float("nan")
                            exec_perm_impact_p95_bps=float(exec_perm_p95) if exec_perm_p95 is not None else float("nan")
                            exec_realized_spread_p50_bps=float(exec_real_p50) if exec_real_p50 is not None else float("nan")
                            exec_perm_impact_delta_sec=int(exec_perm_delta or 0)
                            exec_health_mode=str(exec_mode)
                            exec_health_tighten_add_bps=float(getattr(ctx, "exec_health_tighten_add_bps", 0.0) or 0.0)
                        )
                        trace_gate(
                            ctx
                            stage="gates"
                            name="edge_cost_gate"
                            passed=False
                            veto=True
                            reason_code=str(d.reason_code)
                            metrics={
                                "exec_is_p95": d.exec_is_p95_bps
                                "exec_perm_p95": d.exec_perm_impact_p95_bps
                                "exec_real_p50": d.exec_realized_spread_p50_bps
                                "exec_flags": ",".join(exec_flags)
                            }
                        )
                        if edge_exec_health_veto_total:
                            edge_exec_health_veto_total.labels(symbol=str(symbol or "").upper() or "NA", reason_code=str(d.reason_code)).inc()
                        return d

        try:
            if exec_flags:
                setattr(ctx, "exec_health_flags", ",".join(exec_flags))
        except Exception:
            pass

        k_base = k

        # Apply dynamic K if enabled (adjusts based on volatility)
        k = self._dynamic_k(k_base, ctx) if self.mode == "ev" else k_base

        # ------------------------------------------------------------------
        # NEW: Feature drift alarm (temporary tightening).
        #
        # If market microstructure distributions резко "уплыли" (obi/z_delta/spread/depth)
        # we temporarily tighten gates to reduce false positives.
        #
        # Implementation:
        #   - services/feature_drift_alarm.py writes Redis hash drift:active:* with:
        #       factor > 1, score, feature
        #   - Here we multiply K by that factor:
        #       K_effective = K * drift_factor
        #
        # Fail-open:
        #   - no Redis / no key / invalid ts => factor=1 (no behavior change)
        #
        # NOTE:
        #   We do NOT veto directly in drift alarm; we keep policy local here.
        # ------------------------------------------------------------------
        drift_factor = 1.0
        drift_score = float("nan")
        drift_feat = ""
        try:
            redis_client = getattr(self, "redis", None) or getattr(ctx, "redis", None)
            # Determine session/tf/kind dims consistently.
            tsm = int(normalize_ts_ms(getattr(ctx, "ts_ms", None) or getattr(ctx, "ts", None) or 0))
            if tsm > 0:
                sess = str(getattr(ctx, "session", None) or session_from_ts_ms(int(tsm)) or "na")
                tfv = _canon_tf(getattr(ctx, "tf", None) or getattr(ctx, "timeframe", None) or "na")
                knd = (kind or "na")
                drift_factor, drift_score, drift_feat = _load_drift_active(
                    redis_client
                    symbol=str(symbol or "")
                    venue=str(getattr(ctx, "venue", "") or "na")
                    session=str(sess or "na")
                    tf=str(tfv or "na")
                    kind=str(knd or "na")
                )
                if not math.isfinite(float(drift_factor)) or float(drift_factor) <= 0:
                    drift_factor = 1.0
        except Exception:
            drift_factor = 1.0

        # ------------------------------------------------------------------
        # STRICT: drift-aware K inflation.
        # ------------------------------------------------------------------
        mode = (_cached_getenv("FEATURE_DRIFT_MODE", "") or "").strip().lower()
        tighten = bool(getattr(ctx, "_edge_drift_tighten", False)) or (mode == "enforce")
        if tighten and float(drift_factor) > 1.0:
            k_cap = float(_cached_getenv("EDGE_DRIFT_K_CAP_MULT", "2.5"))
            km = float(min(k_cap, max(1.0, float(drift_factor))))
            k = float(k) * km

        k_eff = float(k)

        # ------------------------------------------------------------------
        # Optional tightening from EntryPolicyGate / drift alarm (soft mode).
        # We do NOT directly veto here; we only increase cost threshold by raising K.
        # This keeps default behavior "not cutting too many signals", while still
        # reacting to spread shock / burst flip / feature drift.
        #
        # Controlled purely by GATE_PROFILE:
        #   - default/soft: mild tighten factors (1.10 .. 1.15)
        #   - strict/hard: larger factors may be attached
        # ------------------------------------------------------------------
        try:
            f1 = float(getattr(ctx, "entry_policy_tighten_k", 1.0) or 1.0)
        except Exception:
            f1 = 1.0
        try:
            f2 = float(getattr(ctx, "feature_drift_tighten_k", 1.0) or 1.0)
        except Exception:
            f2 = 1.0
        try:
            f3 = float(getattr(ctx, "exec_health_tighten_k", 1.0) or 1.0)
        except Exception:
            f3 = 1.0
        try:
            if f1 > 1.0 or f2 > 1.0 or f3 > 1.0:
                k_eff = float(k_eff) * float(max(1.0, f1)) * float(max(1.0, f2)) * float(max(1.0, f3))
        except Exception:
            pass

        # V2 threshold: include buffer_bps (default 0.0)
        buffer_bps = self._get_buffer_bps(ctx, symbol)
        thr = float(k_eff) * (float(fees_bps) + float(slip_bps) + float(buffer_bps))

        # ------------------------------------------------------------------
        # NEW RULE: TP1 Filter (Edge-Cost Gate Micro-R Reject)
        # Ожидаемый TP1 должен быть строго больше 2 * (Commissions + Spread)
        # ------------------------------------------------------------------
        actual_tp1_bps = self._expected_move_bps(ctx, "tp1")
        if math.isfinite(actual_tp1_bps):
            tp1_limit_bps = 2.0 * (float(fees_bps) + float(slip_bps))
            if float(actual_tp1_bps) <= float(tp1_limit_bps):
                d = EdgeCostGateDecision(
                    apply=True, veto=True, reason_code="VETO_TP1_TOO_CLOSE"
                    expected_move_bps=float(actual_tp1_bps), threshold_bps=float(tp1_limit_bps)
                    fees_bps=float(fees_bps), slippage_bps=float(slip_bps)
                    k=float(k_eff), mode=self.mode
                    notes=f"tp1_bps={actual_tp1_bps:.2f} <= 2*(costs)={tp1_limit_bps:.2f}"
                    drift_factor=float(drift_factor)
                    drift_score=float(drift_score)
                    drift_feature=str(drift_feat or "")
                )
                trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=False, veto=True
                           reason_code="VETO_TP1_TOO_CLOSE", 
                           metrics={"tp1_bps": float(actual_tp1_bps), "limit_bps": float(tp1_limit_bps)})
                return d

        # -------------------------
        # EV gate (probability-aware)
        # -------------------------
        if self.mode == "ev":
            ev_bps, tp1_bps, stop_bps, p, n, src = self._ev_bps(ctx)
            
            # Get per-kind p_min (allows different thresholds for different signal types)
            p_min = self._p_min_for_kind(kind)

            # 1) stats missing/invalid
            if not math.isfinite(ev_bps) or not math.isfinite(p) or not math.isfinite(tp1_bps) or not math.isfinite(stop_bps):
                if self.ev_strict_missing_stats:
                    d = EdgeCostGateDecision(
                        apply=True, veto=True, reason_code=self.REASON_EV_MISSING_INPUTS
                        expected_move_bps=float("nan"), threshold_bps=float(thr)
                        fees_bps=float(fees_bps), slippage_bps=float(slip_bps)
                        k=float(k_eff), mode=self.mode
                        notes="strict_missing_stats_or_levels"
                        p_hit_tp1=float(p) if math.isfinite(p) else float("nan")
                        p_min=float(p_min)
                        tp1_bps=float(tp1_bps), stop_bps=float(stop_bps), ev_bps=float(ev_bps)
                        stats_n=int(n), stats_src=str(src)
                        drift_factor=float(drift_factor)
                        drift_score=float(drift_score)
                        drift_feature=str(drift_feat or "")
                    )
                    trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=not d.veto, veto=d.veto
                               reason_code=str(d.reason_code)
                               metrics={"ev_bps": d.ev_bps, "thr": d.threshold_bps, "p": d.p_hit_tp1, "p_min": d.p_min, "n": d.stats_n})
                    return d
                d = EdgeCostGateDecision(
                    apply=True, veto=False, reason_code=self.REASON_OK
                    expected_move_bps=float("nan"), threshold_bps=float(thr)
                    fees_bps=float(fees_bps), slippage_bps=float(slip_bps)
                    k=float(k_eff), mode=self.mode
                    notes="missing_ev_inputs_fail_open"
                    p_hit_tp1=float(p) if math.isfinite(p) else float("nan")
                    p_min=float(p_min)
                    tp1_bps=float(tp1_bps), stop_bps=float(stop_bps), ev_bps=float(ev_bps)
                    stats_n=int(n), stats_src=str(src)
                    drift_factor=float(drift_factor)
                    drift_score=float(drift_score)
                    drift_feature=str(drift_feat or "")
                )
                trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=not d.veto, veto=d.veto
                           reason_code=str(d.reason_code)
                           metrics={"ev_bps": d.ev_bps, "thr": d.threshold_bps, "p": d.p_hit_tp1, "p_min": d.p_min, "n": d.stats_n})
                return d

            # 2) cold-start guard (avoid acting on noisy early estimates)
            if int(n) < int(self.ev_min_trades):
                if self.ev_strict_missing_stats:
                    return EdgeCostGateDecision(
                        apply=True, veto=True, reason_code=self.REASON_EV_INSUFFICIENT_STATS
                        expected_move_bps=float(ev_bps), threshold_bps=float(thr)
                        fees_bps=float(fees_bps), slippage_bps=float(slip_bps)
                        k=float(k_eff), mode=self.mode
                        notes="strict_insufficient_stats"
                        p_hit_tp1=float(p), p_min=float(p_min)
                        tp1_bps=float(tp1_bps), stop_bps=float(stop_bps), ev_bps=float(ev_bps)
                        stats_n=int(n), stats_src=str(src)
                        drift_factor=float(drift_factor)
                        drift_score=float(drift_score)
                        drift_feature=str(drift_feat or "")
                    )
                return EdgeCostGateDecision(
                    apply=True, veto=False, reason_code=self.REASON_OK
                    expected_move_bps=float(ev_bps), threshold_bps=float(thr)
                    fees_bps=float(fees_bps), slippage_bps=float(slip_bps)
                    k=float(k_eff), mode=self.mode
                    notes="insufficient_stats_fail_open"
                    p_hit_tp1=float(p), p_min=float(p_min)
                    tp1_bps=float(tp1_bps), stop_bps=float(stop_bps), ev_bps=float(ev_bps)
                    stats_n=int(n), stats_src=str(src)
                    drift_factor=float(drift_factor)
                    drift_score=float(drift_score)
                    drift_feature=str(drift_feat or "")
                )

            # 3) probability floor (using per-kind threshold)
            if float(p) < float(p_min):
                d = EdgeCostGateDecision(
                    apply=True, veto=True, reason_code=self.REASON_EV_PROB
                    expected_move_bps=float(ev_bps), threshold_bps=float(thr)
                    fees_bps=float(fees_bps), slippage_bps=float(slip_bps)
                    k=float(k_eff), mode=self.mode
                    notes="p_below_min"
                    p_hit_tp1=float(p), p_min=float(p_min)
                    tp1_bps=float(tp1_bps), stop_bps=float(stop_bps), ev_bps=float(ev_bps)
                    stats_n=int(n), stats_src=str(src)
                    drift_factor=float(drift_factor)
                    drift_score=float(drift_score)
                    drift_feature=str(drift_feat or "")
                )
                trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=not d.veto, veto=d.veto
                           reason_code=str(d.reason_code)
                           metrics={"ev_bps": d.ev_bps, "thr": d.threshold_bps, "p": d.p_hit_tp1, "p_min": d.p_min, "n": d.stats_n})
                return d

            # 4) EV >= costs*K (using potentially dynamic K)
            veto = float(ev_bps) < float(thr)
            d = EdgeCostGateDecision(
                apply=True
                veto=bool(veto)
                reason_code=self.REASON_EV_BELOW_K if veto else self.REASON_OK
                expected_move_bps=float(ev_bps),  # keep legacy field meaningful for logs
                threshold_bps=float(thr)
                fees_bps=float(fees_bps)
                slippage_bps=float(slip_bps)
                k=float(k_eff)
                mode=self.mode
                notes=""
                p_hit_tp1=float(p), p_min=float(p_min)
                tp1_bps=float(tp1_bps), stop_bps=float(stop_bps), ev_bps=float(ev_bps)
                stats_n=int(n), stats_src=str(src)
                drift_factor=float(drift_factor)
                drift_score=float(drift_score)
                drift_feature=str(drift_feat or "")
                total_costs_bps=float(fees_bps) + float(slip_bps) + float(buffer_bps)
                buffer_bps=float(buffer_bps)
                edge_source=str(self.mode)
            )
            trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=not d.veto, veto=d.veto
                       reason_code=str(d.reason_code)
                       metrics={"ev_bps": d.ev_bps, "thr": d.threshold_bps, "p": d.p_hit_tp1, "p_min": d.p_min, "n": d.stats_n})
            return d

        # -------------------------
        # Legacy move gates
        # -------------------------
        exp_bps = self._expected_move_bps(ctx, self.mode)
        if not math.isfinite(exp_bps):
            # missing levels -> либо fail-open, либо fail-closed в зависимости от env
            if self.strict_missing_levels:
                d = EdgeCostGateDecision(
                    apply=True, veto=True, reason_code=self.REASON_MISSING_LEVELS
                    expected_move_bps=float("nan"), threshold_bps=float(thr)
                    fees_bps=float(fees_bps), slippage_bps=float(slip_bps)
                    k=float(k_eff), mode=self.mode
                    notes="strict_missing_levels"
                    drift_factor=float(drift_factor)
                    drift_score=float(drift_score)
                    drift_feature=str(drift_feat or "")
                )
                trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=not d.veto, veto=d.veto
                           reason_code=str(d.reason_code)
                           metrics={"ev_bps": d.ev_bps, "thr": d.threshold_bps, "p": d.p_hit_tp1, "p_min": d.p_min, "n": d.stats_n})
                return d
            d = EdgeCostGateDecision(
                apply=True, veto=False, reason_code=self.REASON_OK
                expected_move_bps=float("nan"), threshold_bps=float(thr)
                fees_bps=float(fees_bps), slippage_bps=float(slip_bps)
                k=float(k_eff), mode=self.mode
                notes="missing_levels_fail_open"
                drift_factor=float(drift_factor)
                drift_score=float(drift_score)
                drift_feature=str(drift_feat or "")
            )
            trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=not d.veto, veto=d.veto
                       reason_code=str(d.reason_code)
                       metrics={"ev_bps": d.ev_bps, "thr": d.threshold_bps, "p": d.p_hit_tp1, "p_min": d.p_min, "n": d.stats_n})
            return d
        # Hard floor: reject if expected_move < EDGE_MIN_EXPECTED_MOVE_BPS (anti-churn).
        min_move = self._min_move_for(symbol)
        if min_move > 0.0 and float(exp_bps) < float(min_move):
            d = EdgeCostGateDecision(
                apply=True, veto=True, reason_code="VETO_EDGE_TOO_SMALL"
                expected_move_bps=float(exp_bps), threshold_bps=float(thr)
                fees_bps=float(fees_bps), slippage_bps=float(slip_bps)
                k=float(k_eff), mode=self.mode
                notes=f"min_expected_move_floor={min_move}"
                drift_factor=float(drift_factor)
                drift_score=float(drift_score)
                drift_feature=str(drift_feat or "")
            )
            trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=False, veto=True
                       reason_code="VETO_EDGE_TOO_SMALL"
                       metrics={"expected_move_bps": float(exp_bps), "min_move": float(min_move)})
            return d

        veto = float(exp_bps) < float(thr)
        decision = EdgeCostGateDecision(
            apply=True
            veto=bool(veto)
            reason_code=self.REASON_BELOW_K if veto else self.REASON_OK
            expected_move_bps=float(exp_bps)
            threshold_bps=float(thr)
            fees_bps=float(fees_bps)
            slippage_bps=float(slip_bps)
            k=float(k_eff)
            mode=self.mode
            notes=""
            drift_factor=float(drift_factor)
            drift_score=float(drift_score)
            drift_feature=str(drift_feat or "")
            total_costs_bps=float(fees_bps) + float(slip_bps) + float(buffer_bps)
            buffer_bps=float(buffer_bps)
            edge_source=str(self.mode)
        )

        # =====================================================================
        # DecisionTrace (fail-open)
        # =====================================================================
        try:
            _span.__exit__(None, None, None)
            trace_gate(
                ctx
                stage="gates"
                name="edge_cost_gate"
                passed=bool(decision.apply and (not decision.veto))
                veto=bool(decision.veto)
                reason_code=str(decision.reason_code or "")
                metrics={
                    "apply": bool(decision.apply)
                    "expected_move_bps": float(decision.expected_move_bps)
                    "threshold_bps": float(decision.threshold_bps)
                    "fees_bps": float(decision.fees_bps)
                    "slippage_bps": float(decision.slippage_bps)
                    "k": float(decision.k)
                    "mode": str(getattr(decision, "mode", "") or "")
                    # EV-mode diagnostics (if enabled upstream; defaults are NaN)
                    "p_hit_tp1": float(getattr(decision, "p_hit_tp1", float("nan")))
                    "p_min": float(getattr(decision, "p_min", float("nan")))
                    "tp1_bps": float(getattr(decision, "tp1_bps", float("nan")))
                    "stop_bps": float(getattr(decision, "stop_bps", float("nan")))
                    "ev_bps": float(decision.ev_bps)
                    "stats_n": int(getattr(decision, "stats_n", 0) or 0)
                    "stats_src": str(getattr(decision, "stats_src", "") or "")
                }
                duration_ms=float(_span.ms)
            )
        except Exception:
            pass

        return decision

