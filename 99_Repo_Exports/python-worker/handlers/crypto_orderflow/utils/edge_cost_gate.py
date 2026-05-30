from __future__ import annotations

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

import functools
import asyncio
import contextlib
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Tuple, Union, Callable

from utils.time_utils import get_ny_time_millis


def _cached_getenv(k, d=None): return os.getenv(k, d)

from common.decision_trace import Span, trace_gate

# ---------------------------------------------------------------------
# Prometheus metrics for observability
# ---------------------------------------------------------------------
try:
    from prometheus_client import Counter, Gauge
    edge_time_skew_ms = Gauge(
        "edge_time_skew_ms",
        "Absolute time skew between signal event time and local wall-clock (ms)",
        ["symbol", "reason_code"]
    )
    edge_bad_time_total = Counter(
        "edge_bad_time_total",
        "Total count of signals rejected or penalized due to large time skew",
        ["symbol", "reason"]
    )

    # Execution-health overlay telemetry (bounded labels).
    edge_exec_health_tighten_total = Counter(
        "edge_exec_health_tighten_total",
        "Total count of EdgeCostGate tighten events due to execution-health rollups",
        ["symbol"]
    )
    edge_exec_health_veto_total = Counter(
        "edge_exec_health_veto_total",
        "Total count of EdgeCostGate veto events due to execution-health rollups",
        ["symbol", "reason_code"]
    )

    # Directional p_min bias telemetry (post-calibrator tightening).
    # bias_bucket: bucketed string of applied bias ("0.00","0.02","0.04","0.06","0.08","0.10","0.12+")
    # so cardinality stays bounded regardless of ENV value.
    edge_directional_bias_applied_total = Counter(
        "edge_cost_gate_directional_bias_applied_total",
        "Total signals to which EdgeCostGate applied a directional p_min bias (post-calibrator)",
        ["symbol", "kind", "direction", "countertrend", "bias_bucket"]
    )
except ImportError:
    Counter = Gauge = None
    edge_time_skew_ms = edge_bad_time_total = edge_exec_health_tighten_total = edge_exec_health_veto_total = None
    edge_directional_bias_applied_total = None

# ---------------------------------------------------------------------
# IMPORTANT: timestamp normalization MUST be consistent across pipeline.
# Use ONE canonical timestamp normalizer across the whole pipeline.
# TradeMonitor already uses domain.time_utils.normalize_ts_ms(); gates must do the same
# to avoid regressions when some component suddenly receives seconds / bad clocks / minutes-of-day.
# ---------------------------------------------------------------------
try:  # type: ignore
    from domain.gate_profile import strict_enabled
    from domain.time_utils import normalize_ts_ms, session_from_ts_ms  # type: ignore
except Exception:  # pragma: no cover (tests may import without full deps)
    def normalize_ts_ms(x: Any) -> int:
        try:
            return int(float(x or 0))
        except Exception:
            return 0
    
    def session_from_ts_ms(ts: int) -> str:
        return "na"

    strict_enabled = lambda: False  # type: ignore

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


def _env_str(name: str, default: str) -> str:
    try:
        v = _cached_getenv(name, default)
        return str(v) if v is not None else default
    except Exception:
        return default

# ---------------------------------------------------------------------
# IMPORTANT: timestamp normalization MUST be consistent across pipeline.
# We intentionally reuse domain.time_utils.normalize_ts_ms() everywhere
# to avoid regressions when some ctx.ts/ctx.ts_ms accidentally comes
# as seconds or non-epoch (minutes-of-day, etc.).
# ---------------------------------------------------------------------
# This patch hardens the pipeline against:
#   - ts_ms <= 0 (missing/invalid) => session="na", NO EMA usage (only default/half-spread)
#   - seconds timestamps (10 digits) => safely normalize to ms
#   - non-epoch / too-small timestamps => treat as invalid and skip EMA (fail-open)  # type: ignore
# ---------------------------------------------------------------------------
from domain.time_utils import normalize_ts_ms  # type: ignore

# Centralized key + EMA writer/reader utils for execution-cost statistics.
# We keep this in services/ to allow StatsAggregator to write using identical key format.  # type: ignore
from services.execution_cost_ema import (
    session_from_ts_ms,  # type: ignore
)
import contextlib

# Single source of truth for epoch-ms normalization (already used in TradeMonitor)

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
# Where p_hit_tp1 is attached to ctx from online stats (EMA / rolling),
# see services/ev_tp1_stats.py and stats_aggregator.py patch.
# -----------------------------------------------------------------------------
ExpectedMoveMode = Literal["tp1", "rr", "atr", "ev"]


def _env_float(name: str, default: float) -> float:
    """Безопасное извлечение float из ENV."""
    try:
        return float(_cached_getenv(name, str(default)) or default)
    except Exception:
        return default




def _safe_float(x, default=0.0) -> float:
    """Безопасное извлечение float."""
    try:
        f = float(x)
        return f if math.isfinite(f) else default
    except Exception:
        return default

def _env_int(name: str, default: int) -> int:
    """Безопасное извлечение int из ENV."""
    try:
        return int(float(_cached_getenv(name, str(default)) or default))
    except Exception:
        return default


def _norm_symbol(sym: str) -> str:
    """Нормализация символа: UPPER, без /, без -."""
    return (sym or "").strip().upper().replace("/", "").replace("-", "")


def _parse_csv_set(v: str) -> set[str]:
    """Парсинг CSV строки в множество lowercase значений."""
    out: set[str] = set()
    for x in (v or "").split(","):
        s = x.strip().lower()
        if s:
            out.add(s)
    return out

def _clamp01(x: float) -> float:
    try:
        xx = x
    except Exception:
        return float("nan")
    if not math.isfinite(xx):
        return float("nan")
    return 0.0 if xx < 0.0 else (1.0 if xx > 1.0 else xx)


def _first_float(x: Any) -> float | None:
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





def _env_bool(name: str, default: bool) -> bool:
    v = (_cached_getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _canon_tf(v: Any) -> str:
    s = (v or "").strip().lower()
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
        t = normalize_ts_ms(raw)
    except Exception:
        t = raw
    if t <= 0:
        return 0
    # normalize_ts_ms already handles seconds/non-epoch policy
    return t


def _normalize_ctx_ts_ms(ctx: Any, ts_ms: int | None) -> int:
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
        t = normalize_ts_ms(raw)
    except Exception:
        t = raw
    if t <= 0:
        return 0
    # If someone accidentally passes seconds, normalize to ms.
    # 1e12 ~ 2001-09-09 in ms; 1e9 ~ 2001-09-09 in seconds.
    if t < 1_000_000_000_000:
        if t >= 1_000_000_000:
            return t * 1000
        # too small => not epoch -> treat invalid to avoid wrong sessions/keys
        return 0
    return t


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
                return x
    except Exception:
        pass

    def _get_from(obj: Any, *names: str) -> float | None:
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
        return (ask - bid) / mid * 10_000.0

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
                    return x
        except Exception:
            pass
        sp_of = _compute_spread_from_ask_bid(of)
        if sp_of > 0:
            return sp_of

    return 0.0


def estimate_slippage_bps(
    ctx: Any,
    *,
    redis_client: Any,
    symbol: str,
    venue: str,
    ts_ms: Any,
    kind: str | None = None,
    tf: str | None = None,
    default_bps: float = 5.0,
    use_spread_half: bool = True,
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
    #   - "correct_skip_ema" (default): if ts BAD -> correct to now for audit fields,
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
        max_skew_ms = _env_int("EDGE_TS_MAX_SKEW_MS", 21600000)  # 6h
    except Exception:
        max_skew_ms = 21600000
    try:
        veto_bps = _env_float("EDGE_TS_BAD_VETO_BPS", 1000000.0)
    except Exception:
        veto_bps = 1_000_000.0

    disable_ema = (_cached_getenv("EDGE_DISABLE_EMA", "0") or "").strip().lower() in {"1", "true", "yes", "on"}

    now_ms = get_ny_time_millis()
    # normalize with the shared normalizer (handles seconds->ms, bad strings, NaN, etc.)
    tsm = normalize_ts_ms(ts_ms)

    ts_invalid = False
    ts_reason = ""
    ts_corrected = False

    if tsm <= 0:
        ts_invalid = True
        ts_reason = "ts<=0"
        if edge_bad_time_total:
            edge_bad_time_total.labels(symbol=symbol, reason="ts_zero").inc()
    else:
        skew = abs(tsm - now_ms)
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
        ctx._ts_ms_norm = tsm
        ctx._ts_invalid = ts_invalid
        ctx._ts_corrected = ts_corrected
        ctx._ts_reason = ts_reason
        ctx._ts_policy = policy
        ctx._ts_skew_ms = abs(int(ts_ms or 0) - now_ms) if ts_ms else 0
    except Exception:
        pass

    # Compute base (default vs spread/2). This part must NEVER depend on EMA.
    base = default_bps
    if use_spread_half:
        try:
            # NOTE: имя функции в этом модуле — _extract_spread_bps_from_ctx()
            sp = _extract_spread_bps_from_ctx(ctx)
            if sp > 0 and math.isfinite(sp):
                base = max(base, sp * 0.5)
        except Exception:
            pass

    # If ts invalid => do NOT use EMA (avoid poisoning keys). Optionally force veto.
    if ts_invalid:
        if policy == "veto":
            return max(base, veto_bps)
        return base

    # Valid ts from here.
    if disable_ema or policy == "skip_ema":
        return base

    # only here we may use EMA (ts is valid and profile allows)
    sess = getattr(ctx, "session", None) or session_from_ts_ms(tsm) or "na"
    tfv = _canon_tf(tf or getattr(ctx, "tf", None) or getattr(ctx, "timeframe", None) or "na")
    knd = (
        kind
        or getattr(ctx, "kind", None)
        or getattr(ctx, "signal_kind", None)
        or getattr(ctx, "strategy", None)
        or "na"
    )
    knd = (knd or "na").strip().lower() or "na"

    # Key = symbol×venue×session×tf×kind (backward-compatible fallback below)
    ema = _load_slippage_ema_bps(
        redis_client,
        symbol=(symbol or "").upper(),
        venue=(venue or "na").lower(),
        session=(sess or "na").lower(),
        tf=(tfv or "na").lower(),
        kind=(knd or "na").lower(),
    )
    if ema is not None and math.isfinite(ema) and ema > 0:
        return max(base, ema)
    return base


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
            return normalize_ts_ms(raw)
        return raw
    except Exception:
        return raw


def _load_slippage_ema_bps(
    redis_client: Any, *,
    symbol: str, venue: str, session: str, tf: str, kind: str,
) -> float | None:
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
        symbol_u = (symbol or "").upper()
        venue_l = (venue or "na").lower()
        sess_l  = (session or "na").lower()
        tf_l    = (tf or "na").lower()
        kind_l  = (kind or "na").lower()

        keys = [
            f"slipema:{symbol_u}:{venue_l}:{sess_l}:{tf_l}:{kind_l}",
            f"slipema:{symbol_u}:{venue_l}:{sess_l}:{tf_l}:na",
            f"slipema:{symbol_u}:{venue_l}:{sess_l}:{tf_l}",  # type: ignore
        ]

        min_n = int(float(_cached_getenv("EDGE_SLIP_EMA_MIN_SAMPLES", "20")))  # type: ignore

        for key in keys:
            d = redis_client.hgetall(key)
            if asyncio.iscoroutine(d):
                d.close()
                d = {}
                try:
                    from handlers.crypto_orderflow.config.handler_config import _get_sync_redis
                    sync_redis = _get_sync_redis()
                    if sync_redis is not None:
                        d = sync_redis.hgetall(key) or {}
                except Exception:
                    pass
            else:
                d = d or {}

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
                return ema
    except Exception:
        return None
    return None


def _hget_ema(redis_client: Any, key: str, *, min_n: int) -> float | None:
    """
    Expected hash fields (best-effort, fail-open):
      - samples / n
      - ema_bps / ema_slippage_bps / ema
    """
    try:
        d = redis_client.hgetall(key)
        if asyncio.iscoroutine(d):
            d.close()
            d = {}
            try:
                from handlers.crypto_orderflow.config.handler_config import _get_sync_redis
                sync_redis = _get_sync_redis()
                if sync_redis is not None:
                    d = sync_redis.hgetall(key) or {}
            except Exception:
                pass
        else:
            d = d or {}
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
        n_raw = dd.get("samples") or dd.get("n") or 0
        n = int(n_raw) if n_raw else 0
        if n < min_n:
            return None
        ema_raw = dd.get("ema_bps") or dd.get("ema_slippage_bps") or dd.get("ema") or 0.0
        ema = float(ema_raw) if ema_raw else 0.0
        if ema > 0 and math.isfinite(ema):
            return ema
    except Exception:
        return None
    return None


def _load_drift_active(
    redis_client: Any,
    *,
    symbol: str,
    venue: str,
    session: str,
    tf: str,
    kind: str,
) -> tuple[float, float, str]:
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

    def _read(key: str) -> tuple[float, float, str] | None:
        try:
            d = redis_client.hgetall(key)
            if asyncio.iscoroutine(d):
                d.close()
                d = {}
                try:
                    from handlers.crypto_orderflow.config.handler_config import _get_sync_redis
                    sync_redis = _get_sync_redis()
                    if sync_redis is not None:
                        d = sync_redis.hgetall(key) or {}
                except Exception:
                    pass
            else:
                d = d or {}
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
            s_raw = dd.get("score")
            s = float(s_raw) if s_raw else float("nan")
            feat = str(dd.get("feature") or "")
            if not math.isfinite(f) or f <= 0:
                return None
            return f, s, feat
        except Exception:
            return None

    sym = (symbol or "").upper()
    ven = (venue or "na").lower()
    sess = (session or "na").lower()
    tfv = (tf or "na").lower()
    knd = (kind or "na").lower()

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

def _parse_csv_ints(v: str, *, default: tuple[int, ...]) -> tuple[int, ...]:
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


def _redis_get_float_best_effort(r: Any, key: str) -> float | None:
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
            if asyncio.iscoroutine(v):
                v.close()
                v = None
                try:
                    from handlers.crypto_orderflow.config.handler_config import _get_sync_redis
                    sync_redis = _get_sync_redis()
                    if sync_redis is not None:
                        v = sync_redis.get(key)
                except Exception:
                    pass

            if v is None:
                return None
            if isinstance(v, (bytes, bytearray)):
                v = v.decode("utf-8", errors="ignore")
            f = float(v)
            return f if math.isfinite(f) else None
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
                        return f
    except Exception:
        pass
    return None


def _tca_key_candidates(
    *,
    metric: str,
    symbol: str,
    venue: str,
    session: str,
    tf: str,
    kind: str,
    side: str,
    delta_sec: int | None = None,
) -> tuple[str, ...]:
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
    sym = (symbol or "").upper()
    ven = (venue or "na").lower()
    sess = (session or "na").lower()
    tfv = (tf or "all").lower()
    knd = (kind or "all").lower()
    sde = (side or "all").lower()

    def _mk(ses: str, tf_: str, k: str, sd: str) -> str:
        if delta_sec is None:
            return f"tca:{metric}:{sym}:{ven}:{ses}:{tf_}:{k}:{sd}"
        return f"tca:{metric}:{delta_sec}:{sym}:{ven}:{ses}:{tf_}:{k}:{sd}"

    keys = [
        _mk(sess, tfv, knd, sde),
        _mk(sess, "all", knd, sde),
        _mk(sess, tfv, "all", sde),
        _mk(sess, "all", "all", sde),
        _mk("all", "all", "all", sde),
        _mk(sess, "all", "all", "all"),
        _mk("all", "all", "all", "all"),
    ]
    return tuple(dict.fromkeys(keys))


def _load_exec_health_rollups(
    redis_client: Any,
    *,
    symbol: str,
    venue: str,
    session: str,
    tf: str,
    kind: str,
    side: str,
    delta_list: tuple[int, ...],
) -> tuple[float | None, float | None, float | None, int | None]:
    """
    Reads:
      - is_p95_bps (no delta)
      - perm_impact_p95_bps (max across deltas)
      - realized_spread_p50_bps (min across deltas)  # more negative = worse adverse selection
    Returns: (is_p95, perm_impact_p95_max, realized_p50_min, chosen_delta_for_perm)
    """
    is_p95 = None
    for k in _tca_key_candidates(
        metric="is_p95_bps",
        symbol=symbol, venue=venue, session=session, tf=tf, kind=kind, side=side,
    ):
        is_p95 = _redis_get_float_best_effort(redis_client, k)
        if is_p95 is not None:
            break

    perm_max = None
    perm_delta = None
    for d in delta_list:
        val = None
        for k in _tca_key_candidates(
            metric="perm_impact_p95_bps",
            delta_sec=d,
            symbol=symbol, venue=venue, session=session, tf=tf, kind=kind, side=side,
        ):
            val = _redis_get_float_best_effort(redis_client, k)
            if val is not None:
                break
        if val is None:
            continue
        if perm_max is None or val > perm_max:
            perm_max = val
            perm_delta = d

    real_min = None
    for d in delta_list:
        val = None
        for k in _tca_key_candidates(
            metric="realized_spread_p50_bps",
            delta_sec=d,
            symbol=symbol, venue=venue, session=session, tf=tf, kind=kind, side=side,
        ):
            val = _redis_get_float_best_effort(redis_client, k)
            if val is not None:
                break
        if val is None:
            continue
        if real_min is None or val < real_min:
            real_min = val

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
    def veto_reason(self) -> str | None:
        return self.reason_code if self.veto else None

    @property
    def cost_multiplier(self) -> float:
        return self.k

    @property
    def expected_edge_bps(self) -> float:
        return self.expected_move_bps

    @property
    def required_edge_bps(self) -> float:
        return self.threshold_bps

    @property
    def edge_ratio(self) -> float:
        req = self.threshold_bps
        exp = self.expected_move_bps
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
    apply_kinds: set[str]

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

    # Calibrated-K store (CostKStore) — wired via set_k_store() after construction
    use_calibrated_k: bool = False
    calibrated_k_max_age_ms: int = 3 * 3600 * 1000  # stale after 3h
    _k_store: Any = None  # type: ignore[misc]  # CostKStore | None
    # Calibrated slippage store (SlippageCalReader) — wired via set_slippage_store()
    _slippage_store: Any = None  # type: ignore[misc]  # SlippageCalReader | None

    # Directional p_min bias (additive, applied AFTER calibrator).
    # Targets failure mode "counter-SMT-leader LONGs": tighten threshold for
    # signals against the confirmed cross-asset leader. Master switch defaults
    # OFF — shipping disabled, enable via ENV after shadow validation.
    directional_bias_enabled: bool = False
    directional_bias_long: float = 0.0
    directional_bias_long_countertrend: float = 0.06
    directional_bias_short: float = 0.0
    directional_bias_short_countertrend: float = 0.0

    def set_k_store(self, store: Any) -> None:
        """Wire in a CostKStore for calibrated-K lookup.

        Should be called once at startup after from_env(), e.g.:
            gate.set_k_store(CostKStore.load(redis))
        """
        object.__setattr__(self, "_k_store", store)

    def set_slippage_store(self, store: Any) -> None:
        """Wire in a SlippageCalReader for per-symbol calibrated slippage.

        Should be called once at startup after from_env(), e.g.:
            gate.set_slippage_store(SlippageCalReader(redis))

        When set, evaluate() floors slippage_bps with q75(adverse_bps_t) per (symbol × session).
        Fail-open: if store is stale or has no entry for the symbol, slippage_bps_default is used.
        """
        object.__setattr__(self, "_slippage_store", store)

    @classmethod
    def from_env(cls) -> EdgeCostGate:
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
        # Example: 0.0005 => 5 bps one-way => 10 bps round-trip.
        if _cached_getenv("EDGE_FEES_BPS_DEFAULT") is None and _cached_getenv("CRYPTO_COMMISSION_RATE") is not None:
            try:
                one_way_bps = _env_float("CRYPTO_COMMISSION_RATE", 0.0) * 10_000.0
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

        # Calibrated-K from CostKStore (auto-calibrator)
        use_calibrated_k = _env_bool("EDGE_USE_CALIBRATED_K", False)
        calibrated_k_max_age_ms = int(_env_float("EDGE_CALIBRATED_K_MAX_AGE_MS", 3 * 3600 * 1000))

        # Directional p_min bias (post-calibrator, default disabled).
        # Counter-trend = signal side ≠ SMT bundle leader direction with
        # leader_confirm=1 and non-stale state. See pre_publish_gates.
        directional_bias_enabled = _env_bool("EDGE_DIRECTIONAL_BIAS_ENABLED", False)
        directional_bias_long = _env_float("EDGE_DIRECTIONAL_BIAS_LONG", 0.0)
        directional_bias_long_countertrend = _env_float(
            "EDGE_DIRECTIONAL_BIAS_LONG_COUNTERTREND", 0.06
        )
        directional_bias_short = _env_float("EDGE_DIRECTIONAL_BIAS_SHORT", 0.0)
        directional_bias_short_countertrend = _env_float(
            "EDGE_DIRECTIONAL_BIAS_SHORT_COUNTERTREND", 0.0
        )

        return cls(
            enabled=enabled,
            mode=mode,  # type: ignore[arg-type]
            strict_missing_levels=strict_missing_levels,
            apply_kinds=apply_kinds,
            k_default=k_default,
            k_by_symbol=k_by_symbol,
            fees_bps_default=fees_bps_default,
            slippage_bps_default=slippage_bps_default,
            slippage_use_spread_half=slippage_use_spread_half,
            min_expected_move_bps_default=min_expected_move_bps_default,
            min_expected_move_bps_by_symbol=min_expected_move_bps_by_symbol,
            ev_min_trades=ev_min_trades,
            ev_strict_missing_stats=ev_strict_missing_stats,
            ev_dynamic_k_enabled=ev_dynamic_k_enabled,
            ev_dynamic_k_atr_mult=ev_dynamic_k_atr_mult,
            ev_p_min=ev_p_min,
            ev_p_min_by_kind=ev_p_min_by_kind,
            buffer_base_bps=buffer_base_bps,
            buffer_atr_mult=buffer_atr_mult,
            buffer_spread_mult=buffer_spread_mult,
            buffer_max_bps=buffer_max_bps,
            use_calibrated_k=use_calibrated_k,
            calibrated_k_max_age_ms=calibrated_k_max_age_ms,
            directional_bias_enabled=directional_bias_enabled,
            directional_bias_long=directional_bias_long,
            directional_bias_long_countertrend=directional_bias_long_countertrend,
            directional_bias_short=directional_bias_short,
            directional_bias_short_countertrend=directional_bias_short_countertrend,
        )

    def _k_for(self, symbol: str, regime: str | None = None) -> float:
        """Получение K коэффициента для символа.

        Priority: CostKStore calibrated (if fresh) → symbol ENV override → default.
        """
        s = _norm_symbol(symbol)
        if self.use_calibrated_k and self._k_store is not None:
            try:
                store_age = getattr(self._k_store, "age_ms", self.calibrated_k_max_age_ms + 1)
                if store_age <= self.calibrated_k_max_age_ms:
                    cal_k = self._k_store.get_k(s, regime, default=-1.0)
                    if cal_k > 0:
                        return cal_k
            except Exception:
                pass
        return self.k_by_symbol.get(s, self.k_default)

    def _p_min_for_kind(
        self,
        kind: str,
        *,
        symbol: str = "",
        regime: str = "",
        side: str = "",
        ctx: Any = None,
    ) -> float:
        """
        Получение минимального порога вероятности для kind.

        Permission hierarchy (highest priority first):
          1. PEdgeThresholdReader (per-symbol×regime×kind, when enabled and
             AUTOCAL_P_EDGE_READ_ENABLED=1 and the snapshot is fresh).
          2. EDGE_EV_P_MIN_<KIND> env override (`ev_p_min_by_kind`).
          3. EDGE_EV_P_MIN default (`ev_p_min`).

        Post-hierarchy step: optional directional p_min bias is added on top
        (master switch `directional_bias_enabled`). Counter-trend = signal
        side ≠ SMT bundle leader direction with leader_confirm=1 and a non-
        stale state — same definition as the SMT coherence gate uses.

        The reader's `p_min_for` uses `static_floor` as a safety floor — the
        gate's per-kind ENV value remains the *minimum* required cutoff even
        when the calibrator has dropped τ lower in shadow.
        """
        k = (kind or "").strip().lower()
        static_floor = self.ev_p_min_by_kind.get(k, self.ev_p_min)

        # Read-side adapter (None when disabled / Redis unavailable).
        try:
            from core.p_edge_threshold_reader import get_reader  # type: ignore[import-untyped]
            reader = get_reader()
        except Exception:  # boundary fail-open
            reader = None

        if reader is None:
            base = static_floor
        else:
            try:
                base = float(reader.p_min_for(
                    symbol=symbol or "",
                    regime=regime or "",
                    kind=k,
                    default=static_floor,
                    direction=(side or "*"),
                ))
            except Exception:  # boundary fail-open
                base = static_floor

        # Calibrator-tuned cutoff is honoured even when it sits *below* the
        # static floor — operators can lower the floor in ENV to enable
        # mining lower-p_edge regions, and the reader's `default` already
        # encodes this floor (reader returns at minimum `static_floor`).
        return self._apply_directional_bias(
            base, kind=k, symbol=symbol or "", side=side, ctx=ctx,
        )

    def _apply_directional_bias(
        self,
        base: float,
        *,
        kind: str,
        symbol: str,
        side: str,
        ctx: Any,
    ) -> float:
        """Add directional p_min bias on top of calibrator output.

        Counter-trend criterion (SMT-leader source of truth):
          - side ∈ {"long","short"}
          - ctx.smt_leader_dir ∈ {"UP","DOWN"} (canonical SMT bundle output)
          - ctx.smt_leader_confirm == 1
          - not ctx.smt_state_stale
          - side direction ≠ leader direction

        bias selection:
          - LONG counter-trend → directional_bias_long_countertrend (default 0.06)
          - LONG trend-aligned / no-SMT  → directional_bias_long       (default 0.00)
          - SHORT counter-trend          → directional_bias_short_countertrend (default 0.00)
          - SHORT trend-aligned / no-SMT → directional_bias_short      (default 0.00)

        Result is clipped to [base, TAU_CEIL=0.80]. Master switch
        `directional_bias_enabled=False` short-circuits to `base`.
        Boundary method — must never raise; on any error returns `base`.
        """
        if not self.directional_bias_enabled:
            return base
        try:
            sd = (side or "").strip().lower()
            if sd in ("1", "buy", "bull"):
                sd = "long"
            elif sd in ("-1", "sell", "bear"):
                sd = "short"
            if sd not in ("long", "short"):
                return base

            countertrend = False
            if ctx is not None:
                leader_dir_raw = getattr(ctx, "smt_leader_dir", "") or ""
                leader_dir = str(leader_dir_raw).strip().upper()
                leader_confirm = int(getattr(ctx, "smt_leader_confirm", 0) or 0)
                stale = bool(getattr(ctx, "smt_state_stale", True))
                if leader_dir in ("UP", "DOWN") and leader_confirm == 1 and not stale:
                    leader_side = "long" if leader_dir == "UP" else "short"
                    countertrend = (sd != leader_side)

            if sd == "long":
                bias = (
                    self.directional_bias_long_countertrend
                    if countertrend
                    else self.directional_bias_long
                )
            else:  # short
                bias = (
                    self.directional_bias_short_countertrend
                    if countertrend
                    else self.directional_bias_short
                )

            if bias <= 0.0:
                return base

            # TAU_CEIL is the calibrator's hard upper bound; honour it here so
            # the gate never demands p_min > 0.80 (saturates the grid).
            try:
                from core.p_edge_threshold_calibrator import TAU_CEIL  # type: ignore[import-untyped]
                tau_ceil = float(TAU_CEIL)
            except Exception:
                tau_ceil = 0.80
            new_p_min = min(tau_ceil, max(base, base + bias))

            # Observability: bucket bias to keep label cardinality bounded.
            if edge_directional_bias_applied_total is not None:
                try:
                    if bias < 0.02:
                        bucket = "0.00"
                    elif bias < 0.04:
                        bucket = "0.02"
                    elif bias < 0.06:
                        bucket = "0.04"
                    elif bias < 0.08:
                        bucket = "0.06"
                    elif bias < 0.10:
                        bucket = "0.08"
                    elif bias < 0.12:
                        bucket = "0.10"
                    else:
                        bucket = "0.12+"
                    edge_directional_bias_applied_total.labels(
                        symbol=(symbol or "unknown")[:20],
                        kind=(kind or "unknown")[:20],
                        direction=sd,
                        countertrend="1" if countertrend else "0",
                        bias_bucket=bucket,
                    ).inc()
                except Exception:
                    pass

            return new_p_min
        except Exception:  # boundary fail-open
            return base

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
            if atr is not None and entry is not None and entry > 0:
                atr_bps = (atr / entry) * 10_000.0
            else:
                atr_bps = 0.0

        # 2) Spread contribution
        spread_bps = 0.0
        try:
            sp = _extract_spread_bps_from_ctx(ctx)
            if sp > 0 and math.isfinite(sp):
                spread_bps = sp
        except Exception:
            pass

        # 3) Formula: Base + Mult1*ATR_bps + Mult2*Spread_bps
        buf = self.buffer_base_bps
        buf += atr_bps * self.buffer_atr_mult
        buf += spread_bps * self.buffer_spread_mult

        # 4) Clamp to [0, max]
        return min(self.buffer_max_bps, max(0.0, buf))

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
            return k_base

        # Extract ATR from ctx
        of = getattr(ctx, "of", None)
        atr = (
            getattr(ctx, "atr", None)
            or getattr(ctx, "atr14", None)
            or getattr(ctx, "atr_1m", None)
            or (getattr(of, "atr", None) if of is not None else None)
        )

        if atr is None:
            return k_base

        try:
            atr_f = atr
        except Exception:
            return k_base

        if not math.isfinite(atr_f) or atr_f <= 0.0:
            return k_base

        # Normalize ATR (simplified: assume typical_atr is stored or estimated)
        # For crypto: typical ATR ~ 0.5-2% of price
        # We'll use a simple heuristic: higher ATR => higher K
        # normalized_atr = (atr_f - 1.0) / 1.0  # if typical is 1.0
        # For now, use direct scaling: K *= (1 + mult * min(atr_f, 5.0))

        # Cap ATR contribution to avoid extreme K
        atr_capped = min(float(atr_f), 5.0)
        k_mult = 1.0 + self.ev_dynamic_k_atr_mult * (atr_capped / 2.0)

        return k_base * k_mult

    def _min_move_for(self, symbol: str) -> float:
        sym = _norm_symbol(symbol)
        return self.min_expected_move_bps_by_symbol.get(sym, self.min_expected_move_bps_default)

    def _costs_bps(self, ctx: Any, *, kind: str, symbol: str, tf: str | None = None) -> tuple[float, float]:
        """
        Оценка fees_bps / slippage_bps.
        ---------------------------------------------------------------------
        """
        fees_bps = self.fees_bps_default
        from core.redis_async_guard import sync_or_none as _sync_or_none
        redis_client = _sync_or_none(getattr(self, "redis", None)) or _sync_or_none(getattr(ctx, "redis", None))

        # IMPORTANT (anti-regression):
        #  - НЕ приводим ts к int() здесь. В реальном потоке ts может быть строкой "1700..",
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
            ctx,
            redis_client=redis_client,
            symbol=str(symbol or getattr(ctx, "symbol", "") or ""),
            venue=str(getattr(ctx, "venue", "") or "na"),
            ts_ms=raw_ts,
            # allow ctx.tf to participate in v2 key without depending on caller
            tf=str(getattr(ctx, "tf", None) or getattr(ctx, "timeframe", None) or "na"),
            default_bps=self.slippage_bps_default,
            use_spread_half=self.slippage_use_spread_half,
        )

        # Override with calibrated q75(adverse_bps_t) per (symbol × session) if wired.
        # Uses max() for fail-safe: never reduces below EMA/spread estimate.
        _slip_store = self._slippage_store
        if _slip_store is not None:
            try:
                _sym_str = str(symbol or getattr(ctx, "symbol", "") or "")
                _sess = str(getattr(ctx, "session", None) or "")
                _cal_bps = _slip_store.get_slippage(_sym_str, _sess, default=0.0)
                if _cal_bps > 0.0:
                    slippage_bps = max(slippage_bps, _cal_bps)
            except Exception:
                pass

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
                    sess = str(getattr(ctx, "session", None) or session_from_ts_ms(tsm) or "na")
                    tfv = _canon_tf(getattr(ctx, "tf", None) or getattr(ctx, "timeframe", None) or "na")
                    drift_factor, drift_score, drift_feat = _load_drift_active(
                        redis_client,
                        symbol=(symbol or ""),
                        venue=str(getattr(ctx, "venue", "") or "na"),
                        session=(sess or "na"),
                        tf=(tfv or "na"),
                        kind=(kind or "na"),
                    )
                    if not math.isfinite(drift_factor) or drift_factor <= 0:
                        drift_factor = 1.0
            except Exception:
                drift_factor = 1.0

            if drift_factor > 1.0:
                try:
                    cap = _env_float("EDGE_DRIFT_SLIPPAGE_CAP_MULT", 3.0)
                except Exception:
                    cap = 3.0
                mult = min(cap, max(1.0, drift_factor))
                slippage_bps = slippage_bps * mult

        try:
            ctx._drift_factor = drift_factor
            ctx._drift_score = drift_score
            ctx._drift_feature = drift_feat
            ctx._edge_drift_tighten = tighten
        except Exception:
            pass

        # Cache for Layer A/B/C enforce-gate (avoids second estimate_slippage_bps call).
        # TTL — текущий сигнал; ctx живёт 1 итерацию pipeline.
        try:
            import time as _t
            ctx._cached_slippage_bps = slippage_bps
            ctx._cached_slippage_bps_ts_ms = int(_t.time() * 1000)
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
        aa = a
        bb = b
        if aa <= 0.0 or not math.isfinite(aa) or not math.isfinite(bb):
            return float("nan")
        return abs(bb - aa) / aa * 10_000.0

    def _ev_bps(self, ctx: Any) -> tuple[float, float, float, float, int, str]:
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
        src = getattr(ctx, "tp1_hit_src", "") or ""

        if entry is None or tp1 is None or sl is None or p is None or n is None:
            return float("nan"), float("nan"), float("nan"), float("nan"), 0, src

        try:
            entry_f = entry
            tp1_f = tp1
            sl_f = sl
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

        ev = (p01 * tp1_bps) - ((1.0 - p01) * stop_bps)
        return ev, tp1_bps, stop_bps, p01, nn, src

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
            return self._bps(entry, tp1) if entry is not None and tp1 is not None else float("nan")

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
            risk_bps = self._bps(entry, sl)
            if not math.isfinite(risk_bps):
                return float("nan")
            try:
                return risk_bps * rr_f
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
            move = atr * float(mult)
            return self._bps(entry, entry + move)
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
                apply=False,
                veto=False,
                reason_code=self.REASON_SKIP,
                expected_move_bps=0.0,
                threshold_bps=0.0,
                fees_bps=0.0,
                slippage_bps=0.0,
                k=k,
                mode=self.mode,
                notes="gate_disabled_or_kind_not_applicable",
                drift_factor=1.0,
                drift_score=0.0,
                drift_feature="",
            )
            trace_gate(
                ctx,
                stage="gates",
                name="edge_cost_gate",
                passed=True,
                veto=False,
                reason_code=d.reason_code,
                metrics={"apply": False, "k": d.k, "mode": d.mode},
            )
            return d

        # ------------------------------------------------------------------
        # Costs model hardening:
        # - Fees: legacy (unchanged)
        # - Slippage: measured model with strict ts normalization and fail-open rules
        #
        # If your _costs_bps already returns a "base" slippage (default/half-spread),
        # we keep that AND take max(base, model) to avoid silently loosening the gate.
        # ------------------------------------------------------------------
        fees_bps, slip_bps = self._costs_bps(ctx, kind=(kind or ""), symbol=(symbol or ""), tf=getattr(ctx, "tf", None) or getattr(ctx, "timeframe", None))

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

        delta_list = _parse_csv_ints((_cached_getenv("EXEC_TCA_DELTA_SEC_LIST", "1,5") or "1,5"), default=(1, 5))

        exec_is_p95 = None
        exec_perm_p95 = None
        exec_real_p50 = None
        exec_perm_delta = None
        exec_flags: list = []

        if exec_mode not in {"off", "0", "false"}:
            from core.redis_async_guard import sync_or_none as _sync_or_none
            r_exec = _sync_or_none(getattr(self, "redis", None)) or _sync_or_none(getattr(ctx, "redis", None))
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
                    r_exec,
                    symbol=str(symbol or getattr(ctx, "symbol", "") or ""),
                    venue=ven,
                    session=sess,
                    tf=tfv,
                    kind=knd,
                    side=side_s,
                    delta_list=delta_list,
                )

                # surface for audit/debug
                try:
                    ctx.exec_is_p95_bps = exec_is_p95 if exec_is_p95 is not None else float("nan")
                    ctx.exec_perm_impact_p95_bps = exec_perm_p95 if exec_perm_p95 is not None else float("nan")
                    ctx.exec_realized_spread_p50_bps = exec_real_p50 if exec_real_p50 is not None else float("nan")
                    ctx.exec_perm_impact_delta_sec = exec_perm_delta or 0
                    ctx.exec_health_mode = str(exec_mode)
                except Exception:
                    pass

                is_bad = exec_is_thr > 0 and exec_is_p95 is not None and exec_is_p95 > exec_is_thr
                perm_bad = exec_perm_thr > 0 and exec_perm_p95 is not None and exec_perm_p95 > exec_perm_thr
                adv_bad = exec_real_min > -900 and exec_real_p50 is not None and exec_real_p50 < exec_real_min

                if is_bad:
                    exec_flags.append("is_p95_high")
                if perm_bad:
                    exec_flags.append("perm_impact_high")
                if adv_bad:
                    exec_flags.append("adverse_sel")
  # type: ignore
                # tighten: inflate slippage by excess over threshold (bounded by cap)  # type: ignore
                if exec_mode in {"tighten", "veto"} and exec_flags:  # type: ignore
                    d1 = max(0.0, exec_is_p95 - exec_is_thr) if is_bad else 0.0  # type: ignore
                    d2 = max(0.0, exec_perm_p95 - exec_perm_thr) if perm_bad else 0.0  # type: ignore
                    d3 = max(0.0, exec_real_min - exec_real_p50) if adv_bad else 0.0  # type: ignore
                    add = exec_add_mult * max(d1, d2, d3)
                    add = min(exec_add_cap, max(0.0, add))
                    if add > 0.0:
                        slip_bps = slip_bps + add
                        try:
                            ctx.exec_health_tighten_add_bps = add
                            if edge_exec_health_tighten_total:
                                edge_exec_health_tighten_total.labels(symbol=(symbol or "").upper() or "NA").inc()
                        except Exception:
                            pass
                    if exec_k_mult and exec_k_mult > 1.0:
                        with contextlib.suppress(Exception):
                            ctx.exec_health_tighten_k = exec_k_mult

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
                            apply=True,
                            veto=True,
                            reason_code=str(veto_reason),
                            expected_move_bps=0.0,
                            threshold_bps=0.0,
                            fees_bps=fees_bps,
                            slippage_bps=slip_bps,
                            k=k,
                            mode=self.mode,
                            notes=note,
                            exec_is_p95_bps=exec_is_p95 if exec_is_p95 is not None else float("nan"),
                            exec_perm_impact_p95_bps=exec_perm_p95 if exec_perm_p95 is not None else float("nan"),
                            exec_realized_spread_p50_bps=exec_real_p50 if exec_real_p50 is not None else float("nan"),
                            exec_perm_impact_delta_sec=exec_perm_delta or 0,
                            exec_health_mode=str(exec_mode),
                            exec_health_tighten_add_bps=float(getattr(ctx, "exec_health_tighten_add_bps", 0.0) or 0.0),
                        )
                        trace_gate(
                            ctx,
                            stage="gates",
                            name="edge_cost_gate",
                            passed=False,
                            veto=True,
                            reason_code=d.reason_code,
                            metrics={
                                "exec_is_p95": d.exec_is_p95_bps,
                                "exec_perm_p95": d.exec_perm_impact_p95_bps,
                                "exec_real_p50": d.exec_realized_spread_p50_bps,
                                "exec_flags": ",".join(exec_flags),
                            },
                        )
                        if edge_exec_health_veto_total:
                            edge_exec_health_veto_total.labels(symbol=(symbol or "").upper() or "NA", reason_code=d.reason_code).inc()
                        return d

        try:
            if exec_flags:
                ctx.exec_health_flags = ",".join(exec_flags)
        except Exception:
            pass

        k_base = k

        # Apply dynamic K if enabled (adjusts based on volatility)
        k = self._dynamic_k(k_base, ctx) if self.mode == "ev" else k_base

        # ------------------------------------------------------------------
        # NEW: Feature drift alarm (temporary tightening).
        #
        # If market microstructure distributions резко "уплыли" (obi/z_delta/spread/depth),
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
            from core.redis_async_guard import sync_or_none as _sync_or_none
            redis_client = _sync_or_none(getattr(self, "redis", None)) or _sync_or_none(getattr(ctx, "redis", None))
            # Determine session/tf/kind dims consistently.
            tsm = normalize_ts_ms(getattr(ctx, "ts_ms", None) or getattr(ctx, "ts", None) or 0)
            if tsm > 0:
                sess = str(getattr(ctx, "session", None) or session_from_ts_ms(tsm) or "na")
                tfv = _canon_tf(getattr(ctx, "tf", None) or getattr(ctx, "timeframe", None) or "na")
                knd = (kind or "na")
                drift_factor, drift_score, drift_feat = _load_drift_active(
                    redis_client,
                    symbol=(symbol or ""),
                    venue=str(getattr(ctx, "venue", "") or "na"),
                    session=(sess or "na"),
                    tf=(tfv or "na"),
                    kind=(knd or "na"),
                )
                if not math.isfinite(drift_factor) or drift_factor <= 0:
                    drift_factor = 1.0
        except Exception:
            drift_factor = 1.0

        # ------------------------------------------------------------------
        # STRICT: drift-aware K inflation.
        # ------------------------------------------------------------------
        mode = (_cached_getenv("FEATURE_DRIFT_MODE", "") or "").strip().lower()
        if not mode:
            _fdp = (os.getenv("FEATURE_DRIFT_PROFILE", "") or "").strip().lower()
            if _fdp == "hard":
                mode = "enforce"
            elif _fdp == "tighten":
                mode = "tighten"
        tighten = bool(getattr(ctx, "_edge_drift_tighten", False)) or (mode in {"enforce", "tighten"})
        if tighten and drift_factor > 1.0:
            k_cap = _env_float("EDGE_DRIFT_K_CAP_MULT", 2.5)
            km = min(k_cap, max(1.0, drift_factor))
            k = k * km

        k_eff = k

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
                k_eff = k_eff * max(1.0, f1) * max(1.0, f2) * max(1.0, f3)
        except Exception:
            pass

        # P2 FIX (2026-05-17 audit): Cap K_eff to prevent runaway inflation during stress
        # Three independent factors (drift, entry_policy, exec_health) can multiply to >2×
        # This cap ensures threshold doesn't inflate beyond 2.5× baseline
        _k_eff_cap_mult = _env_float("EDGE_EV_K_EFF_MAX_MULT", 2.5)
        if k_eff > k * _k_eff_cap_mult:
            k_eff = k * _k_eff_cap_mult

        # V2 threshold: include buffer_bps (default 0.0)
        buffer_bps = self._get_buffer_bps(ctx, symbol)
        thr = k_eff * (fees_bps + slip_bps + buffer_bps)

        # P3 FIX (2026-05-17 audit): Enhanced observability for threshold components
        # Track K multiplier inflation for dashboards & alerts
        try:
            _k_ratio = k_eff / k if k > 0 else 1.0
            # Store for downstream trace/metrics collection
            ctx._edge_cost_k_ratio = _k_ratio
            ctx._edge_cost_threshold_bps = thr
            # Alert if K inflation exceeds 1.8× (indicates systemic stress)
            if _k_ratio > 1.8:
                try:
                    from prometheus_client import Counter
                    _edge_cost_k_inflation_alert = Counter(
                        "edge_cost_k_inflation_alert_total",
                        "K multiplier inflation exceeds 1.8x during signal evaluation",
                        ["symbol"]
                    )
                    _edge_cost_k_inflation_alert.labels(symbol=(symbol or "")).inc()
                except Exception:
                    pass
        except Exception:
            pass  # Observability errors don't block signal processing

        # ------------------------------------------------------------------
        # NEW RULE: TP1 Filter (Edge-Cost Gate Micro-R Reject)
        # Ожидаемый TP1 должен быть строго больше 2 * (Commissions + Spread)
        # ------------------------------------------------------------------
        actual_tp1_bps = self._expected_move_bps(ctx, "tp1")
        if math.isfinite(actual_tp1_bps):
            tp1_limit_bps = 2.0 * (fees_bps + slip_bps)
            if actual_tp1_bps <= tp1_limit_bps:
                d = EdgeCostGateDecision(
                    apply=True, veto=True, reason_code="VETO_TP1_TOO_CLOSE",
                    expected_move_bps=actual_tp1_bps, threshold_bps=tp1_limit_bps,
                    fees_bps=fees_bps, slippage_bps=slip_bps,
                    k=k_eff, mode=self.mode,
                    notes=f"tp1_bps={actual_tp1_bps:.2f} <= 2*(costs)={tp1_limit_bps:.2f}",
                    drift_factor=drift_factor,
                    drift_score=drift_score,
                    drift_feature=(drift_feat or ""),
                )
                trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=False, veto=True,
                           reason_code="VETO_TP1_TOO_CLOSE",
                           metrics={"tp1_bps": actual_tp1_bps, "limit_bps": tp1_limit_bps})
                return d

        # -------------------------
        # P1 FIX: SL Sanity Check (2026-05-17 audit)
        # Validate SL is on correct side of entry to catch data corruption
        # -------------------------
        try:
            _entry = _safe_float(
                getattr(ctx, "entry_price", None)
                or getattr(ctx, "entry", None)
                or getattr(ctx, "price", None)
                or (getattr(getattr(ctx, "of", None), "price", None) if getattr(ctx, "of", None) is not None else None)
            )
            _sl = _safe_float(getattr(ctx, "sl_price", None) or getattr(ctx, "sl", None))
            _side_raw = getattr(ctx, "side", None) or getattr(ctx, "direction", None) or getattr(ctx, "dir", None) or ""

            if _entry > 0 and math.isfinite(_sl) and _side_raw:
                _side_u = str(_side_raw).strip().upper()
                if _side_u in ("LONG", "BUY", "1") and _sl >= _entry:
                    d = EdgeCostGateDecision(
                        apply=True, veto=True, reason_code="VETO_INVALID_SL_LONG",
                        expected_move_bps=float("nan"), threshold_bps=thr,
                        fees_bps=fees_bps, slippage_bps=slip_bps,
                        k=k_eff, mode=self.mode,
                        notes=f"SL {_sl:.2f} must be < entry {_entry:.2f} for LONG",
                        drift_factor=drift_factor,
                        drift_score=drift_score,
                        drift_feature=(drift_feat or ""),
                    )
                    trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=False, veto=True,
                               reason_code="VETO_INVALID_SL_LONG",
                               metrics={"entry": _entry, "sl": _sl})
                    return d
                elif _side_u in ("SHORT", "SELL", "-1") and _sl <= _entry:
                    d = EdgeCostGateDecision(
                        apply=True, veto=True, reason_code="VETO_INVALID_SL_SHORT",
                        expected_move_bps=float("nan"), threshold_bps=thr,
                        fees_bps=fees_bps, slippage_bps=slip_bps,
                        k=k_eff, mode=self.mode,
                        notes=f"SL {_sl:.2f} must be > entry {_entry:.2f} for SHORT",
                        drift_factor=drift_factor,
                        drift_score=drift_score,
                        drift_feature=(drift_feat or ""),
                    )
                    trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=False, veto=True,
                               reason_code="VETO_INVALID_SL_SHORT",
                               metrics={"entry": _entry, "sl": _sl})
                    return d
        except Exception:
            pass  # SL check is defensive; don't block signal on validation error

        # -------------------------
        # EV gate (probability-aware)
        # -------------------------
        if self.mode == "ev":
            ev_bps, tp1_bps, stop_bps, p, n, src = self._ev_bps(ctx)

            # Get per-(symbol × regime × kind) p_min — calibrator-tuned when
            # AUTOCAL_P_EDGE_READ_ENABLED=1 and snapshot fresh, else static ENV.
            # Side is extracted here (mirrors the slippage block above on line ~1739)
            # so that _p_min_for_kind can apply optional directional bias for
            # counter-SMT-leader signals (e.g. LONGs against a confirmed bearish leader).
            _regime = str(getattr(ctx, "market_regime", "") or getattr(ctx, "regime", "") or "")
            _side_raw_pm = getattr(ctx, "side", None) or getattr(ctx, "direction", None) or getattr(ctx, "dir", None) or ""
            _side_pm = str(_side_raw_pm).strip().lower()
            p_min = self._p_min_for_kind(
                kind, symbol=symbol or "", regime=_regime, side=_side_pm, ctx=ctx,
            )

            # 1) stats missing/invalid
            if not math.isfinite(ev_bps) or not math.isfinite(p) or not math.isfinite(tp1_bps) or not math.isfinite(stop_bps):
                if self.ev_strict_missing_stats:
                    d = EdgeCostGateDecision(
                        apply=True, veto=True, reason_code=self.REASON_EV_MISSING_INPUTS,
                        expected_move_bps=float("nan"), threshold_bps=thr,
                        fees_bps=fees_bps, slippage_bps=slip_bps,
                        k=k_eff, mode=self.mode,
                        notes="strict_missing_stats_or_levels",
                        p_hit_tp1=p if math.isfinite(p) else float("nan"),
                        p_min=p_min,
                        tp1_bps=tp1_bps, stop_bps=stop_bps, ev_bps=ev_bps,
                        stats_n=n, stats_src=src,
                        drift_factor=drift_factor,
                        drift_score=drift_score,
                        drift_feature=(drift_feat or ""),
                    )
                    trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=not d.veto, veto=d.veto,
                               reason_code=d.reason_code,
                               metrics={"ev_bps": d.ev_bps, "thr": d.threshold_bps, "p": d.p_hit_tp1, "p_min": d.p_min, "n": d.stats_n})
                    return d
                d = EdgeCostGateDecision(
                    apply=True, veto=False, reason_code=self.REASON_OK,
                    expected_move_bps=float("nan"), threshold_bps=thr,
                    fees_bps=fees_bps, slippage_bps=slip_bps,
                    k=k_eff, mode=self.mode,
                    notes="missing_ev_inputs_fail_open",
                    p_hit_tp1=p if math.isfinite(p) else float("nan"),
                    p_min=p_min,
                    tp1_bps=tp1_bps, stop_bps=stop_bps, ev_bps=ev_bps,
                    stats_n=n, stats_src=src,
                    drift_factor=drift_factor,
                    drift_score=drift_score,
                    drift_feature=(drift_feat or ""),
                )
                trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=not d.veto, veto=d.veto,
                           reason_code=d.reason_code,
                           metrics={"ev_bps": d.ev_bps, "thr": d.threshold_bps, "p": d.p_hit_tp1, "p_min": d.p_min, "n": d.stats_n})
                return d

            # 2) cold-start guard (avoid acting on noisy early estimates)
            if n < self.ev_min_trades:
                if self.ev_strict_missing_stats:
                    return EdgeCostGateDecision(
                        apply=True, veto=True, reason_code=self.REASON_EV_INSUFFICIENT_STATS,
                        expected_move_bps=ev_bps, threshold_bps=thr,
                        fees_bps=fees_bps, slippage_bps=slip_bps,
                        k=k_eff, mode=self.mode,
                        notes="strict_insufficient_stats",
                        p_hit_tp1=p, p_min=p_min,
                        tp1_bps=tp1_bps, stop_bps=stop_bps, ev_bps=ev_bps,
                        stats_n=n, stats_src=src,
                        drift_factor=drift_factor,
                        drift_score=drift_score,
                        drift_feature=(drift_feat or ""),
                    )
                return EdgeCostGateDecision(
                    apply=True, veto=False, reason_code=self.REASON_OK,
                    expected_move_bps=ev_bps, threshold_bps=thr,
                    fees_bps=fees_bps, slippage_bps=slip_bps,
                    k=k_eff, mode=self.mode,
                    notes="insufficient_stats_fail_open",
                    p_hit_tp1=p, p_min=p_min,
                    tp1_bps=tp1_bps, stop_bps=stop_bps, ev_bps=ev_bps,
                    stats_n=n, stats_src=src,
                    drift_factor=drift_factor,
                    drift_score=drift_score,
                    drift_feature=(drift_feat or ""),
                )

            # 3) probability floor (using per-kind threshold)
            if p < p_min:
                d = EdgeCostGateDecision(
                    apply=True, veto=True, reason_code=self.REASON_EV_PROB,
                    expected_move_bps=ev_bps, threshold_bps=thr,
                    fees_bps=fees_bps, slippage_bps=slip_bps,
                    k=k_eff, mode=self.mode,
                    notes="p_below_min",
                    p_hit_tp1=p, p_min=p_min,
                    tp1_bps=tp1_bps, stop_bps=stop_bps, ev_bps=ev_bps,
                    stats_n=n, stats_src=src,
                    drift_factor=drift_factor,
                    drift_score=drift_score,
                    drift_feature=(drift_feat or ""),
                )
                trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=not d.veto, veto=d.veto,
                           reason_code=d.reason_code,
                           metrics={"ev_bps": d.ev_bps, "thr": d.threshold_bps, "p": d.p_hit_tp1, "p_min": d.p_min, "n": d.stats_n})
                return d

            # 4) EV >= costs*K (using potentially dynamic K)
            veto = ev_bps < thr
            d = EdgeCostGateDecision(
                apply=True,
                veto=veto,
                reason_code=self.REASON_EV_BELOW_K if veto else self.REASON_OK,
                expected_move_bps=ev_bps,  # keep legacy field meaningful for logs
                threshold_bps=thr,
                fees_bps=fees_bps,
                slippage_bps=slip_bps,
                k=k_eff,
                mode=self.mode,
                notes="",
                p_hit_tp1=p, p_min=p_min,
                tp1_bps=tp1_bps, stop_bps=stop_bps, ev_bps=ev_bps,
                stats_n=n, stats_src=src,
                drift_factor=drift_factor,
                drift_score=drift_score,
                drift_feature=(drift_feat or ""),
                total_costs_bps=fees_bps + slip_bps + buffer_bps,
                buffer_bps=buffer_bps,
                edge_source=self.mode,
            )
            trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=not d.veto, veto=d.veto,
                       reason_code=d.reason_code,
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
                    apply=True, veto=True, reason_code=self.REASON_MISSING_LEVELS,
                    expected_move_bps=float("nan"), threshold_bps=thr,
                    fees_bps=fees_bps, slippage_bps=slip_bps,
                    k=k_eff, mode=self.mode,
                    notes="strict_missing_levels",
                    drift_factor=drift_factor,
                    drift_score=drift_score,
                    drift_feature=(drift_feat or ""),
                )
                trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=not d.veto, veto=d.veto,
                           reason_code=d.reason_code,
                           metrics={"ev_bps": d.ev_bps, "thr": d.threshold_bps, "p": d.p_hit_tp1, "p_min": d.p_min, "n": d.stats_n})
                return d
            d = EdgeCostGateDecision(
                apply=True, veto=False, reason_code=self.REASON_OK,
                expected_move_bps=float("nan"), threshold_bps=thr,
                fees_bps=fees_bps, slippage_bps=slip_bps,
                k=k_eff, mode=self.mode,
                notes="missing_levels_fail_open",
                drift_factor=drift_factor,
                drift_score=drift_score,
                drift_feature=(drift_feat or ""),
            )
            trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=not d.veto, veto=d.veto,
                       reason_code=d.reason_code,
                       metrics={"ev_bps": d.ev_bps, "thr": d.threshold_bps, "p": d.p_hit_tp1, "p_min": d.p_min, "n": d.stats_n})
            return d
        # Hard floor: reject if expected_move < EDGE_MIN_EXPECTED_MOVE_BPS (anti-churn).
        min_move = self._min_move_for(symbol)
        if min_move > 0.0 and exp_bps < min_move:
            d = EdgeCostGateDecision(
                apply=True, veto=True, reason_code="VETO_EDGE_TOO_SMALL",
                expected_move_bps=exp_bps, threshold_bps=thr,
                fees_bps=fees_bps, slippage_bps=slip_bps,
                k=k_eff, mode=self.mode,
                notes=f"min_expected_move_floor={min_move}",
                drift_factor=drift_factor,
                drift_score=drift_score,
                drift_feature=(drift_feat or ""),
            )
            trace_gate(ctx, stage="gates", name="edge_cost_gate", passed=False, veto=True,
                       reason_code="VETO_EDGE_TOO_SMALL",
                       metrics={"expected_move_bps": exp_bps, "min_move": min_move})
            return d

        veto = exp_bps < thr
        decision = EdgeCostGateDecision(
            apply=True,
            veto=veto,
            reason_code=self.REASON_BELOW_K if veto else self.REASON_OK,
            expected_move_bps=exp_bps,
            threshold_bps=thr,
            fees_bps=fees_bps,
            slippage_bps=slip_bps,
            k=k_eff,
            mode=self.mode,
            notes="",
            drift_factor=drift_factor,
            drift_score=drift_score,
            drift_feature=(drift_feat or ""),
            total_costs_bps=fees_bps + slip_bps + buffer_bps,
            buffer_bps=buffer_bps,
            edge_source=self.mode,
        )

        # =====================================================================
        # DecisionTrace (fail-open)
        # =====================================================================
        try:
            _span.__exit__(None, None, None)
            trace_gate(
                ctx,
                stage="gates",
                name="edge_cost_gate",
                passed=(decision.apply and (not decision.veto)),
                veto=decision.veto,
                reason_code=(decision.reason_code or ""),
                metrics={
                    "apply": decision.apply,
                    "expected_move_bps": decision.expected_move_bps,
                    "threshold_bps": decision.threshold_bps,
                    "fees_bps": decision.fees_bps,
                    "slippage_bps": decision.slippage_bps,
                    "k": decision.k,
                    "mode": getattr(decision, "mode", "") or "",
                    # EV-mode diagnostics (if enabled upstream; defaults are NaN)
                    "p_hit_tp1": getattr(decision, "p_hit_tp1", float("nan")),
                    "p_min": getattr(decision, "p_min", float("nan")),
                    "tp1_bps": getattr(decision, "tp1_bps", float("nan")),
                    "stop_bps": getattr(decision, "stop_bps", float("nan")),
                    "ev_bps": decision.ev_bps,
                    "stats_n": getattr(decision, "stats_n", 0) or 0,
                    "stats_src": getattr(decision, "stats_src", "") or "",
                },
                duration_ms=float(_span.ms),
            )
        except Exception:
            pass

        return decision

