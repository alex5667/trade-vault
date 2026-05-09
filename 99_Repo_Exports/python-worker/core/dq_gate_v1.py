import math
import os
from typing import Any

from core.core_snapshot.dq_observe_only import apply_observe_only_book_veto
from core.core_snapshot.runtime_clock import snapshot as runtime_snapshot


def _cfg_get(cfg2: dict[str, Any], key: str, default: Any, *, env: str | None = None, aliases: list[str] | None = None) -> Any:
    """Config getter with optional aliases + env fallback.

    - cfg2 wins over env
    - env is used only when cfg2 missing
    - returns `default` if nothing set / cast fails
    """
    keys = [key] + (aliases or [])
    for k in keys:
        if k in cfg2 and cfg2.get(k) is not None:
            return cfg2.get(k)
    if env:
        v = os.getenv(env)
        if v is not None and v != "":
            return v
    return default


def _cfg_float(cfg2: dict[str, Any], key: str, default: float, *, env: str | None = None, aliases: list[str] | None = None) -> float:
    v = _cfg_get(cfg2, key, default, env=env, aliases=aliases)
    try:
        return float(v)
    except Exception:
        return default


def _cfg_int(cfg2: dict[str, Any], key: str, default: int, *, env: str | None = None, aliases: list[str] | None = None) -> int:
    v = _cfg_get(cfg2, key, default, env=env, aliases=aliases)
    try:
        return int(float(v))
    except Exception:
        return default


def eval_dq_gate(indicators: dict[str, Any], cfg2: dict[str, Any]) -> dict[str, Any]:
    """
    P14 DQ / time-determinism gate (services-ветка).
    Returns a dict that is safe to persist into decision-record.

    Contract:
      - dq_level is always present (0/1/2)
      - dq_reason_bucket ∈ {ok,gap_p95,tick_seq,book_seq,data_health}
      - dq_reasons is always present (list[str])

    Design:
      - dq_pen is always computed (penalty mode)
      - dq_veto is applied only when dq_gate_mode enables it
      - observe-only is applied ONLY to book-seq hard veto (warmup/disabled),
        never suppresses tick-gap or tick-missing-seq hard veto.
    """
    enable = _cfg_int(cfg2, "dq_gate_enable", 0, env="DQ_GATE_ENABLE")
    if not enable:
        return {
            "dq_pen": 0.0,
            "dq_veto": 0,
            "dq_level": 0,
            "dq_reason": "disabled",
            "dq_reason_bucket": "ok",
            "dq_reasons": [],
            "dq_health_score": 1.0,
            "dq_components": {},
        }

    # Extract indicators with default fail-open values (NaN/Inf -> fail open).
    def _get(k: str, default: float = 0.0, aliases: list[str] | None = None) -> float:
        val = indicators.get(k)
        if val is None and aliases:
            for al in aliases:
                val = indicators.get(al)
                if val is not None:
                    break

        if val is None:
            return default
        try:
            fval = float(val)
            if math.isnan(fval) or math.isinf(fval):
                return default
            return fval
        except (ValueError, TypeError):
            return default

    # -----------------------------
    # 1) Inputs
    # -----------------------------
    # DQ / Health
    data_health = _get("data_health", 1.0)
    book_health_ok = _get("book_health_ok", 1.0)
    tick_time_age_ms = _get("tick_time_age_ms", 0.0)

    # Skew/Desync EMAs (higher is worse)
    tick_ts_source_now_ema = _get("tick_ts_source_now_ema", 0.0)
    tick_ts_source_stream_id_ema = _get("tick_ts_source_stream_id_ema", 0.0)

    # B-step: hard/soft controls for runtime gaps and missing sequence EMAs
    tick_gap_p95_ms = _get("tick_gap_p95_ms", 0.0, aliases=["tick_gap_p95"])
    tick_missing_seq_ema = _get("tick_missing_seq_ema", 0.0, aliases=["tick_missing_seq"])
    book_missing_seq_ema = _get("book_missing_seq_ema", 0.0, aliases=["book_missing_seq"])

    # Optional data-health signals (if present)
    feature_nan_rate_ema = _get("feature_nan_rate_ema", 0.0, aliases=["nan_rate_ema", "feature_nan_ema"])
    feature_stuck_sec = _get("feature_stuck_sec", 0.0, aliases=["feature_stuck_s", "feature_stuck_seconds"])

    # -----------------------------
    # 2) Thresholds / defaults
    # -----------------------------
    mode = str(_cfg_get(cfg2, "dq_gate_mode", "penalty", env="DQ_GATE_MODE")).lower()
    pen_max = _cfg_float(cfg2, "dq_pen_max", 0.10, env="DQ_PEN_MAX")

    # data_health thresholds
    data_health_min = _cfg_float(cfg2, "dq_data_health_min", 0.85, env="DQ_DATA_HEALTH_MIN")
    data_health_hard_min = _cfg_float(cfg2, "dq_data_health_hard_min", 0.70, env="DQ_DATA_HEALTH_HARD_MIN")

    # latency/skew thresholds
    age_soft_ms = _cfg_float(cfg2, "dq_tick_age_ms_max", 5000.0, env="DQ_TICK_AGE_MS_MAX")
    age_hard_ms = _cfg_float(cfg2, "dq_tick_age_ms_hard", 15000.0, env="DQ_TICK_AGE_MS_HARD")
    skew_soft_ms = _cfg_float(cfg2, "dq_skew_ema_ms_max", 1000.0, env="DQ_SKEW_EMA_MS_MAX")
    skew_hard_ms = _cfg_float(cfg2, "dq_skew_ema_ms_hard", 5000.0, env="DQ_SKEW_EMA_MS_HARD")

    # tick gap p95 thresholds
    gap_soft_ms = _cfg_float(cfg2, "dq_tick_gap_p95_soft_ms", 3000.0, env="DQ_TICK_GAP_P95_SOFT_MS", aliases=["gap_soft_ms", "dq_gap_soft_ms"])
    gap_hard_ms = _cfg_float(cfg2, "dq_tick_gap_p95_hard_ms", 10000.0, env="DQ_TICK_GAP_P95_HARD_MS", aliases=["gap_hard_ms", "dq_gap_hard_ms"])
    gap_extreme_ms = _cfg_float(cfg2, "dq_tick_gap_p95_extreme_ms", 30000.0, env="DQ_TICK_GAP_P95_EXTREME_MS", aliases=["gap_extreme_ms", "dq_gap_extreme_ms"])

    # missing seq thresholds
    tick_seq_soft = _cfg_float(cfg2, "dq_tick_missing_seq_soft", 2.0, env="DQ_TICK_MISSING_SEQ_SOFT", aliases=["tick_soft", "dq_tick_soft"])
    tick_seq_hard = _cfg_float(cfg2, "dq_tick_missing_seq_hard", 10.0, env="DQ_TICK_MISSING_SEQ_HARD", aliases=["tick_hard", "dq_tick_hard"])

    book_seq_soft = _cfg_float(cfg2, "dq_book_missing_seq_soft", 10.0, env="DQ_BOOK_MISSING_SEQ_SOFT", aliases=["book_soft", "dq_book_soft"])
    # Keep compatibility with older configs that used `book_hard`.
    book_seq_hard = _cfg_float(cfg2, "book_hard", 30.0, env="DQ_BOOK_HARD", aliases=["dq_book_missing_seq_hard", "dq_book_hard_ema", "dq_book_hard"])

    # optional nan/stuck thresholds (data-health bucket)
    nan_soft = _cfg_float(cfg2, "dq_nan_rate_soft", 0.01, env="DQ_NAN_RATE_SOFT")
    nan_hard = _cfg_float(cfg2, "dq_nan_rate_hard", 0.05, env="DQ_NAN_RATE_HARD")
    stuck_soft_s = _cfg_float(cfg2, "dq_feature_stuck_soft_sec", 15.0, env="DQ_FEATURE_STUCK_SOFT_SEC")
    stuck_hard_s = _cfg_float(cfg2, "dq_feature_stuck_hard_sec", 60.0, env="DQ_FEATURE_STUCK_HARD_SEC")

    # -----------------------------
    # 3) Severity evaluation (dq_level, reasons)
    # -----------------------------
    dq_level = 0
    dq_reasons: list[str] = []

    def _bump(level: int, reason: str) -> None:
        nonlocal dq_level
        if reason not in dq_reasons:
            dq_reasons.append(reason)
        dq_level = max(dq_level, int(level))

    # data_health
    if data_health < data_health_min:
        _bump(1, "low_data_health")
    if data_health < data_health_hard_min:
        _bump(2, "low_data_health_hard")

    # book health
    if book_health_ok < 0.5:
        _bump(1, "book_stale")

    # latency / skew
    if tick_time_age_ms > age_soft_ms:
        _bump(1, "latency_spike")
    if tick_time_age_ms > age_hard_ms:
        _bump(2, "latency_hard")

    if tick_ts_source_now_ema > skew_soft_ms:
        _bump(1, "clock_skew_now")
    if tick_ts_source_now_ema > skew_hard_ms:
        _bump(2, "clock_skew_now_hard")

    if tick_ts_source_stream_id_ema > skew_soft_ms:
        _bump(1, "stream_id_skew")
    if tick_ts_source_stream_id_ema > skew_hard_ms:
        _bump(2, "stream_id_skew_hard")

    # tick gap p95
    if tick_gap_p95_ms >= gap_soft_ms:
        _bump(1, "gap_p95_soft")
    if tick_gap_p95_ms >= gap_hard_ms or tick_gap_p95_ms >= gap_extreme_ms:
        _bump(2, "gap_p95_hard")

    # tick missing seq EMA
    if tick_missing_seq_ema >= tick_seq_soft:
        _bump(1, "tick_seq_soft")
    if tick_missing_seq_ema >= tick_seq_hard:
        _bump(2, "tick_seq_hard")

    # book missing seq EMA (book-seq hard can be observe-only)
    if book_missing_seq_ema >= book_seq_soft:
        _bump(1, "book_seq_soft")
    if book_missing_seq_ema >= book_seq_hard:
        _bump(2, "book_seq_hard")

    # Optional data-health signals
    if feature_nan_rate_ema >= nan_soft:
        _bump(1, "nan_rate_soft")
    if feature_nan_rate_ema >= nan_hard:
        _bump(2, "nan_rate_hard")

    if feature_stuck_sec >= stuck_soft_s:
        _bump(1, "feature_stuck_soft")
    if feature_stuck_sec >= stuck_hard_s:
        _bump(2, "feature_stuck_hard")

    # Determine primary reason deterministically (stable priority)
    _prio = [
        "nan_rate_hard",
        "feature_stuck_hard",
        "low_data_health_hard",
        "gap_p95_hard",
        "tick_seq_hard",
        "book_seq_hard",
        "latency_hard",
        "clock_skew_now_hard",
        "stream_id_skew_hard",
        "nan_rate_soft",
        "feature_stuck_soft",
        "low_data_health",
        "gap_p95_soft",
        "tick_seq_soft",
        "book_seq_soft",
        "latency_spike",
        "clock_skew_now",
        "stream_id_skew",
        "book_stale",
    ]
    primary_reason = "ok"
    for r in _prio:
        if r in dq_reasons:
            primary_reason = r
            break
    if primary_reason == "ok" and dq_reasons:
        primary_reason = dq_reasons[0]

    # -----------------------------
    # 4) Penalty score (always computed)
    # -----------------------------
    # Keep the previous "health_score" semantics, but degrade it for B-step signals as well.
    health_score = 1.0


    # health
    if data_health < data_health_min:
        health_score *= max(0.0, min(1.0, data_health))
    if book_health_ok < 0.5:
        health_score *= 0.5

    # latency/skew
    if tick_time_age_ms > age_soft_ms:
        health_score *= 0.1

    if tick_ts_source_now_ema > skew_soft_ms:
        health_score *= 0.7

    if tick_ts_source_stream_id_ema > skew_soft_ms:
        health_score *= 0.8

    # B-step: gap/missing-seq penalties
    if tick_gap_p95_ms >= gap_soft_ms:
        health_score *= 0.8
    if tick_gap_p95_ms >= gap_hard_ms:
        health_score *= 0.2

    if tick_missing_seq_ema >= tick_seq_soft:
        health_score *= 0.85
    if tick_missing_seq_ema >= tick_seq_hard:
        health_score *= 0.3

    if book_missing_seq_ema >= book_seq_soft:
        health_score *= 0.85
    if book_missing_seq_ema >= book_seq_hard:
        health_score *= 0.2

    # Optional nan/stuck penalties
    if feature_nan_rate_ema >= nan_soft:
        health_score *= 0.7
    if feature_nan_rate_ema >= nan_hard:
        health_score *= 0.2
    if feature_stuck_sec >= stuck_soft_s:
        health_score *= 0.8
    if feature_stuck_sec >= stuck_hard_s:
        health_score *= 0.2

    health_score = max(0.0, min(1.0, health_score))

    dq_pen = (1.0 - health_score) * pen_max
    dq_pen = max(0.0, min(pen_max, dq_pen))

    # -----------------------------
    # 5) Veto logic + observe-only for book hard-veto
    # -----------------------------
    veto_mode = mode in ("enforce", "both", "veto")

    # Veto candidates (hard-level reasons)
    hard_reasons = {r for r in dq_reasons if ("_hard" in r) or (r.endswith("hard"))}
    veto_other = 0
    veto_book = 1 if "book_seq_hard" in hard_reasons else 0

    # Non-book hard triggers => veto immediately in veto/enforce mode
    if veto_mode:
        if "gap_p95_hard" in hard_reasons:
            veto_other = 1
        if "tick_seq_hard" in hard_reasons:
            veto_other = 1
        if "low_data_health_hard" in hard_reasons:
            veto_other = 1
        if "nan_rate_hard" in hard_reasons or "feature_stuck_hard" in hard_reasons:
            veto_other = 1
        if "latency_hard" in hard_reasons or "clock_skew_now_hard" in hard_reasons or "stream_id_skew_hard" in hard_reasons:
            veto_other = 1

        # Back-compat: if health_score itself is catastrophically low, also veto.
        if health_score < data_health_hard_min:
            veto_other = 1

    # Apply observe-only ONLY to book veto.
    suppressed = False
    suppress_reason = ""
    if veto_mode and veto_book == 1:
        clock = runtime_snapshot(event_ts_ms=indicators.get("event_ts_ms") or indicators.get("tick_ts_source_now"))
        out = apply_observe_only_book_veto(
            dq_level=2,
            dq_veto=1,
            dq_reason_bucket="book_seq",
            dq_reasons=dq_reasons,
            uptime_sec=clock.uptime_sec,
            cfg=cfg2,
        )
        veto_book = out.dq_veto
        suppressed = out.suppressed
        suppress_reason = out.suppress_reason
    else:
        clock = runtime_snapshot(event_ts_ms=indicators.get("event_ts_ms") or indicators.get("tick_ts_source_now"))

    dq_veto = 1 if (veto_mode and (veto_other == 1 or veto_book == 1)) else 0

    # -----------------------------
    # 6) Output
    # -----------------------------
    bucket = _reason_bucket(primary_reason)

    res: dict[str, Any] = {
        "dq_pen": float(dq_pen),
        "dq_veto": int(dq_veto),
        "dq_level": int(dq_level),
        "dq_reason": str(primary_reason),
        "dq_reason_bucket": str(bucket),
        "dq_reasons": list(dq_reasons),
        "dq_health_score": float(health_score),
        "dq_components": {
            "data_health": data_health,
            "book_health_ok": book_health_ok,
            "tick_time_age_ms": tick_time_age_ms,
            "skew_now_ema_ms": tick_ts_source_now_ema,
            "skew_stream_ema_ms": tick_ts_source_stream_id_ema,
            "tick_gap_p95_ms": tick_gap_p95_ms,
            "tick_missing_seq_ema": tick_missing_seq_ema,
            "book_missing_seq_ema": book_missing_seq_ema,
            "thr": {
                "data_health_min": data_health_min,
                "data_health_hard_min": data_health_hard_min,
                "gap_soft_ms": gap_soft_ms,
                "gap_hard_ms": gap_hard_ms,
                "gap_extreme_ms": gap_extreme_ms,
                "tick_seq_soft": tick_seq_soft,
                "tick_seq_hard": tick_seq_hard,
                "book_seq_soft": book_seq_soft,
                "book_seq_hard": book_seq_hard,
                "nan_soft": nan_soft,
                "nan_hard": nan_hard,
                "stuck_soft_s": stuck_soft_s,
                "stuck_hard_s": stuck_hard_s,
            },
        },
        "uptime_sec": int(clock.uptime_sec),
    }
    if clock.runtime_start_ts_ms is not None:
        res["runtime_start_ts_ms"] = int(clock.runtime_start_ts_ms)
    if suppressed:
        res["dq_veto_suppressed"] = 1
        res["dq_veto_suppressed_reason"] = str(suppress_reason)

    return res


def _reason_bucket(reason: str) -> str:
    """Map reason -> bucket (expanded buckets required by B-step)."""
    if not reason or reason == "ok":
        return "ok"
    r = reason.lower()
    if "gap_p95" in r or "tick_gap" in r:
        return "gap_p95"
    if "tick_seq" in r or "missing_seq" in r:
        return "tick_seq"
    if "book_seq" in r:
        return "book_seq"
    # latency/skew/health/nan/stuck are all treated as data-health bucket
    if "health" in r or "nan" in r or "stuck" in r or "latency" in r or "skew" in r or "clock" in r:
        return "data_health"
    if "book" in r:
        return "data_health"
    return "data_health"
