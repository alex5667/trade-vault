from __future__ import annotations

"""Prometheus metrics for confidence calibration (ROI step).

Why a dedicated module:
  - avoids editing a large existing metrics.py (low merge risk)
  - keeps calibration-related instrumentation isolated and optional

All metrics are fail-open: if prometheus_client is missing or registry conflicts,
we degrade to no-op.
"""



try:
    from prometheus_client import REGISTRY, Counter, Gauge, Histogram  # type: ignore
except Exception:  # pragma: no cover
    Counter = None  # type: ignore
    Gauge = None  # type: ignore
    Histogram = None  # type: ignore
    REGISTRY = None  # type: ignore


def _get_or_create(name: str, kind: str, documentation: str, labelnames: list[str] | None = None, buckets=None):
    """Create or return an already-registered collector with the same name."""
    if Counter is None or Gauge is None or Histogram is None or REGISTRY is None:
        return None
    try:
        if kind == "counter":
            return Counter(name, documentation, labelnames or [])
        if kind == "gauge":
            return Gauge(name, documentation, labelnames or [])
        if kind == "hist":
            if labelnames:
                return Histogram(name, documentation, labelnames, buckets=buckets)
            return Histogram(name, documentation, buckets=buckets)
        return None
    except ValueError:
        # already registered
        try:
            for collector in REGISTRY._collector_to_names:  # type: ignore[attr-defined]
                if name in REGISTRY._collector_to_names[collector]:  # type: ignore[attr-defined]
                    return collector
        except Exception:
            return None
        return None


# ---------------------------------------------------------------------------
# File / lifecycle telemetry
# ---------------------------------------------------------------------------

confidence_cal_present_gauge = _get_or_create(
    "confidence_cal_present",
    "gauge",
    "Whether confidence calibrator file is present and readable (1/0)",
    ["symbol"],
)

confidence_cal_age_ms_gauge = _get_or_create(
    "confidence_cal_age_ms",
    "gauge",
    "Age of confidence calibrator file based on mtime (ms)",
    ["symbol"],
)

confidence_cal_stale_gauge = _get_or_create(
    "confidence_cal_stale",
    "gauge",
    "Whether calibrator file is stale beyond configured max_age_ms (1/0)",
    ["symbol"],
)

confidence_cal_reload_total = _get_or_create(
    "confidence_cal_reload_total",
    "counter",
    "Total calibrator reload attempts (success/fail)"
    ["symbol", "result"],
)

confidence_cal_apply_total = _get_or_create(
    "confidence_cal_apply_total",
    "counter",
    "Total times calibration was applied to a confidence value"
    ["symbol", "key"],
)

confidence_cal_delta_abs_hist = _get_or_create(
    "confidence_cal_delta_abs",
    "hist",
    "Abs(calibrated - raw) confidence shift"
    ["symbol", "key"],
    buckets=[0.0, 0.005, 0.01, 0.02, 0.03, 0.05, 0.08, 0.10, 0.15, 0.20, 0.30, 0.50, 1.0],
)


# ---------------------------------------------------------------------------
# Train-time calibration quality (from calibrator JSON train_report)
# ---------------------------------------------------------------------------

confidence_cal_train_ece_raw_gauge = _get_or_create(
    "confidence_cal_train_ece_raw",
    "gauge",
    "Train-time ECE of raw confidence used to fit calibrator"
    ["symbol"],
)

confidence_cal_train_ece_cal_gauge = _get_or_create(
    "confidence_cal_train_ece_cal",
    "gauge",
    "Train-time ECE after calibration"
    ["symbol"],
)

confidence_cal_train_brier_raw_gauge = _get_or_create(
    "confidence_cal_train_brier_raw",
    "gauge",
    "Train-time Brier of raw confidence used to fit calibrator"
    ["symbol"],
)

confidence_cal_train_brier_cal_gauge = _get_or_create(
    "confidence_cal_train_brier_cal",
    "gauge",
    "Train-time Brier after calibration"
    ["symbol"],
)

confidence_cal_info_gauge = _get_or_create(
    "confidence_cal_info",
    "gauge",
    "Calibrator info (always 1)"
    ["symbol", "type", "schema_version"],
)


def _safe_labels(metric, **labels):
    try:
        return metric.labels(**labels) if metric is not None else None
    except Exception:
        return None


def emit_file_state(symbol: str, *, present: int, age_ms: int, stale: int) -> None:
    try:
        s = symbol or "unknown"
        m = _safe_labels(confidence_cal_present_gauge, symbol=s)
        if m is not None: m.set(float(present))
        m = _safe_labels(confidence_cal_age_ms_gauge, symbol=s)
        if m is not None: m.set(float(age_ms))
        m = _safe_labels(confidence_cal_stale_gauge, symbol=s)
        if m is not None: m.set(float(stale))
    except Exception:
        pass


def emit_train_report(symbol: str, cal_type: str, schema_version: int, raw_ece: float, cal_ece: float, raw_brier: float, cal_brier: float) -> None:
    try:
        s = symbol or "unknown"
        m = _safe_labels(confidence_cal_train_ece_raw_gauge, symbol=s)
        if m is not None: m.set(float(raw_ece))
        m = _safe_labels(confidence_cal_train_ece_cal_gauge, symbol=s)
        if m is not None: m.set(float(cal_ece))
        m = _safe_labels(confidence_cal_train_brier_raw_gauge, symbol=s)
        if m is not None: m.set(float(raw_brier))
        m = _safe_labels(confidence_cal_train_brier_cal_gauge, symbol=s)
        if m is not None: m.set(float(cal_brier))
        m = _safe_labels(confidence_cal_info_gauge, symbol=s, type=(cal_type or "unknown"), schema_version=str(int(schema_version)))
        if m is not None: m.set(1.0)
    except Exception:
        pass


def inc_reload(symbol: str, result: str) -> None:
    try:
        s = symbol or "unknown"
        r = result or "unknown"
        m = _safe_labels(confidence_cal_reload_total, symbol=s, result=r)
        if m is not None: m.inc()
    except Exception:
        pass


def inc_apply(symbol: str, key: str) -> None:
    try:
        s = symbol or "unknown"
        k = key or "confidence"
        m = _safe_labels(confidence_cal_apply_total, symbol=s, key=k)
        if m is not None: m.inc()
    except Exception:
        pass



def obs_delta_abs(symbol: str, key: str, delta_abs: float) -> None:
    try:
        s = symbol or "unknown"
        k = key or "confidence"
        h = _safe_labels(confidence_cal_delta_abs_hist, symbol=s, key=k)
        if h is not None:
            v = float(delta_abs)
            if v < 0: v = -v
            h.observe(v)
    except Exception:
        pass


confidence_cal_bucket_hit_total = _get_or_create(
    "confidence_cal_bucket_hit_total",
    "counter",
    "Count of confidence calibration applications by bucket level/method",
    ["symbol", "arm", "bucket_by", "bucket_level", "method"],
)

confidence_cal_ab_arm_total = _get_or_create(
    "confidence_cal_ab_arm_total",
    "counter",
    "Count of A/B test arm assignments",
    ["symbol", "arm"],
)


def inc_bucket_hit(symbol: str, arm: str, bucket_by: str, bucket_level: str, method: str) -> None:
    try:
        s = symbol or "unknown"
        a = arm or "default"
        bb = bucket_by or "unknown"
        bl = bucket_level or "unknown"
        m = method or "unknown"

        ctr = _safe_labels(confidence_cal_bucket_hit_total, symbol=s, arm=a, bucket_by=bb, bucket_level=bl, method=m)
        if ctr is not None:
            ctr.inc()
    except Exception:
        pass


def inc_ab_arm(symbol: str, arm: str) -> None:
    try:
        s = symbol or "unknown"
        a = arm or "unknown"
        ctr = _safe_labels(confidence_cal_ab_arm_total, symbol=s, arm=a)
        if ctr is not None:
            ctr.inc()
    except Exception:
        pass

