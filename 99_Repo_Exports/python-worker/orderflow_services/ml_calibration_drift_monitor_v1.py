#!/usr/bin/env python3
"""Task 2.1 — rolling 7d Brier/ECE monitor + cusum drift alarm.

Companion to `ml_outcome_calibration_tracker_v1.py` (EMA-based) and
`tools/ml_calibration_health_monitor.py` (snapshot-based). This service
maintains a strict **7-day rolling window** of (p_edge, outcome) pairs
sampled from `trades:closed`, feeds them into `CuSumDriftDetector`
(Page-Hinkley over per-trade Brier), and fires a Telegram alarm when
either:

    rolling_ece > ML_DRIFT_ECE_ALARM_TH     (default 0.05)
    rolling_brier > ML_DRIFT_BRIER_ALARM_TH (default 0.22)
    cusum drift alarm fires                  (PH > λ on Brier residuals)

The cusum detector is shared across (schema × regime) bins so we can see
which slice is drifting.

ENV
---
ML_DRIFT_MONITOR_PORT        Prometheus port (default 9860)
ML_DRIFT_GROUP               Consumer group   (default ml-drift-monitor)
ML_DRIFT_CONSUMER            Consumer name    (default ml-drift-monitor-1)
ML_DRIFT_BATCH               XREADGROUP COUNT (default 100)
ML_DRIFT_ECE_ALARM_TH        ECE alarm threshold (default 0.05)
ML_DRIFT_BRIER_ALARM_TH      Brier alarm threshold (default 0.22)
ML_DRIFT_WINDOW_DAYS         Rolling window size in days (default 7)
ML_DRIFT_PUBLISH_INTERVAL_S  How often to publish gauges + check alarms (default 300)
ML_DRIFT_TG_THROTTLE_S       Min seconds between identical Telegram alerts (default 1800)
ML_DRIFT_SCHEMA              Schema label for cusum bins (default v14_of)

Prometheus metrics
------------------
ml_drift_rolling_ece{schema}                  Rolling ECE over the 7d window
ml_drift_rolling_brier{schema}                Rolling Brier over the 7d window
ml_drift_rolling_n{schema}                    Sample count in window
ml_drift_cusum_ph_score{schema,regime}        Current Page-Hinkley score
ml_drift_cusum_alarms_total{schema,regime}    Counter of fired alarms
ml_drift_alert_total{kind}                    Telegram alerts emitted
"""
from __future__ import annotations

import logging
import math
import os
import signal
import time
from collections import deque
from typing import Any

from prometheus_client import REGISTRY, Counter, Gauge, start_http_server  # type: ignore

from core.cusum_drift_detector import CuSumDriftDetector
from core.redis_client import get_redis
from core.redis_keys import RedisStreams as RS
from utils.time_utils import get_ny_time_millis

logger = logging.getLogger("ml_drift_monitor")


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "")
    try:
        return float(raw) if raw else default
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    try:
        return int(raw) if raw else default
    except (TypeError, ValueError):
        return default


def _decode(v: Any) -> str:
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "ignore")
    return str(v) if v is not None else ""


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        f = float(x)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


def _ece(window: deque) -> float:
    """ECE over 10 deciles of the window."""
    if not window:
        return 0.0
    bins: list[list[float]] = [[0.0, 0.0, 0] for _ in range(10)]
    for p, y in window:
        idx = min(int(p * 10), 9)
        bins[idx][0] += p
        bins[idx][1] += float(y)
        bins[idx][2] += 1
    total = sum(int(b[2]) for b in bins)
    if total == 0:
        return 0.0
    ece = 0.0
    for s_p, s_y, n in bins:
        if n == 0:
            continue
        mean_p = s_p / n
        mean_y = s_y / n
        ece += (n / total) * abs(mean_p - mean_y)
    return ece


def _brier(window: deque) -> float:
    """Mean Brier over the window."""
    if not window:
        return 0.0
    s = 0.0
    for p, y in window:
        s += (p - float(y)) ** 2
    return s / len(window)


def _get_or_create_gauge(name: str, doc: str, labels: list[str]) -> Gauge:
    try:
        return Gauge(name, doc, labels)
    except ValueError:
        for c in REGISTRY._names_to_collectors.values():  # type: ignore[attr-defined]
            if getattr(c, "_name", None) == name:
                return c  # type: ignore[return-value]
        raise


def _get_or_create_counter(name: str, doc: str, labels: list[str]) -> Counter:
    try:
        return Counter(name, doc, labels)
    except ValueError:
        for c in REGISTRY._names_to_collectors.values():  # type: ignore[attr-defined]
            if getattr(c, "_name", None) == name + "_total":
                return c  # type: ignore[return-value]
            if getattr(c, "_name", None) == name:
                return c  # type: ignore[return-value]
        raise


def _field_get(fields: Any, *keys: str) -> Any:
    """Get a field by name, tolerating both str-keyed and bytes-keyed dicts.

    Redis returns bytes keys when decode_responses=False, str keys when True.
    """
    if not isinstance(fields, dict):
        return None
    for k in keys:
        if k in fields:
            return fields[k]
        kb = k.encode("utf-8")
        if kb in fields:
            return fields[kb]
    return None


def _parse_outcome(fields: Any) -> tuple[float, int, str, str] | None:
    """Extract (p_edge, win_int, schema, regime) from a trades:closed row.

    Returns None when the row lacks required fields (skip silently).
    """
    p_raw = _field_get(fields, "ml_prob", "p_edge")
    if p_raw is None:
        return None
    p = _safe_float(_decode(p_raw), -1.0)
    if p < 0.0 or p > 1.0:
        return None

    res_raw = _field_get(fields, "result") or ""
    res = _decode(res_raw).upper().strip()
    if res in ("WIN", "W", "TP", "TP1", "TP2", "TP3"):
        win = 1
    elif res in ("LOSS", "L", "SL", "STOP"):
        win = 0
    else:
        # BE / unknown — skip from calibration
        return None

    schema = _decode(_field_get(fields, "model_ver", "schema") or "").strip().lower()
    if not schema:
        schema = os.getenv("ML_DRIFT_SCHEMA", "v14_of").strip().lower()
    regime = _decode(_field_get(fields, "regime") or "").strip().lower() or "na"
    return p, win, schema, regime


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    port = _env_int("ML_DRIFT_MONITOR_PORT", 9860)
    group = os.getenv("ML_DRIFT_GROUP", "ml-drift-monitor")
    consumer = os.getenv("ML_DRIFT_CONSUMER", "ml-drift-monitor-1")
    batch = _env_int("ML_DRIFT_BATCH", 100)
    window_days = max(1, _env_int("ML_DRIFT_WINDOW_DAYS", 7))
    window_ms = window_days * 24 * 3600 * 1000
    ece_th = _env_float("ML_DRIFT_ECE_ALARM_TH", 0.05)
    brier_th = _env_float("ML_DRIFT_BRIER_ALARM_TH", 0.22)
    publish_interval_s = _env_int("ML_DRIFT_PUBLISH_INTERVAL_S", 300)
    tg_throttle_s = _env_int("ML_DRIFT_TG_THROTTLE_S", 1800)

    metric_ece = _get_or_create_gauge(
        "ml_drift_rolling_ece",
        f"Rolling ECE over {window_days}d (p_edge, outcome) window",
        ["schema"],
    )
    metric_brier = _get_or_create_gauge(
        "ml_drift_rolling_brier",
        f"Rolling Brier over {window_days}d window",
        ["schema"],
    )
    metric_n = _get_or_create_gauge(
        "ml_drift_rolling_n",
        "Samples in rolling window",
        ["schema"],
    )
    metric_ph = _get_or_create_gauge(
        "ml_drift_cusum_ph_score",
        "Page-Hinkley score per (schema, regime)",
        ["schema", "regime"],
    )
    metric_alarms = _get_or_create_counter(
        "ml_drift_cusum_alarms_total",
        "CUSUM drift alarms fired",
        ["schema", "regime"],
    )
    metric_alerts = _get_or_create_counter(
        "ml_drift_alert_total",
        "Telegram alerts emitted",
        ["kind"],
    )

    detector = CuSumDriftDetector(
        warmup=_env_int("ML_DRIFT_CUSUM_WARMUP", 100),
        delta=_env_float("ML_DRIFT_CUSUM_DELTA", 0.005),
        threshold=_env_float("ML_DRIFT_CUSUM_THRESHOLD", 0.30),
        ece_window_size=_env_int("ML_DRIFT_CUSUM_ECE_WIN", 500),
        cooldown_observations=_env_int("ML_DRIFT_CUSUM_COOLDOWN", 50),
    )

    # Per-schema rolling window of (ts_ms, p, y)
    rolling: dict[str, deque] = {}

    redis_client = get_redis()
    stream_key = RS.TRADES_CLOSED

    try:
        redis_client.xgroup_create(stream_key, group, id="$", mkstream=True)
        logger.info("Created consumer group %s on %s", group, stream_key)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            logger.info("Consumer group %s already exists on %s", group, stream_key)
        else:
            logger.error("xgroup_create failed: %s", e)

    start_http_server(port)
    logger.info(
        "ml_drift_monitor :%d window=%dd ece_th=%.3f brier_th=%.3f publish_interval=%ds",
        port, window_days, ece_th, brier_th, publish_interval_s,
    )

    stop = {"flag": False}

    def _sig(_signo: int, _frame: Any) -> None:
        stop["flag"] = True
        logger.info("Shutdown signal received")

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    last_publish_ms = 0
    last_alert: dict[str, int] = {}

    def _trim(win: deque, now_ms: int) -> None:
        cutoff = now_ms - window_ms
        while win and win[0][0] < cutoff:
            win.popleft()

    def _maybe_alert(kind: str, text: str) -> None:
        now = get_ny_time_millis()
        prev = last_alert.get(kind, 0)
        if prev > 0 and (now - prev) < tg_throttle_s * 1000:
            return
        last_alert[kind] = now
        try:
            import html
            safe = html.escape(text)
            redis_client.xadd(
                RS.NOTIFY_TELEGRAM,
                {
                    "type": "alert",
                    "subtype": "ml_calibration_drift",
                    "kind": kind,
                    "ts_ms": str(now),
                    "text": safe,
                },
                maxlen=200000,
                approximate=True,
            )
            metric_alerts.labels(kind=kind).inc()
            logger.warning("ALERT %s: %s", kind, text)
        except Exception as e:
            logger.error("Failed to publish Telegram alert: %s", e)

    while not stop["flag"]:
        try:
            resp = redis_client.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream_key: ">"},
                count=batch,
                block=2000,
            )
        except Exception as e:
            logger.error("XREADGROUP failed: %s", e)
            time.sleep(1.0)
            continue

        now_ms = get_ny_time_millis()
        if resp:
            for _, entries in resp:
                ids_to_ack: list[bytes] = []
                for msg_id, fields in entries:
                    ids_to_ack.append(msg_id)
                    parsed = _parse_outcome(fields)
                    if parsed is None:
                        continue
                    p, y, schema, regime = parsed

                    win = rolling.setdefault(schema, deque())
                    win.append((now_ms, p, y))
                    _trim(win, now_ms)

                    fired = detector.observe(schema=schema, regime=regime, p_hat=p, outcome=y)
                    metric_ph.labels(schema=schema, regime=regime).set(
                        detector.current_ph(schema, regime)
                    )
                    if fired:
                        metric_alarms.labels(schema=schema, regime=regime).inc()
                        _maybe_alert(
                            "cusum",
                            f"ML CALIB DRIFT [cusum] schema={schema} regime={regime}: "
                            f"PH alarm fired (baseline_brier={detector.baseline_brier(schema, regime):.4f}, "
                            f"current_ece={detector.current_ece(schema, regime):.4f}).",
                        )
                if ids_to_ack:
                    try:
                        redis_client.xack(stream_key, group, *ids_to_ack)
                    except Exception as e:
                        logger.warning("XACK failed: %s", e)

        # Periodic publish + threshold alarms
        if (now_ms - last_publish_ms) >= publish_interval_s * 1000:
            last_publish_ms = now_ms
            for schema, win in rolling.items():
                _trim(win, now_ms)
                # Project to (p, y) pairs for ECE / Brier helpers
                pairs = deque((p, y) for (_ts, p, y) in win)
                n = len(pairs)
                metric_n.labels(schema=schema).set(n)
                if n < 50:
                    continue
                cur_ece = _ece(pairs)
                cur_brier = _brier(pairs)
                metric_ece.labels(schema=schema).set(cur_ece)
                metric_brier.labels(schema=schema).set(cur_brier)
                if cur_ece > ece_th:
                    _maybe_alert(
                        f"ece_{schema}",
                        f"ML CALIB DRIFT [ece] schema={schema}: "
                        f"rolling_{window_days}d_ece={cur_ece:.4f} > {ece_th:.4f} "
                        f"(n={n}, brier={cur_brier:.4f}).",
                    )
                if cur_brier > brier_th:
                    _maybe_alert(
                        f"brier_{schema}",
                        f"ML CALIB DRIFT [brier] schema={schema}: "
                        f"rolling_{window_days}d_brier={cur_brier:.4f} > {brier_th:.4f} "
                        f"(n={n}, ece={cur_ece:.4f}).",
                    )


if __name__ == "__main__":
    main()
