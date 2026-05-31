"""
signal_min_conf_calibrator_v1.py — streaming service for SignalMinConfCalibrator.

Reads trades:closed, calibrates per-(kind × regime) confidence threshold, publishes
snapshot to autocal:signal_min_conf:state.

Master switch: SIGNAL_MIN_CONF_CAL_ENFORCE=0 (shadow mode default).
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger("signal_min_conf_cal")


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d


def _env_float(k: str, d: float) -> float:
    try:
        return float(_env(k, str(d)))
    except Exception:
        return d


def _env_bool(k: str, d: bool) -> bool:
    raw = _env(k, "")
    if not raw:
        return d
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        import math
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


def _gauge(name: str, doc: str, labels: list[str]) -> Gauge:
    from prometheus_client import REGISTRY
    try:
        return Gauge(name, doc, labels)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names.get(c, []):
                return c  # type: ignore[return-value]
        raise


def _counter(name: str, doc: str, labels: list[str]) -> Counter:
    from prometheus_client import REGISTRY
    try:
        return Counter(name, doc, labels)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names.get(c, []):
                return c  # type: ignore[return-value]
        raise


def _weight_for_reason(v_gate_reason: str) -> float:
    reason = (v_gate_reason or "").strip().lower()
    if reason.startswith("passed"):
        return 1.0
    if reason.startswith("shadow"):
        return 0.7
    if reason.startswith("context"):
        return 0.5
    if reason.startswith("exec"):
        return 0.3
    return 0.1


def main() -> None:
    import redis  # type: ignore

    from core.redis_keys import RedisKeyPrefixes as RK
    from core.signal_min_conf_calibrator import SignalMinConfCalibrator

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    redis_url = _env("SIGNAL_CONF_CAL_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    in_stream = _env("SIGNAL_CONF_CAL_IN_STREAM", "trades:closed")
    group = _env("SIGNAL_CONF_CAL_GROUP", "signal-conf-cal")
    consumer = _env("SIGNAL_CONF_CAL_CONSUMER", "signal-conf-cal-1")
    out_key = _env("SIGNAL_CONF_CAL_OUT_KEY", RK.AUTOCAL_SIGNAL_MIN_CONF)
    port = _env_int("SIGNAL_CONF_CAL_PORT", 9890)
    batch = _env_int("SIGNAL_CONF_CAL_BATCH", 200)
    snap_sec = _env_int("SIGNAL_CONF_CAL_SNAPSHOT_SEC", 30)
    enforce = _env_bool("SIGNAL_MIN_CONF_CAL_ENFORCE", False)
    auto_enforce = _env_bool("SIGNAL_MIN_CONF_CAL_AUTO_ENFORCE", True)
    window_days = _env_float("SIGNAL_CONF_CAL_WINDOW_DAYS", 7.0)
    min_samples = _env_int("SIGNAL_CONF_CAL_MIN_SAMPLES", 100)
    default_thr = _env_float("SIGNAL_CONF_CAL_DEFAULT_THR", 70.0)
    weights_enabled = _env_bool("REJECT_REASON_WEIGHTS_ENABLED", False)

    rc = redis.from_url(redis_url, decode_responses=True)

    # Consumer group bootstrap (idempotent)
    try:
        rc.xgroup_create(in_stream, group, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create: %s", e)

    cal = SignalMinConfCalibrator(
        enforce=enforce,
        auto_enforce=auto_enforce,
        window_days=window_days,
        min_samples=min_samples,
        default_thr=default_thr,
    )

    # Warm-start
    try:
        raw = rc.get(out_key)
        if raw:
            cal.load_state(json.loads(raw))
            log.info("Warm-started from snapshot (enforce=%s)", cal.enforce)
    except Exception as e:
        log.warning("Warm-start failed: %s", e)

    # Prometheus
    start_http_server(port)
    g_thr = _gauge("signal_min_conf_cal_threshold", "Calibrated confidence threshold", ["kind", "regime"])
    g_shadow = _gauge("signal_min_conf_cal_shadow", "Shadow confidence threshold", ["kind", "regime"])
    g_n = _gauge("signal_min_conf_cal_n_buf", "Buffer samples", ["kind", "regime"])
    c_obs = _counter("signal_min_conf_cal_observed_total", "Observations processed", ["kind"])
    c_skip = _counter("signal_min_conf_cal_skipped_total", "Observations skipped", ["reason"])

    last_snap_ms = 0

    log.info("signal_min_conf_cal started (enforce=%s, port=%d, window_days=%.1f)", enforce, port, window_days)

    while True:
        try:
            resp = rc.xreadgroup(
                groupname=group, consumername=consumer,
                streams={in_stream: ">"}, count=batch, block=2000,
            )
        except Exception as e:
            log.warning("XREADGROUP error: %s", e)
            time.sleep(1)
            continue

        if resp:
            ack_ids = []
            for _stream, messages in resp:
                for msg_id, fields in messages:
                    try:
                        r_mult = _safe_float(fields.get("r_multiple"), float("nan"))
                        if r_mult != r_mult:  # nan check
                            c_skip.labels(reason="r_multiple_invalid").inc()
                            ack_ids.append(msg_id)
                            continue

                        conf_raw = _safe_float(fields.get("confidence"), float("nan"))
                        if conf_raw != conf_raw:
                            c_skip.labels(reason="confidence_missing").inc()
                            ack_ids.append(msg_id)
                            continue
                        # Normalize to 0-100 scale
                        conf_pct = conf_raw * 100.0 if conf_raw <= 1.0 else conf_raw

                        regime = fields.get("market_regime", "") or fields.get("regime", "") or "*"
                        kind = fields.get("kind", "") or fields.get("signal_kind", "") or "*"
                        ts_ms = int(fields.get("ts_ms", int(time.time() * 1000)))
                        w = _weight_for_reason(fields.get("v_gate_reason", "")) if weights_enabled else 1.0

                        cal.observe(kind=kind, regime=regime, conf_pct=conf_pct,
                                    r_multiple=r_mult, ts_ms=ts_ms, w=w)
                        c_obs.labels(kind=kind).inc()
                    except Exception as ex:
                        log.debug("parse error: %s", ex)
                        c_skip.labels(reason="parse_error").inc()
                    ack_ids.append(msg_id)

            if ack_ids:
                try:
                    rc.xack(in_stream, group, *ack_ids)
                except Exception as e:
                    log.warning("XACK error: %s", e)

        now_ms = int(time.time() * 1000)
        if (now_ms - last_snap_ms) >= snap_sec * 1000:
            last_snap_ms = now_ms
            _do_snapshot(cal, rc, out_key, g_thr, g_shadow, g_n, enforce)


def _do_snapshot(
    cal: Any, rc: Any, out_key: str,
    g_thr: Gauge, g_shadow: Gauge, g_n: Gauge, enforce: bool,
) -> None:
    try:
        snap = cal.snapshot()
        rc.set(out_key, json.dumps(snap))
        for row in snap.get("bins", []):
            knd = row["kind"]
            reg = row["regime"]
            g_thr.labels(kind=knd, regime=reg).set(row["committed_thr"])
            g_shadow.labels(kind=knd, regime=reg).set(row["shadow_thr"])
            g_n.labels(kind=knd, regime=reg).set(row["n_buf"])
    except Exception as e:
        import logging
        logging.getLogger("signal_min_conf_cal").warning("snapshot error: %s", e)


if __name__ == "__main__":
    main()
