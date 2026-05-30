"""
entry_rr_min_calibrator_v1.py — streaming service for EntryRRMinCalibrator.

Reads trades:closed, calibrates per-(side × regime) RR floor from winner distribution,
publishes snapshot to autocal:entry_rr_min:state.

Master switch: ENTRY_RR_MIN_CAL_ENFORCE=0 (shadow default).
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger("entry_rr_min_cal")


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
    from core.entry_rr_min_calibrator import EntryRRMinCalibrator

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    redis_url = _env("ENTRY_RR_CAL_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    in_stream = _env("ENTRY_RR_CAL_IN_STREAM", "trades:closed")
    group = _env("ENTRY_RR_CAL_GROUP", "entry-rr-min-cal")
    consumer = _env("ENTRY_RR_CAL_CONSUMER", "entry-rr-min-cal-1")
    out_key = _env("ENTRY_RR_CAL_OUT_KEY", RK.AUTOCAL_ENTRY_RR_MIN)
    port = _env_int("ENTRY_RR_CAL_PORT", 9891)
    batch = _env_int("ENTRY_RR_CAL_BATCH", 200)
    snap_sec = _env_int("ENTRY_RR_CAL_SNAPSHOT_SEC", 60)
    enforce = _env_bool("ENTRY_RR_MIN_CAL_ENFORCE", False)
    auto_enforce = _env_bool("ENTRY_RR_MIN_CAL_AUTO_ENFORCE", True)
    window_days = _env_float("ENTRY_RR_CAL_WINDOW_DAYS", 14.0)
    min_samples = _env_int("ENTRY_RR_CAL_MIN_SAMPLES", 50)
    default_rr = _env_float("ENTRY_RR_CAL_DEFAULT_RR", 1.3)
    weights_enabled = _env_bool("REJECT_REASON_WEIGHTS_ENABLED", False)

    rc = redis.from_url(redis_url, decode_responses=True)

    try:
        rc.xgroup_create(in_stream, group, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create: %s", e)

    cal = EntryRRMinCalibrator(
        enforce=enforce,
        auto_enforce=auto_enforce,
        window_days=window_days,
        min_samples=min_samples,
        default_rr=default_rr,
    )

    try:
        raw = rc.get(out_key)
        if raw:
            cal.load_state(json.loads(raw))
            log.info("Warm-started (enforce=%s)", cal.enforce)
    except Exception as e:
        log.warning("Warm-start failed: %s", e)

    start_http_server(port)
    g_rr = _gauge("entry_rr_min_cal_floor", "Calibrated RR floor", ["side", "regime"])
    g_shadow = _gauge("entry_rr_min_cal_shadow", "Shadow RR floor", ["side", "regime"])
    g_n = _gauge("entry_rr_min_cal_n_buf", "Buffer samples", ["side", "regime"])
    c_obs = _counter("entry_rr_min_cal_observed_total", "Winner observations", ["side"])
    c_skip = _counter("entry_rr_min_cal_skipped_total", "Skipped observations", ["reason"])

    last_snap_ms = 0

    log.info("entry_rr_min_cal started (enforce=%s, port=%d)", enforce, port)

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
                        if r_mult != r_mult:
                            c_skip.labels(reason="r_multiple_invalid").inc()
                            ack_ids.append(msg_id)
                            continue

                        result = fields.get("result", "") or fields.get("close_reason", "") or ""
                        regime = fields.get("market_regime", "") or fields.get("regime", "") or "*"
                        side = fields.get("side", "") or fields.get("direction", "") or "*"
                        ts_ms = int(fields.get("ts_ms", int(time.time() * 1000)))
                        w = _weight_for_reason(fields.get("v_gate_reason", "")) if weights_enabled else 1.0

                        cal.observe(side=side, regime=regime, r_multiple=r_mult,
                                    result=result, ts_ms=ts_ms, w=w)
                        c_obs.labels(side=side).inc()
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
            _do_snapshot(cal, rc, out_key, g_rr, g_shadow, g_n)


def _do_snapshot(cal: Any, rc: Any, out_key: str,
                 g_rr: Gauge, g_shadow: Gauge, g_n: Gauge) -> None:
    try:
        snap = cal.snapshot()
        rc.set(out_key, json.dumps(snap))
        for row in snap.get("bins", []):
            side = row["side"]
            reg = row["regime"]
            g_rr.labels(side=side, regime=reg).set(row["committed_rr_min"])
            g_shadow.labels(side=side, regime=reg).set(row["shadow_rr_min"])
            g_n.labels(side=side, regime=reg).set(row["n_buf"])
    except Exception as e:
        import logging
        logging.getLogger("entry_rr_min_cal").warning("snapshot error: %s", e)


if __name__ == "__main__":
    main()
