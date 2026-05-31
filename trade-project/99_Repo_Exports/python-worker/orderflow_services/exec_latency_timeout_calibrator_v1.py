"""
exec_latency_timeout_calibrator_v1.py — service for ExecLatencyTimeoutCalibrator.

Reads execution audit stream, calibrates PROTECTION_ARM_TIMEOUT_MS adaptively
from p99 arm_latency_ms distribution. Publishes to autocal:exec_latency_timeout:state.

Master switch: EXEC_LATENCY_TIMEOUT_CAL_ENFORCE=0.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from prometheus_client import Gauge, start_http_server

log = logging.getLogger("exec_latency_timeout_cal")


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
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


def main() -> None:
    import redis  # type: ignore

    from core.redis_keys import RedisKeyPrefixes as RK
    from core.exec_latency_timeout_calibrator import ExecLatencyTimeoutCalibrator

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    redis_url = _env("EXEC_LATENCY_CAL_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    in_stream = _env("EXEC_LATENCY_CAL_IN_STREAM", "stream:execution_audit")
    group = _env("EXEC_LATENCY_CAL_GROUP", "exec-latency-timeout-cal")
    consumer = _env("EXEC_LATENCY_CAL_CONSUMER", "exec-latency-timeout-cal-1")
    out_key = _env("EXEC_LATENCY_CAL_OUT_KEY", RK.AUTOCAL_EXEC_LATENCY_TIMEOUT)
    port = _env_int("EXEC_LATENCY_CAL_PORT", 9895)
    batch = _env_int("EXEC_LATENCY_CAL_BATCH", 200)
    snap_sec = _env_int("EXEC_LATENCY_CAL_SNAPSHOT_SEC", 120)
    enforce = _env_bool("EXEC_LATENCY_TIMEOUT_CAL_ENFORCE", False)
    auto_enforce = _env_bool("EXEC_LATENCY_TIMEOUT_CAL_AUTO_ENFORCE", True)

    rc = redis.from_url(redis_url, decode_responses=True)

    try:
        rc.xgroup_create(in_stream, group, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create: %s", e)

    cal = ExecLatencyTimeoutCalibrator(enforce=enforce, auto_enforce=auto_enforce)

    try:
        raw = rc.get(out_key)
        if raw:
            cal.load_state(json.loads(raw))
            log.info("Warm-started (enforce=%s)", cal.enforce)
    except Exception as e:
        log.warning("Warm-start failed: %s", e)

    start_http_server(port)
    try:
        g_exec = Gauge("exec_latency_cal_executor_timeout_ms", "Calibrated executor timeout ms")
        g_router = Gauge("exec_latency_cal_router_timeout_ms", "Calibrated router timeout ms")
        g_p99 = Gauge("exec_latency_cal_p99_latency_ms", "p99 arm latency ms")
    except Exception:
        g_exec = g_router = g_p99 = None  # type: ignore

    last_snap_ms = 0
    log.info("exec_latency_timeout_cal started (enforce=%s, port=%d)", enforce, port)

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
                        lat = _safe_float(
                            fields.get("arm_latency_ms") or fields.get("latency_ms") or fields.get("duration_ms"),
                            float("nan"),
                        )
                        if lat != lat or lat <= 0:
                            ack_ids.append(msg_id)
                            continue
                        ts_ms = int(fields.get("ts_ms", int(time.time() * 1000)))
                        cal.observe(arm_latency_ms=lat, ts_ms=ts_ms)
                    except Exception as ex:
                        log.debug("parse error: %s", ex)
                    ack_ids.append(msg_id)

            if ack_ids:
                try:
                    rc.xack(in_stream, group, *ack_ids)
                except Exception as e:
                    log.warning("XACK error: %s", e)

        now_ms = int(time.time() * 1000)
        if (now_ms - last_snap_ms) >= snap_sec * 1000:
            last_snap_ms = now_ms
            try:
                snap = cal.snapshot()
                rc.set(out_key, json.dumps(snap))
                if g_exec:
                    g_exec.set(snap.get("committed_executor_ms", 2500))
                    g_router.set(snap.get("committed_router_ms", 5000))
                    # p99 via shadow values
                    shadow_exec = snap.get("shadow_executor_ms", 2500)
                    g_p99.set(shadow_exec / cal.executor_mult if cal.executor_mult else shadow_exec)
            except Exception as e:
                log.warning("snapshot error: %s", e)


if __name__ == "__main__":
    main()
