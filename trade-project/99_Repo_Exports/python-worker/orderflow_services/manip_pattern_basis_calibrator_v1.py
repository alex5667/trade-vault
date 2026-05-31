"""
manip_pattern_basis_calibrator_v1.py — service for ManipPatternBasisCalibrator.

Reads metrics:of_gate or stream:manip_observations, calibrates per-symbol
manip detection state-machine constants, publishes to autocal:manip_pattern_basis:state.

Master switch: MANIP_PATTERN_BASIS_CAL_ENFORCE=0.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger("manip_pattern_basis_cal")


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
    from core.manip_pattern_basis_calibrator import ManipPatternBasisCalibrator

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    redis_url = _env("MANIP_BASIS_CAL_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    # Read from manip observations stream (written by manip_patterns.py tracker)
    in_stream = _env("MANIP_BASIS_CAL_IN_STREAM", "stream:manip_observations")
    group = _env("MANIP_BASIS_CAL_GROUP", "manip-basis-cal")
    consumer = _env("MANIP_BASIS_CAL_CONSUMER", "manip-basis-cal-1")
    out_key = _env("MANIP_BASIS_CAL_OUT_KEY", RK.AUTOCAL_MANIP_PATTERN_BASIS)
    port = _env_int("MANIP_BASIS_CAL_PORT", 9894)
    batch = _env_int("MANIP_BASIS_CAL_BATCH", 500)
    snap_sec = _env_int("MANIP_BASIS_CAL_SNAPSHOT_SEC", 120)
    enforce = _env_bool("MANIP_PATTERN_BASIS_CAL_ENFORCE", False)
    auto_enforce = _env_bool("MANIP_PATTERN_BASIS_CAL_AUTO_ENFORCE", True)
    min_samples = _env_int("MANIP_BASIS_CAL_MIN_SAMPLES", 200)

    rc = redis.from_url(redis_url, decode_responses=True)

    try:
        rc.xgroup_create(in_stream, group, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create: %s", e)

    cal = ManipPatternBasisCalibrator(enforce=enforce, auto_enforce=auto_enforce, min_samples=min_samples)

    try:
        raw = rc.get(out_key)
        if raw:
            cal.load_state(json.loads(raw))
            log.info("Warm-started (enforce=%s)", cal.enforce)
    except Exception as e:
        log.warning("Warm-start failed: %s", e)

    start_http_server(port)
    try:
        from prometheus_client import Gauge
        g_build = Gauge("manip_basis_cal_build_mult", "Build mult threshold", ["symbol"])
        g_revert_ms = Gauge("manip_basis_cal_revert_ms", "Revert window ms", ["symbol"])
        c_obs = Counter("manip_basis_cal_observed_total", "Observations", ["symbol"])
    except Exception:
        g_build = g_revert_ms = c_obs = None  # type: ignore

    last_snap_ms = 0
    log.info("manip_pattern_basis_cal started (enforce=%s, port=%d)", enforce, port)

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
                        symbol = fields.get("symbol", "*")
                        lay = _safe_float(fields.get("layering_score"), 0.0)
                        qs = _safe_float(fields.get("quote_stuffing_score"), 0.0)
                        build_ratio = _safe_float(fields.get("build_depth_ratio"), 1.0)
                        revert_ms = _safe_float(fields.get("revert_delay_ms"), 0.0)
                        ts_ms = int(fields.get("ts_ms", int(time.time() * 1000)))
                        cal.observe(symbol=symbol, layering_score=lay, qs_score=qs,
                                    build_depth_ratio=build_ratio, revert_delay_ms=revert_ms,
                                    ts_ms=ts_ms)
                        if c_obs:
                            c_obs.labels(symbol=symbol).inc()
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
                if g_build and g_revert_ms:
                    for row in snap.get("bins", []):
                        s = row["symbol"]
                        g_build.labels(symbol=s).set(row["committed_build_mult"])
                        g_revert_ms.labels(symbol=s).set(row["committed_revert_ms"])
            except Exception as e:
                log.warning("snapshot error: %s", e)


if __name__ == "__main__":
    main()
