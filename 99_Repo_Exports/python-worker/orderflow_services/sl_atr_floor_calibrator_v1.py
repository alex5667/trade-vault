"""
sl_atr_floor_calibrator_v1.py — service for SLATRFloorCalibrator.

Reads trades:closed, calibrates per-(symbol × venue) SL ATR floor from
realized SL/ATR ratios. Publishes to autocal:sl_atr_floor:state.

Master switch: SL_ATR_FLOOR_CAL_ENFORCE=0.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger("sl_atr_floor_cal")


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
    from core.sl_atr_floor_calibrator import SLATRFloorCalibrator

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    redis_url = _env("SL_ATR_FLOOR_CAL_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    in_stream = _env("SL_ATR_FLOOR_CAL_IN_STREAM", "trades:closed")
    group = _env("SL_ATR_FLOOR_CAL_GROUP", "sl-atr-floor-cal")
    consumer = _env("SL_ATR_FLOOR_CAL_CONSUMER", "sl-atr-floor-cal-1")
    out_key = _env("SL_ATR_FLOOR_CAL_OUT_KEY", RK.AUTOCAL_SL_ATR_FLOOR)
    port = _env_int("SL_ATR_FLOOR_CAL_PORT", 9897)
    batch = _env_int("SL_ATR_FLOOR_CAL_BATCH", 200)
    snap_sec = _env_int("SL_ATR_FLOOR_CAL_SNAPSHOT_SEC", 60)
    enforce = _env_bool("SL_ATR_FLOOR_CAL_ENFORCE", False)
    auto_enforce = _env_bool("SL_ATR_FLOOR_CAL_AUTO_ENFORCE", True)
    min_samples = _env_int("SL_ATR_FLOOR_CAL_MIN_SAMPLES", 30)

    rc = redis.from_url(redis_url, decode_responses=True)

    try:
        rc.xgroup_create(in_stream, group, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create: %s", e)

    cal = SLATRFloorCalibrator(enforce=enforce, auto_enforce=auto_enforce, min_samples=min_samples)

    try:
        raw = rc.get(out_key)
        if raw:
            cal.load_state(json.loads(raw))
            log.info("Warm-started (enforce=%s)", cal.enforce)
    except Exception as e:
        log.warning("Warm-start failed: %s", e)

    start_http_server(port)
    try:
        g_floor = Gauge("sl_atr_floor_cal_floor", "Calibrated SL ATR floor", ["symbol", "venue"])
        c_obs = Counter("sl_atr_floor_cal_observed_total", "Observations", ["symbol"])
    except Exception:
        g_floor = c_obs = None  # type: ignore

    last_snap_ms = 0
    log.info("sl_atr_floor_cal started (enforce=%s, port=%d)", enforce, port)

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
                        sl_bps = _safe_float(fields.get("sl_bps") or fields.get("sl_dist_bps"), float("nan"))
                        atr_bps = _safe_float(fields.get("atr_bps") or fields.get("atr_at_entry_bps"), float("nan"))
                        if sl_bps != sl_bps or atr_bps != atr_bps or atr_bps <= 0:
                            ack_ids.append(msg_id)
                            continue
                        symbol = fields.get("symbol", "*")
                        venue = fields.get("venue", "binance")
                        ts_ms = int(fields.get("ts_ms", int(time.time() * 1000)))
                        cal.observe(symbol=symbol, venue=venue, sl_bps=sl_bps,
                                    atr_bps=atr_bps, ts_ms=ts_ms)
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
                if g_floor:
                    for row in snap.get("bins", []):
                        g_floor.labels(symbol=row["symbol"], venue=row["venue"]).set(row["committed_floor"])
            except Exception as e:
                log.warning("snapshot error: %s", e)


if __name__ == "__main__":
    main()
