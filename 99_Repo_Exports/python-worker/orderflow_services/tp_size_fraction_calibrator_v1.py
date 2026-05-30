"""
tp_size_fraction_calibrator_v1.py — service for TPSizeFractionCalibrator.

Reads trades:closed, tracks TP1/TP2/TP3 hit rates per regime,
calibrates TP position-close size fractions.

Master switch: TP_SIZE_FRAC_CAL_ENFORCE=0.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger("tp_size_frac_cal")


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


_TP1_RESULTS = frozenset({"TP1", "tp1", "TP", "tp", "PARTIAL_TP1"})
_TP2_RESULTS = frozenset({"TP2", "tp2", "PARTIAL_TP2"})
_TP3_RESULTS = frozenset({"TP3", "tp3", "FULL_TP", "full_tp"})


def main() -> None:
    import redis  # type: ignore

    from core.redis_keys import RedisKeyPrefixes as RK
    from core.tp_size_fraction_calibrator import TPSizeFractionCalibrator

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    redis_url = _env("TP_SIZE_FRAC_CAL_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    in_stream = _env("TP_SIZE_FRAC_CAL_IN_STREAM", "trades:closed")
    group = _env("TP_SIZE_FRAC_CAL_GROUP", "tp-size-frac-cal")
    consumer = _env("TP_SIZE_FRAC_CAL_CONSUMER", "tp-size-frac-cal-1")
    out_key = _env("TP_SIZE_FRAC_CAL_OUT_KEY", RK.AUTOCAL_TP_SIZE_FRACTIONS)
    port = _env_int("TP_SIZE_FRAC_CAL_PORT", 9898)
    batch = _env_int("TP_SIZE_FRAC_CAL_BATCH", 200)
    snap_sec = _env_int("TP_SIZE_FRAC_CAL_SNAPSHOT_SEC", 300)
    enforce = _env_bool("TP_SIZE_FRAC_CAL_ENFORCE", False)
    auto_enforce = _env_bool("TP_SIZE_FRAC_CAL_AUTO_ENFORCE", True)

    rc = redis.from_url(redis_url, decode_responses=True)

    try:
        rc.xgroup_create(in_stream, group, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create: %s", e)

    cal = TPSizeFractionCalibrator(enforce=enforce, auto_enforce=auto_enforce)

    try:
        raw = rc.get(out_key)
        if raw:
            cal.load_state(json.loads(raw))
            log.info("Warm-started (enforce=%s)", cal.enforce)
    except Exception as e:
        log.warning("Warm-start failed: %s", e)

    start_http_server(port)
    try:
        g_f1 = Gauge("tp_size_frac_f1", "TP1 fraction", ["regime"])
        g_f2 = Gauge("tp_size_frac_f2", "TP2 fraction", ["regime"])
        g_f3 = Gauge("tp_size_frac_f3", "TP3 fraction", ["regime"])
        c_obs = Counter("tp_size_frac_cal_observed_total", "TP observations", ["tp_level"])
    except Exception:
        g_f1 = g_f2 = g_f3 = c_obs = None  # type: ignore

    last_snap_ms = 0
    log.info("tp_size_frac_cal started (enforce=%s, port=%d)", enforce, port)

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
                        result = fields.get("result", "") or fields.get("close_reason", "") or ""
                        regime = fields.get("market_regime", "") or fields.get("regime", "") or "*"
                        ts_ms = int(fields.get("ts_ms", int(time.time() * 1000)))
                        if result in _TP1_RESULTS:
                            cal.observe_tp(regime=regime, tp_level=1, ts_ms=ts_ms)
                            if c_obs:
                                c_obs.labels(tp_level="1").inc()
                        elif result in _TP2_RESULTS:
                            cal.observe_tp(regime=regime, tp_level=2, ts_ms=ts_ms)
                            if c_obs:
                                c_obs.labels(tp_level="2").inc()
                        elif result in _TP3_RESULTS:
                            cal.observe_tp(regime=regime, tp_level=3, ts_ms=ts_ms)
                            if c_obs:
                                c_obs.labels(tp_level="3").inc()
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
                if g_f1:
                    for row in snap.get("bins", []):
                        reg = row["regime"]
                        g_f1.labels(regime=reg).set(row["committed_f1"])
                        g_f2.labels(regime=reg).set(row["committed_f2"])
                        g_f3.labels(regime=reg).set(row["committed_f3"])
            except Exception as e:
                log.warning("snapshot error: %s", e)


if __name__ == "__main__":
    main()
