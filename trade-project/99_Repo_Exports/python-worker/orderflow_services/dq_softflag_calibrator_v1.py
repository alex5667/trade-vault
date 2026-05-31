"""
dq_softflag_calibrator_v1.py — service for DQSoftFlagCalibrator.

Reads stream:book_{SYMBOL} (book update intervals) and signals:of:inputs (spread_bps),
calibrates per-symbol DQ soft-flag thresholds.

Master switch: DQ_SOFT_FLAG_CAL_ENFORCE=0.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger("dq_softflag_cal")


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
    from core.dq_softflag_calibrator import DQSoftFlagCalibrator

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    # Reads OF inputs stream which carries spread_bps and book_update_dt_ms
    redis_url = _env("DQ_SOFT_CAL_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    in_stream = _env("DQ_SOFT_CAL_IN_STREAM", "signals:of:inputs")
    group = _env("DQ_SOFT_CAL_GROUP", "dq-softflag-cal")
    consumer = _env("DQ_SOFT_CAL_CONSUMER", "dq-softflag-cal-1")
    out_key = _env("DQ_SOFT_CAL_OUT_KEY", RK.AUTOCAL_DQ_SOFT_FLAG)
    port = _env_int("DQ_SOFT_CAL_PORT", 9899)
    batch = _env_int("DQ_SOFT_CAL_BATCH", 500)
    snap_sec = _env_int("DQ_SOFT_CAL_SNAPSHOT_SEC", 60)
    enforce = _env_bool("DQ_SOFT_FLAG_CAL_ENFORCE", False)
    auto_enforce = _env_bool("DQ_SOFT_FLAG_CAL_AUTO_ENFORCE", True)
    min_samples = _env_int("DQ_SOFT_CAL_MIN_SAMPLES", 100)

    rc = redis.from_url(redis_url, decode_responses=True)

    try:
        rc.xgroup_create(in_stream, group, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create: %s", e)

    cal = DQSoftFlagCalibrator(enforce=enforce, auto_enforce=auto_enforce, min_samples=min_samples)

    try:
        raw = rc.get(out_key)
        if raw:
            cal.load_state(json.loads(raw))
            log.info("Warm-started (enforce=%s)", cal.enforce)
    except Exception as e:
        log.warning("Warm-start failed: %s", e)

    start_http_server(port)
    try:
        g_stale = Gauge("dq_soft_cal_stale_ms", "Calibrated stale flag ms", ["symbol"])
        g_spread = Gauge("dq_soft_cal_spread_bps", "Calibrated spread flag bps", ["symbol"])
        c_obs = Counter("dq_soft_cal_observed_total", "Observations", ["kind"])
    except Exception:
        g_stale = g_spread = c_obs = None  # type: ignore

    last_snap_ms = 0
    log.info("dq_softflag_cal started (enforce=%s, port=%d)", enforce, port)

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
                        ts_ms = int(fields.get("ts_ms", int(time.time() * 1000)))

                        # Book staleness dt
                        book_dt = _safe_float(
                            fields.get("book_update_dt_ms") or fields.get("book_dt_ms"),
                            float("nan"),
                        )
                        if book_dt == book_dt and book_dt > 0:
                            cal.observe_book_dt(symbol=symbol, dt_ms=book_dt, ts_ms=ts_ms)
                            if c_obs:
                                c_obs.labels(kind="book_dt").inc()

                        # Spread
                        spread = _safe_float(
                            fields.get("spread_bps") or fields.get("bid_ask_spread_bps"),
                            float("nan"),
                        )
                        if spread == spread and spread > 0:
                            cal.observe_spread(symbol=symbol, spread_bps=spread, ts_ms=ts_ms)
                            if c_obs:
                                c_obs.labels(kind="spread").inc()
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
                if g_stale:
                    for row in snap.get("bins", []):
                        sym = row["symbol"]
                        g_stale.labels(symbol=sym).set(row["committed_stale_ms"])
                        g_spread.labels(symbol=sym).set(row["committed_spread_bps"])
            except Exception as e:
                log.warning("snapshot error: %s", e)


if __name__ == "__main__":
    main()
