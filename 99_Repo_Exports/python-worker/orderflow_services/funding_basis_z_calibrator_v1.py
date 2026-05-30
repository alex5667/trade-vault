"""
funding_basis_z_calibrator_v1.py — service for FundingBasisZCalibrator.

Reads signals:of:inputs stream (which carries funding_rate_bps/basis_bps context),
calibrates per-(symbol × vol_regime) funding z and basis bps thresholds.

Master switch: FUNDING_Z_CAL_ENFORCE=0.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger("funding_basis_z_cal")


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
    from core.funding_basis_z_calibrator import FundingBasisZCalibrator

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    redis_url = _env("FUNDING_Z_CAL_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    in_stream = _env("FUNDING_Z_CAL_IN_STREAM", "signals:of:inputs")
    group = _env("FUNDING_Z_CAL_GROUP", "funding-basis-z-cal")
    consumer = _env("FUNDING_Z_CAL_CONSUMER", "funding-basis-z-cal-1")
    out_key = _env("FUNDING_Z_CAL_OUT_KEY", RK.AUTOCAL_FUNDING_Z)
    port = _env_int("FUNDING_Z_CAL_PORT", 9896)
    batch = _env_int("FUNDING_Z_CAL_BATCH", 500)
    snap_sec = _env_int("FUNDING_Z_CAL_SNAPSHOT_SEC", 60)
    enforce = _env_bool("FUNDING_Z_CAL_ENFORCE", False)
    auto_enforce = _env_bool("FUNDING_Z_CAL_AUTO_ENFORCE", True)
    min_samples = _env_int("FUNDING_Z_CAL_MIN_SAMPLES", 100)
    window_days = float(_env("FUNDING_Z_CAL_WINDOW_DAYS", "7"))

    rc = redis.from_url(redis_url, decode_responses=True)

    try:
        rc.xgroup_create(in_stream, group, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create: %s", e)

    cal = FundingBasisZCalibrator(enforce=enforce, auto_enforce=auto_enforce, min_samples=min_samples, window_days=window_days)

    try:
        raw = rc.get(out_key)
        if raw:
            cal.load_state(json.loads(raw))
            log.info("Warm-started (enforce=%s)", cal.enforce)
    except Exception as e:
        log.warning("Warm-start failed: %s", e)

    start_http_server(port)
    try:
        g_fz = Gauge("funding_z_cal_threshold", "Calibrated funding z threshold", ["symbol", "vol_regime"])
        g_bb = Gauge("funding_z_cal_basis_bps", "Calibrated basis bps threshold", ["symbol", "vol_regime"])
        c_obs = Counter("funding_z_cal_observed_total", "Observations", ["symbol"])
    except Exception:
        g_fz = g_bb = c_obs = None  # type: ignore

    last_snap_ms = 0
    log.info("funding_basis_z_cal started (enforce=%s, port=%d)", enforce, port)

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
                        # funding_rate_z from ctx:deriv context
                        funding_z = _safe_float(
                            fields.get("funding_rate_z") or fields.get("funding_z"),
                            float("nan"),
                        )
                        basis_bps = _safe_float(
                            fields.get("basis_bps") or fields.get("deriv_basis_bps"),
                            float("nan"),
                        )
                        if funding_z != funding_z or basis_bps != basis_bps:
                            ack_ids.append(msg_id)
                            continue
                        vol_regime = fields.get("vol_regime", "") or fields.get("regime", "") or "*"
                        ts_ms = int(fields.get("ts_ms", int(time.time() * 1000)))
                        cal.observe(symbol=symbol, vol_regime=vol_regime,
                                    funding_z=funding_z, basis_bps=basis_bps, ts_ms=ts_ms)
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
                if g_fz:
                    for row in snap.get("bins", []):
                        g_fz.labels(symbol=row["symbol"], vol_regime=row["vol_regime"]).set(row["committed_funding_z"])
                        g_bb.labels(symbol=row["symbol"], vol_regime=row["vol_regime"]).set(row["committed_basis_bps"])
            except Exception as e:
                log.warning("snapshot error: %s", e)


if __name__ == "__main__":
    main()
