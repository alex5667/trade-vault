"""
entry_slippage_cap_calibrator_v1.py — service for EntrySlippageCapCalibrator.

Reads trades:closed, calibrates entry-side slippage cap per (symbol × session),
publishes to autocal:entry_slip_cap:state.

Complements slippage_autocal_v1 (which covers trade-exit slippage),
this covers the entry fill vs mid-price gap.

Master switch: ENTRY_SLIP_CAL_ENFORCE=0 (shadow default).
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger("entry_slip_cal")


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


def main() -> None:
    import redis  # type: ignore

    from core.redis_keys import RedisKeyPrefixes as RK
    from core.entry_slippage_cap_calibrator import EntrySlippageCapCalibrator

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    redis_url = _env("ENTRY_SLIP_CAL_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    in_stream = _env("ENTRY_SLIP_CAL_IN_STREAM", "trades:closed")
    group = _env("ENTRY_SLIP_CAL_GROUP", "entry-slip-cap-cal")
    consumer = _env("ENTRY_SLIP_CAL_CONSUMER", "entry-slip-cap-cal-1")
    out_key = _env("ENTRY_SLIP_CAL_OUT_KEY", RK.AUTOCAL_ENTRY_SLIP_CAP)
    port = _env_int("ENTRY_SLIP_CAL_PORT", 9893)
    batch = _env_int("ENTRY_SLIP_CAL_BATCH", 200)
    snap_sec = _env_int("ENTRY_SLIP_CAL_SNAPSHOT_SEC", 60)
    enforce = _env_bool("ENTRY_SLIP_CAP_CAL_ENFORCE", False)
    auto_enforce = _env_bool("ENTRY_SLIP_CAP_CAL_AUTO_ENFORCE", True)
    window_days = _env_float("ENTRY_SLIP_CAL_WINDOW_DAYS", 14.0)
    min_samples = _env_int("ENTRY_SLIP_CAL_MIN_SAMPLES", 20)

    rc = redis.from_url(redis_url, decode_responses=True)

    try:
        rc.xgroup_create(in_stream, group, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create: %s", e)

    cal = EntrySlippageCapCalibrator(
        enforce=enforce,
        auto_enforce=auto_enforce,
        window_days=window_days,
        min_samples=min_samples,
    )

    try:
        raw = rc.get(out_key)
        if raw:
            cal.load_state(json.loads(raw))
            log.info("Warm-started (enforce=%s)", cal.enforce)
    except Exception as e:
        log.warning("Warm-start failed: %s", e)

    start_http_server(port)
    g_cap = _gauge("entry_slip_cal_cap_bps", "Calibrated entry slippage cap bps", ["symbol", "session"])
    g_shadow = _gauge("entry_slip_cal_shadow_bps", "Shadow entry slippage cap bps", ["symbol", "session"])
    c_obs = _counter("entry_slip_cal_observed_total", "Observations", ["symbol"])
    c_skip = _counter("entry_slip_cal_skipped_total", "Skipped", ["reason"])

    last_snap_ms = 0

    log.info("entry_slip_cal started (enforce=%s, port=%d)", enforce, port)

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
                        # Entry slippage: entry_slippage_bps or derived from fill vs signal price
                        slip = _safe_float(
                            fields.get("entry_slippage_bps")
                            or fields.get("adverse_bps_entry")
                            or fields.get("adverse_bps_t"),
                            float("nan"),
                        )
                        if slip != slip or slip < 0:
                            c_skip.labels(reason="slip_invalid").inc()
                            ack_ids.append(msg_id)
                            continue
                        symbol = fields.get("symbol", "") or "*"
                        session = fields.get("session", "") or "*"
                        ts_ms = int(fields.get("ts_ms", int(time.time() * 1000)))
                        cal.observe(symbol=symbol, session=session,
                                    entry_slip_bps=slip, ts_ms=ts_ms)
                        c_obs.labels(symbol=symbol).inc()
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
            try:
                snap = cal.snapshot()
                rc.set(out_key, json.dumps(snap))
                for row in snap.get("bins", []):
                    sym = row["symbol"]
                    sess = row["session"]
                    g_cap.labels(symbol=sym, session=sess).set(row["committed_cap_bps"])
                    g_shadow.labels(symbol=sym, session=sess).set(row["shadow_cap_bps"])
            except Exception as e:
                log.warning("snapshot error: %s", e)


if __name__ == "__main__":
    main()
