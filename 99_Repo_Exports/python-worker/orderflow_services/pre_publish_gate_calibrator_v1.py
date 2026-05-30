"""
pre_publish_gate_calibrator_v1.py — streaming service for PrePublishGateCalibrator.

Reads signals:of:inputs stream, calibrates per-(symbol × regime) delta_z and
OBI thresholds, publishes snapshot to autocal:pre_publish_gate:state.

Master switch: PRE_PUBLISH_GATE_CAL_ENFORCE=0 (shadow mode default).
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger("pre_publish_gate_cal")


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
    from core.pre_publish_gate_calibrator import PrePublishGateCalibrator

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    redis_url = _env("PRE_PUBLISH_GATE_CAL_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    in_stream = _env("PRE_PUBLISH_GATE_CAL_IN_STREAM", "signals:of:inputs")
    group = _env("PRE_PUBLISH_GATE_CAL_GROUP", "pre-publish-gate-cal")
    consumer = _env("PRE_PUBLISH_GATE_CAL_CONSUMER", "pre-publish-gate-cal-1")
    out_key = _env("PRE_PUBLISH_GATE_CAL_OUT_KEY", RK.AUTOCAL_PRE_PUBLISH_GATE)
    port = _env_int("PRE_PUBLISH_GATE_CAL_PORT", 9902)
    batch = _env_int("PRE_PUBLISH_GATE_CAL_BATCH", 500)
    snap_sec = _env_int("PRE_PUBLISH_GATE_CAL_SNAPSHOT_SEC", 60)
    enforce = _env_bool("PRE_PUBLISH_GATE_CAL_ENFORCE", False)
    auto_enforce = _env_bool("PRE_PUBLISH_GATE_CAL_AUTO_ENFORCE", True)
    window_hours = _env_float("PRE_PUBLISH_GATE_CAL_WINDOW_HOURS", 24.0)
    min_samples = _env_int("PRE_PUBLISH_GATE_CAL_MIN_SAMPLES", 50)
    gate_mad_z_mult = _env_float("PRE_PUBLISH_GATE_MAD_Z_MULT", 1.5)
    obi_safety_mult = _env_float("PRE_PUBLISH_GATE_OBI_SAFETY_MULT", 1.2)
    default_delta_z_thr = _env_float("DELTA_Z_THRESHOLD", 2.0)
    default_obi_thr = _env_float("OBI_THRESHOLD", 0.35)

    rc = redis.from_url(redis_url, decode_responses=True)

    # Consumer group bootstrap (idempotent)
    try:
        rc.xgroup_create(in_stream, group, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create: %s", e)

    cal = PrePublishGateCalibrator(
        enforce=enforce,
        auto_enforce=auto_enforce,
        window_hours=window_hours,
        min_samples=min_samples,
        gate_mad_z_mult=gate_mad_z_mult,
        obi_safety_mult=obi_safety_mult,
        default_delta_z_thr=default_delta_z_thr,
        default_obi_thr=default_obi_thr,
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
    g_delta_z = _gauge("pre_publish_gate_cal_delta_z_thr", "Calibrated delta_z threshold", ["symbol", "regime"])
    g_obi = _gauge("pre_publish_gate_cal_obi_thr", "Calibrated OBI threshold", ["symbol", "regime"])
    g_shadow_z = _gauge("pre_publish_gate_cal_shadow_delta_z", "Shadow delta_z threshold", ["symbol", "regime"])
    g_shadow_obi = _gauge("pre_publish_gate_cal_shadow_obi", "Shadow OBI threshold", ["symbol", "regime"])
    g_n = _gauge("pre_publish_gate_cal_n_buf", "Buffer samples", ["symbol", "regime"])
    c_obs = _counter("pre_publish_gate_cal_observed_total", "Observations processed", ["symbol"])
    c_skip = _counter("pre_publish_gate_cal_skipped_total", "Observations skipped", ["reason"])

    last_snap_ms = 0

    log.info(
        "pre_publish_gate_cal started (enforce=%s, port=%d, stream=%s, window_hours=%.1f)",
        enforce, port, in_stream, window_hours,
    )

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
                        # Extract delta_z — try multiple field names
                        delta_z_raw = (
                            fields.get("delta_z") or
                            fields.get("z_delta") or
                            fields.get("of_delta_z")
                        )
                        if delta_z_raw is None or delta_z_raw == "":
                            c_skip.labels(reason="delta_z_missing").inc()
                            ack_ids.append(msg_id)
                            continue
                        delta_z = _safe_float(delta_z_raw, float("nan"))
                        if delta_z != delta_z:  # nan
                            c_skip.labels(reason="delta_z_invalid").inc()
                            ack_ids.append(msg_id)
                            continue

                        # Extract OBI — try multiple field names
                        obi_raw = (
                            fields.get("obi_score") or
                            fields.get("obi") or
                            fields.get("lob_obi_5") or
                            fields.get("of_obi")
                        )
                        obi = _safe_float(obi_raw, float("nan")) if obi_raw else float("nan")
                        if obi != obi:
                            # OBI missing: still observe delta_z with obi=0 (gate uses delta_z primarily)
                            obi = 0.0

                        symbol = (
                            fields.get("symbol") or
                            fields.get("sym") or "*"
                        ).strip().upper()

                        regime = (
                            fields.get("market_regime") or
                            fields.get("regime") or
                            fields.get("entry_regime") or "*"
                        )

                        ts_ms = int(fields.get("ts_ms", int(time.time() * 1000)))

                        cal.observe(
                            symbol=symbol,
                            regime=regime,
                            delta_z=delta_z,
                            obi=obi,
                            ts_ms=ts_ms,
                            w=1.0,
                        )
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
            _do_snapshot(cal, rc, out_key, g_delta_z, g_obi, g_shadow_z, g_shadow_obi, g_n)


def _do_snapshot(
    cal: Any, rc: Any, out_key: str,
    g_delta_z: Gauge, g_obi: Gauge,
    g_shadow_z: Gauge, g_shadow_obi: Gauge,
    g_n: Gauge,
) -> None:
    try:
        snap = cal.snapshot()
        rc.set(out_key, json.dumps(snap))
        for row in snap.get("bins", []):
            sym = row["symbol"]
            reg = row["regime"]
            g_delta_z.labels(symbol=sym, regime=reg).set(row["committed_delta_z_thr"])
            g_obi.labels(symbol=sym, regime=reg).set(row["committed_obi_thr"])
            g_shadow_z.labels(symbol=sym, regime=reg).set(row["shadow_delta_z_thr"])
            g_shadow_obi.labels(symbol=sym, regime=reg).set(row["shadow_obi_thr"])
            g_n.labels(symbol=sym, regime=reg).set(row["n_buf"])
    except Exception as e:
        logging.getLogger("pre_publish_gate_cal").warning("snapshot error: %s", e)


if __name__ == "__main__":
    main()
