"""
tb_cost_bps_calibrator_v1.py — streaming service for TbCostBpsCalibrator.

Reads trades:closed, calibrates per-symbol TB cost estimate
  cost_bps = 2 × spread_p50 + 2 × fee_bps + slip_p50
and publishes snapshot to autocal:tb_cost_bps:state.

Master switch: TB_COST_BPS_CAL_ENFORCE=0 (shadow mode default).
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger("tb_cost_bps_cal")


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
    from core.tb_cost_bps_calibrator import TbCostBpsCalibrator

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    redis_url = _env("TB_COST_BPS_CAL_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    in_stream = _env("TB_COST_BPS_CAL_IN_STREAM", "trades:closed")
    group = _env("TB_COST_BPS_CAL_GROUP", "tb-cost-bps-cal")
    consumer = _env("TB_COST_BPS_CAL_CONSUMER", "tb-cost-bps-cal-1")
    out_key = _env("TB_COST_BPS_CAL_OUT_KEY", RK.AUTOCAL_TB_COST_BPS)
    port = _env_int("TB_COST_BPS_CAL_PORT", 9901)
    batch = _env_int("TB_COST_BPS_CAL_BATCH", 200)
    snap_sec = _env_int("TB_COST_BPS_CAL_SNAPSHOT_SEC", 60)
    enforce = _env_bool("TB_COST_BPS_CAL_ENFORCE", False)
    auto_enforce = _env_bool("TB_COST_BPS_CAL_AUTO_ENFORCE", True)
    window_days = _env_float("TB_COST_BPS_CAL_WINDOW_DAYS", 7.0)
    min_samples = _env_int("TB_COST_BPS_CAL_MIN_SAMPLES", 50)
    default_cost_bps = _env_float("TB_COST_BPS_DEFAULT", 7.0)
    fee_bps = _env_float("TB_COST_BPS_FEE_BPS", 3.0)
    weights_enabled = _env_bool("REJECT_REASON_WEIGHTS_ENABLED", False)

    rc = redis.from_url(redis_url, decode_responses=True)

    # Consumer group bootstrap (idempotent)
    try:
        rc.xgroup_create(in_stream, group, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create: %s", e)

    cal = TbCostBpsCalibrator(
        enforce=enforce,
        auto_enforce=auto_enforce,
        window_days=window_days,
        min_samples=min_samples,
        default_cost_bps=default_cost_bps,
        fee_bps=fee_bps,
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
    g_cost = _gauge("tb_cost_bps_cal_cost", "Calibrated TB cost estimate bps", ["symbol"])
    g_shadow = _gauge("tb_cost_bps_cal_shadow", "Shadow TB cost estimate bps", ["symbol"])
    g_n = _gauge("tb_cost_bps_cal_n_buf", "Buffer samples", ["symbol"])
    c_obs = _counter("tb_cost_bps_cal_observed_total", "Observations processed", ["symbol"])
    c_skip = _counter("tb_cost_bps_cal_skipped_total", "Observations skipped", ["reason"])

    last_snap_ms = 0

    log.info(
        "tb_cost_bps_cal started (enforce=%s, port=%d, window_days=%.1f, fee_bps=%.2f)",
        enforce, port, window_days, fee_bps,
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
                        symbol = (
                            fields.get("symbol") or
                            fields.get("sym") or "*"
                        ).strip().upper()

                        # spread_bps: prefer entry_spread_bps, fallback adverse_bps_t
                        spread_bps_raw = fields.get("entry_spread_bps")
                        if spread_bps_raw is None or spread_bps_raw == "":
                            spread_bps_raw = fields.get("adverse_bps_t", "")
                        spread_bps = _safe_float(spread_bps_raw, float("nan"))
                        if spread_bps != spread_bps:  # nan check
                            c_skip.labels(reason="spread_missing").inc()
                            ack_ids.append(msg_id)
                            continue
                        if spread_bps < 0:
                            c_skip.labels(reason="spread_negative").inc()
                            ack_ids.append(msg_id)
                            continue

                        # slip_bps: prefer entry_slip_bps
                        slip_bps_raw = fields.get("entry_slip_bps", "")
                        slip_bps = _safe_float(slip_bps_raw, float("nan"))
                        if slip_bps != slip_bps:  # nan — use 0 (no slip observed)
                            slip_bps = 0.0

                        ts_ms = int(fields.get("ts_ms", int(time.time() * 1000)))
                        w = _weight_for_reason(fields.get("v_gate_reason", "")) if weights_enabled else 1.0

                        cal.observe(
                            symbol=symbol,
                            spread_bps=spread_bps,
                            slip_bps=slip_bps,
                            ts_ms=ts_ms,
                            w=w,
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
            _do_snapshot(cal, rc, out_key, g_cost, g_shadow, g_n)


def _do_snapshot(
    cal: Any, rc: Any, out_key: str,
    g_cost: Gauge, g_shadow: Gauge, g_n: Gauge,
) -> None:
    try:
        snap = cal.snapshot()
        rc.set(out_key, json.dumps(snap))
        for row in snap.get("bins", []):
            sym = row["symbol"]
            g_cost.labels(symbol=sym).set(row["committed_cost_bps"])
            g_shadow.labels(symbol=sym).set(row["shadow_cost_bps"])
            g_n.labels(symbol=sym).set(row["n_buf"])
    except Exception as e:
        logging.getLogger("tb_cost_bps_cal").warning("snapshot error: %s", e)


if __name__ == "__main__":
    main()
