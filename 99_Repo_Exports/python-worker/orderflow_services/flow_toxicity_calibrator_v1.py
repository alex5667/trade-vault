#!/usr/bin/env python3
"""flow_toxicity_calibrator_v1.py

Feed-side service for Flow Toxicity autocalibrator.
Читает RS.OF_INPUTS, извлекает ofi_norm_z / vpin_cdf из indicators,
наполняет FlowToxicityCalibrator per symbol, публикует снапшоты в Redis.

Wiring:

  signals:of:inputs (XREADGROUP) → observe(symbol, ofi_z, vpin)
    → HSET autocal:flow_toxicity:state {symbol} {json_state}
      → FlowToxicityThresholdReader (services/flow_toxicity_runtime_overrides.py)
        → signal_pipeline: thr_z / thr_vpin per symbol

Lifecycle: disabled → shadow (n >= FLOW_TOX_CAL_MIN_SAMPLES=2000) → enforce.

ENV
  FLOW_TOX_CAL_REDIS_URL       (default REDIS_URL → redis://redis-worker-1:6379/0)
  FLOW_TOX_CAL_GROUP           (default "flow-tox-cal")
  FLOW_TOX_CAL_CONSUMER        (default "flow-tox-cal-1")
  FLOW_TOX_CAL_PORT            (default 9149)
  FLOW_TOX_CAL_BATCH           (default 500)
  FLOW_TOX_CAL_MIN_SAMPLES     (default 2000)
  FLOW_TOX_CAL_ENFORCE         (default 0 — shadow only)
  FLOW_TOX_CAL_AUTO_ENFORCE    (default 1 — auto-promote при n >= MIN_SAMPLES)
  FLOW_TOX_CAL_SNAPSHOT_SEC    (default 30)
  FLOW_TOX_CAL_UPDATE_BAND_Z   (default 0.10)
  FLOW_TOX_CAL_UPDATE_BAND_VPIN(default 0.005)
"""
from __future__ import annotations

import json
import logging
import math
import os
import signal
import time
from typing import Any

from prometheus_client import REGISTRY, Counter, Gauge, start_http_server  # type: ignore

from core.flow_toxicity_calibrator import FlowToxicityCalibrator
from core.redis_client import get_redis
from core.redis_keys import RK, RS

logger = logging.getLogger("flow-tox-cal")


# --------------------------------------------------------------------------
# env helpers
# --------------------------------------------------------------------------


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    raw = _env(name, "")
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = _env(name, "")
    try:
        return float(raw) if raw else default
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name, "")
    if not raw:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _decode(v: Any) -> str:
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "ignore")
    return str(v) if v is not None else ""


def _safe_float(v: Any, default: float = float("nan")) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "ignore")
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


# --------------------------------------------------------------------------
# Prometheus (idempotent)
# --------------------------------------------------------------------------


def _gauge(name: str, doc: str, labels: list[str]) -> Gauge:
    try:
        return Gauge(name, doc, labels)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore[return-value]
        raise


def _counter(name: str, doc: str, labels: list[str]) -> Counter:
    try:
        return Counter(name, doc, labels)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore[return-value]
        raise


# --------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------


def main() -> None:  # pragma: no cover
    logging.basicConfig(
        level=_env("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    port = _env_int("FLOW_TOX_CAL_PORT", 9149)
    group = _env("FLOW_TOX_CAL_GROUP", "flow-tox-cal")
    consumer = _env("FLOW_TOX_CAL_CONSUMER", "flow-tox-cal-1")
    batch = _env_int("FLOW_TOX_CAL_BATCH", 500)
    min_samples = _env_int("FLOW_TOX_CAL_MIN_SAMPLES", 2000)
    enforce = _env_bool("FLOW_TOX_CAL_ENFORCE", False)
    auto_enforce = _env_bool("FLOW_TOX_CAL_AUTO_ENFORCE", True)
    snap_sec = _env_int("FLOW_TOX_CAL_SNAPSHOT_SEC", 30)
    update_band_z = _env_float("FLOW_TOX_CAL_UPDATE_BAND_Z", 0.10)
    update_band_vpin = _env_float("FLOW_TOX_CAL_UPDATE_BAND_VPIN", 0.005)

    logger.info(
        "flow-tox-cal start: port=%d group=%s consumer=%s enforce=%s "
        "auto_enforce=%s min_samples=%d snap=%ds band_z=%.3f band_vpin=%.4f",
        port, group, consumer, enforce, auto_enforce,
        min_samples, snap_sec, update_band_z, update_band_vpin,
    )

    cal = FlowToxicityCalibrator(
        min_samples=min_samples,
        enforce=enforce,
        auto_enforce=auto_enforce,
        update_band_z=update_band_z,
        update_band_vpin=update_band_vpin,
    )

    # ── Prometheus ────────────────────────────────────────────────────────────
    g_up = _gauge("flow_tox_cal_up", "Service up", [])
    g_n = _gauge(
        "flow_tox_cal_n_observations",
        "Total observations per symbol",
        ["symbol"],
    )
    g_thr_z = _gauge(
        "flow_tox_cal_threshold_ofi_z",
        "Committed ofi_norm_z p95 threshold per symbol (0=disabled)",
        ["symbol", "src"],
    )
    g_thr_vpin = _gauge(
        "flow_tox_cal_threshold_vpin_cdf",
        "Committed vpin_cdf p95 threshold per symbol (0=disabled)",
        ["symbol", "src"],
    )
    g_warmup = _gauge(
        "flow_tox_cal_warmup_fraction",
        "n / min_samples per symbol (1.0 = warm)",
        ["symbol"],
    )
    g_snap_lag = _gauge("flow_tox_cal_snapshot_age_ms", "Wall-clock ms since last snapshot", [])
    c_obs = _counter("flow_tox_cal_observed_total", "Signals observed", ["has_ofi", "has_vpin"])
    c_skip = _counter("flow_tox_cal_skipped_total", "Signals skipped", ["reason"])
    c_snap = _counter("flow_tox_cal_snapshots_total", "Snapshot publishes", ["outcome"])

    g_up.set(1)

    # ── Redis init ─────────────────────────────────────────────────────────────
    redis_client = get_redis()
    stream_key = RS.OF_INPUTS

    # Идемпотентное создание consumer group.
    try:
        redis_client.xgroup_create(stream_key, group, id="$", mkstream=True)
        logger.info("Created consumer group %s on %s", group, stream_key)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            logger.debug("Consumer group %s already exists", group)
        else:
            logger.warning("xgroup_create %s: %s", group, e)

    # Восстановить состояние из Redis (если есть сохранённый снапшот).
    try:
        saved = redis_client.hgetall(RK.AUTOCAL_FLOW_TOXICITY)
        if saved:
            loaded = 0
            for _sym_raw, _state_raw in saved.items():
                try:
                    state = json.loads(_decode(_state_raw))
                    cal.load_state(state)
                    loaded += 1
                except Exception:
                    pass
            logger.info("Loaded %d symbol states from Redis", loaded)
    except Exception as e:
        logger.warning("State restore failed (starting fresh): %s", e)

    start_http_server(port)
    logger.info("HTTP /metrics on :%d", port)

    stop = {"flag": False}

    def _sig(_signo: int, _frame: Any) -> None:
        stop["flag"] = True
        logger.info("Shutdown signal received")

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    last_snap_ms = 0

    while not stop["flag"]:
        try:
            resp = redis_client.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream_key: ">"},
                count=batch,
                block=2000,
            )
        except Exception as e:
            logger.error("XREADGROUP failed: %s", e)
            time.sleep(1.0)
            continue

        now_ms = int(time.time() * 1000)
        ack_ids: list[Any] = []

        if resp:
            for _sk, messages in resp:  # type: ignore[union-attr]
                for msg_id, fields in messages:
                    ack_ids.append(msg_id)
                    try:
                        fields = {_decode(k): _decode(v) for k, v in fields.items()}
                        _observe_one(cal, fields, c_obs, c_skip)
                    except Exception as exc:
                        c_skip.labels(reason="exception").inc()
                        logger.warning("observe failed %s: %s", msg_id, exc)

            if ack_ids:
                try:
                    redis_client.xack(stream_key, group, *ack_ids)
                except Exception as e:
                    logger.error("XACK failed: %s", e)

        # ── периодический снапшот ─────────────────────────────────────────────
        if (now_ms - last_snap_ms) >= snap_sec * 1000:
            try:
                syms = cal.all_symbols()
                pipe = redis_client.pipeline(transaction=False)
                for sym in syms:
                    state = cal.dump_state(symbol=sym, updated_ts_ms=now_ms)
                    pipe.hset(RK.AUTOCAL_FLOW_TOXICITY, sym, json.dumps(state))
                if syms:
                    pipe.execute()

                last_snap_ms = now_ms
                g_snap_lag.set(0.0)
                c_snap.labels(outcome="ok").inc()

                # Publish Prometheus gauges per symbol
                for sym in syms:
                    n = cal.n(sym)
                    thr = cal.thresholds(symbol=sym)
                    shadow = cal.shadow_thresholds(symbol=sym)

                    g_n.labels(symbol=sym).set(float(n))
                    g_warmup.labels(symbol=sym).set(
                        min(1.0, n / max(1, min_samples))
                    )
                    g_thr_z.labels(symbol=sym, src=thr.src).set(thr.thr_z)
                    g_thr_vpin.labels(symbol=sym, src=thr.src).set(thr.thr_vpin)
                    if shadow:
                        g_thr_z.labels(symbol=sym, src="shadow").set(shadow.thr_z)
                        g_thr_vpin.labels(symbol=sym, src="shadow").set(shadow.thr_vpin)

                if syms:
                    logger.info(
                        "snapshot: %d symbols published, sample counts: %s",
                        len(syms),
                        {s: cal.n(s) for s in syms[:8]},
                    )
            except Exception as e:
                c_snap.labels(outcome="error").inc()
                logger.error("snapshot publish failed: %s", e)
        else:
            g_snap_lag.set(float(now_ms - last_snap_ms))

    g_up.set(0)
    logger.info("flow-tox-cal stopped")


def _observe_one(
    cal: FlowToxicityCalibrator,
    fields: dict[str, str],
    c_obs: Counter,
    c_skip: Counter,
) -> None:
    """Извлечь ofi_norm_z / vpin_cdf из сообщения OF_INPUTS и подать в калибратор."""
    # payload может быть JSON-строкой в поле "payload" или напрямую в fields
    payload_raw = fields.get("payload") or ""
    if payload_raw:
        try:
            payload = json.loads(payload_raw)
        except Exception:
            c_skip.labels(reason="payload_parse_error").inc()
            return
    else:
        payload = fields

    symbol = str(payload.get("symbol") or fields.get("symbol") or "").strip().upper()
    if not symbol:
        c_skip.labels(reason="no_symbol").inc()
        return

    # indicators — вложенный dict или JSON-строка
    ind_raw = payload.get("indicators")
    if isinstance(ind_raw, str):
        try:
            indicators: dict[str, Any] = json.loads(ind_raw)
        except Exception:
            indicators = {}
    elif isinstance(ind_raw, dict):
        indicators = ind_raw
    else:
        indicators = {}

    ofi_z = _safe_float(indicators.get("ofi_norm_z"), float("nan"))
    vpin = _safe_float(indicators.get("vpin_cdf"), float("nan"))

    has_ofi = math.isfinite(ofi_z)
    has_vpin = math.isfinite(vpin)

    if not has_ofi and not has_vpin:
        c_skip.labels(reason="no_features").inc()
        return

    # Подаём наблюдения (калибратор внутренне фильтрует non-finite)
    if has_ofi:
        cal.observe(symbol=symbol, ofi_z=ofi_z, vpin=vpin if has_vpin else 0.0)
    elif has_vpin:
        cal.observe(symbol=symbol, ofi_z=0.0, vpin=vpin)

    c_obs.labels(
        has_ofi="1" if has_ofi else "0",
        has_vpin="1" if has_vpin else "0",
    ).inc()


if __name__ == "__main__":  # pragma: no cover
    main()
