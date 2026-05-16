from __future__ import annotations

"""tp_hit_trailing_orchestrator_layer_d_consumer.py

Standalone consumer для stream `trail:arm:requests` от Layer D early-arm hook.

Назначение:
  - Хост-процесс (можно интегрировать в tp_hit_trailing_orchestrator).
  - XREADGROUP консюмит arm-requests с HMAC verification.
  - Вызывает _arm_trailing_via_callable() — каждый proection callback.
  - Идемпотентно: per signal_id (Redis SET с TTL).

Контракт payload:
  {
    "signal_id":  str,
    "symbol":     str,
    "side":       "LONG"|"SHORT",
    "mfe_r":      float,
    "mfe_bps":    float,
    "one_r_bps":  float,
    "ts_ms":      int,
    "source":     "MFE_EARLY_ARM",
    "arm_threshold_r": float
  }
  + field "sig" = HMAC-SHA256(payload, LAYER_D_HMAC_SECRET).

Этот файл — **skeleton**. Реальная интеграция в tp_hit_trailing_orchestrator
требует вызова существующего _process_tp1_event / equivalent кода для arm.
Здесь предоставляется отдельный entry для тестирования и поэтапного rollout.

ENV:
  LAYER_D_CONSUMER_ENABLE        0
  LAYER_D_CONSUMER_GROUP         layer_d_arm_cg
  LAYER_D_CONSUMER_NAME          consumer-1
  LAYER_D_ARM_STREAM             trail:arm:requests
  LAYER_D_HMAC_SECRET            ... (verify)
  LAYER_D_DEDUP_TTL_SEC          3600
  LAYER_D_CONSUMER_REDIS_URL     redis://redis-worker-1:6379/0
  LAYER_D_CONSUMER_PROM_PORT     9853
"""

import hashlib
import hmac
import json
import logging
import os
import time
from typing import Any, Callable

import redis
from prometheus_client import Counter as PCounter, Gauge, start_http_server  # type: ignore

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [layer-d-consumer] %(levelname)s %(message)s")
log = logging.getLogger(__name__)

g_up        = Gauge("layer_d_consumer_up", "consumer loop up")
g_lag       = Gauge("layer_d_consumer_lag", "stream pending entries")
c_received  = PCounter("layer_d_consumer_received_total", "received", ["result"])
c_armed     = PCounter("layer_d_consumer_armed_total", "successful arm calls",
                       ["symbol"])
c_errors    = PCounter("layer_d_consumer_errors_total", "errors", ["where"])


def _env(k: str, d: str = "") -> str: return os.environ.get(k, d)


def _verify_hmac(payload_raw: str, sig: str, secret: str) -> bool:
    if not (payload_raw and sig and secret):
        return False
    try:
        rec = json.loads(payload_raw)
        canon = json.dumps(rec, sort_keys=True, separators=(",", ":")).encode()
        expected = hmac.new(secret.encode(), canon, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, sig)
    except Exception:
        return False


def _is_duplicate(r: redis.Redis, signal_id: str, ttl: int) -> bool:
    """SETNX dedup key. Возвращает True если уже было."""
    key = f"layer_d:armed:{signal_id}"
    try:
        ok = r.set(key, "1", nx=True, ex=ttl)
        return not bool(ok)
    except Exception:
        return False


def run(arm_callback: Callable[[dict[str, Any]], bool] | None = None) -> int:
    """Главный цикл консюмера.

    arm_callback(payload_dict) → bool — фактический trail-arm hook.
      Возвращает True при успешной armировке.
      Если None — только логирование (для тестирования контракта).
    """
    if int(_env("LAYER_D_CONSUMER_ENABLE", "0") or "0") == 0:
        log.info("LAYER_D_CONSUMER_ENABLE=0 — exit")
        return 0

    redis_url = _env("LAYER_D_CONSUMER_REDIS_URL", "redis://redis-worker-1:6379/0")
    stream    = _env("LAYER_D_ARM_STREAM", "trail:arm:requests")
    group     = _env("LAYER_D_CONSUMER_GROUP", "layer_d_arm_cg")
    consumer  = _env("LAYER_D_CONSUMER_NAME", "consumer-1")
    secret    = (_env("LAYER_D_HMAC_SECRET", "")
                 or _env("LAYERS_CAL_HMAC_SECRET", "")
                 or _env("RECS_HMAC_SECRET", ""))
    dedup_ttl = int(_env("LAYER_D_DEDUP_TTL_SEC", "3600") or "3600")
    prom_port = int(_env("LAYER_D_CONSUMER_PROM_PORT", "9853") or "9853")

    try:
        start_http_server(prom_port)
        log.info(f"prometheus on :{prom_port}")
    except Exception as ex:
        log.warning(f"prom: {ex}")

    r = redis.from_url(redis_url, decode_responses=True)
    # Создаём consumer group (idempotent)
    try:
        r.xgroup_create(stream, group, id="$", mkstream=True)
        log.info(f"created consumer group {group} on {stream}")
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create: {e}")

    g_up.set(1)
    log.info(f"consuming {stream} group={group} as {consumer}")
    while True:
        try:
            resp = r.xreadgroup(group, consumer, {stream: ">"}, count=64, block=5000)
            if not resp:
                continue
            for _stream, entries in resp:  # type: ignore[union-attr]
                for entry_id, fields in entries:
                    payload_raw = fields.get("payload", "")
                    sig = fields.get("sig", "")
                    if not _verify_hmac(payload_raw, sig, secret):
                        c_received.labels(result="hmac_invalid").inc()
                        log.warning(f"hmac invalid id={entry_id}")
                        try: r.xack(stream, group, entry_id)
                        except Exception: pass
                        continue
                    try:
                        payload = json.loads(payload_raw)
                    except Exception:
                        c_received.labels(result="parse_error").inc()
                        try: r.xack(stream, group, entry_id)
                        except Exception: pass
                        continue
                    sid = str(payload.get("signal_id", "") or "")
                    if not sid:
                        c_received.labels(result="no_sid").inc()
                        try: r.xack(stream, group, entry_id)
                        except Exception: pass
                        continue
                    if _is_duplicate(r, sid, dedup_ttl):
                        c_received.labels(result="duplicate").inc()
                        try: r.xack(stream, group, entry_id)
                        except Exception: pass
                        continue

                    # Реальная armировка
                    armed = False
                    if arm_callback is not None:
                        try:
                            armed = bool(arm_callback(payload))
                        except Exception as ex:
                            log.warning(f"arm_callback error sid={sid}: {ex}")
                            c_errors.labels(where="arm_callback").inc()
                    else:
                        # No callback — только лог
                        log.info(
                            "[NO-CALLBACK] would arm sid=%s symbol=%s mfe_r=%.3f",
                            sid, payload.get("symbol"), payload.get("mfe_r", 0.0),
                        )
                        armed = True  # treat as success for ACK

                    if armed:
                        c_armed.labels(symbol=str(payload.get("symbol", "?"))).inc()
                        c_received.labels(result="ok").inc()
                    else:
                        c_received.labels(result="arm_failed").inc()
                    try: r.xack(stream, group, entry_id)
                    except Exception: pass
        except Exception as ex:
            log.exception(f"loop error: {ex}")
            c_errors.labels(where="loop").inc()
            time.sleep(1)


if __name__ == "__main__":
    raise SystemExit(run(arm_callback=None))
