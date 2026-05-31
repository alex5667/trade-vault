# order_push_dispatcher.py
"""
Order Push Dispatcher — отправка ордеров в go-gateway и уведомлений в Telegram.

ENV:
    GATEWAY_URL              URL базового gateway (default http://127.0.0.1:8090)
    ORDERS_PUSH_URL          Полный URL /orders/push (default {GATEWAY_URL}/orders/push)
    ORDERS_PUSH_TIMEOUT_S    HTTP-timeout в секундах (default 5.0)
    ORDERS_DLQ_STREAM        Redis stream для failed-orders DLQ (default orders:dlq)
    NOTIFY_STREAM            Redis stream для Telegram-уведомлений (default notify:telegram)
    REDIS_URL                URL Redis (default redis://localhost:6379/0)
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

import requests

try:
    import redis as _redis_mod
except ImportError:
    _redis_mod = None  # type: ignore[assignment]

from common.log import setup_logger

log = setup_logger("order_push")

GATEWAY_URL: str = os.getenv("GATEWAY_URL", "http://127.0.0.1:8090")
ORDERS_PUSH_URL: str = os.getenv("ORDERS_PUSH_URL", f"{GATEWAY_URL}/orders/push")
ORDERS_PUSH_TIMEOUT_S: float = float(os.getenv("ORDERS_PUSH_TIMEOUT_S", "5.0"))
ORDERS_DLQ_STREAM: str = os.getenv("ORDERS_DLQ_STREAM", "orders:dlq")
TELEGRAM_NOTIFY_STREAM: str = os.getenv("NOTIFY_STREAM", "notify:telegram")
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
ORDER_PUSH_DISABLED: bool = os.getenv("ORDER_PUSH_DISABLED", "false").lower() in ("true", "1", "yes")
ORDER_PUSH_ENABLE: bool = os.getenv("ORDER_PUSH_ENABLE", "1").lower() in ("true", "1", "yes")

# ---------------------------------------------------------------------------
# Optional Prometheus counter (graceful import — not a hard dependency here)
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter as _Counter, Gauge as _Gauge

    _ORDER_PUSH_ERRORS = _Counter(
        "order_push_errors_total",
        "Total order push failures by reason",
        ["reason"],
    )
    
    _DLQ_DEPTH = _Gauge(
        "order_push_dlq_depth",
        "Current depth of orders:dlq stream"
    )

    def _inc_error(reason: str) -> None:
        _ORDER_PUSH_ERRORS.labels(reason=reason).inc()
        
    def _update_dlq_depth(r: Any) -> None:
        try:
            _DLQ_DEPTH.set(r.xlen(ORDERS_DLQ_STREAM))
        except Exception as e:
            log.debug("Failed to update DLQ depth gauge: %s", e)

except Exception:  # pragma: no cover
    def _inc_error(reason: str) -> None:  # type: ignore[misc]
        pass
        
    def _update_dlq_depth(r: Any) -> None:
        pass


def _publish_dlq(payload: Dict[str, Any], error: str, r: Any = None) -> None:
    """Публикует упавший ордер в DLQ-stream для последующей обработки."""
    if _redis_mod is None:
        return
    try:
        if r is None:
            r = _redis_mod.Redis.from_url(REDIS_URL, decode_responses=True)
            
        try:
            r.xgroup_create(ORDERS_DLQ_STREAM, "dlq_watchers", id="0", mkstream=True)
        except _redis_mod.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                log.debug("Failed to create consumer group: %s", e)
                
        r.xadd(
            ORDERS_DLQ_STREAM,
            {"payload": json.dumps(payload, default=str), "error": error},
            maxlen=10_000,
            approximate=True,
        )
        _update_dlq_depth(r)
    except Exception as dlq_err:
        log.error("DLQ publish failed: %s", dlq_err)


def post_order(payload: Dict[str, Any], r: Any = None) -> Dict[str, Any]:
    """
    Отправить ордер в go-gateway.

    Args:
        payload: {
          "action": "open" | "close" | "scale",
          "symbol": str,
          "side": "LONG" | "SHORT",
          "lot": float,
          "sl": float,
          "tp_levels": list[float],
          "sid": str,          # signal / trace ID
        }
        r: Опциональный Redis-клиент (используется при DLQ fallback).

    Returns:
        {"ok": bool, "status_code": int | None, "payload": dict | None, "reason": str | None}
    """
    sid = payload.get("sid", "?")

    if not ORDER_PUSH_ENABLE or ORDER_PUSH_DISABLED:
        log.warning("Order push disabled by ORDER_PUSH_ENABLE/DISABLED env | sid=%s", sid)
        return {
            "ok": True,
            "status_code": 200,
            "payload": {"stub": True, "msg": "Order push disabled via ENV"},
            "reason": None,
        }

    try:
        resp = requests.post(
            ORDERS_PUSH_URL,
            json=payload,
            timeout=ORDERS_PUSH_TIMEOUT_S,
            headers={"Content-Type": "application/json"},
        )
        if resp.ok:
            log.info("Order pushed ok | sid=%s status=%s", sid, resp.status_code)
            return {
                "ok": True,
                "status_code": resp.status_code,
                "payload": resp.json(),
                "reason": None,
            }

        # Non-2xx от gateway
        body = resp.text[:512]
        log.warning(
            "Order push rejected by gateway | sid=%s status=%s body=%s",
            sid,
            resp.status_code,
            body,
        )
        _inc_error("gateway_rejected")
        _publish_dlq(payload, f"http_{resp.status_code}", r=r)
        return {
            "ok": False,
            "status_code": resp.status_code,
            "payload": None,
            "reason": body,
        }

    except requests.exceptions.Timeout:
        log.error(
            "Order push timeout | sid=%s url=%s timeout=%.1fs",
            sid,
            ORDERS_PUSH_URL,
            ORDERS_PUSH_TIMEOUT_S,
        )
        _inc_error("timeout")
        _publish_dlq(payload, "timeout", r=r)
        return {"ok": False, "status_code": None, "payload": None, "reason": "timeout"}

    except requests.exceptions.ConnectionError as exc:
        log.error("Order push connection error | sid=%s err=%s", sid, exc)
        _inc_error("connection_error")
        _publish_dlq(payload, f"connection_error:{exc}", r=r)
        return {
            "ok": False,
            "status_code": None,
            "payload": None,
            "reason": "connection_error",
        }

    except Exception as exc:
        log.exception("Order push unexpected error | sid=%s err=%s", sid, exc)
        _inc_error("unexpected")
        _publish_dlq(payload, f"unexpected:{exc}", r=r)
        return {
            "ok": False,
            "status_code": None,
            "payload": None,
            "reason": f"unexpected:{exc}",
        }


def publish_telegram(text: str, r: Optional[Any] = None) -> None:
    """
    Опубликовать сообщение в Telegram-stream.

    Args:
        text: Текст сообщения.
        r:    Redis-клиент (опционально; создаётся автоматически если None).
    """
    if _redis_mod is None:
        return
    try:
        if r is None:
            r = _redis_mod.Redis.from_url(REDIS_URL, decode_responses=True)
        r.xadd(TELEGRAM_NOTIFY_STREAM, {"text": text}, maxlen=5_000)
    except Exception as exc:
        log.warning("Failed to publish telegram: %s", exc)
