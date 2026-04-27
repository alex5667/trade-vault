# order_push_dispatcher.py
"""
Order Push Dispatcher - отправка ордеров в go-gateway и уведомлений в Telegram.
"""
from __future__ import annotations
import os
import json
import requests
from typing import Dict, Any

try:
    import redis
except ImportError:
    redis = None

from common.log import setup_logger

log = setup_logger("order_push")

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://127.0.0.1:8090")
ORDERS_PUSH_URL = os.getenv("ORDERS_PUSH_URL", f"{GATEWAY_URL}/orders/push")
TELEGRAM_NOTIFY_STREAM = os.getenv("NOTIFY_STREAM", "notify:telegram")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

def post_order(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Отправить ордер в go-gateway.
    
    Args:
        payload: {
          "action": "open",
          "symbol": "XAUUSD",
          "side": "LONG",
          "lot": 0.03,
          "sl": 3975.5,
          "tp_levels": [3980.0,3983.0,3988.0],
          "sid": "smoke-001"
        }
    
    Returns:
        Ответ от gateway
    """
    log.info("Order push disabled, skipping request to %s (payload sid=%s)", ORDERS_PUSH_URL, payload.get("sid"))
    return {"ok": False, "reason": "order_push_disabled"}

def publish_telegram(text: str, r=None):
    """
    Опубликовать сообщение в Telegram stream.
    
    Args:
        text: Текст сообщения
        r: Redis клиент (опционально)
    """
    if not redis:
        return
    if r is None:
        r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        r.xadd(TELEGRAM_NOTIFY_STREAM, {"text": text}, maxlen=5000)
    except Exception as e:
        log.warning("Failed to publish telegram: %s", e)
