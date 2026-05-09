from __future__ import annotations

import os
from typing import Any

import redis
from core.redis_keys import RedisStreams as RS


def _redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)


def _notify(text: str) -> bool:
    payload: dict[str, Any] = {"text": text}
    chat_id = (os.getenv("ATR_POLICY_TELEGRAM_CHAT_ID", "") or "")
    if chat_id:
        payload["chat_id"] = chat_id
    return bool(_redis().xadd(RS.NOTIFY_TELEGRAM, payload, maxlen=5000, approximate=True))


def publish(result: dict[str, Any]) -> bool:
    cert = result.get("cert", {}) if isinstance(result.get("cert"), dict) else {}
    summary = cert.get("summary", {}) if isinstance(cert.get("summary"), dict) else {}
    text = (
        f"ATR Policy Restore Certification\n"
        f"Drill: {result.get('drill_code','')}\n"
        f"Mode: {result.get('mode','')}\n"
        f"Status: {cert.get('status','')}\n"
        f"Failed checks: {summary.get('failed_checks',[])}"
    )
    return _notify(text)
