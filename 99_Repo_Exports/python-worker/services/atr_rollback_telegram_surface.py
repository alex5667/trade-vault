import json
import os
import logging
from typing import Any, Dict, List

import redis
try:
    from core.redis_client import get_atr_redis
except Exception:
    get_atr_redis = None

logger = logging.getLogger("atr_rollback_telegram")

def _redis():
    if get_atr_redis is not None:
        return get_atr_redis()
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

def _ops_chat_id() -> str:
    return str(os.getenv("ATR_POLICY_TELEGRAM_CHAT_ID", "") or "")

def _rollback_text(rb: Dict[str, Any], cert: Dict[str, Any] = None) -> str:
    txt = (
        f"ATR Rollback Control\n"
        f"Rollback: {rb.get('rollback_id')}\n"
        f"Class: {rb.get('rollback_class')}\n"
        f"Scope: {rb.get('symbol','*')} | {rb.get('scenario','*')} | {rb.get('layer','*')} | v{rb.get('policy_ver','0')}\n"
        f"Status: {rb.get('status')}\n"
    )
    
    txt += "\nTarget:\n"
    if rb.get("target_policy_ver"):
        txt += f"- policy_ver={rb.get('target_policy_ver')}\n"
    if rb.get("target_stage"):
        txt += f"- rollout_stage={rb.get('target_stage')}\n"
    txt += f"- use_last_good={rb.get('use_last_good', False)}\n"
        
    txt += "\nPost-rollback cert:\n"
    if cert:
        txt += f"- status={cert.get('status', 'unknown')}\n"
        if cert.get('status') == 'passed':
            txt += "- All checks passed\n"
        else:
            txt += "- Checks FAILED\n"
    elif rb.get("status") in ("ROLLBACK_CERT_PENDING", "ROLLBACK_EXECUTED"):
        txt += "- pending\n"
    else:
        txt += "- none\n"

    return txt

def _buttons(rollback_id: str) -> List[List[Dict[str, str]]]:
    return [
        [
            {"text": "✅ Approve", "callback": f"atr_rollback:approve:{rollback_id}"},
            {"text": "⏸ Pause", "callback": f"atr_rollback:pause:{rollback_id}"},
        ],
        [
            {"text": "📄 Show Manifest", "callback": f"atr_rollback:manifest:{rollback_id}"},
            {"text": "🔎 Show Post-Cert", "callback": f"atr_rollback:postcert:{rollback_id}"},
        ],
        [
            {"text": "📁 Show Evidence", "callback": f"atr_rollback:evidence:{rollback_id}"},
        ]
    ]

def publish_rollback_to_telegram(rb: Dict[str, Any], cert: Dict[str, Any] = None) -> bool:
    rollback_id = str(rb.get("rollback_id") or "")
    if not rollback_id:
        return False

    payload = {
        "text": _rollback_text(rb, cert),
        "buttons": json.dumps(_buttons(rollback_id), ensure_ascii=False),
    }
    chat_id = _ops_chat_id()
    if chat_id:
        payload["chat_id"] = chat_id

    try:
        _redis().xadd(
            "notify:telegram",
            payload,
            maxlen=int(os.getenv("ATR_POLICY_TELEGRAM_NOTIFY_MAXLEN", "10000")),
            approximate=True,
        )
        return True
    except Exception as e:
        logger.error(f"Failed to publish to TG: {e}")
        return False

def publish_ack(rollback_id: str, action: str, actor: str, note: str = "") -> bool:
    text = (
        f"ATR Rollback Action\n"
        f"Rollback ID: {rollback_id}\n"
        f"Action: {action}\n"
        f"Actor: {actor}\n"
        f"Note: {note or '-'}"
    )
    payload = {"text": text}
    chat_id = _ops_chat_id()
    if chat_id:
        payload["chat_id"] = chat_id
    try:
        _redis().xadd("notify:telegram", payload, maxlen=5000, approximate=True)
        return True
    except Exception:
        return False
