import json
import logging
import os
from typing import Any

import redis
from core.redis_keys import RedisStreams as RS

logger = logging.getLogger("atr_change_control_telegram")

def _redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

def _ops_chat_id() -> str:
    return (os.getenv("ATR_POLICY_TELEGRAM_CHAT_ID", "") or "")

def _change_text(chg: dict[str, Any], replay: dict[str, Any] = None) -> str:
    txt = (
        f"ATR Change Control\n"
        f"Change: {chg.get('change_id')}\n"
        f"Type: {chg.get('change_type')}\n"
        f"Scope: {chg.get('symbol','*')} | {chg.get('scenario','*')} | {chg.get('layer','*')} | v{chg.get('policy_ver','0')}\n"
        f"Status: {chg.get('status')}\n"
    )
    if replay:
        txt += (
            f"\nReplay:\n"
            f"- status={replay.get('status', 'unknown')}\n"
        )
        if "decision_drift_pct" in replay:
            txt += f"- drift={replay.get('decision_drift_pct')}%\n"
        post = replay.get('post_trade', {})
        if post:
            txt += (
                f"- delta_pnl_bps={post.get('delta_pnl_bps','?')}\n"
                f"- delta_slippage_bps={post.get('delta_slippage_bps','?')}\n"
            )

    if chg.get('status') == "APPROVAL_PENDING":
        txt += "\nNeed: 1 approval"

    return txt

def _buttons(change_id: str) -> list[list[dict[str, str]]]:
    return [
        [
            {"text": "✅ Approve", "callback": f"atrchange:approve:{change_id}"},
            {"text": "❌ Reject", "callback": f"atrchange:reject:{change_id}"},
        ],
        [
            {"text": "⏸ Pause", "callback": f"atrchange:pause:{change_id}"},
            {"text": "⏮ Rollback", "callback": f"atrchange:rollback:{change_id}"},
        ],
        [
            {"text": "🔎 Show Artifacts", "callback": f"atrchange:artifacts:{change_id}"},
        ]
    ]

def publish_change_to_telegram(chg: dict[str, Any], replay: dict[str, Any] = None) -> bool:
    change_id = (chg.get("change_id") or "")
    if not change_id:
        return False

    payload = {
        "text": _change_text(chg, replay),
        "buttons": json.dumps(_buttons(change_id), ensure_ascii=False),
    }
    chat_id = _ops_chat_id()
    if chat_id:
        payload["chat_id"] = chat_id

    try:
        _redis().xadd(
            RS.NOTIFY_TELEGRAM,
            payload,
            maxlen=int(os.getenv("ATR_POLICY_TELEGRAM_NOTIFY_MAXLEN", "10000")),
            approximate=True,
        )
        return True
    except Exception as e:
        logger.error(f"Failed to publish to TG: {e}")
        return False

def publish_ack(change_id: str, action: str, actor: str, note: str = "") -> bool:
    text = (
        f"ATR Change Action\n"
        f"Change ID: {change_id}\n"
        f"Action: {action}\n"
        f"Actor: {actor}\n"
        f"Note: {note or '-'}"
    )
    payload = {"text": text}
    chat_id = _ops_chat_id()
    if chat_id:
        payload["chat_id"] = chat_id
    try:
        _redis().xadd(RS.NOTIFY_TELEGRAM, payload, maxlen=5000, approximate=True)
        return True
    except Exception:
        return False
