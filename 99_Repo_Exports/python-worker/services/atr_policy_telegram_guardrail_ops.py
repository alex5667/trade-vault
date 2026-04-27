from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

import redis


def _redis() -> redis.Redis:
    return redis.Redis.from_url(
        os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
        decode_responses=True,
    )


def _chat_id() -> str:
    return str(os.getenv("ATR_POLICY_TELEGRAM_CHAT_ID", "") or "")


def _notify(text: str, buttons: Optional[List[List[Dict[str, str]]]] = None) -> bool:
    payload: Dict[str, Any] = {"text": text}
    if buttons:
        payload["buttons"] = json.dumps(buttons, ensure_ascii=False)
    cid = _chat_id()
    if cid:
        payload["chat_id"] = cid
    try:
        _redis().xadd("notify:telegram", payload, maxlen=10000, approximate=True)
        return True
    except Exception:
        return False


_RISK_EMOJI: Dict[str, str] = {
    "SAFE": "🟢",
    "WARN": "⚠️",
    "BLOCK": "🔴",
}


def publish_guardrail_warning(
    *,
    action: str,
    target: str,
    guard: Dict[str, Any],
    confirm_callback: str,
) -> bool:
    """
    Publish a Telegram warning message with a Confirm / Cancel inline keyboard.

    Called when guardrail returns WARN + require_confirm=True.
    The confirm_callback should be `atrpack:confirm:<token>`.
    """
    risk_class = guard.get("risk_class", "WARN")
    emoji = _RISK_EMOJI.get(risk_class, "⚠️")
    details = guard.get("reason_details", {})
    details_str = ", ".join(f"{k}={v}" for k, v in details.items()) if details else "-"

    text = (
        f"{emoji} ATR Policy Guard Rail\n"
        f"Action: {action}\n"
        f"Target: {target}\n"
        f"Risk: {risk_class}\n"
        f"Reason: {guard.get('reason_code', '')}\n"
        f"Details: {details_str}\n\n"
        f"Tap ⚠️ Confirm to proceed, or ↩️ Cancel to abort."
    )
    buttons = [
        [
            {"text": "⚠️ Confirm", "callback": confirm_callback},
            {"text": "↩️ Cancel", "callback": "atrpack:refresh"},
        ]
    ]
    # Best-effort metrics
    try:
        from prometheus_client import Counter
        Counter(
            "atr_policy_guardrail_confirm_total",
            "ATR policy guardrail confirm dialogs issued",
            ["action"],
        ).labels(action=action).inc()
    except Exception:
        pass

    return _notify(text, buttons=buttons)


def publish_guardrail_block(
    *,
    action: str,
    target: str,
    guard: Dict[str, Any],
) -> bool:
    """
    Publish a short Telegram BLOCK notification (no buttons — action is not possible).
    """
    details = guard.get("reason_details", {})
    details_str = ", ".join(f"{k}={v}" for k, v in details.items()) if details else "-"

    text = (
        f"🔴 ATR Policy BLOCKED\n"
        f"Action: {action}\n"
        f"Target: {target}\n"
        f"Reason: {guard.get('reason_code', '')}\n"
        f"Details: {details_str}"
    )
    # Best-effort metrics
    try:
        from prometheus_client import Counter
        Counter(
            "atr_policy_guardrail_block_total",
            "ATR policy guardrail blocks",
            ["action", "reason_code"],
        ).labels(action=action, reason_code=guard.get("reason_code", "")).inc()
    except Exception:
        pass

    return _notify(text)
