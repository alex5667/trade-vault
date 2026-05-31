from __future__ import annotations

import json
import os
from typing import Any

import redis
from core.redis_keys import RedisStreams as RS


def _redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)


def _ops_chat_id() -> str:
    return (os.getenv("ATR_POLICY_TELEGRAM_CHAT_ID", "") or "")


def _fmt_pct(x: Any) -> str:
    try:
        return f"{float(x):.2f}"
    except Exception:
        return "n/a"


def _proposal_text(p: dict[str, Any]) -> str:
    ev = p.get("evidence", {}) if isinstance(p.get("evidence"), dict) else {}
    stop = ev.get("stop_ttl", {}) if isinstance(ev.get("stop_ttl"), dict) else {}
    trail = ev.get("trailing", {}) if isinstance(ev.get("trailing"), dict) else {}

    return (
        f"ATR Policy Proposal\n"
        f"ID: {p.get('proposal_id','')}\n"
        f"Source: {p.get('source','')}\n"
        f"Symbol: {p.get('symbol','')}\n"
        f"Scenario: {p.get('scenario','')}\n"
        f"Regime: {p.get('regime','')}\n"
        f"Bucket: {p.get('risk_horizon_bucket','')}\n"
        f"\n"
        f"Stop/TTL mode -> {p.get('stop_ttl_mode','')}\n"
        f"Trailing mode -> {p.get('trailing_mode','')}\n"
        f"Reason: {p.get('reason_code','')}\n"
        f"\n"
        f"Stop/TTL evidence:\n"
        f"n canary/control: {stop.get('n_canary','?')} / {stop.get('n_control','?')}\n"
        f"PnL canary/control: {_fmt_pct(stop.get('pnl_canary'))} / {_fmt_pct(stop.get('pnl_control'))}\n"
        f"TP1 canary/control: {_fmt_pct(stop.get('tp1_canary'))} / {_fmt_pct(stop.get('tp1_control'))}\n"
        f"Slip canary/control: {_fmt_pct(stop.get('slip_canary'))} / {_fmt_pct(stop.get('slip_control'))}\n"
        f"\n"
        f"Trailing evidence:\n"
        f"n canary/control: {trail.get('n_canary','?')} / {trail.get('n_control','?')}\n"
        f"PnL canary/control: {_fmt_pct(trail.get('pnl_canary'))} / {_fmt_pct(trail.get('pnl_control'))}\n"
        f"MFE canary/control: {_fmt_pct(trail.get('mfe_canary'))} / {_fmt_pct(trail.get('mfe_control'))}\n"
    )


def _buttons(proposal_id: str) -> list[list[dict[str, str]]]:
    return [
        [
            {"text": "✅ Approve", "callback": f"atrpol:approve:{proposal_id}"},  # type: ignore
            {"text": "❌ Reject", "callback": f"atrpol:reject:{proposal_id}"},
        ],
        [
            {"text": "↩️ Revoke", "callback": f"atrpol:revoke:{proposal_id}"},
            {"text": "🔎 Show", "callback": f"atrpol:show:{proposal_id}"},
        ],
    ]


def publish_policy_proposal_to_telegram(proposal: dict[str, Any]) -> bool:
    proposal_id = (proposal.get("proposal_id") or "")
    if not proposal_id:
        return False

    payload = {
        "text": _proposal_text(proposal),
        "buttons": json.dumps(_buttons(proposal_id), ensure_ascii=False),
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
        try:
            from services.atr_promotion_policy_metrics import atr_policy_tg_proposal_publish_total
            atr_policy_tg_proposal_publish_total.labels(status="ok").inc()
        except Exception:
            pass
        return True
    except Exception:
        try:
            from services.atr_promotion_policy_metrics import atr_policy_tg_proposal_publish_total
            atr_policy_tg_proposal_publish_total.labels(status="failed").inc()
        except Exception:
            pass
        return False


def publish_policy_ack_to_telegram(*, proposal_id: str, action: str, actor: str, note: str = "") -> bool:
    text = (
        f"ATR Policy Decision\n"
        f"ID: {proposal_id}\n"
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
        try:
            from services.atr_promotion_policy_metrics import atr_policy_tg_ack_total
            atr_policy_tg_ack_total.labels(status="ok").inc()
        except Exception:
            pass
        return True
    except Exception:
        try:
            from services.atr_promotion_policy_metrics import atr_policy_tg_ack_total
            atr_policy_tg_ack_total.labels(status="failed").inc()
        except Exception:
            pass
        return False
