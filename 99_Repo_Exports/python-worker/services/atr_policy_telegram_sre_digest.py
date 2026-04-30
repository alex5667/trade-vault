"""ATR Policy Telegram SRE Digest — Phase 3.7.

Sends a compact, action-oriented health digest to the ops Telegram channel.
Uses collect_once() from atr_policy_sre_service — NOT a duplicate of ops pack.

This digest surfaces:
  - backlog status
  - latency SLO snapshot
  - reconcile freshness
  - error counters (denied, expired, revokes, flips)

Only action-worthy information — no noise.

Called from of_timers_worker nightly at 08:20.
"""
from __future__ import annotations

import os
from typing import Any, Dict

import redis

from services.atr_policy_sre_service import collect_once


def _redis() -> redis.Redis:
    return redis.Redis.from_url(
        os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        decode_responses=True
    )


def _chat_id() -> str:
    return str(os.getenv("ATR_POLICY_TELEGRAM_CHAT_ID", "") or "")


def _notify(text: str) -> bool:
    payload: Dict[str, Any] = {"text": text}
    cid = _chat_id()
    if cid:
        payload["chat_id"] = cid
    try:
        _redis().xadd("notify:telegram", payload, maxlen=5000, approximate=True)
        return True
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────────────
# SLO thresholds for inline status markers
# ──────────────────────────────────────────────────────────────────────────────

_THRESHOLDS = {
    "pending_oldest_age_sec": (14400, 86400),     # warn > 4h, page > 24h
    "proposal_to_decision_p95_sec": (14400, None),# warn > 4h
    "approve_to_apply_p95_sec": (300, None),      # warn > 5m
    "reconcile_last_success_age_sec": (300, 900), # warn > 5m, page > 15m
    "confirm_expired_today_total": (5, None),     # warn > 5
    "callback_denied_today_total": (0, None),     # warn > 0
    "flip_today_total": (5, None),                # warn > 5
}


def _badge(key: str, value: float) -> str:
    warn, page = _THRESHOLDS.get(key, (None, None))
    if page is not None and value > page:
        return "🔴"
    if warn is not None and value > warn:
        return "🟡"
    return "🟢"


def build_sre_digest(s: Dict[str, Any] | None = None) -> str:
    """Build compact SRE digest text. Accepts pre-collected stats for testing."""
    if s is None:
        s = collect_once()

    lines = [
        "📊 *ATR Policy SRE Digest*"
        ""
        "*Backlog*"
        f"  {_badge('pending_oldest_age_sec', s['pending_oldest_age_sec'])} "
        f"Pending: {s['pending_total']}  "
        f"Oldest: {s['pending_oldest_age_sec']}s"
        f"  Decided queue: {s['decided_total']}  "
        f"Active policies: {s['active_total']}"
        ""
        "*SLO latencies (p95, 7d)*"
        f"  {_badge('proposal_to_decision_p95_sec', s['proposal_to_decision_p95_sec'])} "
        f"Proposal→Decision: {s['proposal_to_decision_p95_sec']:.0f}s"
        f"  {_badge('approve_to_apply_p95_sec', s['approve_to_apply_p95_sec'])} "
        f"Approve→Apply: {s['approve_to_apply_p95_sec']:.0f}s"
        f"  {_badge('reconcile_last_success_age_sec', s['reconcile_last_success_age_sec'])} "
        f"Reconcile last success: {s['reconcile_last_success_age_sec']}s ago"
        ""
        "*Today's counters*"
        f"  {_badge('revoke_today_total', s['revoke_today_total'])} "
        f"Revokes: {s['revoke_today_total']}"
        f"  {_badge('flip_today_total', s['flip_today_total'])} "
        f"Flips: {s['flip_today_total']}"
        f"  {_badge('confirm_expired_today_total', s['confirm_expired_today_total'])} "
        f"Confirm expired: {s['confirm_expired_today_total']}"
        f"  {_badge('callback_denied_today_total', s['callback_denied_today_total'])} "
        f"Callback denied: {s['callback_denied_today_total']}"
    ]
    return "\n".join(lines)


def publish_sre_digest() -> bool:
    """Collect metrics and push digest to notify:telegram stream."""
    return _notify(build_sre_digest())


if __name__ == "__main__":
    print(build_sre_digest())
    publish_sre_digest()
