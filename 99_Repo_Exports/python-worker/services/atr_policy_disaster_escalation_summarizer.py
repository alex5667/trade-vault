"""ATR Policy Disaster Escalation Summarizer — Phase 3.8 (Disaster Layer).

Reads stream:atr_policy:escalations and stream:atr_policy:rollback_results,
builds a structured Telegram digest summarizing:
  - kill_switch activations
  - rollback events (ok / failed)
  - callback watchdog alerts
  - flip storm events
  - active corruption events

Meant to be called from of_timers_worker (e.g. nightly or hourly)
OR triggered directly when critical events accumulate.

Does NOT auto-revoke policy. Safe to call from hot-path adjacent timers.

ENV:
  ATR_POLICY_DISASTER_SUMMARIZER_LOOKBACK_SEC   default 86400 (24h)
  ATR_POLICY_DISASTER_SUMMARIZER_MAX_EVENTS      default 50
  TELEGRAM_BOT_TOKEN
  TELEGRAM_OPS_CHAT_ID
  REDIS_URL
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import redis
from prometheus_client import Counter

logger = logging.getLogger(__name__)

# ── Prometheus ────────────────────────────────────────────────────────────────

c_summarizer_total = Counter(
    "atr_policy_disaster_summarizer_total",
    "Disaster escalation summarizer runs",
    ["status"],
)

# ── Constants ──────────────────────────────────────────────────────────────────

SEVERITY_ICONS = {
    "CRITICAL": "🔴",
    "ERROR": "🔴",
    "WARN": "🟡",
    "WARNING": "🟡",
    "OK": "🟢",
    "INFO": "ℹ️",
}

EVENT_ICONS: Dict[str, str] = {
    "KILL_SWITCH": "🚫",
    "ROLLBACK": "↩️",
    "FLIP_STORM": "⚡",
    "CORRUPTION": "💥",
    "CALLBACK": "📵",
    "RECONCILE": "⏳",
    "PARTIAL_LOSS": "💧",
    "CHAOS_DRILL": "🔬",
    "MIRROR": "🪞",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _rconn() -> redis.Redis:
    return redis.Redis.from_url(
        os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
        decode_responses=True,
    )


def _lookback_sec() -> int:
    try:
        return int(os.getenv("ATR_POLICY_DISASTER_SUMMARIZER_LOOKBACK_SEC", "86400") or 86400)
    except Exception:
        return 86400


def _max_events() -> int:
    try:
        return int(os.getenv("ATR_POLICY_DISASTER_SUMMARIZER_MAX_EVENTS", "50") or 50)
    except Exception:
        return 50


def _icon_for_event(event_str: str) -> str:
    for keyword, icon in EVENT_ICONS.items():
        if keyword in event_str.upper():
            return icon
    return "ℹ️"


def _read_stream_since(
    r: redis.Redis, stream: str, since_ms: int, max_count: int = 200
) -> List[Dict[str, Any]]:
    """Read stream entries newer than since_ms (epoch_ms)."""
    try:
        entries = r.xrange(stream, min=f"{since_ms}-0", count=max_count)
        return [{"stream_id": sid, **fields} for sid, fields in entries]
    except Exception as exc:
        logger.warning("escalation_summarizer: stream read failed %s: %s", stream, exc)
        return []


# ── Summary builder ───────────────────────────────────────────────────────────

def build_summary(r: Optional[redis.Redis] = None) -> Dict[str, Any]:
    """
    Collect escalation events from the last lookback window.

    Returns:
      events: list of raw event dicts
      counts: {event_class: count}
      kill_switch_active: list of cohorts under kill_switch
      rollback_ok_count: int
      rollback_fail_count: int
      callback_warn_count: int
      callback_critical_count: int
      flip_storm_count: int
      corruption_count: int
    """
    r = r or _rconn()
    lookback_ms = int((time.time() - _lookback_sec()) * 1000)

    esc_events = _read_stream_since(r, "stream:atr_policy:escalations", lookback_ms, _max_events())
    rb_events = _read_stream_since(r, "stream:atr_policy:rollback_results", lookback_ms, _max_events())

    all_events = esc_events + rb_events

    counts: Dict[str, int] = {}
    rollback_ok = 0
    rollback_fail = 0
    callback_warn = 0
    callback_critical = 0
    flip_storm = 0
    corruption = 0

    for ev in all_events:
        event_type = str(ev.get("event", "UNKNOWN"))
        counts[event_type] = counts.get(event_type, 0) + 1

        if "ROLLBACK" in event_type.upper():
            if str(ev.get("rollback_ok", "")) == "True":
                rollback_ok += 1
            else:
                rollback_fail += 1

        if "CALLBACK_WARN" in event_type.upper():
            callback_warn += 1
        if "CALLBACK_CRITICAL" in event_type.upper():
            callback_critical += 1

        if "FLIP_STORM" in event_type.upper():
            flip_storm += 1

        if "CORRUPT" in event_type.upper():
            corruption += 1

    # Active kill_switches
    ks_keys = []
    try:
        cur = 0
        while True:
            cur, keys = r.scan(cur, match="cfg:atr_policy:kill_switch:*", count=10000)
            for k in keys:
                raw = r.get(k)
                if raw:
                    try:
                        obj = json.loads(raw)
                        if obj.get("enabled"):
                            ks_keys.append(k)
                    except Exception:
                        pass
            if cur == 0:
                break
    except Exception:
        pass

    return {
        "events": all_events,
        "counts": counts,
        "kill_switch_active": ks_keys,
        "rollback_ok_count": rollback_ok,
        "rollback_fail_count": rollback_fail,
        "callback_warn_count": callback_warn,
        "callback_critical_count": callback_critical,
        "flip_storm_count": flip_storm,
        "corruption_count": corruption,
        "lookback_sec": _lookback_sec(),
        "total_events": len(all_events),
    }


def format_telegram_message(summary: Dict[str, Any]) -> str:
    """Format the summary as a Telegram HTML message."""
    now_str = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())
    lb_h = summary["lookback_sec"] // 3600

    lines: List[str] = [
        f"<b>🛡️ ATR Policy Disaster SLO — {now_str}</b>",
        f"Window: last {lb_h}h | Total events: {summary['total_events']}",
        "",
    ]

    # Kill switches
    ks = summary.get("kill_switch_active", [])
    if ks:
        lines.append(f"🚫 <b>Kill switches ACTIVE: {len(ks)}</b>")
        for k in ks[:5]:
            lines.append(f"  • <code>{k}</code>")
        if len(ks) > 5:
            lines.append(f"  … and {len(ks) - 5} more")
    else:
        lines.append("🟢 No active kill switches")

    lines.append("")

    # Rollback summary
    rb_ok = summary["rollback_ok_count"]
    rb_fail = summary["rollback_fail_count"]
    if rb_ok + rb_fail > 0:
        rb_icon = "✅" if rb_fail == 0 else "⚠️"
        lines.append(f"{rb_icon} Rollbacks: ok={rb_ok} fail={rb_fail}")

    # Callback watchdog
    cb_warn = summary["callback_warn_count"]
    cb_crit = summary["callback_critical_count"]
    if cb_crit > 0:
        lines.append(f"📵 <b>Callback CRITICAL: {cb_crit}</b>")
    elif cb_warn > 0:
        lines.append(f"📵 Callback WARN: {cb_warn}")

    # Flip storm
    fs = summary["flip_storm_count"]
    if fs > 0:
        lines.append(f"⚡ Flip storm events: {fs}")

    # Corruption
    corr = summary["corruption_count"]
    if corr > 0:
        lines.append(f"💥 Active key corruptions: {corr}")

    # Event type breakdown
    counts = summary.get("counts", {})
    if counts:
        lines.append("")
        lines.append("<b>Event breakdown:</b>")
        for event_type, count in sorted(counts.items(), key=lambda x: -x[1])[:8]:
            icon = _icon_for_event(event_type)
            lines.append(f"  {icon} {event_type}: {count}")

    # Operational note (never auto-revoke)
    lines.append("")
    lines.append("ℹ️ <i>Disaster layer is advisory — no auto-revoke. Manual clear required for kill_switch.</i>")

    return "\n".join(lines)


async def send_telegram_digest(
    r: Optional[redis.Redis] = None,
) -> Dict[str, Any]:
    """
    Build and send the disaster escalation digest to the Telegram ops channel.
    Returns summary dict.
    """
    import aiohttp

    r = r or _rconn()
    summary = build_summary(r)
    text = format_telegram_message(summary)

    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_OPS_CHAT_ID", "")

    if not token or not chat_id:
        logger.warning("escalation_summarizer: TELEGRAM_BOT_TOKEN or TELEGRAM_OPS_CHAT_ID not set")
        c_summarizer_total.labels(status="no_credentials").inc()
        return {**summary, "sent": False, "reason": "NO_CREDENTIALS"}

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_notification": (
            summary["total_events"] == 0 and not summary["kill_switch_active"]
        ),
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                body = await resp.json()
                if resp.status == 200:
                    c_summarizer_total.labels(status="ok").inc()
                    logger.info("escalation_summarizer: digest sent — events=%d", summary["total_events"])
                    return {**summary, "sent": True, "telegram_message_id": body.get("result", {}).get("message_id")}
                else:
                    c_summarizer_total.labels(status="telegram_error").inc()
                    logger.error("escalation_summarizer: telegram error %d: %s", resp.status, body)
                    return {**summary, "sent": False, "telegram_status": resp.status}
    except Exception as exc:
        c_summarizer_total.labels(status="exception").inc()
        logger.exception("escalation_summarizer: send failed: %s", exc)
        return {**summary, "sent": False, "error": str(exc)}


def run_sync_digest() -> Dict[str, Any]:
    """Synchronous wrapper (for of_timers_worker integration)."""
    import asyncio
    return asyncio.run(send_telegram_digest())


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    result = asyncio.run(send_telegram_digest())
    print(json.dumps({k: v for k, v in result.items() if k != "events"}, indent=2, ensure_ascii=False))
