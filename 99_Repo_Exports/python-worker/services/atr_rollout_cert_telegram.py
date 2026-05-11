import json
import logging
import os
from typing import Any

import redis
from core.redis_keys import RedisStreams as RS

logger = logging.getLogger("atr_rollout_cert_telegram")

def _redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

def _ops_chat_id() -> str:
    return (os.getenv("ATR_POLICY_TELEGRAM_CHAT_ID", "") or "")

def notify_telegram(text: str, parse_mode: str = "HTML"):
    payload = {"text": text, "parse_mode": parse_mode}
    chat_id = _ops_chat_id()
    if chat_id:
        payload["chat_id"] = chat_id
    try:
        _redis().xadd(RS.NOTIFY_TELEGRAM, payload, maxlen=10000, approximate=True)
    except Exception as e:
        logger.error(f"Failed to xadd to notify:telegram: {e}")

    """Safely format a float as a percentage with 1 decimal place."""
    if val is None:  # type: ignore
        return "N/A"  # type: ignore
    return f"{str(round(val * 100, 1))}%"  # type: ignore
  # type: ignore
def format_bps(val: float) -> str:
    """Safely format a float as bps with 1 decimal place."""
    if val is None:
        return "N/A"
    prefix = "+" if val > 0 else ""
    return f"{prefix}{str(round(val, 1))}"

def format_checks(checks: dict[str, bool]) -> str:
    """Format boolean checks dictionary into standard list."""
    if not checks:
        return "- no checks"
    return "\n".join([f"- {k}={'✅' if v else '❌'}" for k, v in checks.items()])

def send_cert_start_message(change_id: str, stage: str, thresholds: dict[str, Any]):
    """Notify that a new stage certification has started."""
    text = (
        f"⏳ <b>ATR Rollout Certification Started</b>\n\n"
        f"<b>Change</b>: {change_id}\n"
        f"<b>Stage</b>: {stage}\n"
        f"<b>Thresholds</b>:\n"
        f"<pre>{json.dumps(thresholds, indent=2)}</pre>"
    )
    notify_telegram(text, parse_mode="HTML")

def send_cert_outcome_message(cert_data: dict[str, Any]):
    """Notify certification outcome (PASSED / FAILED / PENDING) with summary."""
    change_id = cert_data.get("change_id", "unknown")
    stage = cert_data.get("rollout_stage", "unknown")
    status = cert_data.get("status", "unknown").upper()
    next_action = cert_data.get("next_action", "WAIT_FOR_DATA")
    checks = cert_data.get("checks", {})
    summary = cert_data.get("summary", {})

    # Emoji based on status
    emoji = "✅" if status == "PASSED" else "❌" if status == "FAILED" else "⏳"

    n_trades = summary.get('n_trades', 0)
    avg_pnl_bps = format_bps(summary.get('avg_pnl_bps'))
    avg_slip = format_bps(summary.get('avg_slippage_bps'))
    stop_rate = format_percentage(summary.get('stop_rate'))  # type: ignore
    tp1_rate = format_percentage(summary.get('tp1_rate'))  # type: ignore
  # type: ignore
    text = (
        f"{emoji} <b>ATR Rollout Certification</b>\n\n"
        f"<b>Change</b>: {change_id}\n"
        f"<b>Stage</b>: {stage}\n"
        f"<b>Status</b>: {status}\n\n"
        f"<b>Checks</b>:\n{format_checks(checks)}\n\n"
        f"<b>Summary</b>:\n"
        f"- n_trades={n_trades}\n"
        f"- avg_pnl_bps={avg_pnl_bps}\n"
        f"- avg_slippage_bps={avg_slip}\n"
        f"- stop_rate={stop_rate}\n"
        f"- tp1_rate={tp1_rate}\n\n"
        f"<b>Next action</b>: {next_action}"
    )
    notify_telegram(text, parse_mode="HTML")

def send_stop_condition_message(change_id: str, stage: str, reason_code: str, evidence: dict[str, Any]):
    """Notify if a hard stop-condition has been hit."""
    text = (
        f"🚨 <b>ATR Rollout Stop-Condition Hit</b> 🚨\n\n"
        f"<b>Change</b>: {change_id}\n"
        f"<b>Stage</b>: {stage}\n"
        f"<b>Reason</b>: {reason_code}\n\n"
        f"<b>Evidence</b>:\n"
        f"<pre>{json.dumps(evidence, indent=2)}</pre>\n\n"
        f"⚠️ <i>Action requires manual rollback or auto-revert triggered!</i>"
    )
    notify_telegram(text, parse_mode="HTML")

def send_closeout_pack_ready(change_id: str, final_status: str, total_trades: int):
    """Notify about completion and closeout pack ready."""
    emoji = "🏆" if final_status == "COMPLETED" else "🚫"
    text = (
        f"{emoji} <b>ATR Rollout Finalized</b>\n\n"
        f"<b>Change</b>: {change_id}\n"
        f"<b>Final Status</b>: {final_status}\n"
        f"<b>Total Evaluated Trades</b>: {total_trades}\n\n"
        f"Evidences packed to <code>atr_rollout_closeout_packs</code> table."
    )
    notify_telegram(text, parse_mode="HTML")
