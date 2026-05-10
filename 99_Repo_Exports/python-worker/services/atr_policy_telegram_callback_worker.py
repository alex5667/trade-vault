from __future__ import annotations

import json
import os
import socket
import time
from typing import Any

import redis

from services.atr_change_control_service import approve_change, get_change, pause_change, request_rollback
from services.atr_change_control_telegram_surface import publish_ack as change_publish_ack
from services.atr_change_control_telegram_surface import publish_change_to_telegram
from services.atr_policy_confirm_tokens import consume_confirm_token, issue_confirm_token
from services.atr_policy_guardrails import arm_cooldown, evaluate_guardrails
from services.atr_policy_operator_bootstrap_service import run_once as run_operator_bootstrap_once
from services.atr_policy_telegram_guardrail_ops import (
    publish_guardrail_block,
    publish_guardrail_warning,
)

# ...
# (lines truncated for brevity in replacement, but I will provide the full block)
from services.atr_policy_telegram_ops import publish_policy_ack_to_telegram, publish_policy_proposal_to_telegram
from services.atr_policy_telegram_pack_service import (
    build_pack_buttons,
    publish_ops_pack,
    resolve_active_ref,
)
from services.atr_policy_telegram_summary_service import (
    _notify,
    publish_summary_menu,
    report_active,
    report_best,
    report_pending,
    report_revoked_today,
    report_worst,
    summary_menu_buttons,
)
from services.atr_policy_workflow import proposal_key, record_decision
from services.atr_rollback_control_service import approve_rollback, get_rollback
import contextlib
from core.redis_keys import RedisStreams as RS

_redis_instance: redis.Redis | None = None

def _redis() -> redis.Redis:
    global _redis_instance
    if _redis_instance is not None:
        return _redis_instance

    url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    # Robust reconnection loop with backoff
    attempts = 0
    max_attempts = 3
    while attempts < max_attempts:
        try:
            _redis_instance = redis.Redis.from_url(url, decode_responses=True)
            _redis_instance.ping()
            return _redis_instance
        except (redis.exceptions.ConnectionError, socket.gaierror) as e:
            attempts += 1
            wait = min(2**attempts, 5)
            print(f"⚠️ Redis connection attempt {attempts} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)

    # Fallback
    return redis.Redis.from_url(url, decode_responses=True)


def _allowed_user_ids() -> set[str]:
    raw = (os.getenv("ATR_POLICY_TELEGRAM_ALLOWED_USER_IDS", "") or "").strip()
    return {x.strip() for x in raw.split(",") if x.strip()}


def _allowed_usernames() -> set[str]:
    raw = (os.getenv("ATR_POLICY_TELEGRAM_ALLOWED_USERNAMES", "") or "").strip()
    return {x.strip().lower() for x in raw.split(",") if x.strip()}


def _allowed_chat_ids() -> set[str]:
    raw = (os.getenv("ATR_POLICY_TELEGRAM_ALLOWED_CHAT_IDS", "") or "").strip()
    return {x.strip() for x in raw.split(",") if x.strip()}


def _is_allowed(evt: dict[str, Any]) -> bool:
    uid = (evt.get("user_id") or "")
    uname = (evt.get("username") or "").lower()
    chat_id = (evt.get("chat_id") or "")
    allow_ids = _allowed_user_ids()
    allow_names = _allowed_usernames()
    allow_chats = _allowed_chat_ids()

    if allow_chats and chat_id not in allow_chats:
        return False
    if allow_ids and uid in allow_ids:
        return True
    if allow_names and uname in allow_names:
        return True
    return not allow_ids and not allow_names


def _parse_callback(cb: str) -> tuple[str, str]:
    # atrpol:approve:<proposal_id>
    parts = (cb or "").split(":")
    if len(parts) != 3 or parts[0] != "atrpol":
        return ("", "")
    return (parts[1].lower(), parts[2])


def _parse_summary_callback(cb: str) -> str:
    # atrsum:pending
    parts = (cb or "").split(":")
    if len(parts) != 2 or parts[0] != "atrsum":
        return ""
    return parts[1].lower()


def _parse_pack_callback(cb: str):
    # atrpack:refresh
    # atrpack:approve:<proposal_id>
    # atrpack:reject:<proposal_id>
    # atrpack:pending:<proposal_id>
    # atrpack:active:<ref>
    # atrpack:revoke:<ref>
    # atrpack:confirm:<token>
    parts = (cb or "").split(":")
    if len(parts) < 2 or parts[0] != "atrpack":
        return ("", "")
    if len(parts) == 2:
        return (parts[1].lower(), "")
    return (parts[1].lower(), parts[2])

def _parse_change_callback(cb: str):
    # atrchange:approve:<change_id>
    # atrchange:reject:<change_id>
    # atrchange:pause:<change_id>
    # atrchange:rollback:<change_id>
    # atrchange:artifacts:<change_id>
    parts = (cb or "").split(":")
    if len(parts) < 2 or parts[0] != "atrchange":
        return ("", "")
    if len(parts) == 2:
        return (parts[1].lower(), "")
    return (parts[1].lower(), parts[2])

def _parse_rollback_callback(cb: str):
    # atr_rollback:approve:<rollback_id>
    # atr_rollback:pause:<rollback_id>
    # atr_rollback:manifest:<rollback_id>
    # atr_rollback:postcert:<rollback_id>
    # atr_rollback:evidence:<rollback_id>
    parts = (cb or "").split(":")
    if len(parts) < 2 or parts[0] != "atr_rollback":
        return ("", "")
    if len(parts) == 2:
        return (parts[1].lower(), "")
    return (parts[1].lower(), parts[2])

def _parse_incident_callback(cb: str):
    # incident|ack_incident|<incident_id>
    # incident|apply_runbook|<incident_id>
    # incident|emergency_freeze|<incident_id>
    parts = (cb or "").split("|")
    if len(parts) < 2 or parts[0] != "incident":
        return ("", "")
    if len(parts) == 2:
        return (parts[1].lower(), "")
    return (parts[1].lower(), parts[2])

def _parse_postmortem_callback(cb: str):
    # postmortem|action|<pm_id>
    parts = (cb or "").split("|")
    if len(parts) < 2 or parts[0] != "postmortem":
        return ("", "")
    if len(parts) == 2:
        return (parts[1].lower(), "")
    return (parts[1].lower(), parts[2])

def _parse_golive_callback(cb: str):
    # golive:action:<pkg_id>:<arg>
    parts = (cb or "").split(":")
    if len(parts) < 2 or parts[0] != "golive":
        return ("", "")
    if len(parts) == 2:
        return (parts[1].lower(), "")
    # Returns (action, pkg_id, optional_arg)
    return (parts[1].lower(), parts[2], parts[3] if len(parts) > 3 else "")

def _dedup_key(proposal_id: str, action: str, user_id: str) -> str:
    return f"dedup:atr_policy_tg:{proposal_id}:{action}:{user_id}"


def handle_event(evt: dict[str, Any]) -> bool:
    if not _is_allowed(evt):
        try:
            from services.atr_promotion_policy_metrics import atr_policy_tg_callback_denied_total
            atr_policy_tg_callback_denied_total.inc()
        except Exception:
            pass
        # SLO-3: daily Redis counter for SRE exporter
        try:
            r_ctr = _redis()
            r_ctr.incr("atr_policy:callback_denied_today_total")
            r_ctr.expire("atr_policy:callback_denied_today_total", 86400)
        except Exception:
            pass
        publish_policy_ack_to_telegram(
            proposal_id=(evt.get("callback") or ""),
            action="DENIED",
            actor=str(evt.get("username") or evt.get("user_id") or "unknown"),
            note="not_authorized",
        )
        return False

    cb = (evt.get("callback") or "")

    # Handle incident callbacks
    inc_action, inc_arg = _parse_incident_callback(cb)
    if inc_action:
        from services.atr_incident_telegram_surface import handle_incident_callback
        actor = str(evt.get("username") or evt.get("user_id") or "unknown")
        payload = {"action": inc_action, "incident_id": inc_arg}
        result = handle_incident_callback(payload, actor)
        publish_policy_ack_to_telegram(
            proposal_id=inc_arg,
            action=inc_action.upper(),
            actor=actor,
            note="ok" if result.get("ok") else result.get("message", "failed")
        )
        return result.get("ok", False)

    # Handle postmortem callbacks
    pm_action, pm_arg = _parse_postmortem_callback(cb)
    if pm_action:
        from services.atr_postmortem_telegram_surface import handle_postmortem_callback
        actor = str(evt.get("username") or evt.get("user_id") or "unknown")
        payload = {"action": pm_action, "postmortem_id": pm_arg}
        result = handle_postmortem_callback(payload, actor)
        publish_policy_ack_to_telegram(
            proposal_id=pm_arg,
            action=pm_action.upper(),
            actor=actor,
            note="ok" if result.get("ok") else result.get("message", "failed")
        )
        return result.get("ok", False)

    # Handle golive callbacks
    gl_action, gl_pkg_id, gl_arg = _parse_golive_callback(cb)
    if gl_action:
        from services.atr_go_live_telegram_surface import handle_golive_callback
        # We need to adapt handle_golive_callback to take split parts if needed,
        # or just pass the full callback_query
        # Here we pass a synthetic payload
        synthetic_query = {
            "data": cb,
            "from": {"username": str(evt.get("username") or evt.get("user_id") or "unknown")}
        }
        result_msg = handle_golive_callback(synthetic_query)
        # Note: In a real bot, we would edit the message here.
        # The surface usually returns the new message body and markup.
        # For simplicity in this worker, we log the action ack.
        publish_policy_ack_to_telegram(
            proposal_id=gl_pkg_id,
            action=f"GOLIVE_{gl_action.upper()}",
            actor=str(evt.get("username") or evt.get("user_id") or "unknown"),
            note="action_received"
        )
        # We also need a way to push the update back to Telegram.
        # Assuming publish_policy_ack_to_telegram does basic notification.
        return True

    # Handle atr_rollback callbacks
    rb_action, rb_arg = _parse_rollback_callback(cb)
    if rb_action:
        actor = str(evt.get("username") or evt.get("user_id") or "unknown")
        user_id = (evt.get("user_id") or "")
        r = _redis()
        # Dedup check
        if not r.set(_dedup_key(rb_arg, rb_action, user_id), "1", nx=True, ex=60):
            return True

        if rb_action in ("manifest", "postcert", "evidence"):
            rb = get_rollback(rb_arg)
            if not rb:
                return rollback_publish_ack(rb_arg, rb_action.upper(), actor, "rollback_not_found")
            publish_rollback_to_telegram(rb)
            return True

        ok = False
        note = "via_telegram"
        if rb_action == "approve":
            ok = approve_rollback(rb_arg, actor)
        elif rb_action == "pause":
            # For phase 6.3 pause can be logged
            ok = True

        rollback_publish_ack(rb_arg, rb_action.upper(), actor, "ok" if ok else "failed")

        rb = get_rollback(rb_arg)
        if rb:
            publish_rollback_to_telegram(rb)
        return ok

    # Handle atrchange callbacks
    cg_action, cg_arg = _parse_change_callback(cb)
    if cg_action:
        actor = str(evt.get("username") or evt.get("user_id") or "unknown")
        user_id = (evt.get("user_id") or "")
        r = _redis()
        # Dedup check
        if not r.set(_dedup_key(cg_arg, cg_action, user_id), "1", nx=True, ex=60):
            return True

        if cg_action == "artifacts":
            # show artifacts path
            chg = get_change(cg_arg)
            if not chg:
                return change_publish_ack(cg_arg, "SHOW_ARTIFACTS", actor, "change_not_found")
            # For now just reshow the change with empty artifacts note
            publish_change_to_telegram(chg)
            return True

        ok = False
        note = "via_telegram"
        if cg_action == "approve":
            ok = approve_change(cg_arg, actor, note=note)
        elif cg_action == "pause":
            ok = pause_change(cg_arg, actor, note=note)
        elif cg_action == "rollback":
            # Just push into rollback state
            ok = request_rollback(cg_arg, {"reason": "manual tg rollback"}, actor=actor)
        elif cg_action == "reject":
            # Not strictly mapped in phase 6 snippet but good to have
            # Can just pause or add reject state later
            ok = pause_change(cg_arg, actor, note="rejected_by_operator")

        change_publish_ack(cg_arg, cg_action.upper(), actor, "ok" if ok else "failed")

        # Publish refreshed view
        chg = get_change(cg_arg)
        if chg:
            publish_change_to_telegram(chg)
        return ok

    pack_action, pack_arg = _parse_pack_callback(cb)
    if pack_action:
        try:
            from services.atr_promotion_policy_metrics import atr_policy_tg_pack_action_total
            atr_policy_tg_pack_action_total.labels(action=pack_action).inc()
        except Exception:
            pass

        actor = str(evt.get("username") or evt.get("user_id") or "unknown")
        if pack_action in {"show", "refresh"}:
            if pack_action == "refresh":
                try:
                    from services.atr_promotion_policy_metrics import atr_policy_tg_pack_refresh_total
                    atr_policy_tg_pack_refresh_total.inc()
                except Exception:
                    pass
            return publish_ops_pack()

        # ── confirm route: second-tap executes a previously issued token ──
        if pack_action == "confirm":
            tok = consume_confirm_token(pack_arg)
            if not tok:
                try:
                    from prometheus_client import Counter
                    Counter(
                        "atr_policy_confirm_token_total",
                        "ATR policy confirm token lifecycle",
                        ["status"],
                    ).labels(status="expired").inc()
                except Exception:
                    pass
                # SLO-7: daily Redis counter for SRE exporter
                try:
                    r_ctr = _redis()
                    r_ctr.incr("atr_policy:confirm_expired_today_total")
                    r_ctr.expire("atr_policy:confirm_expired_today_total", 86400)
                except Exception:
                    pass
                return publish_policy_ack_to_telegram(
                    proposal_id="confirm",
                    action="CONFIRM",
                    actor=actor,
                    note="token_expired",
                )
            if (tok.get("actor") or "") != actor:
                try:
                    from prometheus_client import Counter
                    Counter(
                        "atr_policy_confirm_token_total",
                        "ATR policy confirm token lifecycle",
                        ["status"],
                    ).labels(status="mismatch").inc()
                except Exception:
                    pass
                return publish_policy_ack_to_telegram(
                    proposal_id="confirm",
                    action="CONFIRM",
                    actor=actor,
                    note="actor_mismatch",
                )
            payload = tok.get("payload") if isinstance(tok.get("payload"), dict) else {}
            confirmed_action = (tok.get("action") or "").upper()
            target = (tok.get("target") or "")
            if confirmed_action in {"APPROVE", "REJECT", "REVOKE"}:
                ok = record_decision(
                    target,
                    action=confirmed_action,
                    actor=actor,
                    note="via_telegram_guardrail_confirm",
                )
                if ok and payload:
                    with contextlib.suppress(Exception):
                        arm_cooldown(payload, actor=actor, action=confirmed_action)
                publish_policy_ack_to_telegram(
                    proposal_id=target,
                    action=confirmed_action,
                    actor=actor,
                    note="confirmed_ok" if ok else "confirmed_failed",
                )
                publish_ops_pack()
                return ok
            return False

        if pack_action == "pending":
            raw = _redis().get(proposal_key(pack_arg))
            if not raw:
                return publish_policy_ack_to_telegram(proposal_id=pack_arg, action="SHOW", actor=actor, note="proposal_not_found")
            return publish_policy_proposal_to_telegram(json.loads(raw))

        if pack_action == "approve":
            raw = _redis().get(proposal_key(pack_arg))
            if not raw:
                return publish_policy_ack_to_telegram(
                    proposal_id=pack_arg, action="APPROVE", actor=actor, note="proposal_not_found"
                )
            obj = json.loads(raw)
            g = evaluate_guardrails(obj=obj, action="APPROVE", is_active=False)
            if g["risk_class"] == "BLOCK":
                publish_guardrail_block(action="APPROVE", target=pack_arg, guard=g)
                return publish_policy_ack_to_telegram(
                    proposal_id=pack_arg, action="APPROVE", actor=actor, note=g["reason_code"]
                )
            if g["require_confirm"]:
                token = issue_confirm_token(
                    actor=actor, action="APPROVE", target=pack_arg, payload=obj
                )
                publish_guardrail_warning(
                    action="APPROVE",
                    target=pack_arg,
                    guard=g,
                    confirm_callback=f"atrpack:confirm:{token}",
                )
                return True
            # SAFE path — direct approve
            ok = record_decision(pack_arg, action="APPROVE", actor=actor, note="via_telegram_pack")
            if ok:
                with contextlib.suppress(Exception):
                    arm_cooldown(obj, actor=actor, action="APPROVE")
            publish_policy_ack_to_telegram(
                proposal_id=pack_arg, action="APPROVE", actor=actor, note="ok" if ok else "failed"
            )
            publish_ops_pack()
            return ok

        if pack_action == "reject":
            raw = _redis().get(proposal_key(pack_arg))
            obj = json.loads(raw) if raw else {}
            ok = record_decision(pack_arg, action="REJECT", actor=actor, note="via_telegram_pack")
            if ok and obj:
                with contextlib.suppress(Exception):
                    arm_cooldown(obj, actor=actor, action="REJECT")
            publish_policy_ack_to_telegram(
                proposal_id=pack_arg, action="REJECT", actor=actor, note="ok" if ok else "failed"
            )
            publish_ops_pack()
            return ok

        if pack_action == "revoke":
            try:
                from services.atr_promotion_policy_metrics import atr_policy_tg_pack_revoke_total
                atr_policy_tg_pack_revoke_total.inc()
            except Exception:
                pass
            active_key = resolve_active_ref(pack_arg)
            if not active_key:
                return publish_policy_ack_to_telegram(
                    proposal_id=pack_arg, action="REVOKE", actor=actor, note="active_ref_not_found"
                )
            raw = _redis().get(active_key)
            if not raw:
                return publish_policy_ack_to_telegram(
                    proposal_id=pack_arg, action="REVOKE", actor=actor, note="active_policy_not_found"
                )
            obj = json.loads(raw)
            pid = (obj.get("proposal_id") or "")
            if not pid:
                return publish_policy_ack_to_telegram(
                    proposal_id=pack_arg, action="REVOKE", actor=actor, note="proposal_id_missing"
                )
            g = evaluate_guardrails(obj=obj, action="REVOKE", is_active=True)
            if g["risk_class"] == "BLOCK":
                publish_guardrail_block(action="REVOKE", target=pid, guard=g)
                return publish_policy_ack_to_telegram(
                    proposal_id=pid, action="REVOKE", actor=actor, note=g["reason_code"]
                )
            if g["require_confirm"]:
                token = issue_confirm_token(
                    actor=actor, action="REVOKE", target=pid, payload=obj
                )
                publish_guardrail_warning(
                    action="REVOKE",
                    target=pid,
                    guard=g,
                    confirm_callback=f"atrpack:confirm:{token}",
                )
                return True
            # SAFE path — direct revoke
            ok = record_decision(pid, action="REVOKE", actor=actor, note="via_telegram_pack")
            if ok:
                with contextlib.suppress(Exception):
                    arm_cooldown(obj, actor=actor, action="REVOKE")
            publish_policy_ack_to_telegram(
                proposal_id=pid, action="REVOKE", actor=actor, note="ok" if ok else "failed"
            )
            publish_ops_pack()
            return ok
        if pack_action == "active":
            active_key = resolve_active_ref(pack_arg)
            if not active_key:
                return publish_policy_ack_to_telegram(proposal_id=pack_arg, action="ACTIVE", actor=actor, note="active_ref_not_found")
            raw = _redis().get(active_key)
            if not raw:
                return publish_policy_ack_to_telegram(proposal_id=pack_arg, action="ACTIVE", actor=actor, note="active_policy_not_found")
            obj = json.loads(raw)
            text = (
                f"Active ATR Policy\n"
                f"Source: {obj.get('source','')}\n"
                f"Symbol: {obj.get('symbol','')}\n"
                f"Scenario: {obj.get('scenario','')}\n"
                f"Regime: {obj.get('regime','')}\n"
                f"Bucket: {obj.get('risk_horizon_bucket','')}\n"
                f"Stop/TTL: {obj.get('stop_ttl_mode','')}\n"
                f"Trailing: {obj.get('trailing_mode','')}\n"
                f"Reason: {obj.get('reason_code','')}"
            )
            return _notify(text, buttons=build_pack_buttons())

    summary_action = _parse_summary_callback(cb)
    if summary_action:
        if summary_action == "menu":
            return publish_summary_menu()
        if summary_action == "pending":
            return _notify(report_pending(), buttons=summary_menu_buttons())
        if summary_action == "active":
            return _notify(report_active(), buttons=summary_menu_buttons())
        if summary_action == "revoked":
            return _notify(report_revoked_today(), buttons=summary_menu_buttons())
        if summary_action == "best":
            return _notify(report_best(), buttons=summary_menu_buttons())
        if summary_action == "worst":
            return _notify(report_worst(), buttons=summary_menu_buttons())
        return False

    action, proposal_id = _parse_callback(cb)
    if not action or not proposal_id:
        return False

    r = _redis()
    user_id = (evt.get("user_id") or "")
    if not r.set(_dedup_key(proposal_id, action, user_id), "1", nx=True, ex=60):
        try:
            from services.atr_promotion_policy_metrics import atr_policy_tg_callback_duplicate_total
            atr_policy_tg_callback_duplicate_total.inc()
        except Exception:
            pass
        return True

    try:
        from services.atr_promotion_policy_metrics import atr_policy_tg_callback_total
        atr_policy_tg_callback_total.labels(action=action).inc()
    except Exception:
        pass

    actor = str(evt.get("username") or evt.get("user_id") or "unknown")

    if action == "show":
        raw = r.get(proposal_key(proposal_id))
        if not raw:
            publish_policy_ack_to_telegram(proposal_id=proposal_id, action="SHOW", actor=actor, note="proposal_not_found")
            return False
        proposal = json.loads(raw)
        publish_policy_proposal_to_telegram(proposal)
        return True

    action_map = {
        "approve": "APPROVE",
        "reject": "REJECT",
        "revoke": "REVOKE",
    }
    mapped = action_map.get(action)
    if not mapped:
        return False

    ok = record_decision(
        proposal_id,
        action=mapped,
        actor=actor,
        note="via_telegram",
    )
    publish_policy_ack_to_telegram(
        proposal_id=proposal_id,
        action=mapped,
        actor=actor,
        note="ok" if ok else "failed",
    )
    return ok


def run_forever() -> None:
    r = _redis()
    stream = os.getenv("ATR_POLICY_TELEGRAM_CALLBACK_STREAM", RS.BOT_CALLBACKS)
    group = os.getenv("ATR_POLICY_TELEGRAM_CALLBACK_GROUP", "atr_policy_ops")
    consumer = os.getenv("ATR_POLICY_TELEGRAM_CALLBACK_CONSUMER", f"cb-{int(time.time())}")
    block_ms = int(os.getenv("ATR_POLICY_TELEGRAM_CALLBACK_BLOCK_MS", "5000") or 5000)

    try:
        if os.getenv("ATR_POLICY_OPERATOR_BOOTSTRAP_ENABLE", "1") == "1":
            run_operator_bootstrap_once()
    except Exception:
        pass

    # Ensure we actually have a connection before trying to create group
    try:
        r.ping()
    except Exception as e:
        print(f"❌ Failed to ping Redis at startup: {e}")

    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception as e:
        # BusyGroup is expected if already exists
        if "BUSYGROUP" not in str(e).upper():
            print(f"⚠️ xgroup_create error: {e}")

    while True:
        rows = r.xreadgroup(group, consumer, {stream: ">"}, count=20, block=block_ms)
        if not rows:
            continue
        for _, entries in rows:
            for msg_id, fields in entries:
                try:
                    evt = dict(fields)
                    handle_event(evt)
                finally:
                    with contextlib.suppress(Exception):
                        r.xack(stream, group, msg_id)


if __name__ == "__main__":
    run_forever()
