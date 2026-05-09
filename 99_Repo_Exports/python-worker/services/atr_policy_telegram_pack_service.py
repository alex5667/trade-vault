from __future__ import annotations

import hashlib
import json
import os
from typing import Any

import redis

from core.redis_keys import STREAM_RETENTION
from core.redis_keys import RedisStreams as RS
from services.atr_policy_guardrails import evaluate_guardrails
from services.atr_policy_operator_bootstrap_service import run_once as run_operator_bootstrap_once
import contextlib


def _redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)


def _chat_id() -> str:
    return (os.getenv("ATR_POLICY_TELEGRAM_CHAT_ID", "") or "")


def _top_pending() -> int:
    try:
        return int(os.getenv("ATR_POLICY_TELEGRAM_PACK_PENDING_N", "3") or 3)
    except Exception:
        return 3


def _top_active() -> int:
    try:
        return int(os.getenv("ATR_POLICY_TELEGRAM_PACK_ACTIVE_N", "3") or 3)
    except Exception:
        return 3


def _notify(text: str, buttons: list[list[dict[str, str]]] | None = None) -> bool:
    payload: dict[str, Any] = {"text": text}
    if buttons:
        payload["buttons"] = json.dumps(buttons, ensure_ascii=False)
    cid = _chat_id()
    if cid:
        payload["chat_id"] = cid
    try:
        _redis().xadd(RS.NOTIFY_TELEGRAM, payload, maxlen=STREAM_RETENTION[RS.NOTIFY_TELEGRAM], approximate=True)
        return True
    except Exception:
        return False


def _scan_active_keys() -> list[str]:
    r = _redis()
    cur = 0
    out: list[str] = []
    while True:
        cur, keys = r.scan(cur, match="cfg:atr_policy:active:*", count=10000)
        out.extend(keys)
        if cur == 0:
            break
    return sorted(out)


def _active_ref(key: str) -> str:
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]


def _active_ref_key(ref: str) -> str:
    return f"cfg:atr_policy:active_ref:{ref}"


def _store_active_refs(keys: list[str]) -> None:
    r = _redis()
    ttl = int(os.getenv("ATR_POLICY_TELEGRAM_PACK_REF_TTL_SEC", "86400") or 86400)
    for k in keys:
        r.set(_active_ref_key(_active_ref(k)), k, ex=ttl)


def _pending_items() -> list[dict[str, Any]]:
    r = _redis()
    out: list[dict[str, Any]] = []
    ids = sorted(list(r.smembers("queue:atr_policy:pending") or []))
    for pid in ids[: _top_pending()]:
        raw = r.get(f"cfg:proposals:atr_policy:{pid}")
        if not raw:
            continue
        obj = json.loads(raw)
        if (obj.get("status") or "") != "SUBMITTED":
            continue
        out.append(obj)
    return out


def _active_items() -> list[tuple[str, dict[str, Any]]]:
    r = _redis()
    rows: list[tuple[str, dict[str, Any]]] = []
    keys = _scan_active_keys()[: _top_active()]
    _store_active_refs(keys)
    for key in keys:
        raw = r.get(key)
        if not raw:
            continue
        rows.append((key, json.loads(raw)))
    return rows


def build_pack_text() -> str:
    pending = _pending_items()
    active = _active_items()
    r = _redis()
    pending_total = len(list(r.smembers("queue:atr_policy:pending") or []))
    active_total = len(_scan_active_keys())

    lines = [
        "ATR Policy Ops Pack",
        f"Pending total: {pending_total}",
        f"Active total: {active_total}",
        "",
        "Pending proposals:",
    ]
    if not pending:
        lines.append("- none")
    else:
        for p in pending:
            pid = (p.get("proposal_id") or "")[:8]
            g = evaluate_guardrails(obj=p, action="APPROVE", is_active=False)
            badge = "🟢" if g["risk_class"] == "SAFE" else "🟡" if g["risk_class"] == "WARN" else "🔴"
            lines.append(
                f"- {badge} {p.get('symbol','')} | {p.get('scenario','')} | {p.get('regime','')} | "
                f"{p.get('risk_horizon_bucket','')} | stop={p.get('stop_ttl_mode','')} "
                f"trail={p.get('trailing_mode','')} | id={pid}"
            )

    lines += ["", "Active policies:"]
    if not active:
        lines.append("- none")
    else:
        for key, obj in active:
            g = evaluate_guardrails(obj=obj, action="REVOKE", is_active=True)
            badge = "🟢" if g["risk_class"] == "SAFE" else "🟡" if g["risk_class"] == "WARN" else "🔴"
            lines.append(
                f"- {badge} {obj.get('symbol','')} | {obj.get('scenario','')} | {obj.get('regime','')} | "
                f"{obj.get('risk_horizon_bucket','')} | stop={obj.get('stop_ttl_mode','')} "
                f"trail={obj.get('trailing_mode','')}"
            )

    return "\n".join(lines)


def build_pack_buttons() -> list[list[dict[str, str]]]:
    pending = _pending_items()
    active = _active_items()

    buttons: list[list[dict[str, str]]] = [
        [
            {"text": "🔄 Refresh", "callback": "atrpack:refresh"},
            {"text": "📋 Menu", "callback": "atrsum:menu"},
        ]
    ]

    for p in pending:
        pid = (p.get("proposal_id") or "")
        short = pid[:8]
        buttons.append([
            {"text": f"✅ A {p.get('symbol','')} {short}", "callback": f"atrpack:approve:{pid}"},
            {"text": f"❌ R {short}", "callback": f"atrpack:reject:{pid}"},
        ])
        buttons.append([
            {"text": f"🔎 Show {short}", "callback": f"atrpack:pending:{pid}"}
        ])

    for key, obj in active:
        ref = _active_ref(key)
        buttons.append([
            {"text": f"↩️ Revoke {obj.get('symbol','')} {ref[:6]}", "callback": f"atrpack:revoke:{ref}"},
            {"text": f"🟢 Active {ref[:6]}", "callback": f"atrpack:active:{ref}"},
        ])

    return buttons


def publish_ops_pack() -> bool:
    try:
        from services.atr_promotion_policy_metrics import atr_policy_tg_pack_publish_total
        atr_policy_tg_pack_publish_total.inc()
    except Exception:
        pass
    return _notify(build_pack_text(), build_pack_buttons())


def resolve_active_ref(ref: str) -> str:
    r = _redis()
    key = (r.get(_active_ref_key(ref)) or "")
    if key:
        return key
    # Phase 4.4: on-demand operator UX restore
    with contextlib.suppress(Exception):
        run_operator_bootstrap_once()
    return (r.get(_active_ref_key(ref)) or "")

if __name__ == "__main__":
    print(publish_ops_pack())
