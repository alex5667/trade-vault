"""
Alertmanager → Telegram webhook service.

Converts Alertmanager webhook payloads (POST /alert) into Telegram messages
via the Bot API. Supports optional forum topic routing (TELEGRAM_MESSAGE_THREAD_ID).

Required env vars:
    TELEGRAM_BOT_TOKEN   — Telegram Bot API token (from @BotFather)
    TELEGRAM_CHAT_ID     — Target chat/group/channel ID

Optional env vars:
    TELEGRAM_MESSAGE_THREAD_ID — Forum topic ID (for supergroups with topics enabled)
    WEBHOOK_PORT         — Listening port (default: 8081)
    LOG_LEVEL            — Logging level (default: INFO)

Endpoints:
    POST /alert   — Alertmanager webhook receiver
    GET  /healthz — Health check (always returns {"ok": true})
"""

import os
import time
import json
import logging
from typing import Any, Dict, List, Optional, Tuple
from collections import deque

import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LOG_LEVEL: str = (os.getenv("LOG_LEVEL") or "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
log = logging.getLogger("alertmanager-telegram-webhook")

BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
DEFAULT_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
DEFAULT_THREAD_ID: str = os.getenv("TELEGRAM_MESSAGE_THREAD_ID", "")

# Public base URLs (so links in Telegram work from phone/remote)
RUNBOOKS_BASE_URL = (os.getenv("RUNBOOKS_BASE_URL") or "").strip()
GRAFANA_BASE_URL = (os.getenv("GRAFANA_BASE_URL") or "").strip()
ALERTMANAGER_BASE_URL = (os.getenv("ALERTMANAGER_BASE_URL") or "").strip()

# Global dry-run: do not send messages to Telegram, return preview instead.
GLOBAL_DRY_RUN = (os.getenv("TELEGRAM_DRY_RUN") or "0").strip() in ("1", "true", "yes")

# Anti-spam controls
DEDUP_TTL_S = int(os.getenv("ALERT_DEDUPE_TTL_S", "180"))
RATE_LIMIT_PER_MIN = int(os.getenv("ALERT_RATE_LIMIT_PER_MIN", "30"))

# Optional routing map (JSON). Precedence: component > team > severity > default > env defaults
_ROUTING_RAW = os.getenv("TELEGRAM_ROUTING_JSON", "").strip()
try:
    ROUTING = json.loads(_ROUTING_RAW) if _ROUTING_RAW else {}
    if not isinstance(ROUTING, dict):
        ROUTING = {}
except Exception:
    ROUTING = {}

WEBHOOK_PORT: int = int(os.getenv("WEBHOOK_PORT", "8081"))

_dedupe_cache: Dict[str, float] = {}
_rate_window = deque()  # timestamps (seconds)

# Telegram hard limit is 4096 chars; we leave headroom for safety
_TG_MAX_MSG_LEN: int = 3900
# Max number of individual alert lines to include before truncating
_MAX_ALERT_ITEMS: int = 8
# Max summary length per alert (chars)
_MAX_SUMMARY_LEN: int = 180

app = FastAPI(title="alertmanager-telegram-webhook")


# ---------------------------------------------------------------------------
# Routing and Rate Limiting
# ---------------------------------------------------------------------------

def _prune_dedupe(now_s: float) -> None:
    if not _dedupe_cache:
        return
    # opportunistic prune (keep it cheap)
    if len(_dedupe_cache) > 5000:
        cutoff = now_s - float(DEDUP_TTL_S)
        for k, ts in list(_dedupe_cache.items())[:2000]:
            if ts < cutoff:
                _dedupe_cache.pop(k, None)

def _rate_limited(now_s: float) -> bool:
    # sliding window 60s
    cutoff = now_s - 60.0
    while _rate_window and _rate_window[0] < cutoff:
        _rate_window.popleft()
    if len(_rate_window) >= RATE_LIMIT_PER_MIN:
        return True
    _rate_window.append(now_s)
    return False

def _select_chat(common_labels: Dict[str, Any]) -> Tuple[str, str]:
    sev = str(common_labels.get("severity") or "").strip() or "unknown"
    team = str(common_labels.get("team") or "").strip()
    comp = str(common_labels.get("component") or "").strip()

    def pick(key: str) -> Optional[Dict[str, Any]]:
        v = ROUTING.get(key)
        return v if isinstance(v, dict) else None

    cfg = None
    if comp:
        cfg = pick(f"component:{comp}")
    if cfg is None and team:
        cfg = pick(f"team:{team}")
    if cfg is None:
        cfg = pick(f"severity:{sev}")
    if cfg is None:
        cfg = pick("default")

    chat_id = str((cfg or {}).get("chat_id") or DEFAULT_CHAT_ID)
    thread_id = str((cfg or {}).get("thread_id") or DEFAULT_THREAD_ID)
    return chat_id, thread_id

def _join_url(base: str, path: str) -> str:
    base = (base or "").strip()
    path = (path or "").strip()
    if not base or not path:
        return ""
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if not path.startswith("/"):
        path = "/" + path
    return base.rstrip("/") + path


# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------

def _fmt_alert_line(a: Dict[str, Any]) -> str:
    """Format a single Alertmanager alert dict into a one-line string.

    Keeps the line compact enough for Telegram while including the
    most actionable fields: status, name, severity, job, instance, summary.
    """
    labels: Dict[str, Any] = a.get("labels") or {}
    ann: Dict[str, Any] = a.get("annotations") or {}
    status: str = a.get("status") or "unknown"
    name: str = labels.get("alertname", "unknown_alert")
    sev: str = labels.get("severity", "unknown")
    inst: str = labels.get("instance", "")
    job: str = labels.get("job", "")
    # Prefer 'summary' annotation, fall back to 'description'
    summary: str = ann.get("summary") or ann.get("description") or ""
    if len(summary) > _MAX_SUMMARY_LEN:
        summary = summary[: _MAX_SUMMARY_LEN - 3] + "..."

    parts: List[str] = [f"[{status}] {name}", f"sev={sev}"]
    if job:
        parts.append(f"job={job}")
    if inst:
        parts.append(f"inst={inst}")
    line: str = " | ".join(parts)
    if summary:
        line += f"\n  - {summary}"
    return line


def _extract_links(common_annotations: Dict[str, Any]) -> List[str]:
    links: List[str] = []
    runbook_url = str(common_annotations.get("runbook_url") or "").strip()
    runbook_path = str(common_annotations.get("runbook_path") or "").strip()
    dash_url = str(common_annotations.get("dashboard") or common_annotations.get("grafana") or "").strip()
    dash_path = str(common_annotations.get("dashboard_path") or "").strip()

    if runbook_url:
        links.append(runbook_url)
    elif runbook_path and RUNBOOKS_BASE_URL:
        u = _join_url(RUNBOOKS_BASE_URL, runbook_path)
        if u:
            links.append(u)
    if dash_url:
        links.append(dash_url)
    elif dash_path and GRAFANA_BASE_URL:
        u = _join_url(GRAFANA_BASE_URL, dash_path)
        if u:
            links.append(u)
    if ALERTMANAGER_BASE_URL:
        links.append(_join_url(ALERTMANAGER_BASE_URL, "/#/silences/new"))
    return links

def _build_message(payload: Dict[str, Any]) -> Tuple[str, List[str]]:
    """Build a Telegram-compatible text message from an Alertmanager webhook payload.

    Extracts groupLabels, commonLabels, commonAnnotations, and per-alert details.
    Includes runbook/dashboard links when present. Truncates at _TG_MAX_MSG_LEN.
    """
    status: str = payload.get("status", "unknown")
    alerts: List[Dict[str, Any]] = payload.get("alerts") or []
    group_labels: Dict[str, Any] = payload.get("groupLabels") or {}
    common_labels: Dict[str, Any] = payload.get("commonLabels") or {}
    common_annotations: Dict[str, Any] = payload.get("commonAnnotations") or {}

    title: str = (
        common_labels.get("alertname")
        or group_labels.get("alertname")
        or "alerts"
    )
    sev: str = (
        common_labels.get("severity")
        or group_labels.get("severity")
        or "unknown"
    )
    team: str = common_labels.get("team") or group_labels.get("team") or ""
    comp: str = common_labels.get("component") or group_labels.get("component") or ""
    summary: str = (
        common_annotations.get("summary")
        or common_annotations.get("description")
        or ""
    )

    # Header line: title | severity | status | alert count
    header_parts = [f"{title}", f"sev={sev}", f"status={status}", f"n={len(alerts)}"]
    if team:
        header_parts.append(f"team={team}")
    if comp:
        header_parts.append(f"component={comp}")
    header: str = " | ".join(header_parts)
    if summary:
        header += f"\n{summary}"

    lines: List[str] = [header]

    # Per-alert detail lines (capped to avoid huge messages)
    for a in alerts[:_MAX_ALERT_ITEMS]:
        lines.append(_fmt_alert_line(a))
    if len(alerts) > _MAX_ALERT_ITEMS:
        lines.append(f"... and {len(alerts) - _MAX_ALERT_ITEMS} more")

    # Inline runbook text (short), shown verbatim in message when annotation 'runbook'
    # is present. Capped at 700 chars to stay well within Telegram 4096-char limit.
    rb: str = str(common_annotations.get("runbook") or "").strip()
    if rb:
        rb_short = rb[:700]
        if len(rb) > 700:
            rb_short += "..."
        lines.append("Runbook:")
        lines.append(rb_short)

    # Provide ready-to-use silence matchers (copy/paste)
    # Example: alertname="EdgeStackTrainFailed", team="trade", component="edge_stack"
    if ALERTMANAGER_BASE_URL:
        m_parts = []
        if title:
            m_parts.append(f'alertname="{title}"')
        if team:
            m_parts.append(f'team="{team}"')
        if comp:
            m_parts.append(f'component="{comp}"')
        if m_parts:
            lines.append("Silence matchers:")
            lines.append(", ".join(m_parts))

    # Include runbook/dashboard links when present in common annotations
    links = _extract_links(common_annotations)

    if links:
        lines.append("Links:")
        for l in links[:4]:
            lines.append(f"- {l}")

    # Append Unix timestamp for quick correlation with logs
    lines.append(f"ts={int(time.time())}")

    msg: str = "\n".join(lines)
    # Hard-truncate to Telegram message limit, keeping a safety buffer
    return msg[:_TG_MAX_MSG_LEN], links


# ---------------------------------------------------------------------------
# Telegram delivery
# ---------------------------------------------------------------------------

def _send_telegram(text: str, chat_id: str, thread_id: str) -> None:
    """Send a text message to the configured Telegram chat.

    Logs a warning if credentials are missing. Raises nothing — errors are
    logged and the webhook still returns 200 so Alertmanager doesn't retry.
    """
    if not BOT_TOKEN or not chat_id:
        log.warning(
            "TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set; skipping send"
        )
        return

    if GLOBAL_DRY_RUN:
        log.info("GLOBAL_DRY_RUN=1: skip telegram send")
        return

    url: str = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    # Optional: route to a specific forum topic within a supergroup
    if thread_id:
        try:
            data["message_thread_id"] = int(thread_id)
        except ValueError:
            log.warning("message_thread_id is not an integer: %s", thread_id)

    try:
        r = requests.post(url, json=data, timeout=10)
        if r.status_code >= 300:
            log.error(
                "telegram send failed: status=%s body=%s",
                r.status_code,
                r.text[:400],
            )
        else:
            log.info("telegram message sent ok (status=%s)", r.status_code)
    except requests.RequestException as exc:
        # Network errors are logged but not re-raised; Alertmanager will retry
        log.error("telegram request error: %s", exc)


# ---------------------------------------------------------------------------
# FastAPI endpoints
# ---------------------------------------------------------------------------

@app.post("/alert")
async def alert(request: Request) -> JSONResponse:
    """Alertmanager webhook receiver.

    Accepts Alertmanager webhook JSON payload, builds a Telegram message,
    and sends it. Returns 200 even if Telegram send fails so Alertmanager
    does not retry indefinitely.
    """
    try:
        payload = await request.json()
        if not isinstance(payload, dict):
            log.warning("received non-dict payload; ignoring")
            return JSONResponse(
                {"ok": False, "error": "payload is not dict"}, status_code=400
            )

        req_dry_run = (request.query_params.get("dry_run") or "").strip() in ("1", "true", "yes")
        effective_dry_run = GLOBAL_DRY_RUN or req_dry_run

        now_s = time.time()
        if _rate_limited(now_s):
            return JSONResponse({"ok": True, "skipped": "rate_limited"})

        log.debug("received alert payload: status=%s n=%d",
                  payload.get("status"), len(payload.get("alerts") or []))

        common_labels = payload.get("commonLabels") or {}
        group_key = str(payload.get("groupKey") or "")
        status = str(payload.get("status") or "unknown")
        
        # Dedupe key: groupKey is stable for grouped alerts; fallback to alertname+labels
        dedupe_key = group_key or (
            f"{common_labels.get('alertname','alerts')}|{common_labels.get('severity','unknown')}|"
            f"{common_labels.get('team','')}|{common_labels.get('component','')}|{status}"
        )
        _prune_dedupe(now_s)
        last_ts = _dedupe_cache.get(dedupe_key, 0.0)
        if now_s - last_ts < float(DEDUP_TTL_S):
            return JSONResponse({"ok": True, "skipped": "dedup"})
        _dedupe_cache[dedupe_key] = now_s

        msg, links = _build_message(payload)
        chat_id, thread_id = _select_chat(common_labels)
        if not effective_dry_run:
            _send_telegram(msg, chat_id=chat_id, thread_id=thread_id)
        return JSONResponse({"ok": True, "chat_id": chat_id, "thread_id": thread_id, "dry_run": effective_dry_run, "links": links, "message_preview": msg[:800]})
    except Exception as exc:
        log.exception("webhook error processing alert")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)


@app.get("/healthz")
async def healthz() -> JSONResponse:
    """Liveness probe endpoint — always returns 200."""
    return JSONResponse({"ok": True})


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("Starting alertmanager-telegram-webhook on port %d", WEBHOOK_PORT)
    uvicorn.run(app, host="0.0.0.0", port=WEBHOOK_PORT, log_level=LOG_LEVEL.lower())
