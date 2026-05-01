# -*- coding: utf-8 -*-
from __future__ import annotations
"""core.telegram_client

Minimal Telegram sender (no extra deps).

We intentionally keep it tiny:
  - sendMessage only (text)
  - avoids requests dependency
  - unit-testable via dependency injection

Env expected by services:
  - TELEGRAM_BOT_TOKEN
  - TELEGRAM_CHAT_ID
"""


from dataclasses import dataclass
from typing import Optional, Dict, Any
import json
import urllib.request
import urllib.parse


@dataclass
class TelegramConfig:
    token: str
    chat_id: str
    api_base: str = "https://api.telegram.org"
    timeout_sec: int = 10


def _truncate(s: str, limit: int) -> str:
    if not s:
        return ""
    if len(s) <= limit:
        return s
    return s[: max(0, limit - 20)] + "\n... (truncated)"


def send_message(
    *,
    cfg: TelegramConfig,
    text: str,
    parse_mode: str = "",
    disable_web_preview: bool = True,
) -> bool:
    """Send a Telegram message.

    Telegram hard limit is 4096 chars for message text.
    We trim defensively.
    """
    try:
        msg = _truncate(str(text or ""), 3900)
        if not msg:
            return True
        url = f"{cfg.api_base}/bot{cfg.token}/sendMessage"
        payload: Dict[str, Any] = {
            "chat_id": cfg.chat_id,
            "text": msg,
            "disable_web_page_preview": bool(disable_web_preview),
        }
        pm = (parse_mode or "").strip()
        if pm:
            payload["parse_mode"] = pm
        data = urllib.parse.urlencode(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=int(cfg.timeout_sec)) as resp:
            raw = resp.read().decode("utf-8", "ignore")
            # best-effort: parse ok
            try:
                d = json.loads(raw)
                return bool(d.get("ok", False))
            except Exception:
                # if HTTP 200, assume ok
                return True
    except Exception:
        return False


__all__ = ["TelegramConfig", "send_message"]
