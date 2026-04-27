# -*- coding: utf-8 -*-
"""
Tiny Telegram sender (no external deps).
Env:
  TG_BOT_TOKEN
  TG_CHAT_ID
"""
from __future__ import annotations
import json
import os
import urllib.request
import urllib.parse
from typing import Optional

def send_telegram(text: str, *, token: Optional[str] = None, chat_id: Optional[str] = None, parse_mode: str = "Markdown") -> bool:
    token = token or os.getenv("TG_BOT_TOKEN", "")
    chat_id = chat_id or os.getenv("TG_CHAT_ID", "")
    if not token or not chat_id:
        # try lowercase fallbacks
        token = token or os.getenv("telegram_bot_token", "")
        chat_id = chat_id or os.getenv("telegram_chat_id", "")
        
    if not token or not chat_id:
        return False
        
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    payload = urllib.parse.urlencode(data).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=payload, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
            j = json.loads(raw)
            return bool(j.get("ok", False))
    except Exception:
        return False
