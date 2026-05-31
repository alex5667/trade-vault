from __future__ import annotations

import os
import urllib.parse
import urllib.request


def send_telegram(text: str, *, chat_id: str | None = None, token: str | None = None) -> bool:
    """Minimal Telegram sender via Bot API.

    Uses env:
      TELEGRAM_BOT_TOKEN
      TELEGRAM_CHAT_ID
    """
    tok = token or os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    cid = chat_id or os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not tok or not cid or not text:
        return False

    url = f"https://api.telegram.org/bot{tok}/sendMessage"
    data = {
        "chat_id": cid,
        "text": text,
        "disable_web_page_preview": True,
    }
    body = urllib.parse.urlencode(data).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=body, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            _ = resp.read()
        return True
    except Exception:
        return False

