from __future__ import annotations

import json
import os
import urllib.request
import urllib.parse
from typing import Optional

from .sem_dedup_reporter import TelegramSink


class TelegramHttpSink(TelegramSink):
    """
    Minimal Telegram sender via Bot API.
    Env:
      TG_BOT_TOKEN
      TG_CHAT_ID
      (optional) TG_PARSE_MODE=HTML|MarkdownV2
      (optional) TG_DISABLE_WEB_PAGE_PREVIEW=1
    """
    def __init__(self, *, token: Optional[str] = None, chat_id: Optional[str] = None, timeout_s: float = 3.5) -> None:
        self._token = token or os.getenv("TG_BOT_TOKEN", "")
        self._chat_id = chat_id or os.getenv("TG_CHAT_ID", "")
        self._parse_mode = os.getenv("TG_PARSE_MODE", "")
        self._disable_preview = os.getenv("TG_DISABLE_WEB_PAGE_PREVIEW", "1").strip() in {"1", "true", "True"}
        self._timeout_s = float(timeout_s)

    def send(self, text: str) -> bool:
        if not self._token or not self._chat_id:
            return False
        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        data = {
            "chat_id": self._chat_id,
            "text": text,
            "disable_web_page_preview": bool(self._disable_preview),
        }
        if self._parse_mode:
            data["parse_mode"] = self._parse_mode
        body = urllib.parse.urlencode(data).encode("utf-8")
        req = urllib.request.Request(url, data=body, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self._timeout_s) as resp:
                _ = resp.read()
            return True
        except Exception:
            return False
