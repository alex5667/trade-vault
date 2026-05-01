from __future__ import annotations

import os
import urllib.request
import urllib.parse
from dataclasses import dataclass
from typing import Optional

@dataclass
class TelegramClient:
    """
    Minimal Telegram sender (stdlib only).
    Fail-open: caller should ignore exceptions; this module also guards as much as possible.
    Env:
      TELEGRAM_BOT_TOKEN  (or legacy BOT_TOKEN)
      TELEGRAM_CHAT_ID    (or legacy CHAT_ID)
      TELEGRAM_PARSE_MODE (optional, default HTML)
    """
    token: str
    chat_id: str
    timeout_s: float = 4.0

    @staticmethod
    def from_env() -> Optional["TelegramClient"]:
        # Backward compatible env names:
        #   - preferred: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID
        #   - legacy (used by some older services): BOT_TOKEN / CHAT_ID
        tok = (os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
        cid = (os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID") or "").strip()
        if not tok or not cid:
            return None
        return TelegramClient(token=tok, chat_id=cid)

    def send_text(self, text: str) -> bool:
        try:
            if not text:
                return False
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            parse_mode = (os.getenv("TELEGRAM_PARSE_MODE") or "HTML").strip() or "HTML"
            data = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
            }
            body = urllib.parse.urlencode(data).encode("utf-8")
            req = urllib.request.Request(url, data=body, method="POST")
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                _ = resp.read()
            return True
        except Exception:
            return False
