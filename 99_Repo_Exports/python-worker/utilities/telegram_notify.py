from __future__ import annotations

"""utils.telegram_notify

Small Telegram sender for batch reports.

Env:
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID

Notes:
  - Telegram sendMessage text limit is 4096 chars. We chunk safely.
  - Fail-open: exceptions are swallowed and returned as False.
"""


import os


def _chunks(text: str, limit: int = 3900) -> list[str]:
    s = (text or "")
    if not s:
        return []
    out: list[str] = []
    while s:
        out.append(s[:limit])
        s = s[limit:]
    return out


def send_telegram_message(
    *,
    text: str,
    parse_mode: str = "Markdown",
    disable_web_page_preview: bool = True,
    bot_token: str | None = None,
    chat_id: str | None = None,
) -> bool:
    tok = bot_token or os.getenv("TELEGRAM_BOT_TOKEN", "")
    cid = chat_id or os.getenv("TELEGRAM_CHAT_ID", "")
    if not tok or not cid:
        return False
    try:
        import requests

        url = f"https://api.telegram.org/bot{tok}/sendMessage"
        ok_any = False
        for part in _chunks(text, limit=3900):
            resp = requests.post(
                url,
                timeout=10,
                json={
                    "chat_id": cid,
                    "text": part,
                    "parse_mode": parse_mode,
                    "disable_web_page_preview": bool(disable_web_page_preview),
                },
            )
            ok_any = ok_any or bool(resp.ok)
        return ok_any
    except Exception:
        return False
