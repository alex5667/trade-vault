"""utils.telegram_notify — Thin Telegram sender for batch reports.

Environment variables:
    TELEGRAM_BOT_TOKEN — bot API token.
    TELEGRAM_CHAT_ID   — target chat / channel ID.

Notes:
    - Telegram ``sendMessage`` limit is 4 096 chars; long texts are chunked.
    - Fail-open: all exceptions are swallowed and ``False`` is returned
      so callers are never disrupted by Telegram outages.
"""
from __future__ import annotations

import logging
import os

try:
    import requests as _requests  # optional dependency
    _REQUESTS_AVAILABLE = True
except ImportError:  # pragma: no cover
    _requests = None  # type: ignore[assignment]
    _REQUESTS_AVAILABLE = False

_log = logging.getLogger(__name__)

_TELEGRAM_LIMIT = 4_096
_CHUNK_SIZE = 3_900  # leave headroom for Telegram's limit


def _chunks(text: str, limit: int = _CHUNK_SIZE) -> list[str]:
    """Split *text* into chunks of at most *limit* characters."""
    s = str(text or "")
    if not s:
        return []
    return [s[i : i + limit] for i in range(0, len(s), limit)]


def send_telegram_message(
    *,
    text: str,
    parse_mode: str = "Markdown",
    disable_web_page_preview: bool = True,
    bot_token: str | None = None,
    chat_id: str | None = None,
) -> bool:
    """Send *text* to a Telegram chat, splitting into chunks if needed.

    Args:
        text:                     Message body.
        parse_mode:               Telegram parse mode (``"Markdown"`` / ``"HTML"``).
        disable_web_page_preview: Suppress link previews.
        bot_token:                Override ``TELEGRAM_BOT_TOKEN`` env var.
        chat_id:                  Override ``TELEGRAM_CHAT_ID`` env var.

    Returns:
        ``True`` if at least one chunk was accepted by Telegram, else ``False``.
    """
    if not _REQUESTS_AVAILABLE:
        _log.warning("send_telegram_message: 'requests' package not installed")
        return False

    tok = bot_token or os.getenv("TELEGRAM_BOT_TOKEN", "")
    cid = chat_id or os.getenv("TELEGRAM_CHAT_ID", "")
    if not tok or not cid:
        return False

    try:
        from utils.telegram_analyzer import TelegramMessageAnalyzer
        text = TelegramMessageAnalyzer.analyze(text)
    except Exception:
        # Fail safe if analysis fails
        pass

    url = f"https://api.telegram.org/bot{tok}/sendMessage"
    ok_any = False
    try:
        for part in _chunks(text):
            resp = _requests.post(
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
    except Exception:  # noqa: BLE001 — fail-open by design
        return False
    return ok_any
