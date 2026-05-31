from __future__ import annotations

import os
import sys
import time
from typing import Iterable

import redis
import requests

from tools.recommend_trailing_from_redis import build_trailing_report_markdown_from_env


def _to_bool(v) -> bool:
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _get_redis() -> redis.Redis:
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    return redis.from_url(redis_url, decode_responses=True)


_TG_MAX_LEN = 4000  # Telegram API hard limit is 4096; leave margin for safety


def _split_message(text: str, max_len: int = _TG_MAX_LEN) -> list[str]:
    """Split text into chunks ≤ max_len chars, breaking on newline boundaries."""
    if len(text) <= max_len:
        return [text]
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for line in text.splitlines(keepends=True):
        if current_len + len(line) > max_len and current:
            chunks.append("".join(current))
            current = []
            current_len = 0
        if len(line) > max_len:
            # single too-long line: hard split
            chunks.append(line[:max_len])
            line = line[max_len:]
        current.append(line)
        current_len += len(line)
    if current:
        chunks.append("".join(current))
    return chunks or [text[:max_len]]


def _send_telegram(text: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_ids_raw = os.getenv("TELEGRAM_CHAT_IDS") or os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_ids_raw:
        print("TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID(S) не заданы, пропускаю отправку", file=sys.stderr)
        return

    chat_ids = [cid.strip() for cid in chat_ids_raw.split(",") if cid.strip()]
    if not chat_ids:
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    chunks = _split_message(text)

    for chat_id in chat_ids:
        for i, chunk in enumerate(chunks, 1):
            payload = {
                "chat_id": chat_id,
                "text": chunk,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }
            try:
                resp = requests.post(url, json=payload, timeout=15)
                if resp.status_code >= 400:
                    print(
                        f"Telegram send error for chat {chat_id} chunk {i}/{len(chunks)}: "
                        f"{resp.status_code} {resp.text}",
                        file=sys.stderr,
                    )
            except Exception as e:
                print(f"Telegram send exception for chat {chat_id} chunk {i}/{len(chunks)}: {e}", file=sys.stderr)


def _should_send_telegram(r: redis.Redis, telegram_interval_sec: int, redis_lock_key: str) -> bool:
    """
    Redis-based throttle: returns True only if enough time has elapsed
    since the last successful Telegram send. Uses a simple Redis key with TTL.

    Also prevents duplicate fires on container restart: if the key still
    exists (TTL > 0), we are still within the cooldown window → skip.
    """
    last_sent_raw = r.get(redis_lock_key)
    if last_sent_raw is not None:
        try:
            last_sent = float(last_sent_raw)
            elapsed = time.time() - last_sent
            if elapsed < telegram_interval_sec:
                remaining = int(telegram_interval_sec - elapsed)
                print(
                    f"[trailing-autotune-telegram] throttle: last sent {int(elapsed)}s ago, "
                    f"next in {remaining}s, skipping",
                    file=sys.stderr,
                )
                return False
        except Exception:
            pass
    return True


def _record_telegram_sent(r: redis.Redis, redis_lock_key: str, telegram_interval_sec: int) -> None:
    """Store the timestamp of the last successful send so throttle survives container restarts."""
    try:
        r.set(redis_lock_key, str(time.time()), ex=telegram_interval_sec * 2)
    except Exception as e:
        print(f"[trailing-autotune-telegram] failed to record send timestamp: {e}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    if not _to_bool(os.getenv("TRAILING_AUTOTUNE_TELEGRAM_ENABLED", "1")):
        print("TRAILING_AUTOTUNE_TELEGRAM_ENABLED!=1, выходим", file=sys.stderr)
        return 0

    # Calibration (data refresh) interval – how often we re-read trades and recompute
    calibration_interval_sec = int(os.getenv("TRAILING_AUTOTUNE_INTERVAL_SEC", "3600"))

    # Telegram send interval – independent throttle: how often we actually send to Telegram
    # Default: same as calibration_interval_sec (backwards-compat), but can be set independently
    telegram_interval_sec = int(
        os.getenv("TRAILING_AUTOTUNE_TELEGRAM_INTERVAL_SEC", str(calibration_interval_sec))
    )

    # Window for trades filtering (TRAILING_AUTOTUNE_WINDOW_HOURS → from_ts_ms passed via ENV)
    window_hours_env = os.getenv("TRAILING_AUTOTUNE_WINDOW_HOURS")
    if window_hours_env:
        try:
            window_hours = float(window_hours_env)
        except Exception:
            window_hours = None
    else:
        window_hours = None

    # Report title suffix for identification in Telegram
    report_title_suffix = os.getenv("TRAILING_AUTOTUNE_REPORT_TITLE_SUFFIX", "")

    # Redis key for Telegram send dedup / throttle (per title suffix to avoid cross-instance collision)
    safe_suffix = report_title_suffix.replace(" ", "_").replace("(", "").replace(")", "")
    redis_lock_key = f"trailing_autotune_telegram:last_sent:{safe_suffix or 'default'}"

    r = _get_redis()

    print(
        f"[trailing-autotune-telegram] старт, calibration_interval={calibration_interval_sec}s, "
        f"telegram_interval={telegram_interval_sec}s, window_hours={window_hours}, "
        f"suffix='{report_title_suffix}'",
        file=sys.stderr,
    )

    while True:
        start_ts = int(time.time())
        try:
            # Apply window_hours filter by temporarily injecting FROM_TS via ENV override
            # build_trailing_report_markdown_from_env reads TRAILING_AUTOTUNE_FROM_TS from env
            if window_hours is not None:
                from_ts_ms = int((time.time() - window_hours * 3600) * 1000)
                os.environ["TRAILING_AUTOTUNE_FROM_TS"] = str(from_ts_ms)
            else:
                os.environ.pop("TRAILING_AUTOTUNE_FROM_TS", None)

            md = build_trailing_report_markdown_from_env(r)

            # Inject report title suffix if present and not already there
            if report_title_suffix and md and md.strip():
                # The standard title line is: ### 🔧 Trailing calibration: {source}
                # We append the suffix so it becomes: ### 🔧 Trailing calibration: CryptoOrderFlow (24h Fast)
                first_line_end = md.find("\n")
                if first_line_end > 0:
                    first_line = md[:first_line_end]
                    rest = md[first_line_end:]
                    if report_title_suffix not in first_line:
                        md = first_line.rstrip() + f" {report_title_suffix}" + rest

            if md and md.strip():
                if _should_send_telegram(r, telegram_interval_sec, redis_lock_key):
                    _send_telegram(md)
                    _record_telegram_sent(r, redis_lock_key, telegram_interval_sec)
                    print("[trailing-autotune-telegram] отчёт отправлен в Telegram", file=sys.stderr)
                # else: throttled, already logged inside _should_send_telegram
            else:
                print("[trailing-autotune-telegram] пустой отчёт, ничего не отправлено", file=sys.stderr)
        except Exception as e:
            print(f"[trailing-autotune-telegram] ошибка: {e}", file=sys.stderr)

        elapsed = int(time.time()) - start_ts
        sleep_for = max(5, calibration_interval_sec - elapsed)
        print(f"[trailing-autotune-telegram] следующая калибровка через {sleep_for}s", file=sys.stderr)
        time.sleep(sleep_for)

    # теоретически недостижимо
    # return 0


if __name__ == "__main__":
    raise SystemExit(main())
