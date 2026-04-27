#!/usr/bin/env python3
"""Antigravity Task Watcher — host-side daemon.

Polls Redis ``antigravity:inbox`` every few seconds and writes pending tasks
to ``tasks/inbox.md`` so Antigravity can pick them up in VS Code.

Usage (from scanner_infra root):
    source .env
    python3 scripts/antigravity_task_watcher.py \\
        --redis-url "redis://:${GO_GATEWAY_REDIS_PASS}@127.0.0.1:6379/0" \\
        --output tasks/inbox.md \\
        --poll-interval 5

Sends desktop notification via ``notify-send`` on Linux when a new task arrives.
Auto-opens tasks/inbox.md in VS Code when a new task arrives.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Watch Redis antigravity:inbox → tasks/inbox.md")
    p.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"))
    p.add_argument("--redis-password", default=os.getenv("REDIS_PASSWORD", ""))
    p.add_argument("--inbox-key", default=os.getenv("ANTIGRAVITY_INBOX_KEY", "antigravity:inbox"))
    p.add_argument("--output", default="tasks/inbox.md")
    p.add_argument("--poll-interval", type=float, default=5.0)
    p.add_argument("--notify", action="store_true", default=True, help="Send desktop notification")
    p.add_argument("--no-notify", action="store_false", dest="notify")
    p.add_argument("--auto-open", action="store_true", default=True, help="Auto-open in VS Code")
    p.add_argument("--no-auto-open", action="store_false", dest="auto_open")
    return p.parse_args()


def _connect_redis(url: str, password: str = ""):
    try:
        import redis
    except ImportError:
        print("pip install redis", file=sys.stderr)
        sys.exit(1)
    kwargs = {"decode_responses": True}
    if password:
        kwargs["password"] = password
    return redis.Redis.from_url(url, **kwargs)


def _task_from_json(raw: str) -> Dict[str, Any]:
    try:
        return json.loads(raw)
    except Exception:
        return {"text": raw, "id": "?", "ts": 0}


def _render_inbox(tasks: List[Dict[str, Any]]) -> str:
    """Render tasks as markdown suitable for Antigravity."""
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [
        "# Antigravity Task Inbox",
        f"> Last updated: {now_utc}",
        "",
    ]

    if not tasks:
        lines.append("_No pending tasks._")
        return "\n".join(lines) + "\n"

    lines.append(f"**{len(tasks)} pending task(s)**\n")

    for i, t in enumerate(tasks, 1):
        tid = t.get("id", "?")
        text = t.get("text", "?")
        from_user = t.get("from_user", "")
        from_name = t.get("from_name", "")
        ts = t.get("ts", 0)

        ts_str = ""
        if ts:
            ts_str = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        from_part = f"@{from_user}" if from_user else from_name
        lines.append(f"## Task #{tid}")
        if ts_str:
            lines.append(f"**Time:** {ts_str}")
        if from_part:
            lines.append(f"**From:** {from_part}")
        lines.append(f"\n{text}\n")

    lines.append("---")
    lines.append(
        "_To execute all tasks, type `/tasks` in Antigravity chat. "
        "To mark done: `/done <id>` in Telegram._"
    )
    return "\n".join(lines) + "\n"


def _desktop_notify(title: str, body: str) -> None:
    """Send desktop notification via notify-send (Linux)."""
    try:
        subprocess.run(
            ["notify-send", "--urgency=critical", "--app-name=Antigravity",
             "--icon=dialog-information", title, body],
            timeout=5,
            check=False,
            capture_output=True,
        )
    except FileNotFoundError:
        pass
    except Exception:
        pass


def _auto_open_vscode(filepath: str) -> None:
    """Open file in VS Code (if code CLI is available)."""
    try:
        subprocess.run(
            ["code", "--reuse-window", filepath],
            timeout=5,
            check=False,
            capture_output=True,
        )
    except FileNotFoundError:
        pass
    except Exception:
        pass


def _content_hash(raw_list: List[str]) -> str:
    return hashlib.md5("|".join(raw_list).encode()).hexdigest()


def main() -> int:
    args = _parse_args()
    r = _connect_redis(args.redis_url, args.redis_password)

    # Ensure output directory exists
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"🔭 Watching Redis key '{args.inbox_key}' → {args.output}")
    print(f"   poll interval: {args.poll_interval}s | notify: {args.notify} | auto-open: {args.auto_open}")
    print(f"   redis: {args.redis_url}")
    print(f"   tip: type /tasks in Antigravity chat to auto-execute pending tasks")

    last_hash = ""
    last_count = 0

    while True:
        try:
            raw_list: List[str] = r.lrange(args.inbox_key, 0, -1)
            current_hash = _content_hash(raw_list)

            if current_hash != last_hash:
                tasks = [_task_from_json(raw) for raw in raw_list]
                content = _render_inbox(tasks)
                out_path.write_text(content, encoding="utf-8")

                new_count = len(tasks)
                if new_count > last_count:
                    diff = new_count - last_count
                    newest = tasks[-1] if tasks else {}
                    body = newest.get("text", "")[:100]

                    if args.notify:
                        _desktop_notify(
                            f"📥 {diff} new task(s) in Antigravity",
                            f"{body}\n\nType /tasks in Antigravity to execute",
                        )

                    if args.auto_open:
                        abs_path = str(out_path.resolve())
                        _auto_open_vscode(abs_path)

                    print(f"📥 [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                          f"+{diff} task(s): {body[:60]}")

                last_hash = current_hash
                last_count = new_count

        except KeyboardInterrupt:
            print("\n🛑 Watcher stopped.")
            return 0
        except Exception as e:
            print(f"⚠️ Error: {e}", file=sys.stderr)
            time.sleep(5)
            continue

        time.sleep(args.poll_interval)


if __name__ == "__main__":
    raise SystemExit(main())
