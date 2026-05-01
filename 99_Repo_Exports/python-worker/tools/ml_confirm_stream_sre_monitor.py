#!/usr/bin/env python3
from __future__ import annotations
"""
ml_confirm_stream_sre_monitor.py

SRE monitor for metrics:ml_confirm stream *content*.
- Runs the health check
- Sends Telegram alert if FAIL and cooldown passed

Requires:
- TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
- redis python package

Cooldown stored in Redis key: sre:ml_confirm_stream:last_alert_ts_ms
"""

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore

from tools.check_ml_confirm_stream_health import compute_health, _parse_entry, _now_ms  # type: ignore


def _send_telegram(token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text, "disable_web_page_preview": "true"}).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis_url", default=os.getenv("REDIS_URL") or os.getenv("TB_REDIS_URL") or "redis://localhost:6379/0")
    ap.add_argument("--stream", default=os.getenv("ML_CONFIRM_METRICS_STREAM") or "metrics:ml_confirm")
    ap.add_argument("--count", type=int, default=int(os.getenv("ML_CONFIRM_HEALTH_COUNT") or "500"))
    ap.add_argument("--max_stale_ms", type=int, default=int(os.getenv("ML_CONFIRM_MAX_STALE_MS") or "120000"))
    ap.add_argument("--cooldown_sec", type=int, default=int(os.getenv("ML_CONFIRM_ALERT_COOLDOWN_SEC") or "180"))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--emit-metrics", action="store_true", help="Emit Prometheus metrics (ignored for now)")
    ap.add_argument("--notify", action="store_true", help="Enable Telegram notifications")
    args = ap.parse_args()

    if redis is None:
        print(json.dumps({"ok": False, "reason": "redis_python_not_installed"}, ensure_ascii=False))
        return 2

    r = redis.Redis.from_url(args.redis_url, decode_responses=False)

    try:
        entries = r.xrevrange(args.stream, max="+", min="-", count=args.count)
    except Exception as e:
        report = {"ok": False, "reason": f"redis_error:{type(e).__name__}"}
        print(json.dumps(report, ensure_ascii=False))
        return 2

    samples = []
    for _id, fields in entries:
        try:
            samples.append(_parse_entry(fields))
        except Exception:
            continue

    ok, report = compute_health(samples, _now_ms(), args.max_stale_ms)
    print(json.dumps(report, ensure_ascii=False))

    if ok:
        return 0

    token = os.getenv("TELEGRAM_BOT_TOKEN") or ""
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or ""
    if not token or not chat_id:
        # fail but no telegram configured
        return 2

    cooldown_key = os.getenv("ML_CONFIRM_ALERT_COOLDOWN_KEY") or "sre:ml_confirm_stream:last_alert_ts_ms"
    now_ms = _now_ms()
    try:
        last_ms = int(r.get(cooldown_key) or b"0")
    except Exception:
        last_ms = 0
    if now_ms - last_ms < args.cooldown_sec * 1000:
        return 2

    text = (
        "SRE ALERT | ML_CONFIRM_STREAM\n"
        f"reason={report.get('reason')}\n"
        f"stale_ms={report.get('stale_ms')}\n"
        f"missing_required_rate={report.get('missing_required_rate')}\n"
        f"p_edge_zero_rate={report.get('p_edge_zero_rate')}\n"
        f"err_rate={report.get('err_rate')}\n"
        f"abstain_rate={report.get('abstain_rate')} allow_rate={report.get('allow_rate')}\n"
        f"status_counts={report.get('status_counts')}\n"
        f"stream={args.stream} count={report.get('n')}"
    )

    if args.dry_run:
        return 2

    if args.notify:
        try:
            _send_telegram(token, chat_id, text)
            r.set(cooldown_key, str(now_ms).encode("utf-8"))
        except Exception:
            return 2
    else:
        # Alert suppressed
        return 2

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
