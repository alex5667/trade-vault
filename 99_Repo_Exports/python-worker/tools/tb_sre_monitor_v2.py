from __future__ import annotations

import argparse
import os
import time
from typing import Optional

from common.telegram_notify import send_telegram
from tools.tb_sre_checks import check_tb_health


def _fmt_ms(ms: int) -> str:
    if ms >= 3600_000:
        return f"{ms/3600_000:.2f}h"
    if ms >= 60_000:
        return f"{ms/60_000:.1f}m"
    if ms >= 1_000:
        return f"{ms/1_000:.1f}s"
    return f"{ms}ms"


def send_tb_alert(h, *, prefix: str = "SRE ALERT | TB_LABELER") -> bool:
    text = (
        f"<b>{prefix}</b>\n"
        f"status: <b>{'OK' if h.ok else 'FAIL'}</b>\n"
        f"reason: {h.reason}\n"
        f"input_lag: {_fmt_ms(h.input_lag_ms)}\n"
        f"label_stale: {_fmt_ms(h.label_stale_ms)}\n"
        f"pending: {h.pending}\n"
        f"group_lag: {_fmt_ms(h.group_lag_ms)}\n"
    )
    return send_telegram(text)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis_url", default=os.getenv("REDIS_URL") or os.getenv("TB_REDIS_URL"))
    ap.add_argument("--input_stream", default=os.getenv("TB_INPUT_STREAM") or "signals:of:inputs")
    ap.add_argument("--labels_stream", default=os.getenv("TB_LABELS_STREAM") or "labels:tb")
    ap.add_argument("--group", default=os.getenv("OF_INPUTS_GROUP"))
    ap.add_argument("--max_input_lag_ms", type=int, default=int(os.getenv("TB_MAX_INPUT_LAG_MS") or "120000"))
    ap.add_argument("--max_label_stale_ms", type=int, default=int(os.getenv("TB_MAX_LABEL_STALE_MS") or "300000"))
    ap.add_argument("--max_pending", type=int, default=int(os.getenv("TB_MAX_PENDING") or "5000"))
    ap.add_argument("--cooldown_sec", type=int, default=int(os.getenv("TB_ALERT_COOLDOWN_SEC") or "180"))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--emit-metrics", action="store_true", help="Emit Prometheus metrics (ignored for now)")
    ap.add_argument("--notify", action="store_true", help="Enable Telegram notifications")
    args = ap.parse_args()

    last_sent_key = "tb:sre:last_sent_ts"
    try:
        import redis  # type: ignore
        r = redis.Redis.from_url(args.redis_url or "redis://localhost:6379/0", decode_responses=False)
    except Exception:
        r = None

    h = check_tb_health(
        redis_url=args.redis_url,
        input_stream=args.input_stream,
        labels_stream=args.labels_stream,
        group=args.group,
        max_input_lag_ms=args.max_input_lag_ms,
        max_label_stale_ms=args.max_label_stale_ms,
        max_pending=args.max_pending,
    )

    if h.ok:
        return 0

    now = int(time.time())
    if r is not None:
        last = 0
        try:
            last = int(r.get(last_sent_key) or 0)
        except Exception:
            last = 0
        if now - last < args.cooldown_sec:
            return 0
        try:
            r.setex(last_sent_key, args.cooldown_sec * 2, str(now).encode())
        except Exception:
            pass

    if args.dry_run:
        return 2

    # Respect notify flag if present
    if not args.notify:
        print(f"Alert suppressed (--notify=False): {h.reason}")
        return 2

    ok = send_tb_alert(h)
    return 2 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
