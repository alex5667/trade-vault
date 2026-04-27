from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
"""Redis Streams consumer-group health checker.

Checks health of consumer groups by pending messages, lag, and age of unacknowledged messages.
If a group is missing, auto-creates it at '$' and sends a WARNING alert.

Env:
  STREAM_GROUP_TARGETS: comma-separated list of stream@group
  STREAM_HEALTH_PENDING_MAX: maximum pending messages (default 5000)
  STREAM_HEALTH_LAG_MAX: maximum lag (unconsumed) (default 5000)
  STREAM_HEALTH_MAX_AGE_SEC: maximum age of pending message (default 300)
  STREAM_HEALTH_AUTOCREATE_MISSING: auto-create missing groups at '$' (default 1)
  REDIS_URL: Redis connection URL
  NOTIFY_TELEGRAM_STREAM: Redis stream to send alerts to
"""

import os
import time
import argparse
import redis

# Simple in-process cooldown to avoid flooding Telegram on every invocation
_alerted: dict[str, float] = {}
ALERT_COOLDOWN_SEC = int(os.getenv("STREAM_HEALTH_ALERT_COOLDOWN_SEC", "600"))


def get_ts_ms() -> int:
    return get_ny_time_millis()


def _should_alert(key: str) -> bool:
    now = time.time()
    last = _alerted.get(key, 0)
    if now - last >= ALERT_COOLDOWN_SEC:
        _alerted[key] = now
        return True
    return False


def _send_alert(r: redis.Redis, notify: str, title: str, lines: list[str], level: str = "WARNING") -> None:
    icon = {"ERROR": "🚨", "WARNING": "⚠️", "INFO": "✅"}.get(level, "⚠️")
    body = "\n".join(f"- {l}" for l in lines)
    text = f"{icon} <b>Stream Health Alerts: {title}</b>\n{body}"
    print(f"[ALERT:{level}] {title}\n{body}")
    try:
        r.xadd(notify, {
            "type": "report",
            "subtype": "stream_health",
            "level": level,
            "source": "StreamGroupHealth",
            "text": text,
            "ts_ms": str(get_ts_ms()),
        }, maxlen=50000, approximate=True)
    except Exception as e:
        print(f"[ERROR] Failed to send alert to {notify}: {e}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--targets", default=os.getenv("STREAM_GROUP_TARGETS", ""))
    ap.add_argument("--notify", default=os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"))
    ap.add_argument("--pending-max", type=int, default=int(os.getenv("STREAM_HEALTH_PENDING_MAX", "5000")))
    ap.add_argument("--lag-max", type=int, default=int(os.getenv("STREAM_HEALTH_LAG_MAX", "5000")))
    ap.add_argument("--max-age-sec", type=int, default=int(os.getenv("STREAM_HEALTH_MAX_AGE_SEC", "300")))
    ap.add_argument("--autocreate", type=int, default=int(os.getenv("STREAM_HEALTH_AUTOCREATE_MISSING", "1")))
    args = ap.parse_args()

    if not args.targets:
        print("No targets specified (STREAM_GROUP_TARGETS is empty).")
        return

    try:
        r = redis.Redis.from_url(args.redis_url, decode_responses=True, socket_connect_timeout=5, socket_timeout=5)
        r.ping()
    except Exception as e:
        print(f"[ERROR] Cannot connect to Redis: {e}")
        return

    targets = [x.strip() for x in args.targets.split(",") if x.strip()]

    issues: list[str] = []
    warnings: list[str] = []

    for tgt in targets:
        stream_and_group = tgt.replace(" ", "")
        if "@" not in stream_and_group:
            print(f"[SKIP] Invalid target format (expected stream@group[:id]): {tgt}")
            continue
        stream, group_and_id = stream_and_group.split("@", 1)
        if ":" in group_and_id:
            group, custom_id = group_and_id.split(":", 1)
        else:
            group = group_and_id
            custom_id = "$"

        # ── 1. Stream does not exist yet ──────────────────────────────────────
        if not r.exists(stream):
            msg = f"Stream <code>{stream}</code> does not exist yet — worker may not have started"
            if _should_alert(f"no_stream:{stream}"):
                warnings.append(msg)
            else:
                print(f"[INFO] {msg} (cooldown)")
            continue

        # ── 2. Check group existence ───────────────────────────────────────────
        try:
            groups = r.xinfo_groups(stream)
        except Exception as e:
            issues.append(f"Error reading groups for <code>{stream}</code>: {e}")
            continue

        grp_info = next((g for g in groups if g.get("name") == group), None)

        if not grp_info:
            # Auto-create at '$' so worker picks up from here without replaying history
            created = False
            if args.autocreate:
                try:
                    r.xgroup_create(stream, group, id=custom_id, mkstream=False)
                    created = True
                    print(f"[AUTO-CREATE] Created group '{group}' on '{stream}' at {custom_id}")
                except redis.exceptions.ResponseError as ce:
                    if "BUSYGROUP" in str(ce):
                        # Race — another process created it between our check and create
                        created = True
                        print(f"[INFO] Group '{group}' appeared on '{stream}' (race resolved)")
                    else:
                        print(f"[ERROR] Failed to auto-create group '{group}' on '{stream}': {ce}")
                except Exception as ce:
                    print(f"[ERROR] Unexpected error auto-creating group '{group}': {ce}")

            alert_key = f"missing_group:{stream}:{group}"
            msg = (
                f"Group <code>{group}</code> was missing on <code>{stream}</code>"
                + (f" — auto-created at {custom_id} ✅" if created else " — auto-create failed ❌")
            )
            if _should_alert(alert_key):
                if created:
                    warnings.append(msg)
                else:
                    issues.append(msg)
            else:
                print(f"[INFO] {msg} (cooldown)")
            continue

        # ── 3. Check lag ───────────────────────────────────────────────────────
        lag = grp_info.get("lag", 0) or 0
        pending = grp_info.get("pending", 0) or 0

        if lag > args.lag_max:
            issues.append(f"<code>{stream}@{group}</code> LAG=<b>{lag}</b> &gt; {args.lag_max}")

        if pending > args.pending_max:
            issues.append(f"<code>{stream}@{group}</code> PENDING=<b>{pending}</b> &gt; {args.pending_max}")

        # ── 4. Check oldest pending message age ────────────────────────────────
        if pending > 0:
            try:
                pend_info = r.xpending(stream, group)
                if pend_info and pend_info.get("min"):
                    oldest_id = pend_info["min"]
                    oldest_ts_ms = int(str(oldest_id).split("-")[0])
                    age_sec = (get_ts_ms() - oldest_ts_ms) / 1000.0
                    if age_sec > args.max_age_sec:
                        issues.append(
                            f"<code>{stream}@{group}</code> oldest pending: "
                            f"<b>{age_sec:.0f}s</b> &gt; {args.max_age_sec}s"
                        )
            except Exception as e:
                print(f"[WARN] xpending failed for {stream}@{group}: {e}")

    # ── Send alerts ────────────────────────────────────────────────────────────
    if issues:
        _send_alert(r, args.notify, "Issues Detected", issues, level="ERROR")
    elif warnings:
        _send_alert(r, args.notify, "Warnings", warnings, level="WARNING")
    else:
        print("OK — all stream groups healthy")


if __name__ == "__main__":
    main()
