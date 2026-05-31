# python-worker/tools/tb_sre_monitor_v1.py

from __future__ import annotations

import argparse
import json
import os
from typing import Any

import redis
import requests

from utils.time_utils import get_ny_time_millis
import contextlib
from core.redis_keys import RedisStreams as RS

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

OF_INPUTS_STREAM = os.getenv("OF_INPUTS_STREAM", RS.OF_INPUTS)
OF_INPUTS_GROUP = os.getenv("OF_INPUTS_GROUP") or os.getenv("TB_INPUTS_GROUP") or "tb-labeler"

TB_LABELS_STREAM = os.getenv("TB_LABELS_STREAM", RS.TB_LABELS)
TB_LAST_LABEL_TS_MS_KEY = os.getenv("TB_LAST_LABEL_TS_MS_KEY", "tb:last_label_ts_ms")
TB_LAST_ERR_TS_MS_KEY = os.getenv("TB_LAST_ERR_TS_MS_KEY", "tb:last_err_ts_ms")


def _parse_stream_id(id_: Any) -> int:
    if isinstance(id_, bytes):
        id_ = id_.decode("utf-8", "ignore")
    s = (id_ or "0-0")
    try:
        a, _ = s.split("-", 1)
        return int(a)
    except Exception:
        return 0


def _get_int(r: redis.Redis, key: str) -> int:
    v = r.get(key)
    if v is None:
        return 0
    if isinstance(v, bytes):
        v = v.decode("utf-8", "ignore")
    try:
        return int(float(v))
    except Exception:
        return 0


def _send_telegram(text: str, dry_run: bool) -> None:
    if dry_run:
        print(text)
        return
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
    except Exception:
        return


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis_url", default=REDIS_URL)
    ap.add_argument("--max_group_lag_ms", type=int, default=120000)
    ap.add_argument("--max_pending", type=int, default=5000)
    ap.add_argument("--max_label_stale_ms", type=int, default=300000)
    ap.add_argument("--cooldown_sec", type=int, default=300)
    ap.add_argument("--dry_run", action="store_true")
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=False)
    now = get_ny_time_millis()

    # cooldown key to avoid spam
    cooldown_key = "tb:sre:last_alert_ts_ms"
    last_alert = _get_int(r, cooldown_key)
    if last_alert and now - last_alert < args.cooldown_sec * 1000:
        return 0

    # compute group lag/pending
    bad: dict[str, Any] = {}
    try:
        s_info = r.xinfo_stream(OF_INPUTS_STREAM)
        last_id = s_info.get("last-generated-id") or s_info.get("last_generated_id") or b"0-0"
        last_ms = _parse_stream_id(last_id)
        groups = r.xinfo_groups(OF_INPUTS_STREAM)
        g_row = None
        for g in groups:
            name = g.get("name")
            if isinstance(name, bytes):
                name = name.decode("utf-8", "ignore")
            if str(name) == OF_INPUTS_GROUP:
                g_row = g
                break
        if g_row:
            pending = int(g_row.get("pending", 0))
            del_id = g_row.get("last-delivered-id") or g_row.get("last_delivered_id") or b"0-0"
            del_ms = _parse_stream_id(del_id)
            lag_ms = max(0, last_ms - del_ms)
            if lag_ms > args.max_group_lag_ms or pending > args.max_pending:
                bad["group"] = {"lag_ms": lag_ms, "pending": pending}
        else:
            bad["group"] = {"err": "group_not_found"}
    except Exception as e:
        bad["group"] = {"err": str(e)}

    # labels staleness + last error
    last_label_ts = _get_int(r, TB_LAST_LABEL_TS_MS_KEY)
    last_err_ts = _get_int(r, TB_LAST_ERR_TS_MS_KEY)
    if last_label_ts <= 0:
        # Key never written – TB Labeler has not produced any label yet
        bad["labels"] = {
            "stale_ms": None,
            "reason": "labels_never_written",
            "last_label_ts_ms": 0,
            "last_err_ts_ms": last_err_ts,
        }
    else:
        stale_ms = now - last_label_ts
        if stale_ms > args.max_label_stale_ms:
            bad["labels"] = {"stale_ms": stale_ms, "last_label_ts_ms": last_label_ts, "last_err_ts_ms": last_err_ts}

    if not bad:
        return 0

    text = "TB Labeler SRE alert:\n" + json.dumps(bad, ensure_ascii=False)
    _send_telegram(text, dry_run=args.dry_run)

    with contextlib.suppress(Exception):
        r.setex(cooldown_key, args.cooldown_sec, str(now))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
