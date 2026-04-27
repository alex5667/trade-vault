# python-worker/tools/check_tb_health_v2.py

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import sys
import time
from typing import Any, Dict, Tuple

import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

OF_INPUTS_STREAM = os.getenv("OF_INPUTS_STREAM", "signals:of:inputs")
OF_INPUTS_GROUP = os.getenv("OF_INPUTS_GROUP", "tb-labeler")

TB_LABELS_STREAM = os.getenv("TB_LABELS_STREAM", "labels:tb")
TB_LAST_TS_MS_KEY = os.getenv("TB_LAST_TS_MS_KEY", "tb:last_ts_ms")
TB_LAST_LABEL_TS_MS_KEY = os.getenv("TB_LAST_LABEL_TS_MS_KEY", "tb:last_label_ts_ms")
TB_LAST_ERR_TS_MS_KEY = os.getenv("TB_LAST_ERR_TS_MS_KEY", "tb:last_err_ts_ms")


def _parse_stream_id(id_: Any) -> Tuple[int, int]:
    if isinstance(id_, bytes):
        id_ = id_.decode("utf-8", "ignore")
    s = str(id_ or "0-0")
    try:
        a, b = s.split("-", 1)
        return int(a), int(b)
    except Exception:
        return 0, 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis_url", default=REDIS_URL)
    ap.add_argument("--max_group_lag_ms", type=int, default=120000)
    ap.add_argument("--max_pending", type=int, default=5000)
    ap.add_argument("--max_label_stale_ms", type=int, default=300000)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=False)
    now = get_ny_time_millis()

    out: Dict[str, Any] = {"ok": True, "ts_ms": now, "checks": {}}

    # group info
    try:
        s_info = r.xinfo_stream(OF_INPUTS_STREAM)
        last_id = s_info.get("last-generated-id") or s_info.get("last_generated_id") or b"0-0"
        last_ms, _ = _parse_stream_id(last_id)
        groups = r.xinfo_groups(OF_INPUTS_STREAM)
        g_row = None
        for g in groups:
            name = g.get("name")
            if isinstance(name, bytes):
                name = name.decode("utf-8", "ignore")
            if str(name) == OF_INPUTS_GROUP:
                g_row = g
                break
        if g_row is None:
            out["checks"]["group"] = {"ok": False, "err": "group_not_found"}
            out["ok"] = False
        else:
            pending = int(g_row.get("pending", 0))
            last_del = g_row.get("last-delivered-id") or g_row.get("last_delivered_id") or b"0-0"
            del_ms, _ = _parse_stream_id(last_del)
            lag_ms = max(0, last_ms - del_ms)
            ok = (lag_ms <= args.max_group_lag_ms) and (pending <= args.max_pending)
            out["checks"]["group"] = {"ok": ok, "lag_ms": lag_ms, "pending": pending, "stream_last_ms": last_ms, "group_last_ms": del_ms}
            if not ok:
                out["ok"] = False
    except Exception as e:
        out["checks"]["group"] = {"ok": False, "err": str(e)}
        out["ok"] = False

    # label staleness
    def _get_int(key: str) -> int:
        v = r.get(key)
        if v is None:
            return 0
        if isinstance(v, bytes):
            v = v.decode("utf-8", "ignore")
        try:
            return int(float(v))
        except Exception:
            return 0

    last_label_ts = _get_int(TB_LAST_LABEL_TS_MS_KEY)
    last_inp_ts = _get_int(TB_LAST_TS_MS_KEY)
    last_err_ts = _get_int(TB_LAST_ERR_TS_MS_KEY)

    stale_ms = now - last_label_ts if last_label_ts else 10**12
    ok_labels = stale_ms <= args.max_label_stale_ms
    out["checks"]["labels"] = {"ok": ok_labels, "stale_ms": stale_ms, "last_label_ts_ms": last_label_ts, "last_input_ts_ms": last_inp_ts, "last_err_ts_ms": last_err_ts}
    if not ok_labels:
        out["ok"] = False

    # labels stream len
    try:
        info = r.xinfo_stream(TB_LABELS_STREAM)
        out["checks"]["labels_stream"] = {"ok": True, "length": int(info.get("length", 0))}
    except Exception as e:
        out["checks"]["labels_stream"] = {"ok": False, "err": str(e)}
        out["ok"] = False

    if args.json:
        print(json.dumps(out, ensure_ascii=False))
    else:
        print(out)

    return 0 if out["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
