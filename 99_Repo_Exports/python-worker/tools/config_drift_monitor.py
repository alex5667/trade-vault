from __future__ import annotations
"""Monitor config drift: detect unexpected changes in config:orderflow:<SYMBOL> keys.

Snapshots critical config keys and compares with previous snapshot.
Alerts via Telegram on any changes (important for "managed risk" policy).

Usage:
  python -m tools.config_drift_monitor --symbols BTCUSDT,ETHUSDT
"""

from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import time
from typing import Dict, List

import redis


# Critical config keys to monitor for drift
KEYS = [
    "w_exec_risk",
    "exec_risk_ref_bps",
    "of_score_min",
    "vol_shock_fail_closed",
    "vol_shock_exec_risk_norm_max",
    "saw_chop_fail_closed",
    "meta_model_enable",
    "meta_model_mode",
    "meta_p_min",
    "meta_model_path",
]


def main() -> None:
    ap = argparse.ArgumentParser(description="Monitor config drift in config:orderflow:<SYMBOL>")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), help="Redis URL")
    ap.add_argument("--symbols", default=os.getenv("CANARY_SYMBOLS", "BTCUSDT,ETHUSDT"), help="comma-separated symbols to monitor")
    ap.add_argument("--prefix", default=os.getenv("CFG_HASH_PREFIX", "config:orderflow:"), help="Redis key prefix (default: config:orderflow:)")
    ap.add_argument("--state-key", default=os.getenv("CFG_DRIFT_STATE_KEY", "sre:cfg:last_snapshot"), help="Redis key to store last snapshot (default: sre:cfg:last_snapshot)")
    ap.add_argument("--notify", type=int, default=1, help="send Telegram alert on changes (default: 1)")
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    # Snapshot current configs
    snap: Dict[str, Dict[str, str]] = {}
    for sym in syms:
        hkey = f"{args.prefix}{sym}"
        vals = r.hmget(hkey, KEYS)
        snap[sym] = {k: ("" if v is None else str(v)) for k, v in zip(KEYS, vals)}

    # Load previous snapshot
    prev_raw = r.get(args.state_key)
    prev = json.loads(prev_raw) if prev_raw else {}

    # Detect changes
    changes = []
    for sym in syms:
        p = (prev.get(sym) or {}) if isinstance(prev, dict) else {}
        c = snap.get(sym) or {}
        for k in KEYS:
            if str(p.get(k, "")) != str(c.get(k, "")):
                changes.append((sym, k, str(p.get(k, "")), str(c.get(k, ""))))

    # Save current snapshot (TTL 14 days)
    r.set(args.state_key, json.dumps(snap, ensure_ascii=False, separators=(",", ":")), ex=14 * 86400)

    # Notify on changes
    if changes and args.notify == 1:
        import html
        msg = ["<b>CFG drift</b> (config:orderflow)"]
        for sym, k, old, new in changes[:25]:
            s_esc = html.escape(str(sym), quote=False)
            k_esc = html.escape(str(k), quote=False)
            o_esc = html.escape(str(old), quote=False)
            n_esc = html.escape(str(new), quote=False)
            msg.append(f"- <code>{s_esc}</code> <code>{k_esc}</code>: <code>{o_esc}</code> → <code>{n_esc}</code>")
        if len(changes) > 25:
            msg.append(f"... and {len(changes) - 25} more")
        try:
            r.xadd(
                os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"),
                {"type": "report", "text": "\n".join(msg), "ts": str(get_ny_time_millis())},
                maxlen=200000,
                approximate=True
            )
        except Exception as e:
            print(f"Warning: failed to notify telegram: {e}")


if __name__ == "__main__":
    main()

