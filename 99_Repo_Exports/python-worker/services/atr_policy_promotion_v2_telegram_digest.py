from __future__ import annotations

import json
import os
import redis

def _redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

def run_once() -> bool:
    r = _redis()
    cur = 0
    proposals = []
    
    # We use a cursor loop to get all matching keys
    while True:
        cur, keys = r.scan(cur, match="cfg:suggestions:atr_policy_v2:*", count=10000)
        for key in keys:
            raw = r.get(key)
            if raw:
                proposals.append(json.loads(raw))
        if cur == 0:
            break

    promote = sorted([x for x in proposals if x.get("action") == "PROMOTE"], key=lambda x: float(x.get("score", 0)), reverse=True)[:5]
    rollback = sorted([x for x in proposals if x.get("action") == "ROLLBACK"], key=lambda x: float(x.get("score", 0)))[:5]

    lines = ["ATR Policy Promotion V2", "", "Promote candidates:"]
    for x in promote:
        lines.append(
            f"- {x['symbol']} | {x['scenario']} | {x['layer']} | v{x['policy_ver']} | "
            f"score={float(x['score']):.2f} | cert={x.get('restore_cert_status','-')}"
        )

    lines += ["", "Rollback candidates:"]
    for x in rollback:
        lines.append(
            f"- {x['symbol']} | {x['scenario']} | {x['layer']} | v{x['policy_ver']} | "
            f"score={float(x['score']):.2f} | cert={x.get('restore_cert_status','-')}"
        )

    payload = {"text": "\n".join(lines)}
    chat_id = os.getenv("ATR_POLICY_TELEGRAM_CHAT_ID", "")
    if chat_id:
        payload["chat_id"] = chat_id
    
    r.xadd("notify:telegram", payload, maxlen=5000, approximate=True)
    return True

if __name__ == "__main__":
    run_once()
