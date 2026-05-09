from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import secrets
from typing import Any

import redis

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS


def now_ms() -> int:
    return get_ny_time_millis()


def notify(r: redis.Redis, text: str, buttons=None) -> None:
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), fields, maxlen=200000, approximate=True)


def make_bundle_hset(cfg_key: str, changes: dict[str, str], who: str, ttl: int) -> tuple[str, str, dict[str, Any]]:
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bid = secrets.token_hex(6)
    sig = hmac.new(secret.encode(), bid.encode(), hashlib.sha256).hexdigest()[:8]
    ts = now_ms()
    ops = [{"op": "HSET", "key": cfg_key, "field": k, "value": str(v)} for k, v in changes.items()]
    bundle = {"id": bid, "created_ms": ts, "ttl_sec": ttl, "who": who, "ops": ops, "meta": {"kind": "ml_register_challenger_v1"}}
    return bid, sig, bundle


def write_bundle(r: redis.Redis, bid: str, bundle: dict[str, Any], ttl: int) -> None:
    r.set(f"recs:bundle:{bid}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(f"recs:status:{bid}", "PENDING", ex=ttl)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True)
    ap.add_argument("--meta", required=True)
    ap.add_argument("--ver", required=True)
    args = ap.parse_args()

    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)
    cfg_key = os.getenv("ML_CONFIRM_CFG_KEY", "cfg:ml_confirm")
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    changes = {
        "challenger_model_path": args.model,
        "challenger_meta_path": args.meta,
        "challenger_ver": args.ver,
        "updated_ms": str(now_ms()),
    }

    bid, sig, bundle = make_bundle_hset(cfg_key, changes, who="ml_register_challenger_v1", ttl=ttl)
    write_bundle(r, bid, bundle, ttl)

    buttons = [[
        {"text": "👀 Preview diff", "callback": f"recs:preview2:{bid}:{sig}"},
        {"text": "✅ Confirm apply", "callback": f"recs:confirm:{bid}:{sig}"},
        {"text": "❌ Reject", "callback": f"recs:reject:{bid}:{sig}"},
    ]]

    notify(r,
           "<b>Register ML Challenger</b>\n"
           f"ver=<code>{args.ver}</code>\n"
           f"model=<code>{args.model}</code>\n"
           f"meta=<code>{args.meta}</code>",
           buttons)


if __name__ == "__main__":
    main()
