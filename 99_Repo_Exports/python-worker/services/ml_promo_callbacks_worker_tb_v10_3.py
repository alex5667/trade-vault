from __future__ import annotations

import json
import os
import time
from typing import Any

import redis

from utils.time_utils import get_ny_time_millis
import contextlib
from core.redis_keys import RedisStreams as RS


def _safe_loads(s: Any) -> dict[str, Any]:
    try:
        if s is None:
            return {}
        if isinstance(s, dict):
            return s
        if isinstance(s, bytes):
            s = s.decode("utf-8", "ignore")
        return json.loads(str(s))
    except Exception:
        return {}


def main() -> None:
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

    callbacks_stream = os.getenv("BOT_CALLBACKS_STREAM", RS.BOT_CALLBACKS)
    group = os.getenv("ML_PROMO_GROUP", "ml-promo-tb-v10-3")
    consumer = os.getenv("ML_PROMO_CONSUMER", "c1")

    challenger_key = os.getenv("ML_CFG_CHALLENGER_KEY", "cfg:ml_confirm:challenger")
    champion_key = os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion")

    processed_set = os.getenv("ML_PROMO_PROCESSED_SET", "ml:promo:processed:v10_3")
    processed_ttl_sec = int(os.getenv("ML_PROMO_PROCESSED_TTL_SEC", "604800"))

    with contextlib.suppress(Exception):
        r.xgroup_create(callbacks_stream, group, id="$", mkstream=True)

    while True:
        try:
            resp = r.xreadgroup(group, consumer, {callbacks_stream: ">"}, count=200, block=1000)
        except Exception:
            resp = None

        if not resp:
            time.sleep(0.05)
            continue

        for _stream, msgs in resp:
            for msg_id, fields in msgs:
                if r.sismember(processed_set, msg_id):
                    with contextlib.suppress(Exception):
                        r.xack(callbacks_stream, group, msg_id)
                    continue

                cb = (fields.get("callback", "") or "")
                if cb.startswith("approve:ml_tb_v10_3:"):
                    run_id = cb.split(":", 2)[2]
                    chal = _safe_loads(r.get(challenger_key))
                    if chal and (chal.get("run_id", "")) == run_id:
                        r.set(champion_key, json.dumps(chal, ensure_ascii=False, separators=(",", ":")))
                        r.delete(challenger_key)
                elif cb.startswith("reject:ml_tb_v10_3:"):
                    run_id = cb.split(":", 2)[2]
                    chal = _safe_loads(r.get(challenger_key))
                    if chal and (chal.get("run_id", "")) == run_id:
                        chal["rejected_ms"] = get_ny_time_millis()
                        r.set(challenger_key + ":rejected:" + run_id, json.dumps(chal, ensure_ascii=False, separators=(",", ":")), ex=7*24*3600)
                        r.delete(challenger_key)

                try:
                    r.sadd(processed_set, msg_id)
                    r.expire(processed_set, processed_ttl_sec)
                except Exception:
                    pass
                with contextlib.suppress(Exception):
                    r.xack(callbacks_stream, group, msg_id)


if __name__ == "__main__":
    main()

