import json
import os

import redis

from domain.evidence_keys import MetaKeys
from core.redis_keys import RedisStreams as RS


def main():
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    try:
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        stream = RS.OF_GATE_METRICS

        print(f"Reading from {stream}...")
        data = r.xrevrange(stream, count=5)
        for msg_id, fields in data:
            print(f"MsgID: {msg_id}")
            print(f"Top-level keys: {list(fields.keys())}")
            # print(f"Fields content: {fields}")
            ok = fields.get("ok", "MISSING")
            ok_soft = fields.get("ok_soft", "MISSING")
            meta_veto = fields.get(MetaKeys.VETO, "MISSING")
            print(f"ok={ok}, ok_soft={ok_soft}, meta_veto={meta_veto}")

            if "payload" in fields:
                try:
                    p = json.loads(fields["payload"])
                    print(f"Payload JSON keys: {list(p.keys())}")
                except Exception:
                    print("Payload is not JSON")
            print("-" * 20)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
