
import os
import redis
import json
import time

def main():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    keys = [
        "meta_model:last_status",
        "meta_model:last_train_ts_ms",
        "meta_model:last_train_report",
        "tb:last_ts_ms",
        "tb:last_label_ts_ms",
        "tb:last_err_ts_ms"
    ]

    print(f"--- Redis Status ({redis_url}) ---")
    for k in keys:
        val = r.get(k)
        print(f"\n[{k}]")
        if val:
            try:
                # Try to pretty print JSON
                j = json.loads(val)
                print(json.dumps(j, indent=2))
            except json.JSONDecodeError:
                print(val)
        else:
            print("<None>")

if __name__ == "__main__":
    main()
