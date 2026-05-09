import os
import sys
import time

import redis


def acquire_lock(r: "redis.Redis", key: str, ttl_sec: int) -> bool:
    """
    Redis SET NX EX lock.
    Returns True only for the single winner process.
    """
    try:
        return bool(r.set(key, str(int(time.time())), nx=True, ex=int(ttl_sec)))
    except Exception:
        return False


def main() -> int:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    lock_key = os.getenv("AB_EVAL_LOCK_KEY", "lock:ab_winner_evaluator:v1")
    lock_ttl = int(os.getenv("AB_EVAL_LOCK_TTL_SEC", "3300"))  # 55m

    r = redis.from_url(redis_url, decode_responses=True)
    if not acquire_lock(r, lock_key, lock_ttl):
        # Another instance is running or just ran recently
        return 0

    # Import inside lock to avoid multiple expensive inits under contention
    from services.ab_winner_suggester_service_v2 import ABWinnerSuggesterV2

    try:
        svc = ABWinnerSuggesterV2()
        svc.run_once()  # new method in patch below (sync)
        return 0
    except Exception as e:
        print(f"ab_winner_eval_once error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
