import argparse
import asyncio
import os
import shlex
import subprocess
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import redis.asyncio as aioredis

LOCK_TTL = 60
RENEW_INTERVAL = 20
REDIS_CONNECT_RETRY_DELAY = 10
REDIS_CONNECT_MAX_RETRIES = 30  # 5 minutes total
# Main must wait > LOCK_TTL + LOCK_RETRY_INTERVAL so minik wins the race after a restart.
# LOCK_TTL=60 + retry=30 → 95s gives minik a comfortable window.
MAIN_INITIAL_YIELD_SEC = 95


async def lock_renewer(redis, lock_key, process):
    try:
        while process.poll() is None:
            await asyncio.sleep(RENEW_INTERVAL)
            await redis.expire(lock_key, LOCK_TTL)
    except Exception as e:
        print(f"Error in lock renewer: {e}")


async def connect_redis_with_retry(redis_url: str, job_name: str) -> aioredis.Redis:
    for attempt in range(1, REDIS_CONNECT_MAX_RETRIES + 1):
        client = aioredis.from_url(redis_url, decode_responses=True, socket_connect_timeout=5)
        try:
            await client.ping()
            print(f"[{job_name}] Redis connected on attempt {attempt}.")
            return client
        except Exception as e:
            await client.aclose()
            print(f"[{job_name}] Redis unavailable (attempt {attempt}/{REDIS_CONNECT_MAX_RETRIES}): {e}")
            if attempt < REDIS_CONNECT_MAX_RETRIES:
                await asyncio.sleep(REDIS_CONNECT_RETRY_DELAY)
    print(f"[{job_name}] Could not connect to Redis after {REDIS_CONNECT_MAX_RETRIES} attempts. Exiting.")
    sys.exit(1)


async def main():
    parser = argparse.ArgumentParser(description="Daemon Wrapper to ensure Minik preferred execution with Main Host fallback.")
    parser.add_argument("--job-name", required=True, help="Unique lock name for this daemon")
    parser.add_argument("--is-minik", action="store_true", help="Set this flag on Minik host")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="The command to run after '--'")
    args = parser.parse_args()

    if args.command and args.command[0] == '--':
        args.command = args.command[1:]

    redis_url = os.environ.get("LOCK_REDIS_URL", os.environ.get("REDIS_URL", "redis://redis-worker-1:6379/0"))
    lock_key = f"daemon_lock:{args.job_name}"

    if not args.is_minik:
        print(f"[{args.job_name}] Main host yielding to Minik (initial {MAIN_INITIAL_YIELD_SEC}s wait)...")
        await asyncio.sleep(MAIN_INITIAL_YIELD_SEC)

    redis = await connect_redis_with_retry(redis_url, args.job_name)

    while True:
        try:
            acquired = await redis.set(lock_key, "1", nx=True, ex=LOCK_TTL)
        except Exception as e:
            print(f"[{args.job_name}] Lock check failed: {e}. Retrying in {REDIS_CONNECT_RETRY_DELAY}s...")
            await asyncio.sleep(REDIS_CONNECT_RETRY_DELAY)
            continue

        if acquired:
            print(f"[{args.job_name}] Lock acquired. Starting daemon...")
            break
        else:
            await asyncio.sleep(30)

    cmd = shlex.join(args.command)
    print(f"[{args.job_name}] $ {cmd}")
    process = subprocess.Popen(cmd, shell=True)

    renewer_task = asyncio.create_task(lock_renewer(redis, lock_key, process))

    while process.poll() is None:
        await asyncio.sleep(1)

    renewer_task.cancel()
    print(f"[{args.job_name}] Daemon process exited with code {process.returncode}")

    await redis.delete(lock_key)
    await redis.aclose()
    sys.exit(process.returncode)


if __name__ == "__main__":
    asyncio.run(main())
