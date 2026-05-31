import argparse
import asyncio
import os
import shlex
import sys

# Add parent directory to path to import common
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import redis.asyncio as aioredis


async def main():
    parser = argparse.ArgumentParser(description="Wrapper to ensure Minik preferred execution with Main Host fallback.")
    parser.add_argument("--job-name", required=True, help="Unique lock name for this cron job")
    parser.add_argument("--is-minik", action="store_true", help="Set this flag on Minik host")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="The command to run after '--'")
    args = parser.parse_args()

    # The command parser treats '--' as a separator, so args.command might start with it
    if args.command and args.command[0] == '--':
        args.command = args.command[1:]

    redis_url = os.environ.get("LOCK_REDIS_URL", os.environ.get("REDIS_URL", "redis://redis-worker-1:6379/0"))
    redis = aioredis.from_url(redis_url, decode_responses=True)
    lock_key = f"timer_lock:{args.job_name}"

    # If main host, wait 120 seconds before trying to acquire the lock to give minik priority
    if not args.is_minik:
        print(f"[{args.job_name}] Main host delaying start by 120s to yield to Minik...")
        await asyncio.sleep(120)
    else:
        print(f"[{args.job_name}] Minik host attempting to acquire lock instantly...")

    # Attempt to acquire lock for 3 hours (10800 seconds)
    # This ensures it runs strictly ONCE per daily trigger across both hosts
    acquired = await redis.set(lock_key, "1", nx=True, ex=10800)
    if not acquired:
        print(f"[{args.job_name}] Lock {lock_key} already acquired by another node. Skipping execution.")
        await redis.aclose()
        sys.exit(0)

    print(f"[{args.job_name}] Lock acquired successfully. Executing bounded command.")
    await redis.aclose()

    # Run the bounded command
    cmd = shlex.join(args.command)
    print(f"[{args.job_name}] $ {cmd}")
    ret = os.system(cmd)

    # We do NOT delete the lock on exit.
    # Leaving it to expire avoids double execution if the other host triggers again within 3 hours.
    sys.exit(ret >> 8)

if __name__ == "__main__":
    asyncio.run(main())
