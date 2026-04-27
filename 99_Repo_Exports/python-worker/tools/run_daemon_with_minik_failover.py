import sys
import os
import asyncio
import argparse
import shlex
import subprocess

# Add parent directory to path to import common
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import redis.asyncio as aioredis

LOCK_TTL = 60
RENEW_INTERVAL = 20

async def lock_renewer(redis, lock_key, process):
    try:
        while process.poll() is None:
            await asyncio.sleep(RENEW_INTERVAL)
            # Renew the lock
            await redis.expire(lock_key, LOCK_TTL)
    except Exception as e:
        print(f"Error in lock renewer: {e}")

async def main():
    parser = argparse.ArgumentParser(description="Daemon Wrapper to ensure Minik preferred execution with Main Host fallback.")
    parser.add_argument("--job-name", required=True, help="Unique lock name for this daemon")
    parser.add_argument("--is-minik", action="store_true", help="Set this flag on Minik host")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="The command to run after '--'")
    args = parser.parse_args()

    if args.command and args.command[0] == '--':
        args.command = args.command[1:]

    redis_url = os.environ.get("REDIS_URL", "redis://redis-worker-1:6379/0")
    redis = aioredis.from_url(redis_url, decode_responses=True)
    lock_key = f"daemon_lock:{args.job_name}"
    
    # If main host, initial delay to give minik priority
    if not args.is_minik:
        print(f"[{args.job_name}] Main host yielding to Minik (initial 60s wait)...")
        await asyncio.sleep(60)
        
    while True:
        # Try to acquire lock
        acquired = await redis.set(lock_key, "1", nx=True, ex=LOCK_TTL)
        if acquired:
            print(f"[{args.job_name}] Lock acquired. Starting daemon...")
            break
        else:
            # Minik is running it, or something else is holding the lock
            # We don't exit, we just poll the lock periodically
            await asyncio.sleep(30)
            
    # We have the lock. Launch subprocess.
    cmd = shlex.join(args.command)
    print(f"[{args.job_name}] $ {cmd}")
    process = subprocess.Popen(cmd, shell=True)
    
    # Start renewing task
    renewer_task = asyncio.create_task(lock_renewer(redis, lock_key, process))
    
    # Wait for process to exit natively
    while process.poll() is None:
        await asyncio.sleep(1)
        
    # Process died or exited
    renewer_task.cancel()
    print(f"[{args.job_name}] Daemon process exited with code {process.returncode}")
    
    # Release the lock so another host can take over immediately
    await redis.delete(lock_key)
    await redis.aclose()
    sys.exit(process.returncode)

if __name__ == "__main__":
    asyncio.run(main())
