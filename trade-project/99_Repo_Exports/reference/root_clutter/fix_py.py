import os

target_dir = "/home/alex/front/trade/scanner_infra/python-worker/orderflow_services"
old_str = '"redis://localhost:6379/0"'
new_str = 'os.getenv("REDIS_WORKER_URL", "redis://redis-worker-1:6379/0") or "redis://redis-worker-1:6379/0"'
count = 0

for root, dirs, files in os.walk(target_dir):
    for filename in files:
        if filename.endswith(".py"):
            path = os.path.join(root, filename)
            try:
                with open(path, "r") as f:
                    content = f.read()
                
                if old_str in content:
                    # Let's replace os.getenv("REDIS_URL", "redis://localhost:6379/0") 
                    # with os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
                    # No wait, if os.getenv returns "", the default argument is not used.
                    # We should replace the entire os.getenv(...) block if possible.
                    # Best is just replacing `"redis://localhost:6379/0"` with `"redis://redis-worker-1:6379/0"`
                    # AND handle the case where it returns empty string by manually modifying it?
                    # No, wait. Just replacing "redis://localhost:6379/0" with "redis://redis-worker-1:6379/0" doesn't fix `""`.
                    # Wait, if REDIS_URL="" in the env, os.getenv("REDIS_URL") is "".
                    # Does `redis.from_url("")` throw an error or default to localhost?
                    # The error says "connecting to localhost:6379", which means `redis.from_url("")` DOES default to localhost!
                    
                    pass
            except Exception:
                pass

# If redis.from_url("") defaults to localhost, we MUST replace:
# r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0")...)
# with
# redis_url = os.getenv("REDIS_URL") or "redis://redis-worker-1:6379/0"
# r = redis.from_url(redis_url, ...)

# Let's just fix docker-compose instead!
