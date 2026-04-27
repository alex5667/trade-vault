import redis
import os
import json

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

def check_ethusdt_specs():
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    key = "symbol_specs:ETHUSDT"
    val = r.get(key)
    print(f"Key: {key}")
    if val:
        print(f"Value: {val}")
    else:
        print("Not found")

    # Also check base Ethereum
    key2 = "symbol_specs:ETHUSD"
    val2 = r.get(key2)
    print(f"Key: {key2}")
    if val2:
        print(f"Value: {val2}")
    else:
        print("Not found")

if __name__ == "__main__":
    check_ethusdt_specs()
