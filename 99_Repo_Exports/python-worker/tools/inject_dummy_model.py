from utils.time_utils import get_ny_time_millis
import json
import redis
import time

cfg = {
    "kind": "util_mh_fastlinear_v1",
    "run_id": "bootstrap_dummy",
    "created_ms": get_ny_time_millis(),
    "model_path": "/app/dummy_model_v1.json",
    "mode": "SHADOW",
    "util_floors": {
        "global": {"floor": -0.05},
        "unc_k": 0.5
    }
}

try:
    r = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
    r.ping()
    r.set("cfg:ml_confirm:champion", json.dumps(cfg))
    print("Config injected successfully with model in /app/dummy_model_v1.json!")
except Exception as e:
    print(f"Error connecting to Redis: {e}")
