import redis
import json
import time

try:
    r = redis.Redis.from_url('redis://redis-worker-1:6379/0', decode_responses=True)

    # 1. Back up current
    curr_cfg = r.get('cfg:ml_confirm:champion')
    if curr_cfg:
        print("Backing up current config to cfg:ml_confirm:champion:backup")
        print("Current config:", curr_cfg)
        r.set('cfg:ml_confirm:champion:backup', curr_cfg)
    else:
        print("No current config found to backup.")

    # 2. Set new config
    new_cfg = {
      "schema_version": 1,
      "kind": "edge_stack_v1",
      "run_id": "20260311_000037",
      "created_ms": int(time.time() * 1000),
      "model_path": "/var/lib/trade/ml_models/edge_stack_v1/runs/20260311_000037/edge_stack_v1.joblib",
      "mode": "SHADOW",
      "enforce_share": 0.0,
      "p_min": 0.52
    }

    print("\nSetting new config to cfg:ml_confirm:champion...")
    r.set('cfg:ml_confirm:champion', json.dumps(new_cfg))

    print("\nVerification:")
    print(r.get('cfg:ml_confirm:champion'))
    
except Exception as e:
    import traceback
    traceback.print_exc()
