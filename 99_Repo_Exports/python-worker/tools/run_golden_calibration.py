#!/usr/bin/env python3
import asyncio
import json
import os
import subprocess
import time
from pathlib import Path
from redis import asyncio as aioredis


async def capture_stream(r: aioredis.Redis, stream: str, duration_sec: int) -> list:
    """Capture stream entries for a specific duration."""
    print(f"Capturing stream {stream} for {duration_sec} seconds...")
    end_time = time.time() + duration_sec
    messages = []
    last_id = "0-0"
    
    while time.time() < end_time:
        try:
            entries = await r.xread({stream: last_id}, count=1000, block=1000)
            if entries:
                for _, batch in entries:
                    for mid, fields in batch:
                        payload = fields.get(b"payload")
                        if payload:
                            try:
                                messages.append({"payload": json.loads(payload)})
                            except:
                                pass
                        last_id = mid
        except Exception as e:
            print(f"Error reading stream {stream}: {e}")
            await asyncio.sleep(1)
            
    print(f"Captured {len(messages)} messages from {stream}")
    return messages


async def main():
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    duration = int(os.getenv("CAPTURE_DURATION", "60"))  # Default 60s for testing, use 600-1200 for real golden
    
    r = aioredis.from_url(redis_url)
    
    # 1. Capture Data
    inputs = await capture_stream(r, "signals:of:inputs", duration)
    calib = await capture_stream(r, "signals:calib:effq", duration)
    
    # Save raw captures
    tmp_inputs = "/tmp/of_inputs.ndjson"
    tmp_calib = "/tmp/calib_effq.ndjson"
    
    with open(tmp_inputs, "w") as f:
        for m in inputs:
            f.write(json.dumps(m) + "\n")
            
    with open(tmp_calib, "w") as f:
        for m in calib:
            f.write(json.dumps(m) + "\n")
            
    # 2. Normalize Live Stream
    norm_calib = "tests/data/calib_effq_norm.ndjson"
    subprocess.run(["python3", "tools/calib_normalize.py", "--in", tmp_calib, "--out", norm_calib], check=True)
    
    # 3. Replay from Inputs
    replay_out = "/tmp/calib_replay.ndjson"
    subprocess.run(["python3", "tools/calib_replay_from_inputs.py", "--inputs", tmp_inputs, "--out", replay_out, "--min_samples", "300"], check=True)
    
    # 4. Normalize Replay
    norm_replay = "tests/data/calib_effq_replay_norm.ndjson"
    subprocess.run(["python3", "tools/calib_normalize.py", "--in", replay_out, "--out", norm_replay], check=True)
    
    # 5. Run Golden Test
    result = subprocess.run(["pytest", "tests/test_calib_golden.py", "-v"], capture_output=True, text=True)
    
    success = result.returncode == 0
    status_emoji = "✅" if success else "❌"
    
    report = f"""{status_emoji} **Golden Calibration Report**

Captured: {duration}s
Inputs: {len(inputs)}
Calib Events: {len(calib)}

**Test Result:**
```
{result.stdout[-500:]}
```
"""
    
    print(report)
    
    # 6. Send to Telegram
    try:
        tg_stream = "notify:telegram"
        await r.xadd(tg_stream, {"chat_id": os.getenv("TG_CHAT_ID", ""), "text": report}, maxlen=50000)
        print("Report sent to Telegram")
    except Exception as e:
        print(f"Failed to send to Telegram: {e}")
        
    await r.close()

if __name__ == "__main__":
    asyncio.run(main())
