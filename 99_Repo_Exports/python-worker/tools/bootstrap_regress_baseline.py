from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

"""
Bootstrap regression baseline from existing Redis inputs.

Usage:
  python -m tools.bootstrap_regress_baseline
"""

import json
import logging
import os
import subprocess
import sys
import time

import redis

# Setup basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("BootstrapBaseline")

def main():
    # 1. Config
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    baseline_dir = os.getenv("BASELINE_DIR", "/app/of_reports_baselines")

    # We reuse existing tool logic where possible via subprocess,
    # ensuring we use same environment variables.

    baseline_inputs_path = os.path.join(baseline_dir, "inputs_canary.ndjson")
    baseline_output_path = os.path.join(baseline_dir, "baseline.ndjson")

    logger.info(f"Connecting to Redis at {redis_url}")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    # Retry loop for Redis readiness
    max_retries = 30
    for i in range(max_retries):
        try:
            r.ping()
            logger.info("Redis is ready.")
            break
        except redis.exceptions.BusyLoadingError:
            logger.warning(f"Redis is loading data... (attempt {i+1}/{max_retries})")
            time.sleep(2)
        except Exception as e:
            if i == max_retries - 1:
                logger.error(f"Failed to connect to Redis after retries: {e}")
                sys.exit(1)
            logger.warning(f"Waiting for Redis connection... ({e})")
            time.sleep(2)

    # 2. Check if baseline already exists and is fresh enough
    max_age_days = float(os.getenv("BASELINE_MAX_AGE_DAYS", "30") or 30)
    force = int(os.getenv("BASELINE_FORCE_REFRESH", "0") or 0)
    file_exists = os.path.exists(baseline_inputs_path) and os.path.getsize(baseline_inputs_path) > 0
    file_age_days = 0.0
    if file_exists:
        file_age_days = (time.time() - os.path.getmtime(baseline_inputs_path)) / 86400
    stale = file_exists and file_age_days > max_age_days
    if file_exists and not stale and not force:
        logger.info(f"Baseline inputs exist and are fresh ({file_age_days:.1f}d old, max={max_age_days}d). Skipping export.")
    else:
        logger.info("Baseline inputs missing. Exporting from Redis...")
        os.makedirs(baseline_dir, exist_ok=True)

        # Use propose_baseline_update's export logic by importing or reimplementing
        # Re-implementing simplified version here to be self-contained or better yet, use the code from propose_baseline_update
        # actually, let's use the code we saw in propose_baseline_update.py:export_inputs

        stream = os.getenv("OF_INPUTS_STREAM", RS.OF_INPUTS)
        field = os.getenv("OF_INPUTS_STREAM_FIELD", "payload")
        symbols = {s.strip().upper() for s in os.getenv("CANARY_SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()}

        logger.info(f"Exporting from {stream} for symbols {symbols}...")

        scanned = 0
        written = 0
        max_scan = 500000
        max_write = 50000 # Cap size
        last_id = "+"
        rows = []

        while scanned < max_scan and written < max_write:
            batch = r.xrevrange(stream, max=last_id, min="-", count=2000)
            if not batch:
                break
            if len(batch) == 1 and batch[0][0] == last_id:
                break
            for msg_id, fields in batch:
                scanned += 1
                if msg_id == last_id:
                    continue
                last_id = msg_id

                payload = fields.get(field)
                if not payload:
                    continue
                try:
                    inp = json.loads(payload) if isinstance(payload, str) else json.loads(payload.decode("utf-8"))
                except Exception:
                    continue

                sym = (inp.get("symbol", "")).upper()
                if sym and sym in symbols:
                    rows.append(inp)
                    written += 1
                    if written >= max_write:
                        break

        rows.reverse() # Oldest first
        if not rows:
            logger.error("No inputs found in Redis! Cannot bootstrap.")
            sys.exit(1)

        logger.info(f"Found {len(rows)} inputs. Writing to {baseline_inputs_path}...")
        with open(baseline_inputs_path, "w", encoding="utf-8") as f:
            for x in rows:
                f.write(json.dumps(x, ensure_ascii=False) + "\n")

    # 3. Generate Output (Replay)
    out_exists = os.path.exists(baseline_output_path) and os.path.getsize(baseline_output_path) > 0
    if out_exists and not stale and not force:
        logger.info(f"Baseline output already exists at {baseline_output_path}. Skipping replay.")
    else:
        logger.info(f"Generating baseline output to {baseline_output_path}...")
        try:
             subprocess.check_call([
                sys.executable, "-m", "tools.of_engine_replay_from_inputs",
                "--inputs", baseline_inputs_path,
                "--out", baseline_output_path
            ])
        except subprocess.CalledProcessError as e:
            logger.error(f"Replay failed: {e}")
            sys.exit(1)

    # 4. Reset/Seed Status Keys
    # This unlocks the automated propose_baseline_update loop
    logger.info("Seeding regression status keys in Redis...")
    streak_key = os.getenv("REGRESS_PASS_STREAK_KEY", "sre:regress:pass_streak")
    last_status_key = os.getenv("REGRESS_LAST_STATUS_KEY", "sre:regress:last_status")
    last_ts_key = os.getenv("REGRESS_LAST_TS_KEY", "sre:regress:last_ts_ms")

    r.set(streak_key, "1")
    r.set(last_status_key, "PASS")
    r.set(last_ts_key, str(get_ny_time_millis()))

    logger.info("Done! Bootstrap complete.")

if __name__ == "__main__":
    main()
