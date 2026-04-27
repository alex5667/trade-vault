from utils.time_utils import get_ny_time_millis
# -*- coding: utf-8 -*-
"""
Hourly runner (inside container) with Redis SETNX lock.
Runs:
  1) export_trade_closed_ndjson.py (last 7d)
  2) tm_policy_tuner.py --write-proposals
  3) send short Telegram summary

Schedule: aligns to next full hour.
Lock: prevents double-run if multiple containers started.
"""
import os
import time
import json
import subprocess
import redis
from typing import Tuple

from tools.telegram_send import send_text

def _now_ms() -> int:
    return get_ny_time_millis()

def _sleep_to_next_hour() -> None:
    now = time.time()
    # next hour boundary
    nxt = (int(now) // 3600 + 1) * 3600
    time_to_sleep = max(1, int(nxt - now))
    print(f"Sleeping {time_to_sleep}s until next hour...")
    time.sleep(time_to_sleep)

def _run(cmd: str) -> Tuple[int, str]:
    print(f"Running: {cmd}")
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    out, _ = p.communicate()
    return int(p.returncode or 0), str(out or "")

def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.from_url(redis_url, decode_responses=True)
    lock_key = os.getenv("AB_EVAL_LOCK_KEY", "lock:ab_winner_eval:v1")
    lock_ttl = int(os.getenv("AB_EVAL_LOCK_TTL_SEC", "3300"))  # ~55 min
    since_h = int(os.getenv("AB_EVAL_SINCE_HOURS", "168"))
    out_path = os.getenv("AB_EVAL_NDJSON_OUT", "/tmp/closed_7d.ndjson")
    
    # Optional start immediately flag
    if os.getenv("AB_EVAL_RUN_NOW", "0") == "1":
        print("AB_EVAL_RUN_NOW=1, skipping first sleep.")
    else:
        _sleep_to_next_hour()

    while True:
        # SETNX lock
        if not r.set(lock_key, str(_now_ms()), nx=True, ex=lock_ttl):
            print("Lock already held, skipping this hour.")
            _sleep_to_next_hour()
            continue
            
        try:
            rc1, o1 = _run(f'PYTHONPATH=".:.." python tools/export_trade_closed_ndjson.py --since-hours {since_h} --out {out_path}')
            rc2, o2 = _run(f'PYTHONPATH=".:.." python tools/tm_policy_tuner.py --input {out_path} --window-days 7 --write-proposals')
            
            # Telegram: compact status
            msg = "AB winner evaluator (hourly)\n"
            msg += f"export rc={rc1}\n"
            msg += f"tuner rc={rc2}\n"
            
            # include winners count if present
            try:
                # Find the last line that looks like JSON or contains the result
                last_line = o2.strip().splitlines()[-1]
                if last_line.startswith("{") and last_line.endswith("}"):
                    j = json.loads(last_line)
                else:
                    j = None
            except Exception:
                j = None
                
            if isinstance(j, dict) and "proposals_written" in j:
                msg += f"proposals_written={j['proposals_written']}\n"
            elif isinstance(j, dict) and "winners" in j:
                msg += f"winners_found={len(j['winners'])}\n"
                
            send_text(msg)
        except Exception as e:
            print(f"Error in evaluator loop: {e}")
            send_text(f"AB winner evaluator ERROR: {e}")
        finally:
            # let lock expire naturally (safer under crash)
            pass
        
        _sleep_to_next_hour()

if __name__ == "__main__":
    main()
