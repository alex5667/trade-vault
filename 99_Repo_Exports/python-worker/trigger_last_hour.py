import sys
import os
import logging
import traceback

os.environ["PERIODIC_REPORT_SEND_EMPTY"] = "1"

logging.basicConfig(level=logging.INFO)

try:
    from services.periodic_reporter import PeriodicReporter, canon_source, canon_symbol

    r = PeriodicReporter()
    src = canon_source('CryptoOrderFlow')
    sym = canon_symbol('ALL')

    lock_key = f"report_lock:{src}:{sym}"
    hourly_key = f"report_last_hourly_hour:{src}:{sym}"

    # Clear locks to ensure immediate send
    r.redis.delete(lock_key)
    r.redis.delete(hourly_key)

    print(f"Triggering report for {src} / {sym} with PERIODIC_REPORT_SEND_EMPTY=1...", flush=True)
    r.send_report_for_pair('CryptoOrderFlow', 'ALL', window_seconds=3600)
    print("Report triggered successfully.", flush=True)
except Exception as e:
    print(f"Error triggering report: {e}", flush=True)
    traceback.print_exc()
