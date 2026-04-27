import os
import time
import logging
import signal
import sys
import redis
from prometheus_client import start_http_server, Gauge, REGISTRY
from collections import defaultdict
from typing import List, Dict, Any, Optional

try:
    from common.redis_errors import is_redis_busy_loading_error, is_transient_error
except ImportError:
    # Fallback if the script is run in an environment where common is not available
    def is_redis_busy_loading_error(e): return "LOADING" in str(e).upper()
    def is_transient_error(e): return is_redis_busy_loading_error(e)

# --- Configuration ---
LOG_FORMAT = '{"time":"%(asctime)s", "level":"%(levelname)s", "msg":"%(message)s"}'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("AutoApplyGuardExporter")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
METRICS_PORT = int(os.getenv("AUTO_APPLY_GUARD_EXPORTER_PORT", "9126"))
WIN1M_PREFIX = os.getenv("AUTO_APPLY_GUARD_METRICS_WIN1M_PREFIX", "metrics:auto_apply_guard:win1m")
WINDOWS_MIN = [int(w) for w in os.getenv("AUTO_APPLY_GUARD_WINDOWS_MIN", "5,10,30,60").split(",")]
REASON_TOPN = int(os.getenv("AUTO_APPLY_GUARD_REASON_TOPN", "10"))

# --- Prometheus Metrics ---
# Gauges for each window size
GAUGE_BLOCKED = Gauge('auto_apply_guard_blocked', 'Blocked runs count', ['window'])
GAUGE_RUN_OK = Gauge('auto_apply_guard_run_ok', 'Successful runs count', ['window'])
GAUGE_RUN_ERR = Gauge('auto_apply_guard_run_err', 'Failed runs count', ['window'])
GAUGE_RUN_TOTAL = Gauge('auto_apply_guard_run_total', 'Total runs count', ['window'])
GAUGE_BLOCKED_RATIO = Gauge('auto_apply_guard_blocked_ratio', 'Ratio of blocked to total runs', ['window'])
GAUGE_EXEC_ERR_RATIO = Gauge('auto_apply_guard_exec_err_ratio', 'Ratio of execution errors to attempts', ['window'])
GAUGE_BLOCKED_REASON = Gauge('auto_apply_guard_blocked_reason', 'Blocked reasons count', ['window', 'reason'])

class AutoApplyGuardExporter:
    def __init__(self, redis_url: str):
        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self.running = True
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self, signum, frame):
        logger.info("Received signal, shutting down...")
        self.running = False

    def get_time_bucket(self, ts: float) -> str:
        """Returns the bucket key suffix for a given timestamp (minute-aligned)"""
        # Bucket is aligned to the minute: YYYYMMDDHHMM
        t = time.gmtime(ts)
        return time.strftime("%Y%m%d%H%M", t)

    def get_buckets_for_window(self, now_ts: float, window_minutes: int) -> List[str]:
        """Returns a list of Redis keys for the last N minutes"""
        buckets = []
        # We look back window_minutes. 
        # For a 5m window, we check buckets for t-0, t-1, t-2, t-3, t-4? 
        # Or rigorously t-window_minutes to t?
        # The recommendation implies "rolling windows". 
        # We will collect the last `window_minutes` buckets ending at current minute (inclusive or exclusive? usually inclusive for latest data).
        # Let's include current minute as it builds up.
        
        for i in range(window_minutes):
            ts = now_ts - (i * 60)
            bucket_ts = self.get_time_bucket(ts)
            buckets.append(f"{self.WIN1M_PREFIX}:{bucket_ts}")
        return buckets

    def collect_metrics(self):
        now = time.time()
        
        # Pre-calculate pipe commands might be complex because keys are dynamic. 
        # But we can iterate windows.
        # Efficient approach: Fetch all unique buckets needed across all windows first?
        # Max window is 60m. So we need last 60 buckets.
        
        max_window = max(WINDOWS_MIN)
        needed_buckets_keys = []
        for i in range(max_window + 1): # +1 buffer
             ts = now - (i * 60)
             bucket_ts = self.get_time_bucket(ts)
             needed_buckets_keys.append(f"{WIN1M_PREFIX}:{bucket_ts}")
        
        # Pipeline fetch all needed buckets
        pipe = self.redis.pipeline()
        for key in needed_buckets_keys:
            pipe.hgetall(key)
        
        try:
            results = pipe.execute()
        except redis.RedisError as e:
            if is_redis_busy_loading_error(e):
                logger.warning(f"Redis is loading the dataset in memory, skipping collection: {e}")
            elif is_transient_error(e):
                logger.warning(f"Redis transient error collecting metrics: {e}")
            else:
                logger.error(f"Redis error collecting metrics: {e}")
            return

        # Map key -> data
        data_map: Dict[str, Dict[str, str]] = {}
        for key, val in zip(needed_buckets_keys, results):
            if val:
                data_map[key] = val
            else:
                data_map[key] = {}

        # Aggregate for each window
        for win in WINDOWS_MIN:
            window_str = f"{win}m"
            
            # Determine keys for this window
            # Window 5m = {t, t-1, ..., t-4}
            window_keys = []
            for i in range(win):
                ts = now - (i * 60)
                bucket_ts = self.get_time_bucket(ts)
                key = f"{WIN1M_PREFIX}:{bucket_ts}"
                window_keys.append(key)

            # Sum counters
            blocked_total = 0
            run_ok_total = 0
            run_err_total = 0
            reasons_counter = defaultdict(int)

            for k in window_keys:
                d = data_map.get(k, {})
                blocked_total += int(d.get('blocked_total', 0))
                run_ok_total += int(d.get('run_ok_total', 0))
                run_err_total += int(d.get('run_err_total', 0))
                
                # Reasons are stored as blocked:<reason>
                for field, val in d.items():
                    if field.startswith('blocked:') and field != 'blocked_total':
                        reason_name = field.replace('blocked:', '', 1)
                        reasons_counter[reason_name] += int(val)

            total_runs = blocked_total + run_ok_total + run_err_total
            total_exec_attempts = run_ok_total + run_err_total

            # Metrics Update
            GAUGE_BLOCKED.labels(window=window_str).set(blocked_total)
            GAUGE_RUN_OK.labels(window=window_str).set(run_ok_total)
            GAUGE_RUN_ERR.labels(window=window_str).set(run_err_total)
            GAUGE_RUN_TOTAL.labels(window=window_str).set(total_runs)

            # Ratios
            if total_runs > 0:
                blocked_ratio = blocked_total / total_runs
            else:
                blocked_ratio = 0.0
            GAUGE_BLOCKED_RATIO.labels(window=window_str).set(blocked_ratio)

            if total_exec_attempts > 0:
                exec_err_ratio = run_err_total / total_exec_attempts
            else:
                exec_err_ratio = 0.0
            GAUGE_EXEC_ERR_RATIO.labels(window=window_str).set(exec_err_ratio)

            # Top N Reasons
            # We must clear old reason metrics or they persist? 
            # Prometheus client doesn't "clear" labels easily. 
            # Ideally we set 0 for reasons not in topN if we want them to disappear, or just update.
            # Best practice for gauges with dynamic labels: use a callback or carefully manage.
            # Simpler: just set current ones. Old ones remain until restart. 
            # For this task, we will just set the top N.
            
            sorted_reasons = sorted(reasons_counter.items(), key=lambda x: x[1], reverse=True)
            if REASON_TOPN > 0:
                sorted_reasons = sorted_reasons[:REASON_TOPN]
            
            for reason, count in sorted_reasons:
                GAUGE_BLOCKED_REASON.labels(window=window_str, reason=reason).set(count)

    def run(self):
        logger.info(f"Starting AutoApplyGuardExporter on port {METRICS_PORT}")
        start_http_server(METRICS_PORT)
        
        while self.running:
            try:
                self.collect_metrics()
            except Exception as e:
                logger.error(f"Error in collection loop: {e}", exc_info=True)
            
            # Update every 30s ? or 10s? 
            # Windows are 1m resolution, so 15s or 30s is fine.
            time.sleep(15)
        
        logger.info("Exporter stopped")

if __name__ == "__main__":
    exporter = AutoApplyGuardExporter(REDIS_URL)
    exporter.run()
