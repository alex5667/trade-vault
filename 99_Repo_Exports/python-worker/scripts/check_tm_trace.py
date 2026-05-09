import os
import sys

# Add project root to sys.path
sys.path.append("/app/python-worker")
sys.path.append("/app")

from services.trade_monitor import TradeMonitorService


def check():
    redis_url = os.getenv("REDIS_URL")
    print(f"Testing TradeMonitorService lifecycle tracing with REDIS_URL={redis_url}")

    # We will subclass TradeMonitorService to intercept calls
    class TracingMonitor(TradeMonitorService):
        def __init__(self, *args, **kwargs):
            print("INIT: Starting", flush=True)
            super().__init__(*args, **kwargs)
            print(f"INIT: Finished, final count: {len(self.open_positions)}", flush=True)

        def _recover_open_positions(self):
            print("RECOVER: Starting", flush=True)
            super()._recover_open_positions()
            print(f"RECOVER: Finished, count: {len(self.open_positions)}", flush=True)

        def _housekeep_expired_positions(self, now_ms, current_symbol=None):
            print(f"HOUSEKEEP: Starting, before count: {len(self.open_positions)}", flush=True)
            super()._housekeep_expired_positions(now_ms, current_symbol)
            print(f"HOUSEKEEP: Finished, after count: {len(self.open_positions)}", flush=True)

    try:
        monitor = TracingMonitor(redis_url=redis_url)
    except Exception:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check()
