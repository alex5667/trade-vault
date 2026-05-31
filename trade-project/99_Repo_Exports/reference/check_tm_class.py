import os
import sys

# Add project root to sys.path
sys.path.append("/app/python-worker")
sys.path.append("/app")

from services.trade_monitor import TradeMonitorService

def check():
    print(f"TradeMonitorService class: {TradeMonitorService}")
    # Print the source of __init__ if possible
    import inspect
    try:
        print(f"Init source starts at line: {inspect.getsourcelines(TradeMonitorService.__init__)[1]}")
    except:
        print("Could not get source info")

    redis_url = os.getenv("REDIS_URL")
    monitor = TradeMonitorService(redis_url=redis_url)
    print(f"Final open_positions: {len(monitor.open_positions)}")

if __name__ == "__main__":
    check()
