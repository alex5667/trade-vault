import os
import sys
import traceback

# Add project root to sys.path
sys.path.append("/app/python-worker")
sys.path.append("/app")

from services.trade_monitor import TradeMonitorService

def check():
    redis_url = os.getenv("REDIS_URL")
    print(f"Testing TradeMonitorService init with REDIS_URL={redis_url}")
    
    # We want to catch the internal exception in _recover_open_positions
    # Since it is caught and logged as warning, we might need to monkeypatch or just check results
    
    try:
        monitor = TradeMonitorService(redis_url=redis_url)
        print(f"SUCCESS: monitor.open_positions count: {len(monitor.open_positions)}")
        
        if len(monitor.open_positions) == 0:
            print("Zero positions recovered. Checking repo manually...")
            rows = monitor.repo.load_open_positions(limit=5000)
            print(f"repo.load_open_positions returned {len(rows)} rows")
            if rows:
                print("Trying to call _position_from_hash manually on first row...")
                try:
                    pos = monitor._position_from_hash(rows[0])
                    # print(f"Manual _position_from_hash result: {pos}")
                except Exception as e:
                    print(f"Manual _position_from_hash FAILED: {e}")
                    traceback.print_exc()
    except Exception as e:
        print(f"TradeMonitorService init FAILED with exception: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    check()
