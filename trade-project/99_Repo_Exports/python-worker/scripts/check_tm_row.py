import os
import sys

# Add project root to sys.path
sys.path.append("/app/python-worker")
sys.path.append("/app")

from services.trade_monitor import TradeMonitorService


def check():
    redis_url = os.getenv("REDIS_URL")
    monitor = TradeMonitorService(redis_url=redis_url)

    print("Manual recovery check:")
    rows = monitor.repo.load_open_positions(limit=10)
    print(f"Loaded {len(rows)} rows from repo")

    if not rows:
        return

    h = rows[0]
    print(f"Checking row 0: ID={h.get('id')}, status={h.get('status')}")

    # Trace _position_from_hash
    print("Tracing _position_from_hash...")
    try:
        if h.get("status") != "open":
            print("FAILED: status is not open")
        else:
            print("Status is open. Attempting PositionState init...")
            pos = monitor._position_from_hash(h)
            if pos:
                print(f"SUCCESS: pos.id={pos.id}")
                # Check if it's in open_positions
                monitor.open_positions[pos.id] = pos
                print(f"Open positions count: {len(monitor.open_positions)}")
            else:
                print("FAILED: _position_from_hash returned None")
    except Exception as e:
        print(f"EXCEPTION: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check()
