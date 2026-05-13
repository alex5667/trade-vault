import os
import sys

# Add project root to sys.path
sys.path.append("/app/python-worker")
sys.path.append("/app")

from services.trade_monitor import TradeMonitorService

def check():
    redis_url = os.getenv("REDIS_URL")
    print(f"Connecting to Redis at {redis_url}")
    
    try:
        monitor = TradeMonitorService(redis_url=redis_url)
        open_positions = monitor.open_positions
        
        virtual_positions = [pos for pos in open_positions.values() if getattr(pos, "is_virtual", False)]
        
        # Also check raw repository to be sure
        rows = monitor.repo.load_open_positions(limit=5000)
        virtual_rows = [r for r in rows if r.get("is_virtual") == "1" or r.get("is_virtual") == True]
        
        print("\n=== SUMMARY ===")
        print(f"Total open positions in TradeMonitor: {len(open_positions)}")
        print(f"Open virtual positions in TradeMonitor: {len(virtual_positions)}")
        print(f"Raw open positions in repo: {len(rows)}")
        print(f"Raw virtual positions in repo: {len(virtual_rows)}")
        
        if virtual_positions:
            print("\n=== VIRTUAL POSITIONS (TradeMonitor) ===")
            for pos in virtual_positions[:10]: # Show first 10
                print(f"ID: {pos.id}, Symbol: {pos.symbol}, Direction: {pos.direction}, Entry: {pos.entry_price}")
            if len(virtual_positions) > 10:
                print(f"... and {len(virtual_positions) - 10} more")
                
        if virtual_rows:
            print("\n=== VIRTUAL ROWS (Repo) ===")
            for r in virtual_rows[:10]: # Show first 10
                print(f"ID: {r.get('id')}, Symbol: {r.get('symbol')}, Status: {r.get('status')}")
            if len(virtual_rows) > 10:
                print(f"... and {len(virtual_rows) - 10} more")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check()
