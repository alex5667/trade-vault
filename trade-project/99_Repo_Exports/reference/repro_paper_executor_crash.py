
import sys
import os
import json
from unittest.mock import MagicMock, patch

# Mock redis, pandas, pyarrow before import
sys.modules["redis"] = MagicMock()
sys.modules["redis.exceptions"] = MagicMock()
sys.modules["pandas"] = MagicMock()
sys.modules["pyarrow"] = MagicMock()
sys.modules["pyarrow.parquet"] = MagicMock()

# Ensure python-worker is in path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Mock environment variables
os.environ["REDIS_URL"] = "redis://mock:6379/0"
os.environ["PAPER_SYMBOLS_MODE"] = "manual" # Avoid auto-discovery scanning
os.environ["SYMBOLS"] = "BTCUSDT"

def run_test():
    with patch("redis.Redis") as mock_redis_cls:
        mock_redis = MagicMock()
        mock_redis_cls.from_url.return_value = mock_redis
        
        # Mock scan to return nothing (if auto mode was on)
        mock_redis.scan.return_value = (0, [])
        
        print("Importing PaperExecutor...")
        try:
            from paper_executor import PaperExecutor
        except ImportError as e:
            print(f"Failed to import PaperExecutor: {e}")
            return

        print("Initializing executor...")
        executor = PaperExecutor()
        
        # Prepare valid payload BUT missing 'sl'
        payload = {
            "symbol": "BTCUSDT",
            "side": "BUY",
            "entry": 50000.0,
            "lot": 0.1,
            # "sl": 49000.0  <-- MISSING
        }
        
        # Mock BRPOP result: (key, value)
        # We need to set side_effect because we want it to return once then break or we just call _ingest_order once
        mock_redis.brpop.return_value = ("paper:orders", json.dumps(payload))
        
        print(f"Ingesting payload: {json.dumps(payload)}")
        try:
            executor._ingest_order()
            print("Execution finished without crash.")
            
            # Verify if position was created
            if executor.positions:
                pos = list(executor.positions.values())[0]
                print(f"Position created: {pos}")
                print(f"SL value: {pos.sl}")
            else:
                print("No position created.")
                
        except Exception as e:
            print(f"Crashed with: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    run_test()
