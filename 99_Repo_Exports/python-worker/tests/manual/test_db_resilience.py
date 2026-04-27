from utils.time_utils import get_ny_time_millis
import os
import sys
import logging
import time

# Add python-worker to sys.path if running from project root
sys.path.append(os.getcwd())
# Also try adding /app if running inside container
sys.path.append("/app")

from services.posttrade.decision_snapshot_db import PostgresDecisionSnapshotDB

def test_db_resilience():
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("test_resilience")
    
    # Try to find DSN from environment
    dsn = os.environ.get("DATABASE_URL") or os.environ.get("TRADES_DB_DSN") or os.environ.get("TIMESCALE_DSN")
    if not dsn:
        log.error("DATABASE_URL is not set!")
        return

    log.info("Initializing PostgresDecisionSnapshotDB...")
    db = PostgresDecisionSnapshotDB(dsn=dsn)
    
    # Define a test row
    rows = [
        {
            "ts_decision_ms": get_ny_time_millis(),
            "sid": "test_resilience_sid_" + str(int(time.time())),
            "symbol": "BTCUSDT",
            "venue": "BINANCE",
            "session": "main",
            "tf": "1m",
            "kind": "snapshot",
            "side": "BUY",
            "direction": "LONG",
            "decision_bid": 50000.0,
            "decision_ask": 50001.0,
            "decision_mid": 50000.5,
            "decision_spread_bps": 2.0,
            "decision_depth_bid_5": 10.0,
            "decision_depth_ask_5": 10.0,
            "decision_depth_bid_20": 40.0,
            "decision_depth_ask_20": 40.0,
            "decision_book_slope_bid": 1.0,
            "decision_book_slope_ask": 1.0,
            "decision_dws_bps": 0.5,
            "decision_ofi_norm": 0.1,
            "decision_expected_slippage_bps": 0.5,
            "decision_exec_risk_norm": 0.2,
            "decision_price": 50000.5,
            "tca_ready": True,
            "book_sanity_flags": ["ok"],
            "schema_version": 1,
            "producer": "test_script",
            "ts_insert_ms": get_ny_time_millis(),
            "extra": {"test": True}
        }
    ]

    log.info("Step 1: Get a connection and ensure it's in the pool")
    conn = db._get_connection()
    db._put_connection(conn)
    log.info("Connection placed in pool.")

    log.info("Step 2: Manually close the connection in the pool to simulate server disconnect")
    # This simulates a situation where the pool thinks the connection is fine, but it's actually closed.
    conn.close() 
    log.info("Connection closed manually.")

    log.info("Step 3: Call upsert_decision_snapshots. Expecting automatic recovery...")
    try:
        count = db.upsert_decision_snapshots(rows)
        log.info(f"Success! Upserted {count} rows.")
    except Exception as e:
        log.error(f"Failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    log.info("Step 4: Verification of idempotency (running same batch again)")
    try:
        count = db.upsert_decision_snapshots(rows)
        log.info(f"Success! Re-upserted {count} rows (idempotency check).")
    except Exception as e:
        log.error(f"Idempotency check failed: {e}")
        sys.exit(1)

    log.info("TEST PASSED")

if __name__ == "__main__":
    test_db_resilience()
