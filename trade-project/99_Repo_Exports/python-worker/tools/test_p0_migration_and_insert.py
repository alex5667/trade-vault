import os
import sys

from utils.time_utils import get_ny_time_millis

print("Script started", flush=True)

# Add python-worker to path
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Set DSN for local testing (assuming localhost access)
if not (os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN")):
    os.environ["TRADES_DB_DSN"] = "postgresql://trading:trading_password@127.0.0.1:5432/scanner_analytics"

try:
    from services.analytics_db import get_conn, save_trade_closed
except ImportError as e:
    print(f"Could not import analytics_db. Make sure you are running from python-worker root. Error: {e}", flush=True)
    sys.exit(1)

# Mock TradeClosed object
class MockTradeClosed:
    def __init__(self, order_id, exit_ts_ms, features=None):
        self.order_id = order_id
        self.exit_ts_ms = exit_ts_ms
        self.entry_ts_ms = exit_ts_ms - 60000

        # Required fields for main table
        self.sid = "test_sid"
        self.strategy = "test_strat"
        self.source = "test_src"
        self.symbol = "TESTUSD"
        self.tf = "1m"
        self.direction = "long"
        self.entry_price = 100.0
        self.exit_price = 101.0
        self.lot = 1.0
        self.notional_usd = 100.0
        self.pnl_net = 1.0
        self.pnl_gross = 1.0
        self.fees = 0.0
        self.pnl_pct = 1.0
        self.pnl_if_fixed_exit = 0.0
        self.tp1_hit = False
        self.tp2_hit = False
        self.tp3_hit = False
        self.tp_hits = 0
        self.tp_before_sl = False
        self.trailing_started = False
        self.trailing_active = False
        self.trailing_moves = 0
        self.mfe_pnl = 1.0
        self.mae_pnl = 0.0
        self.giveback = 0.0
        self.missed_profit = 0.0
        self.one_r_money = 1.0
        self.r_multiple = 1.0
        self.duration_ms = 60000
        self.close_reason = "TP"

        # P0 Fields
        self.scenario = "trend_pullback"
        self.regime = "bull_trend"
        self.session = "london"
        self.entry_reason = "signal"
        self.mae_bps = 5.0
        self.mfe_bps = 15.0
        self.time_to_mfe_ms = 30000
        self.hold_ms = 60000
        self.spread_bps_at_entry = 1.0
        self.slippage_bps_est = 0.5
        self.book_age_ms = 100

        self.features = features or {"f1": 0.5, "f2": "val"}

def main():
    print("--- Starting P0 Integration Test ---")

    # 1. Apply Migration (Simulate)
    migration_file = os.path.join(os.path.dirname(__file__), "..", "migrations", "007_create_trades_closed_p0.sql")
    if not os.path.exists(migration_file):
        print(f"Migration file not found: {migration_file}")
        return

    with open(migration_file) as f:
        sql_migration = f.read()

    print("Applying migration...")
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql_migration)
            conn.commit()
        print("Migration applied successfully.")
    except Exception as e:
        print(f"Migration failed (maybe already exists): {e}")

    # 2. Insert Test Trade
    now_ms = get_ny_time_millis()
    order_id = f"test_p0_{now_ms}"

    trade = MockTradeClosed(order_id, now_ms, features={"complex": [1, 2], "score": 99})

    print(f"Inserting trade {order_id}...")
    try:
        save_trade_closed(trade)
        print("Insert successful.")
    except Exception as e:
        print(f"Insert failed: {e}")
        return

    # 3. Verify P0 Data
    print("Verifying data in trades_closed_p0...")
    with get_conn() as conn, conn.cursor() as cur:
        # Check P0 row
        cur.execute("SELECT * FROM trades_closed_p0 WHERE order_id = %s", (order_id,))
        row = cur.fetchone() # returns tuple (or dict if RealDictCursor configured globally?)

        # analytics_db uses RealDictCursor locally in fetch* methods but standard cursor in save_trade_closed context?
        # Let's check type.
        print(f"Row fetched: {row}")
        if not row:
            print("ERROR: Row not found in trades_closed_p0!")
        else:
            # If standard cursor, row is tuple. We need to map manually or use fetching helpers.
            # Let's verify simply by presence.
            print("SUCCESS: P0 row found.")

    # 4. Verify Join Query (from sql/trades_window.sql)
    print("Verifying REPORT JOIN query...")
    sql_path = os.path.join(os.path.dirname(__file__), "trade_diagnostics", "sql", "trades_window.sql")
    if os.path.exists(sql_path):
        with open(sql_path) as f:
            query = f.read()

        # replace params
        query = query.replace(":from_ms", "%(from_ms)s").replace(":to_ms", "%(to_ms)s")

        with get_conn() as conn, conn.cursor() as cur:
            # cur might be standard cursor, let's just see if it runs
            cur.execute(query, {"from_ms": now_ms - 10000, "to_ms": now_ms + 10000})
            rows = cur.fetchall()
            found = False
            for r in rows:
                # r is tuple. finding order_id in it depends on index.
                # order_id is first column usually.
                if str(r[0]) == order_id:
                    found = True
                    print(f"Found in Report Query: {r}")
                    break

            if found:
                print("SUCCESS: Trade found in JOIN query.")
            else:
                print("ERROR: Trade NOT found in JOIN query.")

if __name__ == "__main__":
    main()
