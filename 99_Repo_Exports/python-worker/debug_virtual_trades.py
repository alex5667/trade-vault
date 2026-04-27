import os
import redis
import psycopg2
from psycopg2.extras import RealDictCursor
import datetime

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DB_DSN = os.getenv("TRADES_DB_DSN", "postgresql://postgres:postgres@localhost:5432/scanner_analytics")

def check_redis():
    print("--- Redis Check ---")
    r = redis.from_url(REDIS_URL, decode_responses=True)
    try:
        keys = r.keys("*")
        print(f"Total keys: {len(keys)}")
        
        # Check specific keys
        for pattern in ["orders:open", "paper:*", "active_trade:*", "order:*"]:
            found = r.keys(pattern)
            print(f"Pattern {pattern}: {len(found)} keys found")
            if found and len(found) < 10:
                print(f"  Keys: {found}")

        # Check stream lengths
        for stream in ["trades:closed", "events:trades", "paper:orders", "paper:deals"]:
            try:
                length = r.xlen(stream)
                print(f"Stream {stream} length: {length}")
                if length > 0:
                    msgs = r.xrevrange(stream, count=5)
                    print(f"  Last 5 msgs in {stream}: {msgs}")
            except Exception as e:
                print(f"Error checking stream {stream}: {e}")
    except Exception as e:
        print(f"Redis error: {e}")

def check_db():
    print("\n--- DB Check ---")
    try:
        conn = psycopg2.connect(DB_DSN)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check table existence and columns
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = [row['table_name'] for row in cur.fetchall()]
            print(f"Tables: {tables}")
            
            if 'trades_closed' in tables:
                cur.execute("SELECT count(*) FROM trades_closed")
                count = cur.fetchone()['count']
                print(f"Total trades_closed: {count}")
                
                # Query for last hour
                one_hour_ago = datetime.datetime.now() - datetime.timedelta(hours=1)
                # entry_ts_ms is bigint
                one_hour_ago_ms = int(one_hour_ago.timestamp() * 1000)
                
                cur.execute("""
                    SELECT order_id, sid, symbol, strategy, entry_ts_ms, exit_ts_ms, pnl_net, status 
                    FROM trades_closed 
                    WHERE entry_ts_ms > %s 
                    ORDER BY entry_ts_ms DESC
                """, (one_hour_ago_ms,))
                recent_trades = cur.fetchall()
                print(f"Trades in last hour: {len(recent_trades)}")
                for t in recent_trades:
                    print(f"  {t['order_id']} | {t['symbol']} | {t['strategy']} | {t['status']} | {t['pnl_net']}")
            else:
                print("Table trades_closed not found")
    except Exception as e:
        print(f"DB error: {e}")

if __name__ == "__main__":
    check_redis()
    check_db()
