import os
import sys
import psycopg2

sys.path.append(os.getcwd())

def apply_schema():
    dsn = (os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN")) or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN")) or "postgresql://postgres:12345@scanner-postgres:5432/trade"
    print(f"Connecting to {dsn}...")
    
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        
        # DDLs from init-postgres.sql
        ddls = [
            """
            CREATE TABLE IF NOT EXISTS calibration_state (
                symbol          TEXT NOT NULL,
                regime          TEXT NOT NULL,
                kind            TEXT NOT NULL, -- 'effq', 'atr', 'dn', 'bookrate'
                ts_ms           BIGINT NOT NULL,
                state_json      JSONB NOT NULL,
                updated_at      TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY(symbol, regime, kind)
            );
            """
            "CREATE INDEX IF NOT EXISTS idx_calibration_state_ts ON calibration_state (ts_ms DESC);",
            """
            CREATE TABLE IF NOT EXISTS microbars (
                symbol          TEXT NOT NULL,
                ts_ms           BIGINT NOT NULL,
                o               DOUBLE PRECISION NOT NULL,
                h               DOUBLE PRECISION NOT NULL,
                l               DOUBLE PRECISION NOT NULL,
                c               DOUBLE PRECISION NOT NULL,
                v               DOUBLE PRECISION NOT NULL,
                cvd             DOUBLE PRECISION NOT NULL,
                inserted_at     TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY(symbol, ts_ms)
            );
            """
            "CREATE INDEX IF NOT EXISTS idx_microbars_ts ON microbars (ts_ms DESC);"
        ]
        
        for ddl in ddls:
            print(f"Executing: {ddl.strip().splitlines()[0]}...")
            cur.execute(ddl)
            
        print("✅ Schema applied successfully.")
        conn.close()
    except Exception as e:
        print(f"❌ Failed to apply schema: {e}")
        sys.exit(1)

if __name__ == "__main__":
    apply_schema()
