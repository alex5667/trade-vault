import psycopg2
import sys

def alter_db(dsn):
    print(f"Connecting to {dsn}")
    try:
        conn = psycopg2.connect(dsn)
        with conn.cursor() as cur:
            print("Altering signals table...")
            cur.execute("""
                ALTER TABLE signals 
                ADD COLUMN IF NOT EXISTS session TEXT,
                ADD COLUMN IF NOT EXISTS regime TEXT,
                ADD COLUMN IF NOT EXISTS delta_spike_z DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS obi DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS weak_progress BOOLEAN,
                ADD COLUMN IF NOT EXISTS raw_ctx JSONB,
                ADD COLUMN IF NOT EXISTS experiment_id TEXT,
                ADD COLUMN IF NOT EXISTS experiment_variant TEXT;
            """)
        conn.commit()
        print("Success for " + dsn)
    except Exception as e:
        print(f"Error for {dsn}: {e}")

if __name__ == "__main__":
    alter_db("postgresql://trading:trading_password@postgres:5432/scanner_analytics")
    alter_db("postgresql://trading:trading_password@postgres:5432/trade")
