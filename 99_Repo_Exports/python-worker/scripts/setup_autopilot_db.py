#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import psycopg2

DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5432/scanner_analytics"
TRADES_DB_DSN = os.getenv("TRADES_DB_DSN", DEFAULT_DSN)

SQL = """
CREATE TABLE IF NOT EXISTS autopilot_proposals (
    sid TEXT PRIMARY KEY
    ts TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    group_name TEXT
    symbol TEXT
    regime TEXT
    scenario TEXT
    winner_arm TEXT
    edge_lcb_r FLOAT
    proposal_json JSONB
    status TEXT DEFAULT 'proposed'
);

CREATE INDEX IF NOT EXISTS idx_autopilot_proposals_status ON autopilot_proposals(status);
CREATE INDEX IF NOT EXISTS idx_autopilot_proposals_ts ON autopilot_proposals(ts);
"""

def main():
    print(f"Connecting to {TRADES_DB_DSN}...")
    try:
        conn = psycopg2.connect(TRADES_DB_DSN)
        cur = conn.cursor()
        cur.execute(SQL)
        conn.commit()
        print("✅ Autopilot tables initialized successfully.")
    except Exception as e:
        print(f"❌ Failed to initialize tables: {e}")

if __name__ == "__main__":
    main()
