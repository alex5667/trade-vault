#!/usr/bin/env python3
"""Fast DN gate veto analysis."""
import psycopg2, os, json

dsn = os.getenv('ANALYTICS_DB_DSN') or f"postgresql://trading:{os.getenv('TRADING_PASSWORD', 'trading_password')}@scanner-postgres:5432/scanner_analytics"
conn = psycopg2.connect(dsn, connect_timeout=10)
cur = conn.cursor()

all_syms = {'BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT','XRPUSDT','AVAXUSDT','LINKUSDT',
            'INJUSDT','TAOUSDT','ENAUSDT','JUPUSDT','WLDUSDT','VIRTUALUSDT','XAGUSDT',
            '1000FLOKIUSDT','1000BONKUSDT','1000PEPEUSDT','1000SHIBUSDT','DOGEUSDT','WIFUSDT'}

# 1. Signals last 6h
print("=== SIGNALS (6h) ===")
cur.execute("SELECT symbol, COUNT(*) FROM signals WHERE ts_signal >= NOW()-'6h'::interval GROUP BY symbol ORDER BY COUNT(*) DESC")
sig = dict(cur.fetchall())
for s in sorted(all_syms):
    c = sig.get(s, 0)
    mark = "❌" if c == 0 else "✅"
    print(f"  {mark} {s}: {c}")

# 2. Trades closed last 24h
print("\n=== TRADES_CLOSED (24h) ===")
cur.execute("SELECT symbol, COUNT(*) FROM trades_closed WHERE exit_ts >= NOW()-'24h'::interval GROUP BY symbol ORDER BY COUNT(*) DESC")
trd = dict(cur.fetchall())
for s in sorted(all_syms):
    c = trd.get(s, 0)
    mark = "❌" if c == 0 else "✅"
    print(f"  {mark} {s}: {c}")

# 3. Decision snapshot last 6h
print("\n=== DECISION_SNAPSHOT (6h) ===")
six_h_ms = 6*3600*1000
cur.execute(f"SELECT symbol, COUNT(*) FROM decision_snapshot WHERE ts_decision_ms >= (EXTRACT(EPOCH FROM NOW())*1000 - {six_h_ms})::bigint GROUP BY symbol ORDER BY COUNT(*) DESC")
dec = dict(cur.fetchall())
for s in sorted(all_syms):
    c = dec.get(s, 0)
    mark = "❌" if c == 0 else "✅"
    print(f"  {mark} {s}: {c}")

# 4. Calibration state - DN
print("\n=== CALIBRATION_STATE (dn) ===")
cur.execute("SELECT symbol, kind, LEFT(state_json::text, 250), updated_at FROM calibration_state WHERE kind ILIKE '%dn%' ORDER BY symbol LIMIT 60")
for row in cur.fetchall():
    print(f"  {row[0]} [{row[1]}]: {row[2]} (updated: {row[3]})")

# 5. Summary
no_sig = sorted(all_syms - set(sig.keys()))
no_trd = sorted(all_syms - set(trd.keys()))
print(f"\n=== SUMMARY ===")
print(f"Symbols with 0 signals (6h):  {no_sig}")
print(f"Symbols with 0 trades (24h): {no_trd}")

# 6. For symbols with signals but 0 trades - check if DN gate is the blocker
# Look at edge_gate_events or of_gate_metrics
print("\n=== OF_GATE_METRICS (last 6h, vetoed) ===")
try:
    cur.execute("""
        SELECT symbol, reason_code AS gate_name, ok AS result, COUNT(*) 
        FROM of_gate_metrics 
        WHERE ts >= NOW()-'6h'::interval 
          AND reason_code ILIKE '%dn%'
        GROUP BY symbol, reason_code, ok
        ORDER BY symbol, ok
    """)
    for row in cur.fetchall():
        print(f"  {row[0]} | {row[1]} | {row[2]} | cnt={row[3]}")
except Exception as e:
    print(f"  of_gate_metrics error: {e}")

print("\n=== EDGE_GATE_EVENTS (last 6h, dn veto) ===")
try:
    cur.execute("""
        SELECT symbol, gate_name, passed, COUNT(*) 
        FROM edge_gate_events
        WHERE created_at >= NOW()-'6h'::interval
          AND gate_name ILIKE '%dn%'
        GROUP BY symbol, gate_name, passed
        ORDER BY symbol, passed
    """)
    for row in cur.fetchall():
        print(f"  {row[0]} | {row[1]} | {row[2]} | cnt={row[3]}")
except Exception as e:
    print(f"  edge_gate_events error: {e}")

conn.close()
print("\nDone")
