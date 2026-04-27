#!/usr/bin/env python3
"""Check DN Gate veto rates per symbol."""
import redis
import psycopg2
import os
import json

def main():
    # Redis
    redis_url = os.getenv('REDIS_URL') or os.getenv('ML_REDIS_URL', '')
    try:
        r = redis.from_url(redis_url, decode_responses=True, socket_timeout=5, socket_connect_timeout=5)
        r.ping()
        print("=== Redis connected ===")
    except Exception as e:
        print(f"Redis failed: {e}")
        r = None

    # Symbols
    sym_str = os.getenv('AUTO_CALIBRATION_SYMBOLS', '')
    if not sym_str:
        sym_str = os.getenv('CRYPTO_SYMBOLS', '')
    all_syms = [s.strip() for s in sym_str.split(',') if s.strip()]
    
    # Add meme symbols from Redis sets
    meme_syms = set()
    if r:
        for key in ['crypto:symbols:meme', 'crypto:symbols:meme2', 'crypto:symbols:meme3']:
            try:
                members = r.smembers(key)
                if members:
                    meme_syms.update(members)
            except:
                pass
    all_syms = list(set(all_syms) | meme_syms)
    print(f"All symbols ({len(all_syms)}): {sorted(all_syms)}\n")

    # Redis: DN calibration  
    if r:
        print("=== DN Calibration State (Redis) ===")
        for sym in sorted(all_syms):
            found = False
            for prefix in ['calib:tick_dn', 'calib:dn', 'cfg:dn_calib', 'calibration:tick_dn', 'calibration:dn']:
                try:
                    val = r.get(f'{prefix}:{sym}')
                    if val:
                        print(f"  {sym} [{prefix}]: {val[:300]}")
                        found = True
                        break
                except:
                    pass
            if not found:
                # Try hash
                for hk in ['calibration_state', 'calib_state']:
                    for sk in [f'tick_dn:{sym}', f'dn:{sym}', sym]:
                        try:
                            val = r.hget(hk, sk)
                            if val:
                                print(f"  {sym} [{hk}/{sk}]: {val[:300]}")
                                found = True
                                break
                        except:
                            pass
                    if found:
                        break
            if not found:
                print(f"  {sym}: NO CALIB DATA")

        # Check DN gate passrate keys
        print("\n=== DN Gate scan keys ===")
        try:
            keys = list(r.scan_iter("*dn*", count=200))
            dn_keys = [k for k in keys if 'dn' in k.lower()]
            for k in sorted(dn_keys)[:50]:
                t = r.type(k)
                if t == 'string':
                    v = r.get(k)
                    print(f"  {k} ({t}): {str(v)[:200]}")
                elif t == 'hash':
                    sz = r.hlen(k)
                    print(f"  {k} ({t}): {sz} fields")
                else:
                    print(f"  {k} ({t})")
        except Exception as e:
            print(f"  Scan error: {e}")

    # Postgres
    dsn = os.getenv('TIMESCALE_DSN', '')
    if not dsn:
        dsn = "host=scanner-postgres port=5432 dbname=scanner_analytics user=scanner password="
    
    try:
        conn = psycopg2.connect(dsn, connect_timeout=10)
        cur = conn.cursor()

        # decision_snapshot uses ts_decision_ms (bigint epoch ms)
        print("\n=== Decision Snapshots per symbol (last 6h) ===")
        cur.execute("""
            SELECT symbol, COUNT(*) as cnt
            FROM decision_snapshot
            WHERE ts_decision_ms >= (EXTRACT(EPOCH FROM NOW()) * 1000 - 6*3600*1000)::bigint
            GROUP BY symbol
            ORDER BY cnt DESC
        """)
        rows = cur.fetchall()
        dec_syms = set()
        if rows:
            for r in rows:
                print(f"  {r[0]}: {r[1]} decisions")
                dec_syms.add(r[0])
        else:
            print("  No decision snapshots in last 6h")

        no_dec = set(all_syms) - dec_syms
        if no_dec:
            print(f"\n  ⚠️ Symbols with NO decisions (6h): {sorted(no_dec)}")

        # Signals
        print("\n=== Signals per symbol (last 6h) ===")
        cur.execute("""
            SELECT symbol, COUNT(*) as cnt
            FROM signals
            WHERE ts_signal >= NOW() - INTERVAL '6 hours'
            GROUP BY symbol
            ORDER BY cnt DESC
        """)
        rows2 = cur.fetchall()
        sig_syms = set()
        if rows2:
            for r in rows2:
                print(f"  {r[0]}: {r[1]} signals")
                sig_syms.add(r[0])
        else:
            print("  No signals in last 6h")

        no_sig = set(all_syms) - sig_syms
        if no_sig:
            print(f"\n  ⚠️ Symbols with NO signals (6h): {sorted(no_sig)}")

        # trades_closed
        print("\n=== Trades closed per symbol (last 24h) ===")
        cur.execute("""
            SELECT symbol, COUNT(*) as cnt
            FROM trades_closed
            WHERE exit_ts >= NOW() - INTERVAL '24 hours'
            GROUP BY symbol
            ORDER BY cnt DESC
        """)
        rows3 = cur.fetchall()
        trade_syms = set()
        if rows3:
            for r in rows3:
                print(f"  {r[0]}: {r[1]} trades")
                trade_syms.add(r[0])
        else:
            print("  No closed trades in last 24h")

        no_trades = set(all_syms) - trade_syms
        if no_trades:
            print(f"\n  ⚠️ Symbols with NO trades (24h): {sorted(no_trades)}")

        # Cross reference: symbols with decisions/signals but 0 trades
        # These are likely being killed by the DN gate or other gates
        dec_no_trade = dec_syms - trade_syms
        if dec_no_trade:
            print(f"\n=== Symbols with decisions but NO trades (DN or later gate kills them): {sorted(dec_no_trade)}")

        conn.close()
    except Exception as e:
        print(f"Postgres error: {e}")
        import traceback; traceback.print_exc()

    print("\nDone.")

if __name__ == "__main__":
    main()
