import psycopg2
import os

DB_CONFIG = {
    'host': 'localhost',
    'port': 5434,
    'user': 'postgres',
    'password': '12345',
    'database': 'scanner_analytics'
}

def check_fills_upsert():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        sql = """
            INSERT INTO fills (ts, ts_fill_ms, sid, order_id, sym, venue, side, fill_role, px, qty, fee_bps, 
            bid_at_fill, ask_at_fill, mid_at_fill, event_type, event_id, stream_id, ts_insert_ms) 
            VALUES (now(), 1, 's', 'o', 'S', 'V', 'L', 'E', 1.0, 1.0, 0.0, 
            NULL, NULL, NULL, 'T', 'E', 'ST', 1) 
            ON CONFLICT (sid, ts_fill_ms, fill_role, ts) DO UPDATE SET 
            order_id=excluded.order_id, sym=excluded.sym, venue=excluded.venue, side=excluded.side, px=excluded.px, qty=excluded.qty, fee_bps=excluded.fee_bps, 
            bid_at_fill=excluded.bid_at_fill, ask_at_fill=excluded.ask_at_fill, mid_at_fill=excluded.mid_at_fill, 
            event_type=excluded.event_type, event_id=excluded.event_id, stream_id=excluded.stream_id, ts_insert_ms=excluded.ts_insert_ms
        """
        cur.execute("EXPLAIN " + sql)
        print("Fills Valid!")
    except Exception as e:
        print("Fills Error:", e)

def check_tca_upsert():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        sql = """
            INSERT INTO tca_fill_metrics (ts, ts_fill_ms, sid, sym, venue, side, fill_role, 
            decision_ts_ms, session, tf, kind, decision_mid, 
            mid_t, bid_t, ask_t, mid_t_1s, mid_t_5s, 
            eff_spread_bps, realized_spread_1s_bps, realized_spread_5s_bps, perm_impact_1s_bps, perm_impact_5s_bps, is_bps, 
            px, qty, fee_bps, ts_insert_ms) 
            VALUES (now(), 1, 's', 'S', 'V', 'L', 'E', 
            1, 'S', 'T', 'K', 1.0, 
            1.0, 1.0, 1.0, 1.0, 1.0, 
            1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 
            1.0, 1.0, 1.0, 1) 
            ON CONFLICT (sid, ts_fill_ms, fill_role, ts) DO UPDATE SET 
            decision_ts_ms=excluded.decision_ts_ms, session=excluded.session, tf=excluded.tf, kind=excluded.kind, decision_mid=excluded.decision_mid, 
            mid_t=excluded.mid_t, bid_t=excluded.bid_t, ask_t=excluded.ask_t, mid_t_1s=excluded.mid_t_1s, mid_t_5s=excluded.mid_t_5s, 
            eff_spread_bps=excluded.eff_spread_bps, realized_spread_1s_bps=excluded.realized_spread_1s_bps, realized_spread_5s_bps=excluded.realized_spread_5s_bps, 
            perm_impact_1s_bps=excluded.perm_impact_1s_bps, perm_impact_5s_bps=excluded.perm_impact_5s_bps, is_bps=excluded.is_bps, 
            px=excluded.px, qty=excluded.qty, fee_bps=excluded.fee_bps, ts_insert_ms=excluded.ts_insert_ms
        """
        cur.execute("EXPLAIN " + sql)
        print("TCA Valid!")
    except Exception as e:
        print("TCA Error:", e)

check_fills_upsert()
check_tca_upsert()
