import json
from infra.database import DBManager

def run():
    db = DBManager()
    rows = db.fetch_all("""
        SELECT order_id, is_virtual, close_reason, tp1_hit, tp1_touched, pnl_net, tp1_price, tp2_price, tp3_price
        FROM trades_closed 
        ORDER BY exit_ts_ms DESC LIMIT 17;
    """)
    for row in rows:
        print(row)

if __name__ == "__main__":
    run()
