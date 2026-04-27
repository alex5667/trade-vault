import redis
import json

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Get the last 10 closed trades from the stream
entries = r.xrevrange("trades:closed", count=10)

print(f"{'Order ID':<40} {'Symbol':<10} {'Lot':<10} {'Entry':<10} {'Notional':<10}")
for entry_id, fields in entries:
    oid = fields.get("order_id", fields.get("id", "N/A"))
    symbol = fields.get("symbol", "N/A")
    lot = fields.get("lot", "0")
    entry = fields.get("entry_price", "0")
    notional = fields.get("notional_usd", "0")
    print(f"{oid:<40} {symbol:<10} {lot:<10} {entry:<10} {notional:<10}")

# Also check a few order hashes
print("\nOrder hashes:")
for entry_id, fields in entries:
    oid = fields.get("order_id", fields.get("id"))
    if oid:
        data = r.hgetall(f"order:{oid}")
        print(f"Order {oid}: {data.get('notional_usd', 'N/A')} USDT (lot: {data.get('lot')}, entry: {data.get('entry_price')})")
