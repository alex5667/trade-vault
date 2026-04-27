import json
from core.redis_client import get_redis

r = get_redis()
msgs = r.xrevrange("trades:closed", "+", "-", count=200)

shadow_trades = []
for msg_id, payload in msgs:
    if "data" in payload:
        try:
            data = json.loads(payload["data"])
            is_virt = data.get("is_virtual", False)
            if is_virt:
                shadow_trades.append(data)
        except Exception as e:
            pass

print(f"Found {len(shadow_trades)} VIRTUAL/SHADOW trades.")
if shadow_trades:
    for t in shadow_trades[:10]:
        print(f"ID: {t.get('order_id')} Symbol: {t.get('symbol')} Reason: {t.get('close_reason')} Fees: {t.get('fees')} Fees_USD: {t.get('fees_usd')}")

