#!/usr/bin/env python3
"""Quick script to check recent closed trades from Redis stream"""
from datetime import datetime, timedelta

import redis
from core.redis_keys import RedisStreams as RS

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Calculate one hour ago timestamp in milliseconds
now_utc = datetime(2026, 1, 14, 8, 58, 15)  # Current UTC time
hour_ago = now_utc - timedelta(hours=1)
hour_ago_ms = int(hour_ago.timestamp() * 1000)

print(f"Current time (UTC): {now_utc}")
print(f"Checking trades since: {hour_ago} (timestamp: {hour_ago_ms})")
print("=" * 80)

# Read last 100 entries from trades:closed
entries = r.xrevrange(RS.TRADES_CLOSED, '+', '-', count=100)

recent_trades = []
total_checked = 0

for msg_id, fields in entries:
    total_checked += 1
    exit_ts_ms = fields.get('exit_ts_ms')

    if exit_ts_ms:
        exit_ts = int(exit_ts_ms)
        if exit_ts >= hour_ago_ms:
            symbol = fields.get('symbol', 'N/A')
            direction = fields.get('direction', 'N/A')
            pnl_net = float(fields.get('pnl_net', 0))
            r_multiple = float(fields.get('r_multiple', 0))
            close_reason = fields.get('close_reason', 'N/A')

            exit_dt = datetime.fromtimestamp(exit_ts / 1000)

            recent_trades.append({
                'exit_time': exit_dt,
                'symbol': symbol,
                'direction': direction,
                'pnl_net': pnl_net,
                'r_multiple': r_multiple,
                'close_reason': close_reason,
                'sid': fields.get('sid', 'N/A')
            })

print("\n📊 РЕЗУЛЬТАТЫ:")
print(f"Всего проверено записей: {total_checked}")
print(f"Закрытых сделок за последний час: {len(recent_trades)}")

if recent_trades:
    print("\n🔍 ДЕТАЛИ СДЕЛОК (последние 20):")
    print("-" * 80)

    for i, trade in enumerate(recent_trades[:20], 1):
        pnl_indicator = "✅" if trade['pnl_net'] > 0 else "❌"
        print(f"\n{i}. {pnl_indicator} {trade['symbol']} {trade['direction']}")
        print(f"   Время закрытия: {trade['exit_time'].strftime('%H:%M:%S')}")
        print(f"   PnL: ${trade['pnl_net']:.2f} | R: {trade['r_multiple']:.2f}")
        print(f"   Причина: {trade['close_reason']}")
        print(f"   SID: {trade['sid'][:50]}...")

    # Summary statistics
    total_pnl = sum(t['pnl_net'] for t in recent_trades)
    winners = len([t for t in recent_trades if t['pnl_net'] > 0])
    losers = len([t for t in recent_trades if t['pnl_net'] < 0])

    print("\n📈 СТАТИСТИКА:")
    print(f"   Общий PnL: ${total_pnl:.2f}")
    print(f"   Profitable: {winners} ({100*winners/len(recent_trades):.1f}%)")
    print(f"   Losers: {losers} ({100*losers/len(recent_trades):.1f}%)")
    print(f"   Средний PnL: ${total_pnl/len(recent_trades):.2f}")
else:
    print("\n⚠️ Закрытых сделок за последний час не найдено")
