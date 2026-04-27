#!/usr/bin/env python3
"""
Check all closed trades in PostgreSQL scanner_analytics database
"""
import psycopg2
from datetime import datetime, timezone

def main():
    try:
        # Connect to scanner_analytics database
        conn = psycopg2.connect('postgresql://postgres:12345@localhost:5432/scanner_analytics')
        cursor = conn.cursor()

        print("🔍 Проверка всех закрытых сделок в базе scanner_analytics...")
        print("=" * 80)

        # Check total count of trades
        cursor.execute("SELECT COUNT(*) FROM trades_closed")
        total_count = cursor.fetchone()[0]
        print(f"Всего закрытых сделок в таблице: {total_count}")

        if total_count > 0:
            # Get summary statistics
            cursor.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(CASE WHEN pnl_net > 0 THEN 1 END) as profitable,
                    COUNT(CASE WHEN pnl_net < 0 THEN 1 END) as losing,
                    AVG(pnl_net) as avg_pnl,
                    SUM(pnl_net) as total_pnl,
                    MIN(exit_ts) as oldest_trade,
                    MAX(exit_ts) as newest_trade
                FROM trades_closed
            """)
            stats = cursor.fetchone()

            total, profitable, losing, avg_pnl, total_pnl, oldest, newest = stats

            print(f"\n📊 СТАТИСТИКА:")
            print(f"   Всего сделок: {total}")
            print(f"   Прибыльных: {profitable} ({100*profitable/total:.1f}%)")
            print(f"   Убыточных: {losing} ({100*losing/total:.1f}%)")
            print(f"   Средний PnL: ${avg_pnl:.2f}")
            print(f"   Общий PnL: ${total_pnl:.2f}")
            print(f"   Первая сделка: {oldest}")
            print(f"   Последняя сделка: {newest}")

            # Show last 5 trades
            print(f"\n🔍 ПОСЛЕДНИЕ 5 СДЕЛОК:")
            cursor.execute("""
                SELECT
                    symbol,
                    direction,
                    exit_ts,
                    pnl_net,
                    pnl_pct,
                    close_reason,
                    strategy,
                    source
                FROM trades_closed
                ORDER BY exit_ts DESC
                LIMIT 5
            """)
            recent_trades = cursor.fetchall()

            for i, trade in enumerate(recent_trades, 1):
                symbol, direction, exit_ts, pnl_net, pnl_pct, close_reason, strategy, source = trade
                pnl_indicator = "✅" if pnl_net > 0 else "❌" if pnl_net < 0 else "➖"
                print(f"{i}. {pnl_indicator} {symbol} {direction} | {exit_ts} | PnL: ${pnl_net:.2f} ({pnl_pct*100:.1f}%) | {close_reason}")
        else:
            print("\n⚠️ В таблице trades_closed нет ни одной закрытой сделки")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Ошибка подключения к базе данных: {e}")

if __name__ == "__main__":
    main()
