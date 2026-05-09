#!/usr/bin/env python3
"""
Check for closed trades in the last hour from PostgreSQL database
"""
import os
from datetime import UTC, datetime, timedelta

import psycopg2


def main():
    # Database connection parameters
    # Try the DSN from persistence_manager.py first
    dsn = os.getenv('TRADES_DB_DSN') or os.getenv('PG_DSN') or 'postgresql://postgres:12345@localhost:5432/scanner_analytics'

    # First connect to default postgres database to check what databases exist
    try:
        temp_conn = psycopg2.connect('postgresql://postgres:12345@localhost:5432/postgres')
        temp_cursor = temp_conn.cursor()
        temp_cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        databases = temp_cursor.fetchall()
        temp_cursor.close()
        temp_conn.close()

        print(f"Available databases: {[db[0] for db in databases]}")

        # Use scanner_analytics database (where trades_closed table should be)
        db_name = 'scanner_analytics'
        print(f"Target database: {db_name}")

        dsn = f'postgresql://postgres:12345@localhost:5432/{db_name}'
        print(f"Connecting to database: {db_name}")
        conn = psycopg2.connect(dsn)

    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return

    try:
        print("🔍 Checking for closed trades in the last hour...")
        print("=" * 80)

        # Database connection already established above
        cursor = conn.cursor()

        # Calculate time range
        now = datetime.now(UTC)
        one_hour_ago = now - timedelta(hours=1)

        now_ms = int(now.timestamp() * 1000)
        hour_ago_ms = int(one_hour_ago.timestamp() * 1000)

        print(f"Current time (UTC): {now}")
        print(f"Checking trades since: {one_hour_ago} (timestamp: {hour_ago_ms})")
        print()

        # First check what tables exist
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        tables = cursor.fetchall()
        print(f"Available tables in 'trade' database: {[t[0] for t in tables]}")

        # Look for trade-related tables
        trade_tables = [t[0] for t in tables if 'trade' in t[0].lower()]
        if not trade_tables:
            print("❌ Таблицы с сделками не найдены в базе данных")
            return

        print(f"Найденные таблицы с сделками: {trade_tables}")

        # Try to find a table with closed trades
        closed_table = None
        for table in trade_tables:
            if 'closed' in table.lower():
                closed_table = table
                break

        if not closed_table:
            print("❌ Таблица с закрытыми сделками не найдена")
            return

        print(f"Используем таблицу: {closed_table}")

        # Query closed trades in the last hour
        query = f"""
        SELECT * FROM {closed_table} LIMIT 5
        """
        cursor.execute(query)
        sample = cursor.fetchall()
        print(f"Sample data from {closed_table}: {sample[:1] if sample else 'No data'}")

        # For now, just report that we found the table but need to check its structure
        print(f"✅ Найдена таблица с данными о сделках: {closed_table}")
        print("📝 Для полного анализа нужно проверить структуру таблицы")

        cursor.execute(query, (hour_ago_ms,))
        trades = cursor.fetchall()

        print("📊 РЕЗУЛЬТАТЫ:")
        print(f"Закрытых сделок за последний час: {len(trades)}")
        print()

        if trades:
            print("🔍 ДЕТАЛИ СДЕЛОК:")
            print("-" * 120)

            for i, trade in enumerate(trades[:20], 1):  # Show first 20 trades
                symbol, direction, exit_ts, pnl_net, pnl_pct, close_reason, strategy, source, sid, entry_price, exit_price, lot, notional_usd, tp_hits, trailing_active = trade

                pnl_indicator = "✅" if pnl_net > 0 else "❌" if pnl_net < 0 else "➖"
                trailing_indicator = "🚀" if trailing_active else ""

                print(f"{i:2d}")
                print(f"        Время закрытия: {exit_ts.strftime('%H:%M:%S')}")
                print(f"        PnL: ${pnl_net:.2f} ({pnl_pct*100:.2f}%) | Notional: ${notional_usd:.2f}")
                print(f"        Цена: {entry_price:.4f} → {exit_price:.4f} | Lot: {lot:.4f}")
                print(f"        Причина: {close_reason} | TP Hits: {tp_hits or 0}")
                print(f"        Стратегия: {strategy} | Источник: {source}")
                if sid:
                    print(f"        SID: {sid[:50]}...")
                print()

            # Summary statistics
            total_pnl = sum(t[3] for t in trades)  # pnl_net
            winners = len([t for t in trades if t[3] > 0])
            losers = len([t for t in trades if t[3] < 0])
            total_notional = sum(t[12] for t in trades if t[12])  # notional_usd

            print("📈 СТАТИСТИКА:")
            print(f"   Общий PnL: ${total_pnl:.2f}")
            print(f"   Общий Notional: ${total_notional:.2f}")
            print(f"   Profitable: {winners} ({100*winners/len(trades):.1f}%)")
            print(f"   Losers: {losers} ({100*losers/len(trades):.1f}%)")
            print(f"   Средний PnL: ${total_pnl/len(trades):.2f}")
            print(f"   Средний Notional: ${total_notional/len(trades):.2f}")
        else:
            print("⚠️ Закрытых сделок за последний час не найдено")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Ошибка подключения к базе данных: {e}")
        print("Проверьте настройки подключения к PostgreSQL")

if __name__ == "__main__":
    main()
