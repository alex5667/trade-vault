#!/usr/bin/env python3
"""
Проверка открытых сделок за последний час
"""
import os
import sys
import redis
import psycopg2
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

# Add project root to sys.path
sys.path.append("/app/python-worker")
sys.path.append("/app")


def check_redis_open_positions(redis_url: str) -> Dict[str, Any]:
    """Проверяет открытые позиции в Redis"""
    print("=" * 80)
    print("📊 ПРОВЕРКА ОТКРЫТЫХ ПОЗИЦИЙ В REDIS")
    print("=" * 80)
    
    try:
        r = redis.from_url(redis_url, decode_responses=True)
        
        # Получаем все ID открытых позиций
        open_ids = r.smembers("orders:open")
        print(f"\n✅ Всего открытых позиций: {len(open_ids)}")
        
        if not open_ids:
            print("   ℹ️  Нет открытых позиций в Redis")
            return {"total": 0, "last_hour": 0, "positions": []}
        
        # Собираем данные о каждой позиции
        positions = []
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        one_hour_ago_ms = now_ms - (60 * 60 * 1000)
        
        for pos_id in open_ids:
            pos_hash = r.hgetall(f"order:{pos_id}")
            if not pos_hash:
                continue
                
            entry_ts_ms = int(pos_hash.get("entry_ts_ms", 0))
            symbol = pos_hash.get("symbol", "UNKNOWN")
            direction = pos_hash.get("direction", "UNKNOWN")
            entry_price = float(pos_hash.get("entry_price", 0))
            sl = float(pos_hash.get("sl", 0))
            strategy = pos_hash.get("strategy", "UNKNOWN")
            
            positions.append({
                "id": pos_id,
                "symbol": symbol,
                "direction": direction,
                "entry_price": entry_price,
                "sl": sl,
                "strategy": strategy,
                "entry_ts_ms": entry_ts_ms,
                "age_minutes": (now_ms - entry_ts_ms) // (60 * 1000)
            })
        
        # Сортируем по времени (новые сначала)
        positions.sort(key=lambda x: x["entry_ts_ms"], reverse=True)
        
        # Позиции за последний час
        last_hour_positions = [p for p in positions if p["entry_ts_ms"] >= one_hour_ago_ms]
        
        print(f"\n🔥 Открытых позиций за последний час: {len(last_hour_positions)}")
        print("-" * 80)
        
        if last_hour_positions:
            print(f"\n{'ID':<25} {'Symbol':<12} {'Dir':<6} {'Entry':<10} {'SL':<10} {'Age':<8} {'Strategy':<15}")
            print("-" * 100)
            for pos in last_hour_positions:
                print(f"{pos['id']:<25} {pos['symbol']:<12} {pos['direction']:<6} "
                      f"{pos['entry_price']:<10.5f} {pos['sl']:<10.5f} "
                      f"{pos['age_minutes']:>3}m {pos['strategy']:<15}")
        
        print("\n📊 Все открытые позиции по символам:")
        symbol_counts = {}
        for pos in positions:
            symbol_counts[pos["symbol"]] = symbol_counts.get(pos["symbol"], 0) + 1
        
        for symbol, count in sorted(symbol_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"   {symbol:<12}: {count} позиций")
        
        return {
            "total": len(positions),
            "last_hour": len(last_hour_positions),
            "positions": last_hour_positions
        }
        
    except Exception as e:
        print(f"❌ Ошибка при проверке Redis: {e}")
        import traceback
        traceback.print_exc()
        return {"total": 0, "last_hour": 0, "positions": []}


def check_postgres_closed_trades(dsn: str) -> Dict[str, Any]:
    """Проверяет закрытые сделки в PostgreSQL за последний час"""
    print("\n" + "=" * 80)
    print("📊 ПРОВЕРКА ЗАКРЫТЫХ СДЕЛОК В POSTGRESQL")
    print("=" * 80)
    
    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        
        # Вычисляем временной диапазон
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        one_hour_ago_ms = now_ms - (60 * 60 * 1000)
        
        # Запрос закрытых сделок за последний час
        query = """
            SELECT 
                order_id,
                symbol,
                direction,
                entry_price,
                exit_price,
                pnl_net,
                close_reason,
                exit_ts_ms,
                strategy,
                (exit_ts_ms - entry_ts_ms) / 1000.0 as duration_sec
            FROM trades_closed
            WHERE exit_ts_ms >= %s
            ORDER BY exit_ts_ms DESC
            LIMIT 100
        """
        
        cur.execute(query, (one_hour_ago_ms,))
        rows = cur.fetchall()
        
        print(f"\n✅ Закрытых сделок за последний час: {len(rows)}")
        
        if rows:
            print("-" * 100)
            print(f"\n{'Order ID':<25} {'Symbol':<12} {'Dir':<6} {'Entry':<10} {'Exit':<10} "
                  f"{'PnL':<10} {'Reason':<20} {'Duration':<10}")
            print("-" * 120)
            
            total_pnl = 0.0
            wins = 0
            losses = 0
            
            for row in rows:
                order_id, symbol, direction, entry_price, exit_price, pnl_net, close_reason, exit_ts_ms, strategy, duration_sec = row
                
                # Вычисляем возраст закрытия
                age_minutes = (now_ms - exit_ts_ms) // (60 * 1000)
                
                pnl_color = "🟢" if pnl_net > 0 else "🔴"
                
                print(f"{order_id:<25} {symbol:<12} {direction:<6} "
                      f"{entry_price:<10.5f} {exit_price:<10.5f} "
                      f"{pnl_color} {pnl_net:>8.2f} {close_reason:<20} "
                      f"{duration_sec:>7.1f}s")
                
                total_pnl += pnl_net
                if pnl_net > 0:
                    wins += 1
                else:
                    losses += 1
            
            print("-" * 120)
            print(f"\n📊 Статистика за последний час:")
            print(f"   Всего сделок: {len(rows)}")
            print(f"   Прибыльных: {wins} ({wins / len(rows) * 100:.1f}%)")
            print(f"   Убыточных: {losses} ({losses / len(rows) * 100:.1f}%)")
            print(f"   Общий PnL: ${total_pnl:.2f}")
            
            # Группировка по причинам закрытия
            print(f"\n📊 По причинам закрытия:")
            close_reasons = {}
            for row in rows:
                reason = row[6]
                close_reasons[reason] = close_reasons.get(reason, 0) + 1
            
            for reason, count in sorted(close_reasons.items(), key=lambda x: x[1], reverse=True):
                print(f"   {reason:<20}: {count}")
        
        cur.close()
        conn.close()
        
        return {
            "count": len(rows),
            "trades": rows
        }
        
    except Exception as e:
        print(f"❌ Ошибка при проверке PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        return {"count": 0, "trades": []}


def main():
    """Главная функция"""
    print("\n" + "=" * 80)
    print("🔍 ПРОВЕРКА ОТКРЫТЫХ СДЕЛОК ЗА ПОСЛЕДНИЙ ЧАС")
    print("=" * 80)
    print(f"Текущее время: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    # Получаем параметры подключения из переменных окружения
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    postgres_dsn = os.getenv("TRADES_DB_DSN", 
                            "postgresql://trading:trading_password@postgres:5432/scanner_analytics")
    
    print(f"Redis URL: {redis_url}")
    print(f"PostgreSQL DSN: {postgres_dsn.replace('trading_password', '***')}")
    
    # Проверяем Redis
    redis_result = check_redis_open_positions(redis_url)
    
    # Проверяем PostgreSQL
    pg_result = check_postgres_closed_trades(postgres_dsn)
    
    # Итоговая сводка
    print("\n" + "=" * 80)
    print("📊 ИТОГОВАЯ СВОДКА")
    print("=" * 80)
    print(f"✅ Открытых позиций (всего): {redis_result['total']}")
    print(f"🔥 Открытых позиций (за последний час): {redis_result['last_hour']}")
    print(f"✅ Закрытых сделок (за последний час): {pg_result['count']}")
    print("=" * 80)


if __name__ == "__main__":
    main()

