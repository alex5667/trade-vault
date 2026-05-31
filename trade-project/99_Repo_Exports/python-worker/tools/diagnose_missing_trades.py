#!/usr/bin/env python3
"""
Диагностика: почему не находится сделок для символов 1000PEPEUSDT, 1000SHIBUSDT, etc.

Проверяет:
1. Сколько сделок в trades:closed для этих символов
2. Какие source используются
3. Есть ли открытые позиции (orders:open)
4. Есть ли сигналы для этих символов
"""

import os
from collections import defaultdict

import redis
from core.redis_keys import RedisStreams as RS

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

SYMBOLS = ["1000PEPEUSDT", "1000SHIBUSDT", "1000FLOKIUSDT", "1000BONKUSDT"]
SOURCES = ["CryptoOrderFlow", "cryptoorderflow", "Crypto-OrderFlow"]  # возможные варианты source

def canon_symbol(s: str) -> str:
    return (s or "").strip().upper()

def canon_source(s: str) -> str:
    s = (s or "").strip()
    sl = s.lower()
    if sl in ("cryptoorderflow", "crypto-orderflow"):
        return "CryptoOrderFlow"
    return s or "Unknown"

def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)

    print("=" * 80)
    print("🔍 ДИАГНОСТИКА: Почему нет сделок для символов")
    print("=" * 80)
    print()

    # 1. Проверяем trades:closed stream
    print("1️⃣ Проверка trades:closed stream")
    print("-" * 80)

    entries = r.xrevrange(RS.TRADES_CLOSED, max="+", min="-", count=10000) or []
    print(f"Всего записей в stream: {len(entries)}")

    trades_by_symbol: dict[str, list[dict]] = defaultdict(list)
    trades_by_source: dict[str, int] = defaultdict(int)

    for msg_id, fields in entries:
        symbol = canon_symbol(fields.get("symbol", ""))
        source = canon_source(fields.get("source") or fields.get("strategy_source", ""))

        trades_by_source[source] += 1

        if symbol in SYMBOLS:
            trades_by_symbol[symbol].append({
                "msg_id": msg_id,
                "source": source,
                "symbol": symbol,
                "order_id": fields.get("order_id", ""),
                "exit_ts_ms": fields.get("exit_ts_ms", "0"),
                "pnl_net": fields.get("pnl_net", "0"),
            })

    print("\nТоп-10 source в stream:")
    for source, count in sorted(trades_by_source.items(), key=lambda x: -x[1])[:10]:
        print(f"  {source}: {count} сделок")

    print("\nСделки для целевых символов:")
    for symbol in SYMBOLS:
        trades = trades_by_symbol[symbol]
        print(f"  {symbol}: {len(trades)} сделок")

        if trades:
            # Группируем по source
            by_source = defaultdict(list)
            for t in trades:
                by_source[t["source"]].append(t)

            print("    По source:")
            for source, source_trades in by_source.items():
                print(f"      {source}: {len(source_trades)} сделок")
                # Показываем последние 3
                for t in source_trades[:3]:
                    print(f"        - order_id={t['order_id']}, exit_ts={t['exit_ts_ms']}, pnl={t['pnl_net']}")

    # 2. Проверяем orders:open
    print("\n2️⃣ Проверка открытых позиций (orders:open)")
    print("-" * 80)

    open_orders = r.smembers("orders:open") or set()
    print(f"Всего открытых позиций: {len(open_orders)}")

    open_by_symbol: dict[str, list[str]] = defaultdict(list)

    for order_id in open_orders:
        order_key = f"order:{order_id}"
        order_data = r.hgetall(order_key) or {}
        symbol = canon_symbol(order_data.get("symbol", ""))
        source = canon_source(order_data.get("source", ""))

        if symbol in SYMBOLS:
            open_by_symbol[symbol].append(f"{order_id} (source={source})")

    for symbol in SYMBOLS:
        orders = open_by_symbol[symbol]
        if orders:
            print(f"  {symbol}: {len(orders)} открытых позиций")
            for o in orders[:5]:
                print(f"    - {o}")
        else:
            print(f"  {symbol}: нет открытых позиций")

    # 3. Проверяем signals streams
    print("\n3️⃣ Проверка сигналов (signals:cryptoorderflow)")
    print("-" * 80)

    signal_streams = [
        "signals:cryptoorderflow",
        RS.CRYPTO_RAW,
        RS.SIGNALS_UNIFIED,
    ]

    for stream_name in signal_streams:
        try:
            entries = r.xrevrange(stream_name, max="+", min="-", count=1000) or []
            if not entries:
                continue

            signals_by_symbol: dict[str, int] = defaultdict(int)

            for msg_id, fields in entries:
                symbol = canon_symbol(fields.get("symbol", ""))
                if symbol in SYMBOLS:
                    signals_by_symbol[symbol] += 1

            if signals_by_symbol:
                print(f"  {stream_name}:")
                for symbol in SYMBOLS:
                    count = signals_by_symbol[symbol]
                    if count > 0:
                        print(f"    {symbol}: {count} сигналов (последние 1000 записей)")
        except Exception as e:
            print(f"  {stream_name}: ошибка чтения - {e}")

    # 4. Проверяем stats
    print("\n4️⃣ Проверка статистики (stats:*)")
    print("-" * 80)

    for symbol in SYMBOLS:
        # Проверяем разные варианты ключей
        stats_keys = [
            f"stats:orderflow:{symbol}:tick",
            f"stats:orderflow:{symbol}:1m",
            f"stats:cryptoorderflow:{symbol}:tick",
        ]

        for key in stats_keys:
            try:
                data = r.hgetall(key)
                if data:
                    total = data.get("total", "0")
                    wins = data.get("wins", "0")
                    print(f"  {key}: total={total}, wins={wins}")
                    break
            except Exception:
                pass

    # 5. Рекомендации
    print("\n5️⃣ РЕКОМЕНДАЦИИ")
    print("-" * 80)

    for symbol in SYMBOLS:
        trades_count = len(trades_by_symbol[symbol])
        open_count = len(open_by_symbol[symbol])

        print(f"\n{symbol}:")
        print(f"  - Закрытых сделок: {trades_count}")
        print(f"  - Открытых позиций: {open_count}")

        if trades_count == 0 and open_count == 0:
            print("  ⚠️  ПРОБЛЕМА: Нет ни закрытых, ни открытых сделок")
            print("     → Возможно, сигналы не генерируются или не обрабатываются")
        elif trades_count == 0 and open_count > 0:
            print("  ⚠️  ПРОБЛЕМА: Есть открытые позиции, но нет закрытых")
            print("     → Возможно, позиции не закрываются (застряли в открытом состоянии)")
        elif trades_count < 40:
            print(f"  ⚠️  ПРОБЛЕМА: Мало закрытых сделок ({trades_count} < 40)")
            print("     → Нужно больше сделок для рекомендаций")

            # Проверяем source
            symbol_trades = trades_by_symbol[symbol]
            if symbol_trades:
                sources = set(t["source"] for t in symbol_trades)
                print(f"     → Найденные source: {sources}")
                print("     → Убедитесь, что фильтр source совпадает с реальными данными")

if __name__ == "__main__":
    main()

