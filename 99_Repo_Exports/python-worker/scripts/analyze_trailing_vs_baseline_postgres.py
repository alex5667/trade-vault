# scripts/analyze_trailing_vs_baseline_postgres.py
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import psycopg2  # pip install psycopg2-binary
import math

import sys
import os
# Try to find the root by looking for 'analytics' directory
current_dir = os.path.dirname(os.path.abspath(__file__))
# Check if we are in 'scripts' directory relative to the root
root_dir = os.path.dirname(current_dir)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

try:
    from analytics.tag_stats import Trade, TagStats
except ImportError:
    # If not found, maybe we are in a different structure
    # Fallback to current dir's parent
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
    from analytics.tag_stats import Trade, TagStats

# Compatibility aliases
TradeRow = Trade

def mean(data: List[float]) -> float:
    if not data: return 0.0
    return sum(data) / len(data)

def stddev(data: List[float]) -> float:
    if len(data) < 2: return 0.0
    mu = mean(data)
    var = sum((x - mu)**2 for x in data) / (len(data) - 1)
    return math.sqrt(max(0.0, var))

def max_drawdown(equity: List[float]) -> float:
    if not equity: return 0.0
    peak = equity[0]
    mdd = 0.0
    for x in equity:
        if x > peak: peak = x
        dd = peak - x
        if dd > mdd: mdd = dd
    return mdd

def print_global_report(symbol, source, stats):
    print(f"Global report for {symbol} ({source}): {stats}")

def print_tag_report(tag_stats, max_tags=5):
    print(f"Tag report: {tag_stats[:max_tags]}")


# ----------------------------
# Утилиты приведения типов
# ----------------------------

def _to_float(v, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        return float(v)
    except Exception:
        return default


def _to_int(v, default: int = 0) -> int:
    if v is None:
        return default
    try:
        return int(float(v))
    except Exception:
        return default


def _to_bool(v) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ("1", "t", "true", "yes", "y")


def parse_ts_arg(val: Optional[str]) -> Optional[int]:
    """
    Парсер для --from / --to:
    - если только цифры → считаем это exit_ts_ms (ms от эпохи)
    - иначе пытаемся разобрать как дату/время (UTC) и конвертировать в ms.
      Поддерживаются:
      - 'YYYY-MM-DD'
      - 'YYYY-MM-DDTHH:MM'
      - 'YYYY-MM-DDTHH:MM:SS' и т.п. (ISO 8601).
    """
    if not val:
        return None
    s = val.strip()
    if not s:
        return None

    if s.isdigit():
        return int(s)

    # пробуем дату
    try:
        # только дата
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            dt = datetime.strptime(s, "%Y-%m-%d")
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            # обобщённый ISO-формат
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception as e:
        raise ValueError(f"Не могу разобрать значение времени '{s}': {e}") from e


# ----------------------------
# Загрузка сделок из Postgres
# ----------------------------

def load_trades_from_postgres(
    conn
    source: str
    symbol: str
    limit: int = 200
    since_days: Optional[int] = None
) -> List[Trade]:
    """
    Загружает сделки из PostgreSQL для анализа trailing vs baseline.
    """
    cur = conn.cursor()

    # Базовый запрос
    cols = """
        source
        symbol
        exit_ts_ms
        pnl_net
        pnl_if_fixed_exit
        one_r_money
        giveback
        missed_profit
        mfe_pnl
        mae_pnl
        trailing_started
        trailing_active
        close_reason
        close_reason_raw
        close_reason_detail
        entry_tag
        strategy
        strong_gate_ok
    """

    sql = f"SELECT {cols} FROM trades_closed WHERE source = %s AND symbol = %s"
    params = [source, symbol]

    # Добавляем фильтр по времени, если указан
    if since_days is not None and since_days > 0:
        cutoff_ts = int((datetime.now(timezone.utc).timestamp() - since_days * 24 * 3600) * 1000)
        sql += " AND exit_ts_ms >= %s"
        params.append(cutoff_ts)

    sql += " ORDER BY exit_ts_ms DESC LIMIT %s"
    params.append(limit)

    cur.execute(sql, params)
    rows = cur.fetchall()

    trades: List[Trade] = []

    for row in rows:
        (
            source_val
            symbol_val
            exit_ts_ms
            pnl_net
            pnl_if_fixed_exit
            one_r_money
            giveback
            missed_profit
            mfe_pnl
            mae_pnl
            trailing_started
            trailing_active
            close_reason
            close_reason_raw
            close_reason_detail
            entry_tag
            strategy
            strong_gate_ok
        ) = row

        t = Trade(
            source=source_val or "Unknown"
            symbol=symbol_val or "UNKNOWN"
            exit_ts_ms=_to_int(exit_ts_ms)
            pnl_net=_to_float(pnl_net)
            pnl_if_fixed_exit=_to_float(pnl_if_fixed_exit)
            one_r_money=_to_float(one_r_money)
            giveback=_to_float(giveback)
            missed_profit=_to_float(missed_profit)
            mfe_pnl=_to_float(mfe_pnl)
            mae_pnl=_to_float(mae_pnl)
            trailing_started=_to_bool(trailing_started)
            trailing_active=_to_bool(trailing_active)
            close_reason=close_reason or ""
            close_reason_raw=close_reason_raw or ""
            close_reason_detail=close_reason_detail or ""
            entry_tag=entry_tag or ""
            strategy=(strategy or "")
            strong_gate_ok=_to_bool(strong_gate_ok)
        )
        trades.append(t)

    # Для анализа удобнее по возрастанию времени
    trades.sort(key=lambda t: t.exit_ts_ms)
    return trades


# ----------------------------
# Анализ функций
# ----------------------------

def analyze_global(trades: List[Trade]) -> Dict[str, float]:
    """
    Глобальный анализ trailing vs baseline по всем сделкам.
    """
    if not trades:
        return {"total_trades": 0}

    global_stats = TagStats(tag="__GLOBAL__")
    for trade in trades:
        global_stats.add_trade(trade)

    result = global_stats.finalize()
    result["total_trades"] = len(trades)
    return result


def analyze_by_tag(trades: List[Trade], min_trades: int = 5) -> List[Dict[str, float]]:
    """
    Анализ по entry_tag с фильтрацией по минимальному количеству сделок.
    """
    tag_stats: Dict[str, TagStats] = {}

    for trade in trades:
        tag = trade.entry_tag or "<untagged>"
        if tag not in tag_stats:
            tag_stats[tag] = TagStats(tag=tag)
        tag_stats[tag].add_trade(trade)

    results = []
    for tag, stats in tag_stats.items():
        result = stats.finalize()
        if result["n"] >= min_trades:
            result["total_trades_analyzed"] = len(trades)
            results.append(result)

    # Сортировка по delta_expectancy_r (лучшие паттерны сверху)
    results.sort(key=lambda x: x.get("delta_expectancy_r", 0), reverse=True)
    return results


def analyze_by_strong_gate(trades: List[Trade], min_trades: int = 5) -> List[Dict[str, float]]:
    """
    Анализ по strong_gate_ok (сильные vs слабые сигналы) с фильтрацией по минимальному количеству сделок.
    """
    gate_stats: Dict[str, TagStats] = {}

    for trade in trades:
        # Определяем категорию сигнала
        gate_label = "Strong" if getattr(trade, "strong_gate_ok", False) else "Weak"
        
        if gate_label not in gate_stats:
            gate_stats[gate_label] = TagStats(tag=gate_label)
        gate_stats[gate_label].add_trade(trade)

    results = []
    for label, stats in gate_stats.items():
        result = stats.finalize()
        if result["n"] >= min_trades:
            result["total_trades_analyzed"] = len(trades)
            results.append(result)

    # Сортировка: Strong сигналы первыми
    results.sort(key=lambda x: 1 if x.get("tag") == "Strong" else 0, reverse=True)
    return results


# ----------------------------
# CLI интерфейс
# ----------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Анализ trailing vs baseline для принятия решений о настройках трейлинга"
    )
    parser.add_argument(
        "--dsn"
        required=True
        help="PostgreSQL DSN, например: postgresql://user:pass@localhost:5432/scanner_analytics"
    )
    parser.add_argument("--source", default="CryptoOrderFlow", help="Источник стратегии")
    parser.add_argument("--symbol", required=True, help="Символ (ETHUSDT, BTCUSDT и т.д.)")
    parser.add_argument("--limit", type=int, default=200, help="Максимальное количество сделок")
    parser.add_argument("--since-days", type=int, default=None, help="Анализировать за последние N дней")
    parser.add_argument("--min-trades-per-tag", type=int, default=10, help="Мин. сделок на тег")

    args = parser.parse_args()

    # Подключаемся к БД
    conn = psycopg2.connect(args.dsn)
    try:
        # Загружаем сделки
        trades = load_trades_from_postgres(
            conn=conn
            source=args.source
            symbol=args.symbol
            limit=args.limit
            since_days=args.since_days
        )

        if not trades:
            print(f"Нет сделок для {args.source}/{args.symbol}")
            return

        print(f"📊 Анализ {len(trades)} сделок для {args.source}/{args.symbol}")
        print("=" * 80)

        # Глобальный анализ
        global_result = analyze_global(trades)
        print("\n🌍 ГЛОБАЛЬНЫЙ АНАЛИЗ:")
        print(f"  Всего сделок: {global_result['total_trades']}")
        print(f"  Expectancy R (managed): {global_result.get('expectancy_r', 0):+.3f}")
        print(f"  Expectancy R (baseline): {global_result.get('expectancy_fixed_r', 0):+.3f}")
        print(f"  ΔExp_R: {global_result.get('delta_expectancy_r', 0):+.3f}")
        print(f"  Better trades: {global_result.get('share_better', 0):.1%}")
        print(f"  Worse trades: {global_result.get('share_worse', 0):.1%}")
        print(f"  Trailing share: {global_result.get('trailing_share', 0):.1%}")
        # Решения на основе анализа
        delta_exp_r = global_result.get("delta_expectancy_r", 0)
        share_better = global_result.get("share_better", 0)
        share_worse = global_result.get("share_worse", 0)

        print("\n🎯 РЕКОМЕНДАЦИИ:")
        if delta_exp_r > 0.05 and share_better > 0.55:
            print("  ✅ Трейлинг полезен глобально - оставить текущие настройки")
        elif delta_exp_r < -0.05 and share_worse > 0.55:
            print("  ⚠️  Трейлинг вреден - рассмотреть ослабление или отключение")
        else:
            print("  🤔 Нейтральный эффект - мониторить дальше")

        # Анализ по тегам
        tag_results = analyze_by_tag(trades, args.min_trades_per_tag)

        if tag_results:
            print("\n🏷️  АНАЛИЗ ПО ТЕГАМ (топ-5):")
            print(f"{'Тег':<20} {'ΔExpR':>7} {'Better':>7} {'Worse':>6} {'Trail%':>7} {'Решение'}")
            print("-" * 80)

            for result in tag_results[:5]:  # Показываем топ-5
                tag = result["tag"][:19]
                delta_exp = result.get("delta_expectancy_r", 0)
                better_pct = result.get("share_better", 0)
                worse_pct = result.get("share_worse", 0)
                trail_pct = result.get("trailing_share", 0)

                # Логика принятия решений
                if delta_exp > 0.1 and better_pct > 0.6:
                    decision = "✅ Усилить"
                elif delta_exp < -0.05 and worse_pct > 0.5:
                    decision = "⚠️  Ослабить"
                else:
                    decision = "🤔 Мониторить"

                print(f"{tag:<20} {delta_exp:>7.3f} {better_pct:>7.1%} {worse_pct:>6.1%} {trail_pct:>7.1%} {decision}")
        print("\n📋 РЕЗЮМЕ:")
        print(f"  Анализ завершен для {args.source}/{args.symbol}")
        print(f"  Данные: {len(trades)} сделок")
        if args.since_days:
            print(f"  Период: последние {args.since_days} дней")

    finally:
        conn.close()


if __name__ == "__main__":
    main()