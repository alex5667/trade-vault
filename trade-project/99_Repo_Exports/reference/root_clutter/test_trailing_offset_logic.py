#!/usr/bin/env python3
"""
Test script to analyze TRAILING_TP1_OFFSET_ATR logic in the codebase.
"""

from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class MockTrade:
    pnl_net: float
    one_r_money: float
    mfe_pnl: float
    giveback: float
    missed_profit: float
    atr: float
    entry_price: float
    direction: str


def r_or_zero(pnl: float, one_r: float) -> float:
    if one_r is None or abs(one_r) < 1e-9:
        return 0.0
    return pnl / one_r


def simulate_offset_mult(trade: MockTrade, offset_mult: float) -> Dict[str, Any]:
    """
    Симулирует результат сделки с заданным offset_mult.
    Упрощенная модель из simulate_trailing_offsets.py
    """
    if trade.mfe_pnl <= 0 or trade.atr <= 0:
        return {"would_stop_early": False, "simulated_pnl": trade.pnl_net, "simulated_r": r_or_zero(trade.pnl_net, trade.one_r_money)}

    mfe_r = trade.mfe_pnl / trade.one_r_money
    current_r = trade.pnl_net / trade.one_r_money

    # Упрощенная модель: чем меньше offset_mult, тем больше giveback (ранние стопы)
    giveback_r = (trade.giveback / trade.one_r_money) if trade.giveback > 0 else 0.0
    missed_r = (trade.missed_profit / trade.one_r_money) if trade.missed_profit > 0 else 0.0

    # Чем меньше offset_mult, тем больше giveback (ранние стопы)
    simulated_giveback_r = giveback_r * (1.0 / offset_mult)
    # Чем больше offset_mult, тем больше missed (поздние стопы)
    simulated_missed_r = missed_r * offset_mult

    simulated_r = mfe_r - simulated_giveback_r - simulated_missed_r
    simulated_pnl = simulated_r * trade.one_r_money

    would_be_better = (simulated_giveback_r < giveback_r * 0.7) and (simulated_missed_r < missed_r * 0.7)

    return {
        "simulated_pnl": simulated_pnl,
        "simulated_r": simulated_r,
        "simulated_giveback_r": simulated_giveback_r,
        "simulated_missed_r": simulated_missed_r,
        "would_be_better": would_be_better,
        "actual_r": current_r,
        "mfe_r": mfe_r,
    }


def analyze_offset_mult_range(trades: List[MockTrade], offset_mults: List[float]) -> Dict[float, Dict[str, Any]]:
    """
    Анализирует различные offset_mult на выборке сделок.
    """
    results: Dict[float, Dict[str, Any]] = {}

    for offset_mult in offset_mults:
        simulated_rs: List[float] = []
        giveback_rs: List[float] = []
        missed_rs: List[float] = []
        better_count = 0

        for trade in trades:
            sim = simulate_offset_mult(trade, offset_mult)
            simulated_rs.append(sim["simulated_r"])
            giveback_rs.append(sim["simulated_giveback_r"])
            missed_rs.append(sim["simulated_missed_r"])
            if sim["would_be_better"]:
                better_count += 1

        n = len(trades)
        avg_simulated_r = sum(simulated_rs) / n if n > 0 else 0.0
        avg_giveback_r = sum(giveback_rs) / n if n > 0 else 0.0
        avg_missed_r = sum(missed_rs) / n if n > 0 else 0.0

        results[offset_mult] = {
            "avg_expectancy_r": avg_simulated_r,
            "avg_giveback_r": avg_giveback_r,
            "avg_missed_r": avg_missed_r,
            "share_better": better_count / n if n > 0 else 0.0,
            "sample_size": n,
        }

    return results


def recommend_offset_mult(analysis: Dict[float, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Рекомендует оптимальный offset_mult на основе анализа.
    """
    if not analysis:
        return {"recommended": 0.6, "reason": "no_data"}

    best_offset = 0.6
    best_score = -float('inf')

    for offset, stats in analysis.items():
        expectancy = stats["avg_expectancy_r"]
        giveback = stats["avg_giveback_r"]
        missed = stats["avg_missed_r"]
        better_share = stats["share_better"]

        giveback_penalty = giveback * 0.5
        missed_penalty = missed * 0.3

        score = expectancy - giveback_penalty - missed_penalty + better_share * 0.2

        if score > best_score:
            best_score = score
            best_offset = offset

    return {
        "recommended": best_offset,
        "reason": "max_expectancy_min_giveback",
        "analysis": analysis,
    }


def test_trailing_offset_logic():
    """Test the TRAILING_TP1_OFFSET_ATR logic with sample data."""

    print("=== Testing TRAILING_TP1_OFFSET_ATR Logic ===\n")

    # Sample trades with realistic data
    trades = [
        # ETHUSDT-like: higher volatility, more giveback
        MockTrade(pnl_net=1.2, one_r_money=1.0, mfe_pnl=2.5, giveback=0.8, missed_profit=0.5, atr=1.2, entry_price=1800, direction="LONG"),
        MockTrade(pnl_net=0.8, one_r_money=1.0, mfe_pnl=2.0, giveback=0.6, missed_profit=0.6, atr=1.1, entry_price=1820, direction="LONG"),
        MockTrade(pnl_net=1.5, one_r_money=1.0, mfe_pnl=3.0, giveback=0.9, missed_profit=0.6, atr=1.3, entry_price=1790, direction="LONG"),

        # BTCUSDT-like: lower volatility, less giveback but more missed
        MockTrade(pnl_net=1.0, one_r_money=1.0, mfe_pnl=1.8, giveback=0.3, missed_profit=0.5, atr=0.8, entry_price=25000, direction="LONG"),
        MockTrade(pnl_net=0.9, one_r_money=1.0, mfe_pnl=1.6, giveback=0.2, missed_profit=0.5, atr=0.7, entry_price=25200, direction="LONG"),
        MockTrade(pnl_net=1.2, one_r_money=1.0, mfe_pnl=2.1, giveback=0.4, missed_profit=0.5, atr=0.9, entry_price=24800, direction="LONG"),
    ]

    # Test different offset_mult values
    offset_mults = [0.2, 0.3, 0.4, 0.6, 0.8, 1.0]

    print("Sample trades data:")
    for i, trade in enumerate(trades, 1):
        print(".3f"            f"  Giveback: {trade.giveback:.3f}R, Missed: {trade.missed_profit:.3f}R")
    print()

    # Analyze different offset_mult values
    analysis = analyze_offset_mult_range(trades, offset_mults)

    print("Analysis of different offset_mult values:")
    print("offset | exp_R | giveback_R | missed_R | better% | n")
    print("-" * 55)

    for offset in sorted(analysis.keys()):
        stats = analysis[offset]
        print(
            f"{offset:6.2f} | "
            f"{stats['avg_expectancy_r']:5.3f} | "
            f"{stats['avg_giveback_r']:10.3f} | "
            f"{stats['avg_missed_r']:9.3f} | "
            f"{stats['share_better']*100:6.1f}% | "
            f"{stats['sample_size']:2d}"
        )
    print()

    # Get recommendation
    recommendation = recommend_offset_mult(analysis)
    print(f"Recommended TRAILING_TP1_OFFSET_ATR: {recommendation['recommended']:.2f}")
    print(f"Reason: {recommendation['reason']}")
    print()

    print("=== Current Implementation Status ===")
    print("✅ Упрощенная симуляция на основе giveback/missed_profit реализована")
    print("✅ Логика выбора оптимального offset_mult реализована")
    print("✅ Интеграция с Redis symbol_specs реализована")
    print("⚠️  Полноценная симуляция на исторических данных ТИКОВ/МИНУТ НЕ РЕАЛИЗОВАНА")
    print("⚠️  Автоматическая калибровка TRAILING_TP1_OFFSET_ATR НЕ ВКЛЮЧЕНА в auto_calibration_service")

    print("\n=== What the Current Logic Does ===")
    print("1. Берет сделки с запущенным трейлингом")
    print("2. Для каждого offset_mult симулирует результат на основе giveback/missed")
    print("3. Выбирает offset с максимальным expectancy - giveback_penalty - missed_penalty")
    print("4. Сохраняет рекомендацию в Redis для ручной настройки")

    print("\n=== What the Current Logic DOESN'T Do ===")
    print("❌ Не симулирует на реальных исторических данных цены")
    print("❌ Не проверяет, когда именно цена возвращается к new_sl после TP1")
    print("❌ Не запускается автоматически каждые N сделок")
    print("❌ Не интегрирована в основной пайплайн сигналов")


if __name__ == "__main__":
    test_trailing_offset_logic()
