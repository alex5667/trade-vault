#!/usr/bin/env python3
"""
Тест нового формата крипто сигнала с процентом депозита и размером с плечом
"""

import sys
import os

# Добавляем python-worker в путь
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "python-worker"))

from core.crypto_signal_formatter import CryptoSignal, CryptoSignalFormatter
from datetime import datetime, timezone

# Создаем тестовый сигнал аналогичный вашему примеру
signal = CryptoSignal(
    sid="crypto-of:BTCUSDT:1764824502259",
    symbol="BTCUSDT",
    side="SHORT",
    entry=93270.00,
    sl=93288.00,
    tp_levels=[93246.60, 93234.00, 93221.40],  # TP1, TP2, TP3
    lot=0.00001030,  # 0.96 USDT / 93270 = 0.00001030 BTC
    atr=30.00,
    confidence=0.83,
    ts=1764824502259,
    source="CryptoOrderFlow",
    reason_mix={
        "p_delta": 0.96,
        "p_speed": 10.07,
        "p_legacy": 3.00,
        "p_confirm": 1.00
    },
    confirmations=["iceberg_refresh=3"],
    trail_profile="rocket_v1",
    position_size_usd=0.96,  # Размер позиции в USDT
    deposit=100.0,  # Депозит
    leverage=100.0  # Плечо 100x
)

# Форматируем сообщение
message = CryptoSignalFormatter.format_telegram_message(signal)

print("=" * 80)
print("НОВЫЙ ФОРМАТ СИГНАЛА")
print("=" * 80)
print(message)
print("=" * 80)
print()
print("ОБЪЯСНЕНИЕ:")
print("- Volume 0.96 USDT = размер позиции в долларах из депозита")
print("- (0.96% депозита) = 0.96 / 100 * 100% = 0.96%")
print("- с плечом 100x = 96 USDT = реальный размер позиции на рынке")
print()
print("Таким образом:")
print("  • Используете из депозита: 0.96 USDT (0.96%)")
print("  • Реальный размер позиции: 96 USDT (за счет плеча 100x)")
print("  • Количество BTC: 0.96 / 93270 = 0.00001030 BTC")
print("=" * 80)

