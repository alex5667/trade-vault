#!/usr/bin/env python3
"""
Тестовый скрипт для проверки новой системы калибровки TRAILING_TP1_OFFSET_ATR
"""

import os
import psycopg2
from tools.trailing_tp1_calibration import calibrate_trailing_offset, score_offset

def test_calibration():
    """Тестирование калибровки на примере ETHUSDT"""

    # Параметры подключения
    dsn = os.getenv("TRADES_DB_DSN", "postgresql://postgres:postgres@localhost:5432/scanner_analytics")

    try:
        conn = psycopg2.connect(dsn)
        print("✅ Подключение к БД успешно")

        # Тестируем калибровку для ETHUSDT
        source = "CryptoOrderFlow"
        symbol = "ETHUSDT"
        offset_mults = [0.3, 0.4, 0.5, 0.6, 0.7]

        print(f"🔍 Калибровка для {symbol}...")
        print(f"Тестируем offset_mult: {offset_mults}")

        best_stats, all_stats = calibrate_trailing_offset(
            conn=conn,
            source=source,
            symbol=symbol,
            offset_mult_list=offset_mults,
            limit=50,  # меньше для теста
            use_mfe_exit=False,
        )

        if best_stats is None:
            print("❌ Нет данных для калибровки")
            return

        print("\n📊 Результаты по offset_mult:")
        for s in all_stats:
            sc = score_offset(s)
            print(
                f"offset={s.offset_mult:.2f} "
                f"count={s.count} "
                f"expR={s.expectancy_r:.3f} "
                f"giveback={s.avg_giveback_r:.3f} "
                f"missed={s.avg_missed_r:.3f} "
                f"fake={s.share_fake_stopout:.3f} "
                f"score={sc:.3f}"
            )

        print(f"\n🎯 Рекомендация:")
        print(
            f"offset={best_stats.offset_mult:.2f} "
            f"expR={best_stats.expectancy_r:.3f} "
            f"giveback={best_stats.avg_giveback_r:.3f} "
            f"missed={best_stats.avg_missed_r:.3f} "
            f"fake={best_stats.share_fake_stopout:.3f} "
            f"count={best_stats.count}"
        )

    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    test_calibration()
