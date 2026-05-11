"""
Конфигурация TP (Take Profit) для торговых позиций.

Вынесено в отдельный модуль для избежания circular imports.
"""

import os


def parse_tp_ratio(value: str = None) -> list[float]:  # type: ignore
    """
    Парсинг переменной окружения TP_RATIO или переданной строки.
    
    Формат: "0.5,0.3,0.2" или "50,30,20" (проценты)
    По умолчанию: [0.50, 0.30, 0.20]
    
    Returns:
        Список долей закрытия для TP1, TP2, TP3
    """
    if not value:
        value = os.getenv("TP_RATIO")  # type: ignore

    if not value:
        return [0.50, 0.30, 0.20]  # Значение по умолчанию

    try:
        parts = [part.strip() for part in value.split(",") if part.strip()]
        if len(parts) < 3:
            # Если указано меньше 3 значений, дополняем нулями
            parts.extend(["0.0"] * (3 - len(parts)))

        ratios = []
        for part in parts[:3]:
            ratio = float(part)
            # Если значение > 1, считаем это процентами и конвертируем
            if ratio > 1.0:
                ratio = ratio / 100.0
            ratios.append(ratio)

        # Нормализация: если сумма > 1, нормализуем
        total = sum(ratios)
        if total > 1.0:
            ratios = [r / total for r in ratios]

        return ratios
    except (ValueError, TypeError) as e:
        import logging
        logging.warning(f"⚠️ Invalid TP_RATIO format '{value}', using default [0.50, 0.30, 0.20]: {e}")
        return [0.50, 0.30, 0.20]


# Значение по умолчанию для использования в других модулях
TP_RATIOS_DEFAULT = tuple(parse_tp_ratio())

