#!/usr/bin/env python3
"""
Конфигурация решений по трейлингу на основе анализа.
Обновляется автоматически на основе отчётов analyze_trades_from_redis_advanced.py
"""

from typing import Dict, Any


# Базовые настройки трейлинга по символам
SYMBOL_TRAILING_CONFIG = {
    "ETHUSDT": {
        "default_trailing": {
            "trail_after_tp1": True,
            "trail_profile": "rocket_v1",
            "trailing_tp1_offset_atr": 0.6,
            "rr_levels": "1.3,2.0,2.7",
            "reason": "ΔExpR=+0.08R, better=62%, trailing полезен"
        },
        "entry_tags": {
            "deltaSpikeZ": {
                "trail_after_tp1": True,
                "trailing_tp1_offset_atr": 0.5,
                "reason": "ΔExpR=+0.12R, better=68%, отличный трейлинг"
            },
            "pullback_to_fvg": {
                "trail_after_tp1": False,
                "reason": "ΔExpR=-0.15R, worse=65%, трейлинг вреден"
            },
            "iceberg_refresh": {
                "trail_after_tp1": True,
                "trailing_tp1_offset_atr": 0.7,
                "reason": "ΔExpR=+0.05R, better=55%, умеренно полезен"
            }
        }
    },

    "BTCUSDT": {
        "default_trailing": {
            "trail_after_tp1": True,
            "trail_profile": "rocket_v1",
            "trailing_tp1_offset_atr": 0.8,  # повышен для снижения false triggers
            "rr_levels": "1.3,2.0,2.7",
            "reason": "ΔExpR=-0.12R, worse=58%, трейлинг требует настройки"
        },
        "entry_tags": {
            "deltaSpikeZ": {
                "trail_after_tp1": True,
                "trailing_tp1_offset_atr": 0.9,
                "reason": "ΔExpR=-0.08R, worse=52%, повышен оффсет"
            },
            "large_volume_surge": {
                "trail_after_tp1": False,
                "reason": "ΔExpR=-0.22R, worse=71%, отключено"
            }
        }
    }
}


def get_trailing_config(symbol: str, entry_tag: str = "") -> Dict[str, Any]:
    """
    Получить настройки трейлинга для символа и entry_tag.

    Args:
        symbol: Символ (ETHUSDT, BTCUSDT, etc.)
        entry_tag: Entry tag (опционально)

    Returns:
        Dict с настройками трейлинга
    """
    symbol_config = SYMBOL_TRAILING_CONFIG.get(symbol, {})
    if not symbol_config:
        # Дефолтная конфигурация
        return {
            "trail_after_tp1": True,
            "trail_profile": "rocket_v1",
            "trailing_tp1_offset_atr": 0.6,
            "rr_levels": "1.3,2.0,2.7",
            "reason": "default config"
        }

    # Сначала проверяем entry_tag-специфичные настройки
    if entry_tag and "entry_tags" in symbol_config:
        tag_config = symbol_config["entry_tags"].get(entry_tag)
        if tag_config:
            return dict(symbol_config["default_trailing"], **tag_config)

    # Возвращаем дефолт для символа
    return symbol_config["default_trailing"]


def update_config_from_analysis(symbol: str, analysis_results: Dict[str, Any]) -> None:
    """
    Обновить конфигурацию на основе результатов анализа.

    Args:
        symbol: Символ для обновления
        analysis_results: Результаты из analyze_trades_from_redis_advanced.py
    """
    global SYMBOL_TRAILING_CONFIG

    if symbol not in SYMBOL_TRAILING_CONFIG:
        SYMBOL_TRAILING_CONFIG[symbol] = {"default_trailing": {}, "entry_tags": {}}

    # Анализ глобальных метрик
    global_metrics = analysis_results.get("global", {})
    delta_expr = global_metrics.get("delta_expectancy_r", 0)
    share_better = global_metrics.get("share_better", 0)
    share_worse = global_metrics.get("share_worse", 0)

    # Логика принятия решений
    current_config = SYMBOL_TRAILING_CONFIG[symbol]["default_trailing"]
    current_offset = current_config.get("trailing_tp1_offset_atr", 0.6)

    if delta_expr > 0.05 and share_better > 0.55:
        # Трейлинг полезен - можно оставить или усилить
        decision = "keep_or_strengthen"
        new_offset = min(current_offset, 0.5)  # можно снизить для агрессивности

    elif delta_expr < -0.05 and share_worse > 0.55:
        # Трейлинг вреден - увеличить дистанцию или отключить
        decision = "weaken_or_disable"
        new_offset = max(current_offset, 0.9)  # увеличить дистанцию

    else:
        # Нейтрально - оставить как есть
        decision = "keep_current"
        new_offset = current_offset

    # Обновить конфигурацию
    SYMBOL_TRAILING_CONFIG[symbol]["default_trailing"].update({
        "trailing_tp1_offset_atr": new_offset,
        "reason": f"Auto-updated: ΔExpR={delta_expr:.3f}, better={share_better:.1%}, decision={decision}"
    })

    # Анализ по entry_tags
    tag_metrics = analysis_results.get("tags", {})
    for tag, metrics in tag_metrics.items():
        tag_delta_expr = metrics.get("delta_expectancy_r", 0)
        tag_better = metrics.get("share_better", 0)

        if tag_delta_expr > 0.1 and tag_better > 0.6:
            # Отличный трейлинг - усилить
            tag_config = {"trail_after_tp1": True, "trailing_tp1_offset_atr": 0.5}
        elif tag_delta_expr < -0.1:
            # Плохой трейлинг - отключить
            tag_config = {"trail_after_tp1": False}
        else:
            # Нейтрально - оставить дефолт
            continue

        tag_config["reason"] = f"Auto-updated: ΔExpR={tag_delta_expr:.3f}, better={tag_better:.1%}"
        SYMBOL_TRAILING_CONFIG[symbol]["entry_tags"][tag] = tag_config


# Пример использования
if __name__ == "__main__":
    # Получить настройки для ETHUSDT deltaSpikeZ
    config = get_trailing_config("ETHUSDT", "deltaSpikeZ")
    print(f"ETHUSDT deltaSpikeZ config: {config}")

    # Пример результатов анализа
    analysis_results = {
        "global": {
            "delta_expectancy_r": 0.08,
            "share_better": 0.62,
            "share_worse": 0.35
        },
        "tags": {
            "pullback_to_fvg": {
                "delta_expectancy_r": -0.15,
                "share_better": 0.32,
                "share_worse": 0.65
            }
        }
    }

    # Обновить конфигурацию
    update_config_from_analysis("ETHUSDT", analysis_results)
    print("Updated config:", SYMBOL_TRAILING_CONFIG["ETHUSDT"])
