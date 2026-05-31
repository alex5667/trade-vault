# config/unified_pipeline.py
import os
from enum import Enum


class UnifiedPipelineMode(Enum):
    LEGACY = "legacy"  # 0 → полностью legacy
    UNIFIED = "unified"  # 1 → только unified (без fallback)
    SAFE = "safe"       # safe → unified + fallback


def parse_unified_mode_from_env() -> UnifiedPipelineMode:
    """
    Парсит режим unified pipeline из ENV USE_UNIFIED_PIPELINE.

    "0" / "legacy" → UnifiedPipelineMode.LEGACY
    "1" / "unified" → UnifiedPipelineMode.UNIFIED
    "safe" / "" (пусто) → UnifiedPipelineMode.SAFE (по умолчанию)
    """
    raw = os.getenv("USE_UNIFIED_PIPELINE", "").strip().lower()

    if raw in ("0", "legacy"):
        return UnifiedPipelineMode.LEGACY
    elif raw in ("1", "unified"):
        return UnifiedPipelineMode.UNIFIED
    elif raw in ("", "safe"):
        # По умолчанию "safe" — это удобно для постепенного rollout
        return UnifiedPipelineMode.SAFE
    else:
        # Неизвестное значение → логируем и идём в safe
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Unknown USE_UNIFIED_PIPELINE value '{raw}', defaulting to SAFE mode")
        return UnifiedPipelineMode.SAFE


# Глобальная переменная режима (инициализируется один раз)
_current_mode = None


def get_unified_mode() -> UnifiedPipelineMode:
    """Возвращает текущий режим unified pipeline."""
    global _current_mode
    if _current_mode is None:
        _current_mode = parse_unified_mode_from_env()
    return _current_mode


def set_unified_mode_for_testing(mode: UnifiedPipelineMode) -> None:
    """Устанавливает режим для тестирования (только для тестов)."""
    global _current_mode
    _current_mode = mode
