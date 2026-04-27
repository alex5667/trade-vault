from __future__ import annotations

from typing import Tuple

from signal_scoring import reason_registry as rr


def normalize_reason_code(reason: str) -> str:
    """
    Конвертирует любую легаси/отладочную строку причины в стабильный структурированный reason_code.

    Почему:
      - валидаторы исторически возвращали строки свободного формата ("near_big_wall", "bo_l2_veto", ...)
      - раскатка / дашборды требуют стабильных ключей с ограниченной кардинальностью
      - reason_registry является единственным источником правды для маппинга + uint16 wire ids
    """
    try:
        return rr.normalize_reason(reason=reason, reason_code="")[1]  # return reason_code
    except Exception:
        # Fail-open: никогда не крашить горячий путь из-за конвертации причины.
        return "OK"


def reason_u16(reason_code: str) -> int:
    """
    Маппит структурированный reason_code -> стабильный uint16 (wire ABI).
    """
    try:
        return rr.reason_code_to_u16(reason_code)
    except Exception:
        return 0


def normalize_and_u16(reason: str) -> Tuple[str, int]:
    rc = normalize_reason_code(reason)
    return rc, reason_u16(rc)
