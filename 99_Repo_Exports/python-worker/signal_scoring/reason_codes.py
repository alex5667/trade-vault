from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class ReasonCode(str, Enum):
    """
    Stable, structured veto/decision codes.
    Эти строки — контракт для:
      - метрик (labels)
      - калибровки порогов
      - телеметрии/дашбордов
      - downstream consumer'ов (TG/WS)
    """

    OK = "OK"

    # Generic veto reasons
    VETO_UNKNOWN = "VETO_UNKNOWN"
    VETO_CONF_BELOW_MIN = "VETO_CONF_BELOW_MIN"
    VETO_SPREAD_WIDE = "VETO_SPREAD_WIDE"

    # L2 policies / quality
    VETO_L2_MISSING = "VETO_L2_MISSING"
    VETO_L2_STALE = "VETO_L2_STALE"
    VETO_L2_BAD = "VETO_L2_BAD"  # NaN/Inf/schema-corrupt

    # Specific microstructure / spoof risks
    VETO_WALL_NEAR = "VETO_WALL_NEAR"
    VETO_L3_SPOOF_RISK = "VETO_L3_SPOOF_RISK"

    # Regime-based vetoes
    VETO_REGIME_RANGE_BREAKOUT = "VETO_REGIME_RANGE_BREAKOUT"


@dataclass(frozen=True)
class LegacyReasonMap:
    """
    Механизм обратной совместимости:
    старые engine.reason строки (bo_l2_stale/...) -> стабильные ReasonCode.
    """

    legacy: str
    code: ReasonCode


# NOTE: добавляйте сюда по мере миграции остальных veto-веток.
_LEGACY_MAP: list[LegacyReasonMap] = [
    LegacyReasonMap("ok", ReasonCode.OK)
    LegacyReasonMap("conf_below_min_veto", ReasonCode.VETO_CONF_BELOW_MIN)
    LegacyReasonMap("spread_wide", ReasonCode.VETO_SPREAD_WIDE)
    LegacyReasonMap("bo_l2_missing", ReasonCode.VETO_L2_MISSING)
    LegacyReasonMap("bo_l2_stale", ReasonCode.VETO_L2_STALE)
    LegacyReasonMap("bo_l2_bad", ReasonCode.VETO_L2_BAD)
    LegacyReasonMap("bo_l2_veto", ReasonCode.VETO_L2_BAD),  # legacy umbrella -> лучше уточнять
    LegacyReasonMap("range_breakout_veto", ReasonCode.VETO_REGIME_RANGE_BREAKOUT)
    LegacyReasonMap("wall_near_veto", ReasonCode.VETO_WALL_NEAR)
    LegacyReasonMap("l3_spoof_risk", ReasonCode.VETO_L3_SPOOF_RISK)
]


def legacy_reason_to_code(legacy_reason: Optional[str]) -> ReasonCode:
    """
    Best-effort маппинг legacy 'reason' в стабильный ReasonCode.
    Если не нашли — возвращаем VETO_UNKNOWN (и отдельно считаем метрикой).
    """
    if not legacy_reason:
        return ReasonCode.VETO_UNKNOWN
    lr = str(legacy_reason).strip()
    for m in _LEGACY_MAP:
        if m.legacy == lr:
            return m.code
    return ReasonCode.VETO_UNKNOWN


def is_valid_reason_code(code: Optional[str]) -> bool:
    if not code:
        return False
    try:
        ReasonCode(str(code))
        return True
    except Exception:
        return False
