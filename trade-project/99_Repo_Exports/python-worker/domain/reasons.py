from enum import Enum, StrEnum
from typing import Any

from common.enums import VetoReason


class ReasonCode(StrEnum):
    """
    Reason code for signal generation pipeline gates.
    Maps to VetoReason for backward compatibility.
    """
    pass

# Dynamically recreate using Enum API to avoid issues
ReasonCode = Enum('ReasonCode', {item.name: item.value for item in VetoReason}, type=str)

def normalize_reason(r: Any) -> str:
    """Normalize reason code to string"""
    if isinstance(r, Enum):
        return str(r.value)
    return str(r)
