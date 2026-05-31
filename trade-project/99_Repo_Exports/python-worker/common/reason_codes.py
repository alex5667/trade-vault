from __future__ import annotations

"""
Structured veto/emission reason codes.

Goals:
  - reason_code: stable, human-readable enum string (wire/debug).
  - reason_u16: stable compact uint16 id (wire/metrics).
  - strict-mode: fail fast if reason_code is missing/unknown (staging/CI).

IMPORTANT:
  - Once you ship a reason_u16 mapping to downstream consumers, treat it as ABI.
  - Add new codes only at the end. Do not renumber existing values.
"""


from enum import StrEnum


class ReasonCode(StrEnum):
    # Generic / schema / numeric
    OK = "OK"
    VETO_BAD_NUMERIC = "VETO_BAD_NUMERIC"  # NaN/Inf/invalid inputs
    VETO_INTERNAL_ERROR = "VETO_INTERNAL_ERROR"

    # Market quality / guards
    VETO_SPREAD_WIDE = "VETO_SPREAD_WIDE"
    VETO_COOLDOWN = "VETO_COOLDOWN"
    VETO_TOUCH_SUPPRESSED = "VETO_TOUCH_SUPPRESSED"

    # L2/L3/HTF availability
    VETO_L2_STALE = "VETO_L2_STALE"
    VETO_L2_MISSING = "VETO_L2_MISSING"
    VETO_L3_SPOOF_RISK = "VETO_L3_SPOOF_RISK"

    # Geometry / HTF / regime
    VETO_REGIME_RANGE_BREAKOUT = "VETO_REGIME_RANGE_BREAKOUT"
    VETO_WALL_NEAR = "VETO_WALL_NEAR"
    VETO_MP_CONTRA = "VETO_MP_CONTRA"
    VETO_TAKER_RATE_LOW = "VETO_TAKER_RATE_LOW"
    VETO_NO_WALL_OR_REFILL = "VETO_NO_WALL_OR_REFILL"
    VETO_NO_BLOCKING_CONFIRM = "VETO_NO_BLOCKING_CONFIRM"

    # Min confidence veto
    VETO_CONF_BELOW_MIN = "VETO_CONF_BELOW_MIN"


# Stable uint16 ids. Keep them ABI-stable.
REASON_U16_BY_CODE: dict[str, int] = {
    ReasonCode.OK.value: 0,
    ReasonCode.VETO_BAD_NUMERIC.value: 1,
    ReasonCode.VETO_INTERNAL_ERROR.value: 2,
    ReasonCode.VETO_SPREAD_WIDE.value: 10,
    ReasonCode.VETO_COOLDOWN.value: 11,
    ReasonCode.VETO_TOUCH_SUPPRESSED.value: 12,
    ReasonCode.VETO_L2_STALE.value: 20,
    ReasonCode.VETO_L2_MISSING.value: 21,
    ReasonCode.VETO_L3_SPOOF_RISK.value: 30,
    ReasonCode.VETO_REGIME_RANGE_BREAKOUT.value: 40,
    ReasonCode.VETO_WALL_NEAR.value: 41,
    ReasonCode.VETO_MP_CONTRA.value: 42,
    ReasonCode.VETO_TAKER_RATE_LOW.value: 60,
    ReasonCode.VETO_NO_WALL_OR_REFILL.value: 61,
    ReasonCode.VETO_NO_BLOCKING_CONFIRM.value: 62,
    ReasonCode.VETO_CONF_BELOW_MIN.value: 50,
}


def normalize_reason_code(code: str) -> str:
    # Normalize to our canonical wire format
    return (code or "").strip().upper()


def code_to_u16(reason_code: str) -> int:
    rc = normalize_reason_code(reason_code)
    return int(REASON_U16_BY_CODE.get(rc, 0))


def ensure_reason_fields(
    *,
    reason: str,
    reason_code: str,
    strict: bool,
) -> tuple[str, str, int]:
    """
    Ensures reason/reason_code/reason_u16 are always present and consistent.
    In strict mode, raises ValueError if reason_code is missing/unknown.
    """
    r = (reason or "").strip()
    rc = normalize_reason_code(reason_code)
    if not rc:
        if strict:
            raise ValueError(f"STRICT_REASON_CODES: missing reason_code for reason='{r}'")
        # best-effort fallback
        rc = ReasonCode.VETO_INTERNAL_ERROR.value if r else ReasonCode.OK.value
    u16 = REASON_U16_BY_CODE.get(rc)
    if u16 is None:
        if strict:
            raise ValueError(f"STRICT_REASON_CODES: unknown reason_code='{rc}' for reason='{r}'")
        rc = ReasonCode.VETO_INTERNAL_ERROR.value
        u16 = REASON_U16_BY_CODE[rc]
    return r, rc, int(u16)
