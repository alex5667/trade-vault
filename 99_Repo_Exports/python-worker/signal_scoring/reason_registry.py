from __future__ import annotations

import os
from collections.abc import Iterable

# =============================================================================
# Structured reason codes -> stable uint16 ids (wire-format)
#
# Зачем:
# - reason_code (строка) удобен для дебага/дашборда
# - reason_u16 (uint16) нужен для стабильного компактного payload/логов/стримов
#
# Правило стабильности:
# - НИКОГДА не переиспользовать и не перенумеровывать уже выданные значения.
# - Только добавлять новые коды в конец.
# =============================================================================

# NOTE: держим числа в "человеческом" диапазоне, но влезаем в uint16.
_REASON_CODE_U16: dict[str, int] = {
    # ------------------------------------------------------------
    # Decision / outcome codes (не только veto)
    #
    # Зачем:
    # - downstream (дашборд/Telegram/консьюмеры outbox) могут считать
    #   распределения outcome по kind/symbol без парсинга текста.
    #
    # Правило:
    # - OK/soft коды живут в том же реестре, что и VETO_*,
    #   чтобы не плодить 2 несовместимых справочника.
    # ------------------------------------------------------------
    "OK": 1,
    # soft-penalty: не запрещаем сигнал, но явно маркируем ухудшение качества
    "SOFT_QUALITY": 10,
    "SOFT_L2_MISSING_FAIL_OPEN": 12,
    "SOFT_GEO_MISSING": 13,

    # Common veto
    "VETO_SPREAD_WIDE": 100,
    "VETO_L2_MISSING": 101,
    "VETO_L2_STALE": 102,
    "VETO_WALL_NEAR": 103,
    "VETO_L3_SPOOF_RISK": 104,
    "VETO_REGIME_RANGE_BREAKOUT": 105,
    "VETO_TAKER_RATE_LOW": 106,
    "VETO_NO_WALL_OR_REFILL": 107,
    "VETO_NO_BLOCKING_CONFIRM": 108,

    # Generic / scoring gates
    "CONF_BELOW_MIN_VETO": 200,
    "VETO_CONF_BELOW_MIN": 220,

    # Veto codes
    "VETO_BAD_NUMERIC": 201,
    "VETO_COOLDOWN": 202,
    "VETO_GENERIC": 203,
    "VETO_INTERNAL_ERROR": 204,
    "VETO_MP_CONTRA": 205,
    "VETO_TOPN_ALERT_COUNT": 206,
    "VETO_TOPN_ALERT_SHARE": 207,
    "VETO_TOPN_CHANGE_COOLDOWN_MS": 208,
    "VETO_TOPN_CHANGE_MIN_SHARE": 209,
    "VETO_TOPN_COOLDOWN_MS": 210,
    "VETO_TOPN_FAMILY_CHANGE_COOLDOWN_MS": 211,
    "VETO_TOPN_FAMILY_CHANGE_MIN_DELTA": 212,
    "VETO_TOPN_FAMILY_CHANGE_MIN_SHARE": 213,
    "VETO_TOPN_FAMILY_CHANGE_MIN_TOTAL_DELTA": 214,
    "VETO_TOPN_FAMILY_CHANGE_MIN_TOTAL_RATIO": 215,
    "VETO_TOPN_MIN_TOTAL": 216,
    "VETO_TOPN_N": 217,
    "VETO_TOPN_WINDOW_MS": 218,
    "VETO_TOUCH_SUPPRESSED": 219,

    # --- SOFT (non-veto) codes: keep in separate numeric band (1000+) to avoid collisions ---
    "SOFT_L3_MISSING": 1001,
    "SOFT_HTF_MISSING": 1002,
    "SOFT_L2_STALE_EXTREME": 1003,
    "SOFT_REASON_MAX": 1004,

    # Legacy aliases (preserve old u16 values for compatibility)
    "SOFT_L3_MISSING_LEGACY": 11,  # старый id для SOFT_L3_MISSING

    # Unknown/fallback codes (унифицированы на VETO_UNKNOWN)
    "VETO_UNKNOWN": 255,  # канон для неизвестных veto
    "UNKNOWN_VETO": 255,  # алиас для совместимости
}

# Канонический decode (u16 → string) с явным выбором канона для алиасов

# Какие u16 допускают алиасы (одно значение -> несколько строк)
_ALLOWED_DUP_U16: set[int] = {255}  # VETO_UNKNOWN/UNKNOWN_VETO

# Явные предпочтения (канон) для конкретных u16
_CANONICAL_BY_U16_OVERRIDE: dict[int, str] = {
    255: "VETO_UNKNOWN",  # алиасы декодируются в канон
}

# 1) build reverse map (u16 -> list of codes)
_U16_TO_CODES: dict[int, list[str]] = {}
for rc, u in _REASON_CODE_U16.items():
    _U16_TO_CODES.setdefault(int(u), []).append(rc)

# 2) validate duplicates (кроме разрешённых)
_bad_dups = {u: rcs for u, rcs in _U16_TO_CODES.items() if len(rcs) > 1 and u not in _ALLOWED_DUP_U16}
if _bad_dups:
    raise ValueError(f"Duplicate u16 values not allowed: {_bad_dups}")

# 3) choose canonical per u16 (стабильный выбор через сортировку)
_U16_TO_CANONICAL: dict[int, str] = {}
for u, rcs in _U16_TO_CODES.items():
    _U16_TO_CANONICAL[u] = sorted(rcs)[0]  # первый по алфавиту

# 4) override canonical where needed
for u, rc in _CANONICAL_BY_U16_OVERRIDE.items():
    if _REASON_CODE_U16.get(rc) != u:
        raise ValueError(f"Canonical override mismatch: u16={u} rc={rc}")
    _U16_TO_CANONICAL[u] = rc

# -----------------------------------------------------------------------------
# Legacy mapping (строки из старых веток/обёрток) -> structured reason_code
#
# Важно: этот слой позволяет постепенно унифицировать причины,
# даже если часть кода пока возвращает "near_big_wall"/"bo_l2_stale" и т.п.
# -----------------------------------------------------------------------------
_LEGACY_TO_STRUCT: dict[str, str] = {
    # breakout / L2
    "bo_l2_missing": "VETO_L2_MISSING",
    "bo_l2_stale": "VETO_L2_STALE",
    "l2_missing": "VETO_L2_MISSING",
    "l2_stale": "VETO_L2_STALE",
    # унификация "wall near" (после вашего решения: это veto)
    "near_big_wall": "VETO_WALL_NEAR",
    "wall_near": "VETO_WALL_NEAR",
    "VETO_WALL_NEAR": "VETO_WALL_NEAR",

    # spread
    "spread_wide": "VETO_SPREAD_WIDE",
    "VETO_SPREAD_WIDE": "VETO_SPREAD_WIDE",

    # absorption
    "taker_rate_low": "VETO_TAKER_RATE_LOW",
    "VETO_TAKER_RATE_LOW": "VETO_TAKER_RATE_LOW",
    "no_wall_or_refill": "VETO_NO_WALL_OR_REFILL",
    "VETO_NO_WALL_OR_REFILL": "VETO_NO_WALL_OR_REFILL",
    "no_blocking_confirm": "VETO_NO_BLOCKING_CONFIRM",
    "VETO_NO_BLOCKING_CONFIRM": "VETO_NO_BLOCKING_CONFIRM",

    # scoring gate
    "conf_below_min_veto": "CONF_BELOW_MIN_VETO",
    "CONF_BELOW_MIN_VETO": "CONF_BELOW_MIN_VETO",

    # decision/outcome legacy (на будущее; сейчас почти не используется)
    "ok": "OK",
    "soft_quality": "SOFT_QUALITY",
}


def legacy_reason_to_code(reason: str) -> str:
    """
    Convert legacy/free-form reason into structured reason_code.
    Возвращает строку structured-кода (например "VETO_WALL_NEAR").

    Поведение:
    - если reason уже structured и известен -> вернём как есть
    - если reason legacy -> маппим
    - если reason неизвестен -> "UNKNOWN_VETO" (fail-open по кодам)
    """
    r = (reason or "").strip()
    if not r:
        return "UNKNOWN_VETO"
    if r in _REASON_CODE_U16:
        return r
    if r in _LEGACY_TO_STRUCT:
        return _LEGACY_TO_STRUCT[r]
    return "UNKNOWN_VETO"


def reason_code_to_u16(reason_code: str, *, strict: bool | None = None) -> int:
    """
    Structured reason_code -> stable uint16.

    strict:
      - None: берём из env STRICT_REASON_CODES (default 0)
      - True: неизвестный code => ValueError (лучше для CI/канареек)
      - False: неизвестный code => 0 (fail-open)
    """
    if strict is None:
        strict = os.getenv("STRICT_REASON_CODES", "0").lower() in {"1", "true", "yes", "on"}

    c = (reason_code or "").strip()
    if not c:
        c = "VETO_UNKNOWN"  # канон
    v = _REASON_CODE_U16.get(c)
    if v is not None:
        return int(v)
    if strict:
        raise ValueError(f"Unknown reason_code for u16 mapping: {c}")
    # fail-open: неизвестный код не должен блокировать сигнал/валидацию
    return 0


def reason_codes_to_u16s(codes: Iterable[str]) -> list[int]:
    out: list[int] = []
    for c in codes:
        u = reason_code_to_u16(c)
        if u:
            out.append(u)
    return out


def is_known_reason_code(reason_code: str) -> bool:
    """Утилита для тестов/валидации конфигов."""
    return (reason_code or "").strip() in _REASON_CODE_U16


def u16_to_reason_code(u16: int) -> str:
    """
    Reverse mapping used by formatter/debug tooling.
    Unknown codes decode to VETO_UNKNOWN (канон).
    O(1) lookup using pre-built canonical map.
    """
    return _U16_TO_CANONICAL.get(int(u16), "VETO_UNKNOWN")


def normalize_reason(reason: str, reason_code: str | None = None) -> tuple[str, str, int]:
    """
    Returns (original_reason, structured_code, u16_id)
    """
    if reason_code:
        # Explicit override
        rc = reason_code
    else:
        rc = legacy_reason_to_code(reason)
    u = reason_code_to_u16(rc)
    return reason, rc, u


# Compatibility aliases
map_legacy_reason_code = legacy_reason_to_code

def iter_known_reason_codes() -> Iterable[str]:
    """Return an iterable of all registered reason codes."""
    return _REASON_CODE_U16.keys()

def iter_golden_aliases() -> Iterable[str]:
    """Return an iterable of all legacy aliases."""
    return _LEGACY_TO_STRUCT.keys()

LEGACY_REASON_ALIASES = _LEGACY_TO_STRUCT


# =============================================================================
# CI/check script for validating all VETO_/SOFT_ codes used in codebase
# =============================================================================

def check_all_reason_codes_registered() -> None:
    """
    Validate that all VETO_/SOFT_ string literals in codebase are registered in _REASON_CODE_U16.
    This prevents silent 0-u16 mappings in production.
    """
    import re
    import subprocess
    import sys
    from pathlib import Path

    REPO_ROOT = Path(__file__).resolve().parents[2]  # python-worker/signal_scoring -> repo root

    known = set(_REASON_CODE_U16.keys())

    # ripgrep all string literals with VETO_/SOFT_
    rg = subprocess.run(
        ["rg", "-n", r'["\'](VETO_[A-Z0-9_]+|SOFT_[A-Z0-9_]+)["\']', str(REPO_ROOT)],
        capture_output=True,
        text=True,
    )

    if rg.returncode not in (0, 1):  # 1 = nothing found
        print(rg.stderr, file=sys.stderr)
        sys.exit(2)

    pat = re.compile(r'["\'](VETO_[A-Z0-9_]+|SOFT_[A-Z0-9_]+)["\']')
    found: set[str] = set()

    for line in rg.stdout.splitlines():
        m = pat.search(line)
        if m:
            found.add(m.group(1))

    unknown = sorted(found - known)

    if unknown:
        print("Unknown reason codes used in codebase (not in _REASON_CODE_U16):")
        for x in unknown:
            print("  -", x)
        sys.exit(1)

    print("OK: all VETO_/SOFT_ codes are registered.")


if __name__ == "__main__":
    check_all_reason_codes_registered()
