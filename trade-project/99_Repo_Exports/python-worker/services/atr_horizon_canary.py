from __future__ import annotations

"""atr_horizon_canary.py — Phase 2.3A: canary-enforce router for horizon DQ gate.

Modes (ATR_HORIZON_GATE_MODE):
  off     — no enforcement ever (full shadow)
  shadow  — default; shadow always computed, never enforced (default)
  canary  — enforce on sticky subset (symbol × regime × scenario × sid hash < share)
  enforce — enforce on all allowed symbols

ENV:
  ATR_HORIZON_GATE_MODE        : off | shadow | canary | enforce  (default: shadow)
  ATR_HORIZON_GATE_SYMBOLS     : comma-separated symbol allowlist  (default: all)
  ATR_HORIZON_GATE_CANARY_SHARE: float 0..1                        (default: 0.0)

Rollback: ATR_HORIZON_GATE_MODE=shadow  →  instant, no code deploy.
"""

import hashlib
import os
from dataclasses import asdict, dataclass
from typing import Any


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return default


def _env_set(name: str) -> set[str]:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return set()
    return {x.strip().upper() for x in raw.split(",") if x.strip()}


def _stable_u01(key: str) -> float:
    """Deterministic uniform-[0,1) from string key (SHA-1)."""
    h = hashlib.sha1(key.encode("utf-8")).hexdigest()
    return (int(h[:8], 16) % 10_000) / 10_000.0


@dataclass(frozen=True)
class HorizonGateCanaryDecision:
    mode: str
    should_enforce: bool
    share_used: float
    sticky_key: str
    reason_code: str


def should_enforce_horizon_gate(
    *,
    symbol: str,
    sid: str,
    regime: str = "",
    scenario: str = "",
) -> dict[str, Any]:
    """Return enforcement decision for horizon DQ gate.

    Always fail-open: any exception → shadow/no-enforce dict.

    Returns dict (not dataclass) so callers can safely use .get() without import.
    """
    try:
        mode = (os.getenv("ATR_HORIZON_GATE_MODE", "shadow") or "shadow").strip().lower()
        allow_symbols = _env_set("ATR_HORIZON_GATE_SYMBOLS")
        share = max(0.0, min(1.0, _env_float("ATR_HORIZON_GATE_CANARY_SHARE", 0.0)))

        symbol_u = (symbol or "").upper()
        regime_l = (regime or "na").lower()
        scenario_l = (scenario or "na").lower()
        sid_s = (sid or "")
        sticky_key = f"{symbol_u}|{regime_l}|{scenario_l}|{sid_s}"

        if mode == "off":
            return asdict(HorizonGateCanaryDecision(
                mode=mode,
                should_enforce=False,
                share_used=0.0,
                sticky_key=sticky_key,
                reason_code="HZ_GATE_OFF",
            ))

        if mode == "enforce":
            if allow_symbols and symbol_u not in allow_symbols:
                return asdict(HorizonGateCanaryDecision(
                    mode=mode,
                    should_enforce=False,
                    share_used=1.0,
                    sticky_key=sticky_key,
                    reason_code="HZ_GATE_SYMBOL_FILTERED",
                ))
            return asdict(HorizonGateCanaryDecision(
                mode=mode,
                should_enforce=True,
                share_used=1.0,
                sticky_key=sticky_key,
                reason_code="HZ_GATE_ENFORCE_ALL",
            ))

        if mode == "canary":
            if allow_symbols and symbol_u not in allow_symbols:
                return asdict(HorizonGateCanaryDecision(
                    mode=mode,
                    should_enforce=False,
                    share_used=share,
                    sticky_key=sticky_key,
                    reason_code="HZ_GATE_SYMBOL_FILTERED",
                ))
            u = _stable_u01(sticky_key)
            selected = (u < share)
            return asdict(HorizonGateCanaryDecision(
                mode=mode,
                should_enforce=selected,
                share_used=share,
                sticky_key=sticky_key,
                reason_code="HZ_GATE_CANARY_SELECTED" if selected else "HZ_GATE_CANARY_SHADOW",
            ))

        # default = shadow (any unrecognised value)
        return asdict(HorizonGateCanaryDecision(
            mode="shadow",
            should_enforce=False,
            share_used=share,
            sticky_key=sticky_key,
            reason_code="HZ_GATE_SHADOW_ONLY",
        ))
    except Exception:
        # absolute fail-open: never block trading on a routing bug
        return {
            "mode": "shadow",
            "should_enforce": False,
            "share_used": 0.0,
            "sticky_key": "",
            "reason_code": "HZ_GATE_ERROR_FALLBACK",
        }
