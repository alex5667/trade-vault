from __future__ import annotations
"""atr_horizon_trailing_canary.py — Phase 2.6: canary router for trailing offset surface.

Separate from the gate canary (atr_horizon_canary.py / ATR_HORIZON_GATE_MODE).
Controls whether trailing offset ATR is replaced by profile-derived values.

Modes (ATR_HORIZON_TRAILING_MODE):
  off     — shadow only, never replace trailing offsets
  shadow  — default; candidate surface is always computed but never applied
  canary  — replace on sticky subset (symbol × regime × scenario × sid hash < share)
  enforce — replace on all allowed symbols

ENV:
  ATR_HORIZON_TRAILING_MODE         : off | shadow | canary | enforce  (default: shadow)
  ATR_HORIZON_TRAILING_SYMBOLS      : comma-separated symbol allowlist  (default: all)
  ATR_HORIZON_TRAILING_CANARY_SHARE : float 0..1                        (default: 0.0)

Rollback: ATR_HORIZON_TRAILING_MODE=shadow  →  instant, no code deploy.
"""

import hashlib
import os
from dataclasses import asdict, dataclass
from typing import Any, Dict


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return default


def _env_set(name: str) -> set[str]:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return set()
    return {x.strip().upper() for x in raw.split(",") if x.strip()}


def _u01(key: str) -> float:
    """Deterministic uniform-[0,1) from string key (SHA-1)."""
    h = hashlib.sha1(key.encode("utf-8")).hexdigest()
    return (int(h[:8], 16) % 10_000) / 10_000.0


@dataclass(frozen=True)
class TrailingSurfaceCanaryDecision:
    mode: str
    should_apply: bool
    share_used: float
    sticky_key: str
    reason_code: str


def should_apply_trailing_surface(
    *,
    symbol: str,
    sid: str,
    regime: str = "",
    scenario: str = "",
) -> Dict[str, Any]:
    """Return application decision for live stop/entry surface.

    Always fail-open: any exception → shadow/no-apply dict.

    Returns dict (not dataclass) so callers can safely use .get() without import.
    """
    try:
        mode = str(os.getenv("ATR_HORIZON_TRAILING_MODE", "shadow") or "shadow").strip().lower()
        share = max(0.0, min(1.0, _env_float("ATR_HORIZON_TRAILING_CANARY_SHARE", 0.0)))
        allow_symbols = _env_set("ATR_HORIZON_TRAILING_SYMBOLS")

        symbol_u = str(symbol or "").upper()
        regime_l = str(regime or "na").lower()
        scenario_l = str(scenario or "na").lower()
        sid_s = str(sid or "")
        sticky_key = f"{symbol_u}|{regime_l}|{scenario_l}|{sid_s}"

        if mode == "off":
            return asdict(TrailingSurfaceCanaryDecision(
                mode=mode,
                should_apply=False,
                share_used=0.0,
                sticky_key=sticky_key,
                reason_code="TRAILING_SURFACE_OFF",
            ))

        if mode == "enforce":
            if allow_symbols and symbol_u not in allow_symbols:
                return asdict(TrailingSurfaceCanaryDecision(
                    mode=mode,
                    should_apply=False,
                    share_used=1.0,
                    sticky_key=sticky_key,
                    reason_code="TRAILING_SURFACE_SYMBOL_FILTERED",
                ))
            return asdict(TrailingSurfaceCanaryDecision(
                mode=mode,
                should_apply=True,
                share_used=1.0,
                sticky_key=sticky_key,
                reason_code="TRAILING_SURFACE_ENFORCE_ALL",
            ))

        if mode == "canary":
            if allow_symbols and symbol_u not in allow_symbols:
                return asdict(TrailingSurfaceCanaryDecision(
                    mode=mode,
                    should_apply=False,
                    share_used=share,
                    sticky_key=sticky_key,
                    reason_code="TRAILING_SURFACE_SYMBOL_FILTERED",
                ))
            u = _u01(sticky_key)
            selected = bool(u < share)
            return asdict(TrailingSurfaceCanaryDecision(
                mode=mode,
                should_apply=selected,
                share_used=share,
                sticky_key=sticky_key,
                reason_code="TRAILING_SURFACE_CANARY_APPLY" if selected else "TRAILING_SURFACE_CANARY_SHADOW",
            ))

        # default = shadow (any unrecognised value)
        return asdict(TrailingSurfaceCanaryDecision(
            mode="shadow",
            should_apply=False,
            share_used=share,
            sticky_key=sticky_key,
            reason_code="TRAILING_SURFACE_SHADOW_ONLY",
        ))

    except Exception:
        # absolute fail-open: never block trading on a routing bug
        return {
            "mode": "shadow",
            "should_apply": False,
            "share_used": 0.0,
            "sticky_key": "",
            "reason_code": "TRAILING_SURFACE_ERROR_FALLBACK",
        }
