from __future__ import annotations

"""atr_horizon_live_surface_canary.py — Phase 2.4A: canary router for live stop/entry TTL surface.

Separate from the gate canary (atr_horizon_canary.py / ATR_HORIZON_GATE_MODE).
Controls whether sl_price / tp1_price / max_signal_age_ms are replaced by ATR-derived values.

Modes (ATR_HORIZON_LIVE_SURFACE_MODE):
  off     — shadow only, never replace live prices
  shadow  — default; candidate surface is always computed but never applied
  canary  — replace on sticky subset (symbol × regime × scenario × sid hash < share)
  enforce — replace on all allowed symbols

ENV:
  ATR_HORIZON_LIVE_SURFACE_MODE         : off | shadow | canary | enforce  (default: shadow)
  ATR_HORIZON_LIVE_SURFACE_SYMBOLS      : comma-separated symbol allowlist  (default: all)
  ATR_HORIZON_LIVE_SURFACE_CANARY_SHARE : float 0..1                        (default: 0.0)

Rollback: ATR_HORIZON_LIVE_SURFACE_MODE=shadow  →  instant, no code deploy.
trailing is NOT controlled by this router — trailing stays on get_atr(pos.symbol) path.
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


def _u01(key: str) -> float:
    """Deterministic uniform-[0,1) from string key (SHA-1)."""
    h = hashlib.sha1(key.encode("utf-8")).hexdigest()
    return (int(h[:8], 16) % 10_000) / 10_000.0


@dataclass(frozen=True)
class LiveSurfaceCanaryDecision:
    mode: str
    should_apply: bool
    share_used: float
    sticky_key: str
    reason_code: str


def should_apply_live_surface(
    *,
    symbol: str,
    sid: str,
    regime: str = "",
    scenario: str = "",
) -> dict[str, Any]:
    """Return application decision for live stop/entry surface.

    Always fail-open: any exception → shadow/no-apply dict.

    Returns dict (not dataclass) so callers can safely use .get() without import.
    """
    try:
        mode = (os.getenv("ATR_HORIZON_LIVE_SURFACE_MODE", "shadow") or "shadow").strip().lower()
        share = max(0.0, min(1.0, _env_float("ATR_HORIZON_LIVE_SURFACE_CANARY_SHARE", 0.0)))
        allow_symbols = _env_set("ATR_HORIZON_LIVE_SURFACE_SYMBOLS")

        symbol_u = (symbol or "").upper()
        regime_l = (regime or "na").lower()
        scenario_l = (scenario or "na").lower()
        sid_s = (sid or "")
        sticky_key = f"{symbol_u}|{regime_l}|{scenario_l}|{sid_s}"

        if mode == "off":
            return asdict(LiveSurfaceCanaryDecision(
                mode=mode,
                should_apply=False,
                share_used=0.0,
                sticky_key=sticky_key,
                reason_code="LIVE_SURFACE_OFF",
            ))

        if mode == "enforce":
            if allow_symbols and symbol_u not in allow_symbols:
                return asdict(LiveSurfaceCanaryDecision(
                    mode=mode,
                    should_apply=False,
                    share_used=1.0,
                    sticky_key=sticky_key,
                    reason_code="LIVE_SURFACE_SYMBOL_FILTERED",
                ))
            return asdict(LiveSurfaceCanaryDecision(
                mode=mode,
                should_apply=True,
                share_used=1.0,
                sticky_key=sticky_key,
                reason_code="LIVE_SURFACE_ENFORCE_ALL",
            ))

        if mode == "canary":
            if allow_symbols and symbol_u not in allow_symbols:
                return asdict(LiveSurfaceCanaryDecision(
                    mode=mode,
                    should_apply=False,
                    share_used=share,
                    sticky_key=sticky_key,
                    reason_code="LIVE_SURFACE_SYMBOL_FILTERED",
                ))
            u = _u01(sticky_key)
            selected = bool(u < share)
            return asdict(LiveSurfaceCanaryDecision(
                mode=mode,
                should_apply=selected,
                share_used=share,
                sticky_key=sticky_key,
                reason_code="LIVE_SURFACE_CANARY_APPLY" if selected else "LIVE_SURFACE_CANARY_SHADOW",
            ))

        # default = shadow (any unrecognised value)
        return asdict(LiveSurfaceCanaryDecision(
            mode="shadow",
            should_apply=False,
            share_used=share,
            sticky_key=sticky_key,
            reason_code="LIVE_SURFACE_SHADOW_ONLY",
        ))

    except Exception:
        # absolute fail-open: never block trading on a routing bug
        return {
            "mode": "shadow",
            "should_apply": False,
            "share_used": 0.0,
            "sticky_key": "",
            "reason_code": "LIVE_SURFACE_ERROR_FALLBACK",
        }
