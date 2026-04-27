"""Helpers to control Prometheus label cardinality and emission rate.

Why:
- Per-symbol labels can explode cardinality in crypto pipelines.
- Some gauges (EMA) don't need per-tick updates; throttling reduces CPU.

ENV (recommended):
  TICK_QUALITY_SYMBOL_ALLOWLIST=BTCUSDT,ETHUSDT
  TICK_QUALITY_SYMBOL_LABEL_MODE=collapse   # collapse|skip|allow
  TICK_QUALITY_EMA_UPDATE_MIN_MS=250        # emit EMA gauges at most every N ms per label
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Set


def _parse_allowlist(raw: Optional[str]) -> Optional[Set[str]]:
    if not raw:
        return None
    parts = [p.strip().upper() for p in str(raw).split(",") if p.strip()]
    return set(parts) if parts else None


def normalize_symbol(symbol: Optional[str]) -> str:
    return (symbol or "").strip().upper()


def symbol_label(symbol: Optional[str], allowlist: Optional[Set[str]], mode: str = "collapse") -> Optional[str]:
    """Return a safe label value, or None to skip metric emission.

    mode:
      - allow: always return SYMBOL (no protection)
      - collapse: if not in allowlist -> "__other__"
      - skip: if not in allowlist -> None (do not emit metrics for that symbol)
    """
    sym = normalize_symbol(symbol)
    m = (mode or "collapse").strip().lower()
    if m not in ("allow", "collapse", "skip"):
        m = "collapse"

    if allowlist is None:
        # No allowlist configured -> keep as-is, but still avoid empty labels.
        return sym or "__empty__"

    if sym in allowlist:
        return sym

    if m == "skip":
        return None
    if m == "allow":
        return sym or "__empty__"
    return "__other__"


def should_emit(now_ms: int, last_emit_ms: int, min_interval_ms: int) -> bool:
    """Time-based throttle guard."""
    try:
        mi = int(min_interval_ms)
        if mi <= 0:
            return True
        return int(now_ms) - int(last_emit_ms) >= mi
    except Exception:
        return True


@dataclass
class TickMetricLimiter:
    """Stateful limiter for per-symbol emission."""

    allowlist: Optional[Set[str]]
    mode: str = "collapse"
    ema_min_update_ms: int = 250

    def label(self, symbol: Optional[str]) -> Optional[str]:
        return symbol_label(symbol, self.allowlist, self.mode)
