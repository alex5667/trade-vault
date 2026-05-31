from __future__ import annotations

"""StreamIntegrityGate (P5).

A light-weight gate that consumes P5 integrity telemetry and optionally vetoes.

Default policy:
- default/soft: annotate only
- strict: tighten/annotate only (no veto)
- hard: veto on severe sequence gaps or schema drift

This gate intentionally does *not* duplicate tick/book continuity logic.
It only uses already computed indicators:
  - tick_seq_gap_rate_ema / tick_seq_max_gap_window / tick_schema_changed
  - book_seq_gap_rate_ema / book_seq_max_gap_window / book_schema_hash

Thresholds are opt-in; if env is 0, gate is effectively monitor-only.
"""

import os
from dataclasses import dataclass
from typing import Any


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _profile() -> str:
    return os.getenv("GATE_PROFILE", os.getenv("STREAM_INTEGRITY_PROFILE", "default") or "default").strip().lower()


@dataclass
class StreamIntegrityDecision:
    apply: bool
    veto: bool
    gate: str
    reason_code: str
    flags: list[str]
    notes: str = ""


class StreamIntegrityGate:
    def __init__(
        self,
        *,
        enabled: bool,
        mode: str,
        max_gap_rate_ema: float,
        max_gap_window: int,
        veto_on_schema_change: bool,
    ) -> None:
        self.enabled = bool(enabled)
        self.mode = (mode or "auto").strip().lower()
        self.max_gap_rate_ema = float(max_gap_rate_ema)
        self.max_gap_window = int(max_gap_window)
        self.veto_on_schema_change = bool(veto_on_schema_change)

    @staticmethod
    def from_env() -> StreamIntegrityGate:
        enabled = bool(int(os.getenv("STREAM_INTEGRITY_GATE_ENABLED", "1") or 1))
        mode = os.getenv("STREAM_INTEGRITY_MODE", "auto")
        return StreamIntegrityGate(
            enabled=enabled,
            mode=str(mode),
            max_gap_rate_ema=_f(os.getenv("DATA_MAX_SEQ_GAP_RATE_EMA", "0"), 0.0),
            max_gap_window=_i(os.getenv("DATA_MAX_SEQ_GAP_WINDOW", "0"), 0),
            veto_on_schema_change=bool(int(os.getenv("DATA_VETO_ON_SCHEMA_CHANGE", "0") or 0)),
        )

    def _effective_mode(self) -> str:
        if self.mode in ("monitor", "tighten", "veto"):
            return self.mode
        p = _profile()
        if p == "hard":
            return "veto"
        if p == "strict":
            return "tighten"
        return "monitor"

    def evaluate(self, *, indicators: dict[str, Any], symbol: str) -> StreamIntegrityDecision:
        if not self.enabled:
            return StreamIntegrityDecision(apply=False, veto=False, gate="StreamIntegrityGate", reason_code="", flags=[])

        mode = self._effective_mode()
        flags: list[str] = []

        # Inputs
        tg = _f(indicators.get("tick_seq_gap_rate_ema", 0.0), 0.0)
        bg = _f(indicators.get("book_seq_gap_rate_ema", 0.0), 0.0)
        tmax = _i(indicators.get("tick_seq_max_gap_window", 0), 0)
        bmax = _i(indicators.get("book_seq_max_gap_window", 0), 0)
        schema_tick = _i(indicators.get("tick_schema_changed", 0), 0)

        if self.max_gap_rate_ema > 0 and max(tg, bg) >= self.max_gap_rate_ema:
            flags.append("gap_rate_ema_high")
        if self.max_gap_window > 0 and max(tmax, bmax) >= self.max_gap_window:
            flags.append("gap_window_high")
        if self.veto_on_schema_change and schema_tick == 1:
            flags.append("schema_changed")

        if not flags:
            return StreamIntegrityDecision(apply=False, veto=False, gate="StreamIntegrityGate", reason_code="", flags=[])

        if mode != "veto":
            return StreamIntegrityDecision(apply=True, veto=False, gate="StreamIntegrityGate", reason_code="STREAM_INTEGRITY", flags=flags, notes=f"mode={mode}")

        # hard veto
        reason = "VETO_STREAM_INTEGRITY"
        if "schema_changed" in flags:
            reason = "VETO_SCHEMA_DRIFT"
        elif "gap_window_high" in flags:
            reason = "VETO_SEQ_GAP_WINDOW"
        elif "gap_rate_ema_high" in flags:
            reason = "VETO_SEQ_GAP_RATE"

        return StreamIntegrityDecision(apply=True, veto=True, gate="StreamIntegrityGate", reason_code=reason, flags=flags, notes=f"symbol={symbol}")
