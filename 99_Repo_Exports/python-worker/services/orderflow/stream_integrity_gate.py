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
import time
from typing import Any
from core.signal_payload import GateDecisionV1


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




class StreamIntegrityGate:
    def __init__(
        self,
        *,
        enabled: bool,
        mode: str,
        max_gap_rate_ema: float,
        max_dup_rate_ema: float = 0.0,
        max_gap_window: int,
        veto_on_schema_change: bool,
    ) -> None:
        self.enabled = bool(enabled)
        self.mode = (mode or "auto").strip().lower()
        self.max_gap_rate_ema = float(max_gap_rate_ema)
        # Duplicate-rate veto threshold (opt-in via DATA_MAX_DUP_RATE / DATA_MAX_DUP_RATE_EMA).
        self.max_dup_rate_ema = float(max_dup_rate_ema)
        self.max_gap_window = int(max_gap_window)
        self.veto_on_schema_change = bool(veto_on_schema_change)

    @staticmethod
    def from_env() -> StreamIntegrityGate:
        enabled = bool(int(os.getenv("STREAM_INTEGRITY_GATE_ENABLED", "1") or 1))
        mode = os.getenv("STREAM_INTEGRITY_MODE", "auto")
        # DATA_MAX_SEQ_GAP_RATE is an alias for DATA_MAX_SEQ_GAP_RATE_EMA (shorter name).
        gap_rate_env = os.getenv("DATA_MAX_SEQ_GAP_RATE_EMA", os.getenv("DATA_MAX_SEQ_GAP_RATE", "0"))
        # DATA_MAX_DUP_RATE is an alias for DATA_MAX_DUP_RATE_EMA.
        dup_rate_env = os.getenv("DATA_MAX_DUP_RATE_EMA", os.getenv("DATA_MAX_DUP_RATE", "0"))
        return StreamIntegrityGate(
            enabled=enabled,
            mode=str(mode),
            max_gap_rate_ema=_f(gap_rate_env, 0.0),
            max_dup_rate_ema=_f(dup_rate_env, 0.0),
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

    def evaluate(self, *, indicators: dict[str, Any], symbol: str) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = int(indicators.get("ts_ms", 0) or 0)

        def _make_res(decision: str, reason: str, flags: list[str], notes: dict[str, Any] = None) -> GateDecisionV1:  # type: ignore
            latency_us = int((time.monotonic() - t0) * 1_000_000)  # type: ignore
            return GateDecisionV1(
                stage="dq_integrity",
                gate="StreamIntegrityGate",
                decision=decision,
                reason_code=reason,
                severity="CRITICAL" if decision == "DENY" else "INFO",
                profile=_profile(),
                fail_policy="OPEN",
                ts_event_ms=ts_ev_ms,
                ts_decision_ms=ts_dec_ms,
                latency_us=latency_us,
                inputs_hash="",
                notes={
                    "flags": flags,
                    **(notes or {})
                }
            )

        if not self.enabled:
            return _make_res("ABSTAIN", "DISABLED", [], {"msg": "gate_disabled"})

        mode = self._effective_mode()
        flags: list[str] = []

        # Inputs
        tg = _f(indicators.get("tick_seq_gap_rate_ema", 0.0), 0.0)
        bg = _f(indicators.get("book_seq_gap_rate_ema", 0.0), 0.0)
        td = _f(indicators.get("tick_seq_dup_rate_ema", 0.0), 0.0)
        bd = _f(indicators.get("book_seq_dup_rate_ema", 0.0), 0.0)
        tmax = _i(indicators.get("tick_seq_max_gap_window", 0), 0)
        bmax = _i(indicators.get("book_seq_max_gap_window", 0), 0)
        schema_tick = _i(indicators.get("tick_schema_changed", 0), 0)

        if self.max_gap_rate_ema > 0 and max(tg, bg) >= self.max_gap_rate_ema:
            flags.append("gap_rate_ema_high")
        if self.max_dup_rate_ema > 0 and max(td, bd) >= self.max_dup_rate_ema:
            flags.append("dup_rate_ema_high")
        if self.max_gap_window > 0 and max(tmax, bmax) >= self.max_gap_window:
            flags.append("gap_window_high")
        if self.veto_on_schema_change and schema_tick == 1:
            flags.append("schema_changed")

        if not flags:
            return _make_res("ALLOW", "OK", [], {"msg": "no_flags"})

        if mode != "veto":
            return _make_res("ALLOW", "STREAM_INTEGRITY", flags, {"mode": mode})

        # hard veto
        reason = "VETO_STREAM_INTEGRITY"
        if "schema_changed" in flags: reason = "VETO_SCHEMA_DRIFT"
        elif "gap_window_high" in flags: reason = "VETO_SEQ_GAP_WINDOW"
        elif "gap_rate_ema_high" in flags: reason = "VETO_SEQ_GAP_RATE"
        elif "dup_rate_ema_high" in flags: reason = "VETO_DUP_RATE"

        return _make_res("DENY", reason, flags, {"symbol": symbol})
