from __future__ import annotations

"""BookSanityGate (P5).

A separate gate from DataQualityGate:
- DataQualityGate focuses on time (epoch ms, lag, out-of-order) and upstream
  quarantine semantics.
- BookSanityGate focuses on market microstructure sanity (crossed BBO, NaNs,
  negative depth) and tick-to-book symptoms.

Policy:
- default/soft: annotate only (never veto)
- strict: tighten (optional; here we only annotate to keep P5 minimal)
- hard: veto on a finite set of flags

The gate is designed to be fail-open and should never throw.
"""

import os
import time
from typing import Any
from core.signal_payload import GateDecisionV1




def _profile() -> str:
    return os.getenv("GATE_PROFILE", os.getenv("BOOK_SANITY_PROFILE", "default") or "default").strip().lower()


class BookSanityGate:
    def __init__(
        self,
        *,
        enabled: bool,
        mode: str,
        veto_trade_outside_bbo: bool = False,
        outside_bbo_max_dist_bps: float = 0.0,
    ) -> None:
        self.enabled = bool(enabled)
        self.mode = (mode or "auto").strip().lower()
        # Optional: hard-veto when a trade prints outside the current BBO.
        self.veto_trade_outside_bbo = bool(veto_trade_outside_bbo)
        self.outside_bbo_max_dist_bps = float(max(0.0, float(outside_bbo_max_dist_bps or 0.0)))

    @staticmethod
    def from_env() -> BookSanityGate:
        enabled = bool(int(os.getenv("BOOK_SANITY_GATE_ENABLED", "1") or 1))
        mode = os.getenv("BOOK_SANITY_MODE", "auto")
        return BookSanityGate(
            enabled=enabled,
            mode=str(mode),
            veto_trade_outside_bbo=bool(int(os.getenv("BOOK_SANITY_VETO_TRADE_OUTSIDE_BBO", "0") or 0)),
            outside_bbo_max_dist_bps=float(os.getenv("BOOK_SANITY_OUTSIDE_BBO_MAX_DIST_BPS", "0") or 0.0),
        )

    def _effective_mode(self) -> str:
        if self.mode in ("monitor", "tighten", "veto"):
            return self.mode
        # auto
        p = _profile()
        if p in ("hard", "strict"):
            return "veto" if p == "hard" else "monitor"
        return "monitor"

    def evaluate(self, *, indicators: dict[str, Any], symbol: str) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = int(indicators.get("ts_ms", 0) or 0)
        
        def _make_res(decision: str, reason: str, flags: list[str], notes: dict[str, Any] = None) -> GateDecisionV1:
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="dq_integrity",
                gate="BookSanityGate",
                decision=decision,
                reason_code=reason,
                severity="CRITICAL" if decision == "DENY" else "INFO",
                profile=_profile(),
                fail_policy="OPEN",
                ts_event_ms=ts_ev_ms,
                ts_decision_ms=ts_dec_ms,
                latency_us=latency_us,
                inputs_hash="", # computed by orchestrator if needed
                notes={
                    "flags": flags,
                    **(notes or {})
                }
            )

        if not self.enabled:
            return _make_res("ABSTAIN", "DISABLED", [], {"msg": "gate_disabled"})

        flags = []
        try:
            raw = indicators.get("book_sanity_flags")
            if isinstance(raw, list):
                flags = [str(x) for x in raw if x]
            elif isinstance(raw, str):
                flags = [s.strip() for s in raw.split(",") if s.strip()]
        except Exception:
            flags = []

        mode = self._effective_mode()
        outside_bbo = bool(int(indicators.get("trade_outside_bbo", 0) or 0)) or ("trade_outside_bbo" in flags)
        try:
            outside_bbo_dist_bps = float(indicators.get("trade_outside_bbo_dist_bps", 0.0) or 0.0)
        except Exception:
            outside_bbo_dist_bps = 0.0

        if not flags and not outside_bbo:
            return _make_res("ALLOW", "OK", [], {"msg": "no_flags"})

        # Veto conditions
        veto_flags = {"crossed_bbo", "nan_px", "nan_depth", "neg_qty"}
        do_veto = any(f in veto_flags for f in flags)
        do_trade_outside_veto = bool(
            outside_bbo
            and self.veto_trade_outside_bbo
            and (self.outside_bbo_max_dist_bps <= 0.0 or outside_bbo_dist_bps >= self.outside_bbo_max_dist_bps)
        )

        if mode != "veto":
            return _make_res("ALLOW", "BOOK_SANITY_FLAGS", flags, {"mode": mode})

        if do_veto or do_trade_outside_veto:
            reason = "VETO_BOOK_SANITY"
            if "crossed_bbo" in flags: reason = "VETO_BOOK_CROSS"
            elif "nan_depth" in flags or "nan_px" in flags: reason = "VETO_BOOK_NAN"
            elif "neg_qty" in flags: reason = "VETO_BOOK_NEG_QTY"
            elif do_trade_outside_veto: reason = "VETO_TRADE_OUTSIDE_BBO"

            return _make_res("DENY", reason, flags + (["trade_outside_bbo"] if outside_bbo and "trade_outside_bbo" not in flags else []), {"symbol": symbol, "dist_bps": outside_bbo_dist_bps})

        return _make_res("ALLOW", "BOOK_SANITY_FLAGS", flags, {"mode": mode})
