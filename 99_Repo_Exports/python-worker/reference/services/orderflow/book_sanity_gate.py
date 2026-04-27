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
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


@dataclass
class BookSanityDecision:
    apply: bool
    veto: bool
    gate: str
    reason_code: str
    flags: List[str]
    notes: str = ""


def _profile() -> str:
    return str(os.getenv("GATE_PROFILE", os.getenv("BOOK_SANITY_PROFILE", "default")) or "default").strip().lower()


class BookSanityGate:
    def __init__(
        self,
        *,
        enabled: bool,
        mode: str,
    ) -> None:
        self.enabled = bool(enabled)
        self.mode = str(mode or "auto").strip().lower()

    @staticmethod
    def from_env() -> "BookSanityGate":
        enabled = bool(int(os.getenv("BOOK_SANITY_GATE_ENABLED", "1") or 1))
        mode = os.getenv("BOOK_SANITY_MODE", "auto")
        return BookSanityGate(enabled=enabled, mode=str(mode))

    def _effective_mode(self) -> str:
        if self.mode in ("monitor", "tighten", "veto"):
            return self.mode
        # auto
        p = _profile()
        if p in ("hard", "strict"):
            return "veto" if p == "hard" else "monitor"
        return "monitor"

    def evaluate(self, *, indicators: Dict[str, Any], symbol: str) -> BookSanityDecision:
        if not self.enabled:
            return BookSanityDecision(apply=False, veto=False, gate="BookSanityGate", reason_code="", flags=[])

        flags = []
        try:
            raw = indicators.get("book_sanity_flags")
            if isinstance(raw, list):
                flags = [str(x) for x in raw if x]
            elif isinstance(raw, str):
                # may arrive as CSV
                flags = [s.strip() for s in raw.split(",") if s.strip()]
        except Exception:
            flags = []

        mode = self._effective_mode()

        # default behavior: annotate only if any flags exist
        if not flags:
            return BookSanityDecision(apply=False, veto=False, gate="BookSanityGate", reason_code="", flags=[])

        # Veto conditions (finite set)
        veto_flags = {"crossed_bbo", "nan_px", "nan_depth", "neg_qty"}
        do_veto = any(f in veto_flags for f in flags)

        if mode != "veto":
            return BookSanityDecision(
                apply=True,
                veto=False,
                gate="BookSanityGate",
                reason_code="BOOK_SANITY_FLAGS",
                flags=flags,
                notes=f"mode={mode}",
            )

        if do_veto:
            # Deterministic reason (first match)
            reason = "VETO_BOOK_SANITY"
            if "crossed_bbo" in flags:
                reason = "VETO_BOOK_CROSS"
            elif "nan_depth" in flags or "nan_px" in flags:
                reason = "VETO_BOOK_NAN"
            elif "neg_qty" in flags:
                reason = "VETO_BOOK_NEG_QTY"

            return BookSanityDecision(
                apply=True,
                veto=True,
                gate="BookSanityGate",
                reason_code=str(reason),
                flags=flags,
                notes=f"symbol={symbol}",
            )

        return BookSanityDecision(
            apply=True,
            veto=False,
            gate="BookSanityGate",
            reason_code="BOOK_SANITY_FLAGS",
            flags=flags,
            notes=f"mode={mode}",
        )
