from __future__ import annotations
"""tick_flow_full.core.dq_observe_only

Observe-only rollout guard for DQ hard-veto on BOOK sequence degradation.

Spec (from the implementation plan)
----------------------------------
* dq_level is always computed (0/1/2)
* dq_veto may be suppressed for the first 24–48h after process start to avoid
  the system "dying" on book-gap during warmup / initial deploy.

Suppression rule:
    if dq_level == 2 and the reason is book_seq (or includes book_hard)
    and dq_book_veto_enabled is True
    and uptime_sec >= dq_observe_only_sec
    -> dq_veto = 1
    else -> dq_veto = 0 (dq_level stays 2)

This module is intentionally self-contained and only depends on a monotonic
uptime value supplied by the caller.

Integration point (expected)
----------------------------
Call apply_observe_only_book_veto() inside tick_flow_full/core/dq_gate_v1.py
*after* policy v2 has decided dq_level/reasons and computed dq_veto.

Because the full code for dq_gate_v1.py was not present in the uploaded archive,
we keep integration as a small, well-documented insertion.
"""


import os
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional, Sequence, Tuple


def _cfg_get(cfg: Any, key: str, default: Any) -> Any:
    """Get cfg value from dict-like or object-like configs.

    Order:
      1) cfg[key] if cfg is Mapping
      2) getattr(cfg, key)
      3) environment fallbacks (for backward compatibility)
      4) default
    """
    if cfg is not None:
        if isinstance(cfg, Mapping):
            if key in cfg:
                return cfg[key]
        else:
            v = getattr(cfg, key, None)
            if v is not None:
                return v

    # Backward-compatible env fallbacks.
    # New names:
    if key == "dq_book_veto_enabled":
        env = os.getenv("DQ_BOOK_VETO_ENABLED")
        if env is not None:
            return env.strip().lower() in ("1", "true", "yes", "y", "on")
    if key == "dq_observe_only_sec":
        # Prefer explicit observe-only env; fall back to older warmup var if used.
        for env_key in ("DQ_OBSERVE_ONLY_SEC", "DQ_BOOK_VETO_WARMUP_S"):
            env = os.getenv(env_key)
            if env is not None:
                try:
                    return int(env)
                except ValueError:
                    # Ignore malformed env and fall through.
                    break

    return default


def _reason_is_book_seq(reason: str) -> bool:
    r = (reason or "").lower()
    # Support multiple naming conventions.
    return (
        "book_seq" in r
        or "book_missing_seq" in r
        or "book_hard" in r
        or "book-gap" in r
        or "book_gap" in r
    )


def is_book_veto_case(
    dq_reason_bucket: Optional[str],
    dq_reasons: Optional[Sequence[str]],
) -> bool:
    """Return True if the current hard-veto came from book-seq degradation."""
    if (dq_reason_bucket or "").lower() in ("book_seq", "book", "bookseq"):
        return True
    if not dq_reasons:
        return False
    return any(_reason_is_book_seq(r) for r in dq_reasons)


@dataclass(frozen=True)
class ObserveOnlyDecision:
    dq_veto: int
    suppressed: bool
    suppress_reason: Optional[str]


def apply_observe_only_book_veto(
    *,
    dq_level: int,
    dq_veto: int,
    dq_reason_bucket: Optional[str],
    dq_reasons: Optional[Sequence[str]],
    uptime_sec: float,
    cfg: Any = None,
) -> ObserveOnlyDecision:
    """Apply observe-only gating to book-seq hard-veto.

    This function never changes dq_level or dq_reasons. It only decides whether
    dq_veto should remain 1 or be suppressed to 0.
    """

    # Only meaningful when policy says this is a HARD event AND wants to veto.
    if int(dq_level) != 2 or int(dq_veto) != 1:
        return ObserveOnlyDecision(dq_veto=int(dq_veto), suppressed=False, suppress_reason=None)

    if not is_book_veto_case(dq_reason_bucket, dq_reasons):
        # Never suppress non-book vetoes (tick_seq/gap_p95/data_health).
        return ObserveOnlyDecision(dq_veto=1, suppressed=False, suppress_reason=None)

    enabled = bool(_cfg_get(cfg, "dq_book_veto_enabled", False))
    observe_only_sec = int(_cfg_get(cfg, "dq_observe_only_sec", 86400))
    if observe_only_sec < 0:
        observe_only_sec = 0

    if not enabled:
        return ObserveOnlyDecision(dq_veto=0, suppressed=True, suppress_reason="book_veto_disabled")

    # uptime_sec must be monotonic-proc-time (caller responsibility).
    if float(uptime_sec) < float(observe_only_sec):
        return ObserveOnlyDecision(dq_veto=0, suppressed=True, suppress_reason="observe_only")

    return ObserveOnlyDecision(dq_veto=1, suppressed=False, suppress_reason=None)
