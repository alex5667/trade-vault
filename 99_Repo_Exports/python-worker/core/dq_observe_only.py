from __future__ import annotations
"""Observe-only warmup logic for DQ veto.

B1 requirement: system should not "die" from book-gap during the first 24–48h
(or other configured observe-only window), but dq_level/reasons and metrics must
be visible.

This module only suppresses hard veto for the `book_seq` bucket.
"""


from dataclasses import dataclass
import os
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class ObserveOnlyResult:
    dq_veto: int
    suppressed: bool
    suppress_reason: Optional[str] = None


def _bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return bool(v)
    try:
        s = str(v).strip().lower()
    except Exception:
        return default
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def _int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


def _cfg_get(cfg: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in cfg:
            return cfg.get(k)
    return None


def apply_observe_only_book_veto(
    *,
    dq_level: int,
    dq_veto: int,
    dq_reason_bucket: str,
    dq_reasons: List[str],
    uptime_sec: int,
    cfg: Dict[str, Any],
) -> ObserveOnlyResult:
    """Suppress hard veto for book_seq until enabled + warmup passed."""

    # Only book_seq bucket is guarded (per spec).
    if int(dq_level) != 2 or str(dq_reason_bucket) != "book_seq":
        return ObserveOnlyResult(dq_veto=int(dq_veto), suppressed=False, suppress_reason=None)

    # Enabled flag: cfg wins, env is fallback.
    enabled = _cfg_get(cfg, "dq_book_veto_enabled", "DQ_BOOK_VETO_ENABLED")
    if enabled is None:
        enabled = os.getenv("DQ_BOOK_VETO_ENABLED")
    enabled_b = _bool(enabled, default=False)

    # Observe-only window seconds: cfg wins, env is fallback.
    observe_only = _cfg_get(cfg, "dq_observe_only_sec", "DQ_OBSERVE_ONLY_SEC", "dq_book_veto_warmup_s", "DQ_BOOK_VETO_WARMUP_S")
    if observe_only is None:
        observe_only = os.getenv("DQ_OBSERVE_ONLY_SEC") or os.getenv("DQ_BOOK_VETO_WARMUP_S")
    observe_only_s = _int(observe_only, default=86400)
    if observe_only_s < 0:
        observe_only_s = 0

    # Desired veto for book_seq per B1:
    # veto becomes active only after enable + warmup.
    if not enabled_b:
        return ObserveOnlyResult(dq_veto=0, suppressed=True, suppress_reason="disabled")

    if int(uptime_sec) < int(observe_only_s):
        return ObserveOnlyResult(dq_veto=0, suppressed=True, suppress_reason="warmup")

    return ObserveOnlyResult(dq_veto=1, suppressed=False, suppress_reason=None)
