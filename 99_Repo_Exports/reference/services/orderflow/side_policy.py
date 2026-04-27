"""Side (aggressor) policy helpers.

Unknown-side policy (env: CRYPTO_OF_UNKNOWN_SIDE_POLICY):
- ignore_delta (default): keep tick; signed qty downstream should become 0.0 (fail-open)
- drop: ACK and skip tick processing
- quarantine: publish sampled payload to Redis stream and skip processing

Sampling is deterministic by event-time ms, so retries/replay won't change the sampled set.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


VALID_UNKNOWN_SIDE_POLICIES = ("ignore_delta", "drop", "quarantine")


def normalize_unknown_side_policy(raw: Optional[str]) -> str:
    v = (raw or "ignore_delta").strip().lower()
    aliases = {
        "ignore": "ignore_delta",
        "keep": "ignore_delta",
        "pass": "ignore_delta",
        "none": "ignore_delta",
        "0": "ignore_delta",
        "false": "ignore_delta",
    }
    v = aliases.get(v, v)
    if v not in VALID_UNKNOWN_SIDE_POLICIES:
        return "ignore_delta"
    return v


def is_unknown_side_tick(tick: Dict[str, Any]) -> bool:
    """True when tick has no reliable aggressor side."""
    try:
        side = str(tick.get("side") or "").strip().upper()
        if side in ("BUY", "SELL"):
            return False
        # If maker flag is present, side can be inferred (Binance semantics).
        if tick.get("is_buyer_maker", None) is not None:
            return False
        return True
    except Exception:
        return False


def deterministic_sample(key_ms: int, rate: float) -> bool:
    """Deterministic sampling by ms-key (stable across retries/replays)."""
    try:
        r = float(rate)
        if r <= 0.0:
            return False
        if r >= 1.0:
            return True
        k = abs(int(key_ms)) % 10000
        return k < int(r * 10000.0)
    except Exception:
        return False

