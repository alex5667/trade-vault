"""Pure predicates for notification routing decisions.

Extracted from signal_pipeline.py so the gate logic is testable in isolation
without spinning up SignalPipeline + Redis + orchestrator state.
"""
from __future__ import annotations

import os
from typing import Any, Mapping


def _env_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in ("1", "true", "yes", "on")


def should_skip_telegram_virtual(
    enriched_signal: Mapping[str, Any],
    *,
    env_value: str | None = None,
) -> bool:
    """Return True iff the signal must be blocked from Telegram because it's
    virtual/shadow AND the operator opt-out is enabled.

    Reads BOTH ``virtual`` (bool) and ``is_virtual`` (int) so external
    producers that inject only the wire-level int flag cannot bypass the
    gate. Default operator policy is to skip — pass ``CRYPTO_NOTIFY_SKIP_VIRTUAL=0``
    to restore legacy shadow-to-Telegram behaviour.

    Args:
        enriched_signal: the dict passed downstream to the outbox.
        env_value: override ``CRYPTO_NOTIFY_SKIP_VIRTUAL`` (useful for tests).
                   When None, reads the env var.

    Returns:
        True if the signal should be blocked from Telegram.
    """
    raw = env_value if env_value is not None else os.getenv("CRYPTO_NOTIFY_SKIP_VIRTUAL", "1")
    skip_enabled = _env_truthy(raw)
    if not skip_enabled:
        return False
    if bool(enriched_signal.get("virtual")):
        return True
    try:
        return bool(int(enriched_signal.get("is_virtual", 0) or 0))
    except (TypeError, ValueError):
        return False
