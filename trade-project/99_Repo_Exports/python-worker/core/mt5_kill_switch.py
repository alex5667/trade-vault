"""MT5 execution path kill switch.

MT5 is intentionally disabled in production (2026-05-19) — no MT5 EA is
connected and no consumer reads `orders:queue:mt5`. Without this guard,
``OrderPayloadBuilder`` defaults `venue="mt5"` and silently piles unread
orders into the Redis stream (PEL/maxlen growth, debugging confusion).

To re-enable MT5:
    export MT5_ENABLED=1

…and deploy the MT5 bridge (mt5_bridge/, orders_http_bridge.py, or
orders_router.py) plus a consumer for `orders:queue:mt5`.

The switch is a pure read of the env var so tests can patch it via
monkeypatch.setenv without module reload.
"""
from __future__ import annotations

import os


def mt5_enabled() -> bool:
    """True iff MT5_ENABLED env is a truthy value.

    Default: False. Any non-true value (including empty/unset) disables
    the entire MT5 publish/poll path.
    """
    return (os.getenv("MT5_ENABLED", "0") or "").strip().lower() in (
        "1", "true", "yes", "on",
    )
