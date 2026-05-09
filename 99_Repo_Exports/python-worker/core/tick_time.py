"""Re-export from the canonical common.tick_time module.

common.tick_time is the single source of truth for tick time policy.
This shim keeps backward compatibility for any importer that uses core.tick_time.
"""

from common.tick_time import (  # noqa: F401
    SanitizeResult,
    TickTimeGuard,
    TickTimePolicy,
    TsVerifyResult,
    apply_tick_time_policy,
    verify_bucketed_ts,
)

__all__ = [
    "TickTimePolicy",
    "TickTimeGuard",
    "SanitizeResult",
    "TsVerifyResult",
    "apply_tick_time_policy",
    "verify_bucketed_ts",
]
