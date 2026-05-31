from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Tuple


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x)
    except Exception:
        return d


RETRYABLE_REASONS = {
    "ROLLBACK_EXECUTOR_TIMEOUT",
    "ROLLBACK_EXECUTOR_UNAVAILABLE",
    "ROLLBACK_VERIFY_INCONCLUSIVE",
    "ROLLBACK_STATE_RACE",
}

HARD_STOP_REASONS = {
    "ROLLBACK_TARGET_MISSING",
    "ROLLBACK_BASELINE_MISSING",
    "ROLLBACK_POLICY_DENIED",
}


@dataclass
class RetryDecision:
    should_retry: bool
    next_attempt: int
    backoff_sec: int
    reason_code: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "should_retry": int(self.should_retry),
            "next_attempt": self.next_attempt,
            "backoff_sec": self.backoff_sec,
            "reason_code": self.reason_code,
        }


def compute_retry_decision(
    event: Dict[str, Any],
    *,
    max_attempts: int = 2,
    base_backoff_sec: int = 300,
    max_backoff_sec: int = 3600,
) -> RetryDecision:
    attempt = _i(event.get("attempt"), 0)
    reason = _s(event.get("failure_reason"))
    if reason in HARD_STOP_REASONS:
        return RetryDecision(False, attempt, 0, "ROLLBACK_RETRY_HARD_STOP")
    if reason not in RETRYABLE_REASONS:
        return RetryDecision(False, attempt, 0, "ROLLBACK_RETRY_NOT_ELIGIBLE")
    if attempt >= int(max_attempts):
        return RetryDecision(False, attempt, 0, "ROLLBACK_RETRY_LIMIT_REACHED")
    next_attempt = attempt + 1
    backoff = min(int(max_backoff_sec), int(base_backoff_sec) * (2 ** max(0, attempt)))
    return RetryDecision(True, next_attempt, backoff, "ROLLBACK_RETRY_SCHEDULED")
