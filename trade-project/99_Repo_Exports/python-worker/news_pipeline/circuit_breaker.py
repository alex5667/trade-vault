"""news_pipeline.circuit_breaker

Per-provider circuit breaker backed by Redis.
Tracks timeout_rate, invalid_json_rate, login_errors, median_latency.
Opens the circuit for cooldown_sec when thresholds are exceeded.

Redis keys:
  news:llm:cb:{provider}:state   → "open" | "closed"  (TTL = cooldown_sec)
  news:llm:cb:{provider}:stats   → HASH {requests, timeouts, invalid_json, login_errors, latency_sum}
  (stats expire after window_sec)
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

log = logging.getLogger(__name__)

_CB_ENABLE       = os.getenv("NEWS_LLM_PROVIDER_CB_ENABLE", "1") == "1"
_WINDOW_SEC      = int(os.getenv("NEWS_LLM_PROVIDER_CB_WINDOW_SEC", "600"))
_COOLDOWN_SEC    = int(os.getenv("NEWS_LLM_PROVIDER_CB_COOLDOWN_SEC", "1800"))
_MAX_TIMEOUT_RATE    = float(os.getenv("NEWS_LLM_PROVIDER_CB_TIMEOUT_RATE", "0.50"))
_MAX_INVALID_RATE    = float(os.getenv("NEWS_LLM_PROVIDER_CB_INVALID_JSON_RATE", "0.30"))
_MAX_LOGIN_ERRORS    = int(os.getenv("NEWS_LLM_PROVIDER_CB_LOGIN_ERRORS", "3"))


def _state_key(provider: str) -> str:
    return f"news:llm:cb:{provider}:state"


def _stats_key(provider: str) -> str:
    return f"news:llm:cb:{provider}:stats"


def is_open(provider: str, redis: Any) -> bool:
    """True → circuit is OPEN (provider should be skipped)."""
    if not _CB_ENABLE:
        return False
    try:
        return redis.get(_state_key(provider)) == "open"
    except Exception:
        return False  # fail-open: if Redis unreachable, allow the call


def record_outcome(
    provider: str,
    redis: Any,
    *,
    status: str,       # "ok" | "timeout" | "invalid_json" | "login_error" | other
    latency_ms: int,
) -> None:
    """Record one outcome and re-evaluate circuit state."""
    if not _CB_ENABLE:
        return
    try:
        sk = _stats_key(provider)
        pipe = redis.pipeline()
        pipe.hincrby(sk, "requests", 1)
        pipe.hincrbyfloat(sk, "latency_sum", latency_ms)
        if status == "timeout":
            pipe.hincrby(sk, "timeouts", 1)
        elif status in ("invalid_json", "schema_error"):
            pipe.hincrby(sk, "invalid_json", 1)
        elif status == "login_error":
            pipe.hincrby(sk, "login_errors", 1)
        pipe.expire(sk, _WINDOW_SEC)
        pipe.execute()

        _maybe_open(provider, redis)
    except Exception as exc:
        log.debug("circuit_breaker.record_outcome failed: %r", exc)


def _maybe_open(provider: str, redis: Any) -> None:
    try:
        stats = redis.hgetall(_stats_key(provider))
        requests = int(stats.get("requests", 0))
        if requests < 5:
            return  # too few samples

        timeouts     = int(stats.get("timeouts", 0))
        invalid_json = int(stats.get("invalid_json", 0))
        login_errors = int(stats.get("login_errors", 0))

        timeout_rate = timeouts / requests
        invalid_rate = invalid_json / requests

        reasons = []
        if timeout_rate > _MAX_TIMEOUT_RATE:
            reasons.append(f"timeout_rate={timeout_rate:.0%}")
        if invalid_rate > _MAX_INVALID_RATE:
            reasons.append(f"invalid_json_rate={invalid_rate:.0%}")
        if login_errors >= _MAX_LOGIN_ERRORS:
            reasons.append(f"login_errors={login_errors}")

        if reasons:
            redis.set(_state_key(provider), "open", ex=_COOLDOWN_SEC)
            log.warning("circuit_breaker OPEN provider=%s reasons=%s cooldown=%ds",
                        provider, reasons, _COOLDOWN_SEC)
    except Exception as exc:
        log.debug("circuit_breaker._maybe_open failed: %r", exc)


def reset(provider: str, redis: Any) -> None:
    """Manually close circuit (for testing / admin)."""
    try:
        redis.delete(_state_key(provider), _stats_key(provider))
    except Exception:
        pass
