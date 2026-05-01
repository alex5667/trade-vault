from __future__ import annotations
"""
XADD OOM / circuit-breaker scaffolding (shadow-mode skeleton).

Purpose
-------
Give us early warning when Redis starts rejecting XADDs due to
`OOM command not allowed when used memory > 'maxmemory'`, and expose the hook
for a future circuit breaker without changing production behaviour today.

Design
------
Two Prometheus metrics:
  * redis_xadd_oom_total{shard, stream}        — every OOM response observed
  * redis_xadd_breaker_state{scope}             — 0 closed / 1 open / 2 half-open

One enum + one class:
  * BreakerState (Enum)
  * XaddOomBreaker — observe(exc), should_allow(), on_success()

Modes
-----
XADD_BREAKER_MODE env var:
  * shadow (default)  — count OOMs, log at WARN, NEVER block xadds
  * enforce           — open breaker after N OOMs within window, deny xadds
                        while open, half-open after cooldown

Invariants
----------
* Closed by default: no state file means breaker is closed.
* No external dependencies beyond prometheus_client (optional) and stdlib.
* Non-fatal: every metric op is wrapped, so a metrics outage never affects
  the xadd hot path.
* Import is cheap; state is process-local.
* NEVER enforces automatically — only when env XADD_BREAKER_MODE=enforce is
  explicitly set. This keeps Phase 2 shadow-safe per CLAUDE.md rollout policy.

Integration
-----------
Call sites wrap an xadd in the pattern::

    breaker = get_xadd_oom_breaker()
    if not breaker.should_allow(shard="worker-1", stream=stream):
        # in enforce mode, breaker is open → caller falls back (DLQ or skip)
        ...
    else:
        try:
            redis.xadd(stream, fields, maxlen=N, approximate=True)
            breaker.on_success(shard="worker-1", stream=stream)
        except ResponseError as exc:
            breaker.observe(exc, shard="worker-1", stream=stream)
            raise
"""


import logging
import os
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Tuple

try:
    from prometheus_client import Counter, Gauge
except Exception:  # pragma: no cover
    Counter = Gauge = None  # type: ignore


log = logging.getLogger("xadd_oom_breaker")


def _metric(factory, name: str, *args, **kwargs):
    if factory is None:
        return None
    try:
        return factory(name, *args, **kwargs)
    except ValueError:
        # already registered (process re-import)
        return None


REDIS_XADD_OOM_TOTAL = _metric(
    Counter,
    "redis_xadd_oom_total",
    "XADD responses where Redis rejected the write due to OOM",
    ["shard", "stream"],
)

REDIS_XADD_BREAKER_STATE = _metric(
    Gauge,
    "redis_xadd_breaker_state",
    "XADD circuit breaker state: 0=closed, 1=open, 2=half_open",
    ["scope"],
)


class BreakerState(Enum):
    CLOSED = 0
    OPEN = 1
    HALF_OPEN = 2


@dataclass
class _ScopeState:
    state: BreakerState = BreakerState.CLOSED
    failures_in_window: int = 0
    window_start_ms: int = 0
    opened_at_ms: int = 0


@dataclass
class XaddOomBreakerConfig:
    mode: str = field(default_factory=lambda: os.getenv("XADD_BREAKER_MODE", "shadow"))
    # Threshold within window that trips the breaker in enforce mode
    failure_threshold: int = field(default_factory=lambda: int(os.getenv("XADD_BREAKER_FAILURES", "10")))
    window_ms: int = field(default_factory=lambda: int(os.getenv("XADD_BREAKER_WINDOW_MS", "60000")))
    # Cooldown before moving OPEN → HALF_OPEN (ms)
    cooldown_ms: int = field(default_factory=lambda: int(os.getenv("XADD_BREAKER_COOLDOWN_MS", "30000")))


def _is_oom(exc: BaseException) -> bool:
    """Best-effort detection of Redis OOM rejection responses.

    redis-py raises redis.exceptions.ResponseError with a message starting
    with "OOM command not allowed when used memory > 'maxmemory'.".
    We match on the OOM substring to stay resilient to wording changes.
    """
    return "OOM" in (str(exc) or "")


class XaddOomBreaker:
    """Process-local circuit breaker with per-scope state.

    Scope is any string — typical choices are shard name ("worker-1") or
    stream name. Counters and state gauges live under Prometheus labels.
    Keeps the hot path lock-light: a single mutex per breaker instance.
    """

    def __init__(self, cfg: Optional[XaddOomBreakerConfig] = None) -> None:
        self.cfg = cfg or XaddOomBreakerConfig()
        self._lock = threading.Lock()
        self._by_scope: Dict[str, _ScopeState] = {}
        if self.cfg.mode not in ("shadow", "enforce"):
            log.warning("Unknown XADD_BREAKER_MODE=%r, falling back to shadow", self.cfg.mode)
            self.cfg.mode = "shadow"

    # ── Observation path ──────────────────────────────────────────────────

    def observe(self, exc: BaseException, *, shard: str = "unknown", stream: str = "unknown") -> bool:
        """Record an xadd failure. Returns True when the response was OOM."""
        is_oom = _is_oom(exc)
        if not is_oom:
            return False

        try:
            if REDIS_XADD_OOM_TOTAL is not None:
                REDIS_XADD_OOM_TOTAL.labels(shard=shard, stream=stream).inc()
        except Exception:
            pass

        scope = shard
        now_ms = int(time.time() * 1000)
        with self._lock:
            st = self._by_scope.setdefault(scope, _ScopeState())
            # Reset window if it elapsed since the first recorded failure.
            if now_ms - st.window_start_ms > self.cfg.window_ms:
                st.failures_in_window = 0
                st.window_start_ms = now_ms
            st.failures_in_window += 1

            if self.cfg.mode == "enforce" and st.state == BreakerState.CLOSED:
                if st.failures_in_window >= self.cfg.failure_threshold:
                    st.state = BreakerState.OPEN
                    st.opened_at_ms = now_ms
                    self._emit_state(scope, st.state)
                    log.error(
                        "xadd_oom_breaker[%s] OPEN: %d OOMs in %dms (threshold=%d)",
                        scope, st.failures_in_window, self.cfg.window_ms, self.cfg.failure_threshold,
                    )
            elif self.cfg.mode == "shadow":
                if st.failures_in_window == 1:
                    log.warning(
                        "xadd_oom_breaker[%s] shadow: first OOM in window (mode=shadow, no-op)",
                        scope,
                    )
                elif st.failures_in_window >= self.cfg.failure_threshold:
                    # Would have opened in enforce mode — log loudly, still no-op.
                    log.warning(
                        "xadd_oom_breaker[%s] shadow: WOULD_OPEN (%d OOMs in %dms)",
                        scope, st.failures_in_window, self.cfg.window_ms,
                    )
        return True

    def should_allow(self, *, shard: str = "unknown", stream: str = "unknown") -> bool:
        """Gate check before xadd. In shadow mode always returns True."""
        if self.cfg.mode == "shadow":
            return True

        now_ms = int(time.time() * 1000)
        with self._lock:
            st = self._by_scope.get(shard)
            if st is None or st.state == BreakerState.CLOSED:
                return True

            if st.state == BreakerState.OPEN:
                if now_ms - st.opened_at_ms >= self.cfg.cooldown_ms:
                    st.state = BreakerState.HALF_OPEN
                    self._emit_state(shard, st.state)
                    log.info("xadd_oom_breaker[%s] HALF_OPEN (cooldown elapsed)", shard)
                    return True  # let one probe through
                return False

            # HALF_OPEN: allow probes (caller reports back via on_success/observe)
            return True

    def on_success(self, *, shard: str = "unknown", stream: str = "unknown") -> None:
        """Report a successful xadd. Used to close a half-open breaker."""
        with self._lock:
            st = self._by_scope.get(shard)
            if st is None:
                return
            if st.state == BreakerState.HALF_OPEN:
                st.state = BreakerState.CLOSED
                st.failures_in_window = 0
                self._emit_state(shard, st.state)
                log.info("xadd_oom_breaker[%s] CLOSED (recovered)", shard)

    # ── Helpers ───────────────────────────────────────────────────────────

    def _emit_state(self, scope: str, state: BreakerState) -> None:
        try:
            if REDIS_XADD_BREAKER_STATE is not None:
                REDIS_XADD_BREAKER_STATE.labels(scope=scope).set(state.value)
        except Exception:
            pass

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {
                scope: {
                    "state": st.state.name,
                    "failures_in_window": st.failures_in_window,
                    "window_start_ms": st.window_start_ms,
                    "opened_at_ms": st.opened_at_ms,
                }
                for scope, st in self._by_scope.items()
            }


_singleton: Optional[XaddOomBreaker] = None
_singleton_lock = threading.Lock()


def get_xadd_oom_breaker() -> XaddOomBreaker:
    """Return the process-wide breaker instance (lazy singleton)."""
    global _singleton
    if _singleton is None:
        with _singleton_lock:
            if _singleton is None:
                _singleton = XaddOomBreaker()
    return _singleton
