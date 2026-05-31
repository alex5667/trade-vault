from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Execution Bootstrap Supervisor (P1.2.3).

Combines two dependency checks into a single readiness gate that the
executor MUST pass before becoming ready to process orders:

  1. Projection cluster — verifies ExecutionProjectionWorker health (lag,
     cursor age, leader lease) so derived state is always consistent.
  2. User-stream contour — verifies Binance listenKey lifecycle and WebSocket
     heartbeat freshness via `orders:user_stream:status`.

Both checks are required by default; each can be disabled independently via
feature flags for rollback:

  EXEC_BOOTSTRAP_REQUIRE_PROJECTION_READY=0   # disable projection gate
  EXEC_BOOTSTRAP_REQUIRE_USER_STREAM_READY=0  # disable user-stream gate
  EXEC_BOOTSTRAP_REQUIRE_READY=0              # bypass entire gate in main()

ENV knobs (all optional, sane defaults):

  USER_STREAM_STATUS_KEY               default: orders:user_stream:status
  USER_STREAM_MAX_STALE_MS             default: 45000  (45 s)
  EXEC_BOOTSTRAP_USER_STREAM_GRACE_MS  default: same as USER_STREAM_MAX_STALE_MS
  EXEC_BOOTSTRAP_TIMEOUT_MS            default: 0 (infinite wait)
  EXEC_BOOTSTRAP_POLL_MS               default: 500
  EXEC_BOOTSTRAP_HEALTH_PORT           default: 8787
"""

import argparse
import json
import os
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from typing import Any

try:  # pragma: no cover
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:  # pragma: no cover
    from prometheus_client import REGISTRY, Gauge
except Exception:  # pragma: no cover
    Gauge = None  # type: ignore
    REGISTRY = None  # type: ignore

try:  # pragma: no cover
    from services.execution_projection_worker import ExecutionProjectionWorker, _worker_from_env
except Exception:  # pragma: no cover
    from execution_projection_worker import ExecutionProjectionWorker, _worker_from_env  # type: ignore


# ---------------------------------------------------------------------------
# Prometheus gauge helpers — safe if prometheus_client is not installed
# ---------------------------------------------------------------------------

def _metric(factory, name: str, *args, **kwargs):
    """Register or return an existing Prometheus gauge (fail-silent)."""
    if factory is None:
        return None
    try:
        return factory(name, *args, **kwargs)
    except ValueError:
        # Already registered — return existing collector
        return getattr(REGISTRY, '_names_to_collectors', {}).get(name) if REGISTRY is not None else None


def _ms_now() -> int:
    return get_ny_time_millis()


def _s(v: Any) -> str:
    if v is None:
        return ''
    if isinstance(v, bytes):
        return v.decode('utf-8', 'replace')
    return str(v)


def _i(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _b(v: Any, default: bool = False) -> bool:
    """Parse a truthy/falsy value from various representations."""
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = _s(v).strip().lower()
    if s in {'1', 'true', 'yes', 'on', 'y'}:
        return True
    if s in {'0', 'false', 'no', 'off', 'n'}:
        return False
    return default


# ---------------------------------------------------------------------------
# Prometheus gauges
# ---------------------------------------------------------------------------

TRADE_EXECUTION_BOOTSTRAP_READY = _metric(
    Gauge,
    'trade_execution_bootstrap_ready',
    'Whether projection cluster and user-stream contour are healthy enough for executor startup (1 or 0).',
)
TRADE_EXECUTION_BOOTSTRAP_PROJECTION_READY = _metric(
    Gauge,
    'trade_execution_bootstrap_projection_ready',
    'Projection-cluster dependency readiness for executor bootstrap (1 or 0).',
)
TRADE_EXECUTION_BOOTSTRAP_USER_STREAM_READY = _metric(
    Gauge,
    'trade_execution_bootstrap_user_stream_ready',
    'User-stream contour readiness for executor bootstrap (1 or 0).',
)
TRADE_EXECUTION_BOOTSTRAP_BLOCKED = _metric(
    Gauge,
    'trade_execution_bootstrap_blocked',
    'Whether executor bootstrap is currently blocked by a dependency failure (1 or 0).',
)
TRADE_EXECUTION_BOOTSTRAP_LAST_BLOCK_TIMESTAMP_SECONDS = _metric(
    Gauge,
    'trade_execution_bootstrap_last_block_timestamp_seconds',
    'Unix timestamp of the latest persisted bootstrap block incident.',
)


# ---------------------------------------------------------------------------
# Data contracts
# ---------------------------------------------------------------------------

@dataclass
class BootstrapDependencyStatus:
    """Health snapshot for a single bootstrap dependency (projection or user-stream)."""
    ready: bool
    reason: str
    detail: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class BootstrapHealthSnapshot:
    """Combined readiness snapshot returned by the supervisor."""
    ok: bool
    ready: bool
    reason: str
    projection: dict[str, Any]
    user_stream: dict[str, Any]
    checked_at_ms: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class BootstrapBlockIncident:
    """Snapshot of the latest bootstrap block event persisted to Redis.

    Stored under EXEC_BOOTSTRAP_LAST_BLOCK_KEY so operators can inspect
    the last known block reason and its runbook actions via the API.
    """
    ready: bool
    reason: str
    checked_at_ms: int
    projection: dict[str, Any]
    user_stream: dict[str, Any]
    runbook_actions: list[str]
    status_key: str
    last_block_key: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Core supervisor
# ---------------------------------------------------------------------------

class ExecutionBootstrapSupervisor:
    """Checks both the projection cluster and the user-stream contour.

    The executor MUST NOT become ready until both dependencies report healthy.
    Each dependency gate can be individually bypassed via require_* flags so
    an operator can disable a gate without redeploying code.
    """

    def __init__(
        self,
        redis_client: Any,
        *,
        projection_worker: ExecutionProjectionWorker | None = None,
        user_stream_status_key: str = 'orders:user_stream:status',
        user_stream_max_stale_ms: int = 45000,
        user_stream_bootstrap_grace_ms: int = 45000,
        require_projection_ready: bool = True,
        require_user_stream_ready: bool = True,
        status_key: str = 'orders:execution:bootstrap:status',
        last_block_key: str = 'orders:execution:bootstrap:last_block',
        block_ttl_sec: int = 86400,
        projection_lag_readyz_max_ms: int = 30000,
    ) -> None:
        self.r = redis_client
        # Allow injection for tests; fall back to ENV-wired worker
        self.projection_worker = projection_worker or _worker_from_env(redis_client)
        self.user_stream_status_key = (user_stream_status_key or 'orders:user_stream:status')
        # Clamp to at least 1 second to avoid accidental zero
        self.user_stream_max_stale_ms = max(int(user_stream_max_stale_ms or 45000), 1000)
        self.user_stream_bootstrap_grace_ms = max(int(user_stream_bootstrap_grace_ms or 45000), 1000)
        self.require_projection_ready = bool(require_projection_ready)
        self.require_user_stream_ready = bool(require_user_stream_ready)
        # Lag threshold forwarded to health_snapshot() — wired from EXEC_PROJECTION_HEALTH_MAX_LAG_MS
        self.projection_lag_readyz_max_ms = max(int(projection_lag_readyz_max_ms or 30000), 1000)
        # P1.2.4: persisted bootstrap block state
        self.status_key = (status_key or 'orders:execution:bootstrap:status')
        self.last_block_key = (last_block_key or 'orders:execution:bootstrap:last_block')
        self.block_ttl_sec = max(int(block_ttl_sec or 86400), 60)

    # ------------------------------------------------------------------
    # Individual dependency checks
    # ------------------------------------------------------------------

    def projection_status(self) -> BootstrapDependencyStatus:
        """Ask the projection worker for its health snapshot.

        Handles both dict-returning (P1.2.1) and dataclass-returning
        implementations for forward/backward compatibility.
        """
        try:
            raw = self.projection_worker.health_snapshot(
                lag_readyz_max_ms=self.projection_lag_readyz_max_ms,
            )
            # health_snapshot() returns a plain dict in ExecutionProjectionWorker
            if isinstance(raw, dict):
                snap = raw
            else:
                # Future-proof: handle dataclass variants with to_dict()
                snap = raw.to_dict() if hasattr(raw, 'to_dict') else dict(raw)
            ready = bool(snap.get('ready'))
            reason = _s(snap.get('reason') or ('ok' if ready else 'projection_unready'))
            # Expose leader identity under a stable alias for downstream consumers
            if 'worker_id' in snap and 'leader_owner_id' not in snap:
                snap = dict(snap)
                snap['leader_owner_id'] = snap['worker_id']
        except Exception as exc:
            snap = {'error': str(exc)}
            ready = False
            reason = 'projection_exception'
        # If projection not required, treat as ready regardless
        return BootstrapDependencyStatus(
            ready=(ready or not self.require_projection_ready),
            reason=reason,
            detail=snap,
        )

    def user_stream_status(self) -> BootstrapDependencyStatus:
        """Evaluate freshness of the user-stream status key.

        Freshness anchors (highest non-zero wins):
          last_event_ms, last_ingest_ms, last_keepalive_ms,
          ws_connected_ms, listen_key_started_ms,

        Bootstrap grace allows a briefly-connected WS to be considered
        ready even before the first event arrives, provided:
          - ws_connected recently (within grace window)
          - listen_key is present
        """
        now_ms = _ms_now()
        try:
            raw = self.r.get(self.user_stream_status_key)
            doc = json.loads(raw) if raw else {}
            if not isinstance(doc, dict):
                doc = {}
        except Exception as exc:
            return BootstrapDependencyStatus(
                ready=False,
                reason='user_stream_status_error',
                detail={'error': str(exc)},
            )

        if not doc:
            return BootstrapDependencyStatus(
                ready=(not self.require_user_stream_ready),
                reason='user_stream_missing',
                detail={},
            )

        connected = _b(doc.get('connected'), False)
        last_event_ms = _i(doc.get('last_event_ms'))
        last_ingest_ms = _i(doc.get('last_ingest_ms'))
        last_keepalive_ms = _i(doc.get('last_keepalive_ms'))
        ws_connected_ms = _i(doc.get('ws_connected_ms'))
        listen_key_started_ms = _i(doc.get('listen_key_started_ms'))
        # Pick the most recent activity anchor across all heartbeat fields
        freshest_ms = max(
            last_event_ms, last_ingest_ms, last_keepalive_ms,
            ws_connected_ms, listen_key_started_ms,
        )
        age_ms = max(0, now_ms - freshest_ms) if freshest_ms else now_ms
        have_listen_key = bool(_s(doc.get('listen_key')))
        status = _s(doc.get('status'))

        ready = False
        reason = 'user_stream_missing'
        if not connected:
            reason = 'user_stream_disconnected'
        elif freshest_ms <= 0:
            reason = 'user_stream_no_freshness_anchor'
        elif age_ms <= self.user_stream_max_stale_ms:
            # Normal healthy path: recent activity within staleness window
            ready = True
            reason = 'ok'
        elif ws_connected_ms and (now_ms - ws_connected_ms) <= self.user_stream_bootstrap_grace_ms and have_listen_key:
            # Bootstrap grace: WS just connected, first event not yet delivered
            ready = True
            reason = 'bootstrap_grace'
        else:
            reason = 'user_stream_stale'

        detail = {
            'status_key': self.user_stream_status_key,
            'connected': connected,
            'status': status,
            'have_listen_key': have_listen_key,
            'freshest_ms': freshest_ms,
            'age_ms': age_ms,
            'last_event_ms': last_event_ms,
            'last_ingest_ms': last_ingest_ms,
            'last_keepalive_ms': last_keepalive_ms,
            'ws_connected_ms': ws_connected_ms,
            'listen_key_started_ms': listen_key_started_ms,
        }
        # Include raw doc fields for extra observability (status, listen_key, etc.)
        detail.update(doc)
        return BootstrapDependencyStatus(
            ready=(ready or not self.require_user_stream_ready),
            reason=reason,
            detail=detail,
        )

    # ------------------------------------------------------------------
    # P1.2.4: Runbook / incident persistence helpers
    # ------------------------------------------------------------------

    def _runbook_actions_for_reason(
        self, reason: str, projection: dict[str, Any], user_stream: dict[str, Any]
    ) -> list[str]:
        """Build human-readable runbook action list based on the block reason."""
        actions: list[str] = []
        if reason.startswith('projection:'):
            detail = projection.get('detail') or {}
            if 'no_leader' in reason or detail.get('reason') == 'no_leader':
                actions.append(
                    'Проверьте lease/fencing ключи projection worker и убедитесь, что есть ровно один активный лидер.'
                )
            if 'cursor' in reason or 'lag' in reason or 'stale' in reason:
                actions.append(
                    'Проверьте lag/cursor projection worker; при необходимости выполните --print-health и --rebuild-all.'
                )
            actions.append(
                'Проверьте service execution-state-projection-worker и endpoint execution-state-projection-health /readyz.'
            )
        if reason.startswith('user_stream:'):
            detail = user_stream.get('detail') or {}
            if detail.get('connected') is False:
                actions.append(
                    'Перезапустите Binance user-stream worker, проверьте listenKey lifecycle и websocket connectivity.'
                )
            if detail.get('have_listen_key') is False:
                actions.append(
                    'Проверьте создание listenKey и keepalive scheduler; без listenKey executor должен оставаться blocked.'
                )
            actions.append(
                'Проверьте freshness last_event_ms / last_ingest_ms и alert user-stream stale/disconnected.'
            )
        if not actions:
            actions.append(
                'Проверьте /api/execution-bootstrap/health и журналы projection/user-stream сервисов.'
            )
        return actions

    def _persist_json(self, key: str, payload: dict[str, Any], *, ttl_sec: int = 0) -> None:
        """Write JSON payload to Redis key, with optional TTL."""
        try:
            body = json.dumps(payload, ensure_ascii=False, sort_keys=True)
            if ttl_sec > 0:
                self.r.set(key, body, ex=int(ttl_sec))
            else:
                self.r.set(key, body)
        except Exception:
            pass

    def _load_json(self, key: str) -> dict[str, Any]:
        """Load JSON from Redis key; return empty dict on any error."""
        try:
            raw = self.r.get(key)
            doc = json.loads(raw) if raw else {}
            return doc if isinstance(doc, dict) else {}
        except Exception:
            return {}

    def _record_snapshot(self, snap: BootstrapHealthSnapshot) -> None:
        """Persist snapshot to Redis and update blocked/last-block gauges."""
        payload = snap.to_dict(),
        payload['status_key'] = self.status_key,
        payload['last_block_key'] = self.last_block_key,
        self._persist_json(self.status_key, payload, ttl_sec=self.block_ttl_sec),
        blocked = not bool(snap.ready),
        try:
            if TRADE_EXECUTION_BOOTSTRAP_BLOCKED:
                TRADE_EXECUTION_BOOTSTRAP_BLOCKED.set(1.0 if blocked else 0.0),
        except Exception:
            pass
        if blocked:
            # Persist the incident for operator inspection
            incident = BootstrapBlockIncident(
                ready=snap.ready,
                reason=snap.reason,
                checked_at_ms=snap.checked_at_ms,
                projection=snap.projection,
                user_stream=snap.user_stream,
                runbook_actions=self._runbook_actions_for_reason(
                    snap.reason, snap.projection, snap.user_stream
                ),
                status_key=self.status_key,
                last_block_key=self.last_block_key,
            )
            self._persist_json(self.last_block_key, incident.to_dict(), ttl_sec=self.block_ttl_sec)
            try:
                if TRADE_EXECUTION_BOOTSTRAP_LAST_BLOCK_TIMESTAMP_SECONDS:
                    TRADE_EXECUTION_BOOTSTRAP_LAST_BLOCK_TIMESTAMP_SECONDS.set(
                        float(snap.checked_at_ms) / 1000.0
                    )
            except Exception:
                pass

    def latest_block(self) -> dict[str, Any]:
        """Return latest persisted bootstrap block incident from Redis."""
        return self._load_json(self.last_block_key)

    def latest_status(self) -> dict[str, Any]:
        """Return latest persisted bootstrap status snapshot from Redis."""
        return self._load_json(self.status_key)

    def runbook_snapshot(self) -> dict[str, Any]:
        """Return combined runbook payload: current snapshot, latest block, and actions."""
        current = self.health_snapshot().to_dict()
        latest_block = self.latest_block()
        latest_reason = _s(latest_block.get('reason'))
        actions = latest_block.get('runbook_actions') if isinstance(
            latest_block.get('runbook_actions'), list
        ) else None
        if not actions:
            actions = self._runbook_actions_for_reason(
                latest_reason or _s(current.get('reason')),
                current.get('projection') or {},
                current.get('user_stream') or {},
            )
        return {
            'current': current,
            'latest_block': latest_block,
            'runbook_actions': actions,
            'status_key': self.status_key,
            'last_block_key': self.last_block_key,
        }

    # ------------------------------------------------------------------
    # Combined snapshot
    # ------------------------------------------------------------------

    def health_snapshot(self) -> BootstrapHealthSnapshot:
        """Return combined readiness; both dependencies must pass."""
        projection = self.projection_status()
        user_stream = self.user_stream_status()
        checked_at_ms = _ms_now()
        ready = bool(projection.ready and user_stream.ready)
        reason = 'ok'
        if not projection.ready:
            reason = f'projection:{projection.reason}'
        elif not user_stream.ready:
            reason = f'user_stream:{user_stream.reason}'

        # Update Prometheus gauges (fail-silent if not installed)
        try:
            if TRADE_EXECUTION_BOOTSTRAP_PROJECTION_READY:
                TRADE_EXECUTION_BOOTSTRAP_PROJECTION_READY.set(1.0 if projection.ready else 0.0)
            if TRADE_EXECUTION_BOOTSTRAP_USER_STREAM_READY:
                TRADE_EXECUTION_BOOTSTRAP_USER_STREAM_READY.set(1.0 if user_stream.ready else 0.0)
            if TRADE_EXECUTION_BOOTSTRAP_READY:
                TRADE_EXECUTION_BOOTSTRAP_READY.set(1.0 if ready else 0.0)
        except Exception:
            pass

        snap = BootstrapHealthSnapshot(
            ok=ready,
            ready=ready,
            reason=reason,
            projection=projection.to_dict(),
            user_stream=user_stream.to_dict(),
            checked_at_ms=checked_at_ms,
        )
        # P1.2.4: persist snapshot and update blocked gauges on every check
        self._record_snapshot(snap)
        return snap

    def wait_until_ready(self, *, timeout_ms: int = 0, poll_ms: int = 500) -> BootstrapHealthSnapshot:
        """Block until both dependencies are ready or timeout expires.

        Args:
            timeout_ms: Maximum wait time in ms; 0 means infinite.
            poll_ms:    Poll interval in ms (minimum 50 ms enforced).

        Returns:
            Last BootstrapHealthSnapshot (ready may be False on timeout).
        """
        deadline_ms = (_ms_now() + max(int(timeout_ms or 0), 0)) if int(timeout_ms or 0) > 0 else 0
        last = self.health_snapshot()
        while not last.ready:
            if deadline_ms and _ms_now() >= deadline_ms:
                return last  # timed out — caller decides how to handle
            time.sleep(max(0.05, int(poll_ms or 500) / 1000.0))
            last = self.health_snapshot()
        return last


# ---------------------------------------------------------------------------
# ENV wiring helpers (used by health server and executor gate)
# ---------------------------------------------------------------------------

def _redis_from_env() -> Any:  # pragma: no cover
    if redis is None:
        raise RuntimeError('redis package is required for execution_bootstrap_supervisor')
    return redis.from_url(os.getenv('REDIS_URL', 'redis://redis-worker-1:6379/0'), decode_responses=True)


def _supervisor_from_env(redis_client: Any) -> ExecutionBootstrapSupervisor:
    """Build ExecutionBootstrapSupervisor from ENV variables."""
    return ExecutionBootstrapSupervisor(
        redis_client,
        projection_worker=_worker_from_env(redis_client),
        user_stream_status_key=os.getenv('USER_STREAM_STATUS_KEY', 'orders:user_stream:status'),
        user_stream_max_stale_ms=int(os.getenv('USER_STREAM_MAX_STALE_MS', '45000')),
        user_stream_bootstrap_grace_ms=int(
            os.getenv('EXEC_BOOTSTRAP_USER_STREAM_GRACE_MS', os.getenv('USER_STREAM_MAX_STALE_MS', '45000'))
        ),
        require_projection_ready=_b(os.getenv('EXEC_BOOTSTRAP_REQUIRE_PROJECTION_READY', '1'), True),
        require_user_stream_ready=_b(os.getenv('EXEC_BOOTSTRAP_REQUIRE_USER_STREAM_READY', '1'), True),
        # Projection lag threshold — forwarded to health_snapshot(lag_readyz_max_ms=…)
        projection_lag_readyz_max_ms=int(
            os.getenv('EXEC_PROJECTION_HEALTH_MAX_LAG_MS', '30000')
        ),
        # P1.2.4: persisted bootstrap block state keys
        status_key=os.getenv('EXEC_BOOTSTRAP_STATUS_KEY', 'orders:execution:bootstrap:status'),
        last_block_key=os.getenv('EXEC_BOOTSTRAP_LAST_BLOCK_KEY', 'orders:execution:bootstrap:last_block'),
        block_ttl_sec=int(os.getenv('EXEC_BOOTSTRAP_BLOCK_TTL_SEC', '86400')),
    )


def wait_until_env_ready(*, timeout_ms: int = 0, poll_ms: int = 500) -> BootstrapHealthSnapshot:
    """Convenience shim: wire from ENV and block until ready.

    Called by binance_executor.main() when EXEC_BOOTSTRAP_REQUIRE_READY=1.
    """
    r = _redis_from_env()
    sup = _supervisor_from_env(r)
    return sup.wait_until_ready(timeout_ms=timeout_ms, poll_ms=poll_ms)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:  # pragma: no cover
    p = argparse.ArgumentParser(description='Execution bootstrap supervisor')
    p.add_argument('--print-health', action='store_true',
                   help='print one combined bootstrap health snapshot and exit')
    # P1.2.4: new CLI ops tools for incident triage
    p.add_argument('--print-last-block', action='store_true',
                   help='print latest persisted bootstrap block incident and exit')
    p.add_argument('--print-runbook', action='store_true',
                   help='print runbook payload with latest block reason and actions and exit')
    p.add_argument('--wait-until-ready', action='store_true',
                   help='block until projection cluster and user-stream contour are healthy')
    p.add_argument('--timeout-ms', type=int,
                   default=int(os.getenv('EXEC_BOOTSTRAP_TIMEOUT_MS', '0')))
    p.add_argument('--poll-ms', type=int,
                   default=int(os.getenv('EXEC_BOOTSTRAP_POLL_MS', '500')))
    p.add_argument('--run-executor', action='store_true',
                   help='wait until ready and then start BinanceExecutor in-process')
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:  # pragma: no cover
    args = _parse_args(argv)
    r = _redis_from_env()
    sup = _supervisor_from_env(r)
    # P1.2.4: incident / runbook ops commands (read-only, no polling)
    if args.print_last_block:
        print(json.dumps(sup.latest_block(), ensure_ascii=False, indent=2))
        return 0
    if args.print_runbook:
        print(json.dumps(sup.runbook_snapshot(), ensure_ascii=False, indent=2))
        return 0
    if args.print_health:
        print(json.dumps(sup.health_snapshot().to_dict(), ensure_ascii=False, indent=2))
        return 0
    if args.wait_until_ready or args.run_executor:
        snap = sup.wait_until_ready(
            timeout_ms=int(args.timeout_ms or 0),
            poll_ms=int(args.poll_ms or 500),
        )
        print(json.dumps(snap.to_dict(), ensure_ascii=False, indent=2))
        if not snap.ready:
            return 2
        if args.run_executor:
            try:
                from services.binance_executor import BinanceExecutor
            except Exception:
                from binance_executor import BinanceExecutor  # type: ignore
            BinanceExecutor().run_forever()
        return 0
    # Default: print current snapshot and exit 0
    print(json.dumps(sup.health_snapshot().to_dict(), ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
