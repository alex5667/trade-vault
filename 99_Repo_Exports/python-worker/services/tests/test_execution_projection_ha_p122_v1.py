from __future__ import annotations

"""Tests: P1.2.2 HA-safe execution projection worker (LeaderLease, fencing, health).

Suite covers:
1.  LeaderLease.acquire() sets Redis key with NX
2.  Non-leader node skips batch (skipped_not_leader=True)
3.  Fencing token increments on each acquisition by a new worker
4.  run_once() returns idle=True when not leader
5.  Stale-writer detection aborts batch
6.  health_snapshot() returns correct leader / lag_ms / ready fields
7.  health_snapshot() with lease disabled: always leader, ready
8.  set_cursor_to_tip() moves cursor to latest stream ID
9.  rebuild_sid() rebuilds state for a single SID
10. rebuild_all() rebuilds state for multiple SIDs
"""

# ---------------------------------------------------------------------------
# Module loading via importlib (matches pattern of existing p121 tests)
# ---------------------------------------------------------------------------
import importlib.util
import json
import sys
from pathlib import Path

from utils.time_utils import get_ny_time_millis

worker_mod_path = Path(__file__).parent.parent / 'execution_projection_worker.py'
worker_spec = importlib.util.spec_from_file_location('execution_projection_worker_p122', worker_mod_path)
worker_mod = importlib.util.module_from_spec(worker_spec)  # type: ignore[arg-type]
sys.modules[worker_spec.name] = worker_mod  # type: ignore[index]
assert worker_spec.loader is not None
worker_spec.loader.exec_module(worker_mod)  # type: ignore[union-attr]

# Grab classes from the loaded module
LeaderLease = worker_mod.LeaderLease
ExecutionProjectionWorker = worker_mod.ExecutionProjectionWorker
ProjectionBatchResult = worker_mod.ProjectionBatchResult


# ---------------------------------------------------------------------------
# Fake Redis for tests — fixed-time stream IDs (current epoch)
# ---------------------------------------------------------------------------

_BASE_MS = get_ny_time_millis()  # current ms so cursor_age_ms is near 0


class FakeRedis:
    """In-memory fake that handles the Redis commands used by the worker + lease."""

    def __init__(self) -> None:
        self.kv: dict = {}         # key → value
        self.kv_px: dict = {}      # key → expiry_epoch_ms (or None)
        self.streams: dict = {}    # stream_key → list[(stream_id, fields)]
        self._seq: int = 0

    # --- Basic KV ---
    def get(self, key: str):
        # Respect simulated expiry
        exp = self.kv_px.get(key)
        if exp is not None and exp < get_ny_time_millis():
            self.kv.pop(key, None)
            self.kv_px.pop(key, None)
            return None
        return self.kv.get(key)

    def set(self, key: str, val, ex=None, px=None, nx: bool = False):
        if nx:
            if self.get(key) is not None:
                return None  # key already exists → NX fails
        self.kv[key] = val
        if ex is not None:
            self.kv_px[key] = get_ny_time_millis() + int(ex) * 1000
        elif px is not None:
            self.kv_px[key] = get_ny_time_millis() + int(px)
        else:
            self.kv_px.pop(key, None)
        return True

    def delete(self, *keys):
        count = 0
        for k in keys:
            if k in self.kv:
                self.kv.pop(k)
                self.kv_px.pop(k, None)
                count += 1
        return count

    def incr(self, key: str) -> int:
        v = int(self.kv.get(key) or 0) + 1
        self.kv[key] = str(v)
        return v

    def pexpire(self, key: str, ms: int) -> None:
        """Extend expiry of key by ms milliseconds from now."""
        if key in self.kv:
            self.kv_px[key] = get_ny_time_millis() + int(ms)

    def scan(self, cursor, match: str = '*', count: int = 100):
        """Simplified scan: return all matching keys in one shot."""
        import fnmatch
        pattern = match.replace('*', '**') if match else '*'
        matched = [k for k in self.kv if fnmatch.fnmatch(k, match)]
        return 0, matched

    # --- Streams ---
    def xadd(self, stream_key: str, fields: dict, maxlen=None, approximate=None) -> str:
        self._seq += 1
        # Use current time base so stream IDs appear recent
        sid = f'{_BASE_MS + self._seq}-0'
        self.streams.setdefault(stream_key, []).append((sid, dict(fields)))
        return sid

    def xrange(self, stream_key: str, start='-', end='+', count=None):
        rows = list(self.streams.get(stream_key, []))
        if count is not None:
            rows = rows[: int(count)]
        return rows

    def xrevrange(self, stream_key: str, start: str = '+', end: str = '-', count=None):
        rows = list(reversed(self.streams.get(stream_key, [])))
        if count is not None:
            rows = rows[: int(count)]
        return rows


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _push_event(r: FakeRedis, sid: str, symbol: str = 'BTCUSDT',
                fsm_state: str = 'ENTRY_ACKED', order_id: int = 100) -> None:
    """Push a minimal state_transition event into orders:exec."""
    now_ms = get_ny_time_millis()
    r.xadd('orders:exec', {
        'sid': sid,
        'symbol': symbol,
        'event_type': 'state_transition',
        'action': 'open',
        'status': 'ok',
        'fsm_state': fsm_state,
        'binance_order_id': str(order_id),
        'ts_event_ms': str(now_ms),
    })


def _mk_worker(r: FakeRedis, *, lease: LeaderLease | None = None) -> ExecutionProjectionWorker:
    return ExecutionProjectionWorker(
        r,
        exec_stream='orders:exec',
        state_key_prefix='orders:state:',
        state_ttl_sec=86400,
        cursor_key='orders:exec:projection:cursor',
        batch_size=500,
        leader_lease=lease,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_leader_lease_acquire_sets_redis_key():
    """LeaderLease.acquire() must set the lease key with NX semantics."""
    r = FakeRedis()
    lease = LeaderLease(r, lease_key='orders:exec:projection:leader',
                        fence_key='orders:exec:projection:fence',
                        lease_ttl_ms=5000, renew_interval_ms=100000,
                        worker_id='worker-A')
    result = lease.acquire()
    assert result is True
    assert lease.is_leader()
    assert r.get('orders:exec:projection:leader') == 'worker-A'
    # Fence token should be 1 after first acquisition
    assert int(r.get('orders:exec:projection:fence') or 0) == 1
    lease.release()


def test_non_leader_skips_batch():
    """run_once() must return skipped_not_leader=True when lease not held."""
    r = FakeRedis()
    _push_event(r, 'sid-standby')

    # Worker B acquires lease
    lease_a = LeaderLease(r, lease_key='test:leader', fence_key='test:fence',
                          lease_ttl_ms=5000, renew_interval_ms=100000,
                          worker_id='worker-A')
    lease_a.acquire()

    # Worker B does NOT acquire — is standby
    lease_b = LeaderLease(r, lease_key='test:leader', fence_key='test:fence',
                          lease_ttl_ms=5000, renew_interval_ms=100000,
                          worker_id='worker-B')
    lease_b.acquire()  # should fail because A holds it

    worker_b = _mk_worker(r, lease=lease_b)
    result = worker_b.run_once()

    assert result.skipped_not_leader is True
    assert result.processed == 0
    assert result.idle is True
    lease_a.release()


def test_fencing_token_monotonically_increments():
    """Each new lease acquisition by a different worker should increment the fence counter."""
    r = FakeRedis()
    lease_a = LeaderLease(r, lease_key='test:leader2', fence_key='test:fence2',
                          lease_ttl_ms=5000, renew_interval_ms=100000,
                          worker_id='worker-A')
    lease_a.acquire()
    token_a = lease_a.fencing_token()
    assert token_a >= 1
    lease_a.release()

    # Simulate leader failover: B acquires
    lease_b = LeaderLease(r, lease_key='test:leader2', fence_key='test:fence2',
                          lease_ttl_ms=5000, renew_interval_ms=100000,
                          worker_id='worker-B')
    lease_b.acquire()
    token_b = lease_b.fencing_token()
    assert token_b > token_a, 'Fencing token must be strictly larger after failover'
    lease_b.release()


def test_run_once_idle_when_not_leader():
    """run_once() should be idle and not write state when not leader."""
    r = FakeRedis()
    _push_event(r, 'sid-x')

    # Occupy the lease with another holder
    r.set('test:leader3', 'someone-else', nx=True, px=5000)

    lease = LeaderLease(r, lease_key='test:leader3', fence_key='test:fence3',
                        lease_ttl_ms=5000, renew_interval_ms=100000,
                        worker_id='this-worker')
    lease.acquire()  # fails — someone-else holds it

    worker = _mk_worker(r, lease=lease)
    result = worker.run_once()
    assert result.idle is True
    assert result.processed == 0
    # orders:state must remain absent
    assert r.get('orders:state:sid-x') is None


def test_stale_writer_aborts_batch():
    """When fencing token is behind Redis, run_once() should abort (stale_writer=True)."""
    r = FakeRedis()
    _push_event(r, 'sid-stale')

    lease = LeaderLease(r, lease_key='test:leader4', fence_key='test:fence4',
                        lease_ttl_ms=5000, renew_interval_ms=100000,
                        worker_id='me')
    lease.acquire()  # token = 1, is_leader = True

    # Simulate another worker acquiring while we were GC-paused: fence advances to 2
    r.kv['test:fence4'] = '2'

    worker = _mk_worker(r, lease=lease)
    result = worker.run_once()
    # Our token (1) < remote (2) → stale writer
    assert result.stale_writer is True
    assert result.processed == 0


def test_health_snapshot_leader_fields():
    """health_snapshot() should reflect leader=True and reasonable lag when is_leader."""
    r = FakeRedis()
    _push_event(r, 'sid-health')

    lease = LeaderLease(r, lease_key='test:leader5', fence_key='test:fence5',
                        lease_ttl_ms=5000, renew_interval_ms=100000,
                        worker_id='health-worker')
    lease.acquire()

    worker = _mk_worker(r, lease=lease)
    worker.run_until_idle()

    snap = worker.health_snapshot(lag_readyz_max_ms=30000)
    assert snap['leader'] is True
    assert snap['lease_enabled'] is True
    assert snap['fencing_token'] >= 1
    assert snap['cursor'] != ''
    assert isinstance(snap['lag_ms'], int)
    assert isinstance(snap['ready'], bool)
    lease.release()


def test_health_snapshot_no_lease_always_leader():
    """Without a lease (single-node mode), health_snapshot() must always report leader=True."""
    r = FakeRedis()
    _push_event(r, 'sid-nolease')
    worker = _mk_worker(r)  # no lease
    worker.run_until_idle()

    snap = worker.health_snapshot(lag_readyz_max_ms=30000)
    assert snap['leader'] is True
    assert snap['lease_enabled'] is False
    # With fresh events and low threshold, should be ready
    assert snap['ready'] is True


def test_set_cursor_to_tip():
    """set_cursor_to_tip() must move the projection cursor to the latest stream ID."""
    r = FakeRedis()
    _push_event(r, 'sid-tip-1')
    _push_event(r, 'sid-tip-2')

    worker = _mk_worker(r)
    tip = worker.set_cursor_to_tip()

    # cursor must match the last entry in the stream
    last_id = r.streams['orders:exec'][-1][0]
    assert tip == last_id
    assert r.get('orders:exec:projection:cursor') == last_id


def test_rebuild_sid_rebuilds_single_sid():
    """rebuild_sid() must replay only events for the given SID."""
    r = FakeRedis()
    _push_event(r, 'sid-rebuild-A', fsm_state='ENTRY_ACKED', order_id=555)
    _push_event(r, 'sid-rebuild-B', fsm_state='PROTECTED', order_id=666)

    worker = _mk_worker(r)
    count = worker.rebuild_sid('sid-rebuild-A')

    assert count >= 1
    state_a = json.loads(r.get('orders:state:sid-rebuild-A'))
    assert state_a['fsm_state'] == 'ENTRY_ACKED'
    # B should not be written
    assert r.get('orders:state:sid-rebuild-B') is None


def test_rebuild_all_rebuilds_multiple_sids():
    """rebuild_all() must replay events for all SIDs and update their state."""
    r = FakeRedis()
    _push_event(r, 'sid-all-1', fsm_state='ENTRY_ACKED', order_id=100)
    _push_event(r, 'sid-all-2', fsm_state='PROTECTED', order_id=200)
    _push_event(r, 'sid-all-1', fsm_state='PROTECTED', order_id=300)

    worker = _mk_worker(r)
    counts = worker.rebuild_all()

    assert 'sid-all-1' in counts
    assert 'sid-all-2' in counts
    assert counts['sid-all-1'] == 2
    assert counts['sid-all-2'] == 1

    state_1 = json.loads(r.get('orders:state:sid-all-1'))
    assert state_1['fsm_state'] == 'PROTECTED'
    state_2 = json.loads(r.get('orders:state:sid-all-2'))
    assert state_2['fsm_state'] == 'PROTECTED'
