"""P1.2.3 Bootstrap Supervisor unit tests.

Tests exercise the combined readiness gate that the executor must pass
before starting. Three scenarios:

  1. Happy path — projection healthy + user-stream fresh → ready
  2. User-stream stale — supervisor reports unready (boot is blocked)
  3. HTTP health endpoint — combined readiness visible via /readyz
"""
from pathlib import Path
import importlib.util
import json
import socket
import sys
import threading
import time
import urllib.request


# ---------------------------------------------------------------------------
# Dynamic module loading so tests work without installing the package
# ---------------------------------------------------------------------------

def _load(alias, path):
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


_SVCDIR = Path(__file__).parent.parent

exec_mod = _load('binance_executor_p123', _SVCDIR / 'binance_executor.py')
worker_mod = _load('execution_projection_worker_p123', _SVCDIR / 'execution_projection_worker.py')
sup_mod = _load('execution_bootstrap_supervisor_p123', _SVCDIR / 'execution_bootstrap_supervisor.py')
health_mod = _load('execution_bootstrap_health_server_p123', _SVCDIR / 'execution_bootstrap_health_server.py')

LeaderLease = worker_mod.LeaderLease


# ---------------------------------------------------------------------------
# FakeRedis — minimal in-memory implementation, no external dependencies
# ---------------------------------------------------------------------------

class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.streams = {}
        self._seq = 0

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, ex=None, nx=False, xx=False, px=None):
        exists = key in self.kv
        if nx and exists:
            return False
        if xx and not exists:
            return False
        self.kv[key] = value
        return True

    def incr(self, key):
        cur = int(self.kv.get(key) or 0)
        cur += 1
        self.kv[key] = str(cur)
        return cur

    def delete(self, key):
        existed = key in self.kv
        self.kv.pop(key, None)
        return 1 if existed else 0

    def scan_iter(self, match=None):
        if not match or match == '*':
            yield from list(self.kv.keys())
            return
        if match.endswith('*'):
            prefix = match[:-1]
            for key in list(self.kv.keys()):
                if str(key).startswith(prefix):
                    yield key
            return
        for key in list(self.kv.keys()):
            if key == match:
                yield key

    def xadd(self, key, fields, maxlen=None, approximate=None):
        self._seq += 1
        sid = f"{int(time.time() * 1000) + self._seq}-0"
        self.streams.setdefault(key, []).append((sid, dict(fields)))
        return sid

    def xrange(self, key, start='-', end='+', count=None):
        rows = list(self.streams.get(key, []))

        def _ge(stream_id, ref):
            a, b = stream_id.split('-', 1)
            c, d = ref.split('-', 1)
            return (int(a), int(b)) >= (int(c), int(d))

        if start not in ('-', '+'):
            rows = [row for row in rows if _ge(row[0], start)]
        if count is not None:
            rows = rows[: int(count)]
        return rows

    def xrevrange(self, key, end='+', start='-', count=None):
        rows = list(reversed(self.streams.get(key, [])))
        if count is not None:
            rows = rows[: int(count)]
        return rows

    def pexpire(self, key, ms):
        return True


def _mk_exec(redis_obj, *, inline_projection=False):
    """Build a minimal BinanceExecutor shell for test use (no API keys needed)."""
    ex = exec_mod.BinanceExecutor.__new__(exec_mod.BinanceExecutor)
    ex.r = redis_obj
    ex.exec_stream = 'orders:exec'
    ex.state_key_prefix = 'orders:state:'
    ex.state_ttl = 86400
    ex.exec_rehydrate_on_state_miss = True
    ex.exec_replay_scan_count = 500
    ex.exec_replay_checkpoint_key_prefix = 'orders:exec:replay:cursor:'
    ex.exec_replay_quarantine_on_mismatch = False
    ex.exec_journal_primary = True
    ex.exec_state_derived_view = True
    ex.exec_inline_state_projection = inline_projection
    ex.execution_journal = None
    return ex


def _mk_worker(r, owner_id, lease_key='orders:exec:projection:leader',
               fencing_key='orders:exec:projection:fencing'):
    """Build a worker with leader lease already acquired."""
    lease = LeaderLease(
        r,
        lease_key=lease_key,
        fence_key=fencing_key,
        lease_ttl_ms=10000,
        renew_interval_ms=5000,
        worker_id=owner_id,
    )
    lease.acquire()
    return worker_mod.ExecutionProjectionWorker(
        r,
        exec_stream='orders:exec',
        state_key_prefix='orders:state:',
        leader_lease=lease,
    )


# ---------------------------------------------------------------------------
# Test 1: happy path — both dependencies healthy
# ---------------------------------------------------------------------------

def test_bootstrap_supervisor_ready_when_projection_and_user_stream_are_healthy():
    r = FakeRedis()
    ex = _mk_exec(r, inline_projection=False)
    # Write one exec event so projection worker has something to consume
    ex._exec_event({
        'sid': 'sid-bootstrap',
        'symbol': 'BTCUSDT',
        'action': 'open',
        'event_type': 'state_transition',
        'status': 'ok',
        'fsm_state': 'PROTECTED',
        'binance_order_id': 101,
    })
    worker = _mk_worker(r, 'leader-bootstrap')
    worker.run_once()
    # Write a fresh user-stream status document
    now_ms = int(time.time() * 1000)
    r.set('orders:user_stream:status', json.dumps({
        'connected': True,
        'listen_key': 'lk-1',
        'status': 'stream_live',
        'last_keepalive_ms': now_ms,
        'last_ingest_ms': now_ms,
        'last_event_ms': now_ms,
        'ws_connected_ms': now_ms,
    }))
    sup = sup_mod.ExecutionBootstrapSupervisor(r, projection_worker=worker)
    snap = sup.health_snapshot()
    assert snap.ready is True
    assert snap.reason == 'ok'
    assert snap.projection['ready'] is True
    assert snap.user_stream['ready'] is True


# ---------------------------------------------------------------------------
# Test 2: stale user-stream — supervisor must block
# ---------------------------------------------------------------------------

def test_bootstrap_supervisor_fails_when_user_stream_is_stale():
    r = FakeRedis()
    ex = _mk_exec(r, inline_projection=False)
    ex._exec_event({
        'sid': 'sid-stale',
        'symbol': 'ETHUSDT',
        'action': 'open',
        'event_type': 'state_transition',
        'status': 'ok',
        'fsm_state': 'ENTRY_ACKED',
    })
    worker = _mk_worker(r, 'leader-stale')
    worker.run_once()
    # Write a stale user-stream status (120 s old)
    stale_ms = int(time.time() * 1000) - 120000
    r.set('orders:user_stream:status', json.dumps({
        'connected': True,
        'listen_key': 'lk-stale',
        'status': 'stream_live',
        'last_keepalive_ms': stale_ms,
        'last_ingest_ms': stale_ms,
        'last_event_ms': stale_ms,
        'ws_connected_ms': stale_ms,
    }))
    # user_stream_max_stale_ms=1000 → 120 s is way beyond the threshold
    sup = sup_mod.ExecutionBootstrapSupervisor(r, projection_worker=worker, user_stream_max_stale_ms=1000)
    snap = sup.wait_until_ready(timeout_ms=5, poll_ms=1)
    assert snap.ready is False
    assert snap.reason == 'user_stream:user_stream_stale'


# ---------------------------------------------------------------------------
# Test 3: HTTP endpoint returns 200 with ready=True on /readyz
# ---------------------------------------------------------------------------

def test_bootstrap_health_http_endpoint_reports_combined_readiness():
    r = FakeRedis()
    ex = _mk_exec(r, inline_projection=False)
    ex._exec_event({
        'sid': 'sid-http',
        'symbol': 'SOLUSDT',
        'action': 'open',
        'event_type': 'state_transition',
        'status': 'ok',
        'fsm_state': 'PROTECTED',
    })
    worker = _mk_worker(r, 'leader-http')
    worker.run_once()
    now_ms = int(time.time() * 1000)
    r.set('orders:user_stream:status', json.dumps({
        'connected': True,
        'listen_key': 'lk-http',
        'status': 'stream_live',
        'last_keepalive_ms': now_ms,
        'last_ingest_ms': now_ms,
        'ws_connected_ms': now_ms,
    }))
    sup = sup_mod.ExecutionBootstrapSupervisor(r, projection_worker=worker)

    # Bind a random free port
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    host, port = sock.getsockname()
    sock.close()

    server = health_mod.HTTPServer((host, port), health_mod._Handler)
    server.supervisor = sup  # type: ignore[attr-defined]
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:
        with urllib.request.urlopen(f'http://{host}:{port}/readyz', timeout=2) as resp:
            payload = json.loads(resp.read().decode('utf-8'))
            assert resp.status == 200
            assert payload['ready'] is True
            # projection detail should include worker_id == leader identity
            assert payload['projection']['detail']['worker_id'] == 'leader-http'
    finally:
        server.shutdown()
        th.join(timeout=2)
