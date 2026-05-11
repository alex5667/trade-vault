import importlib.util
import sys
import types
from pathlib import Path

# Allow running from repo root or from services/ directly
root = Path(__file__).resolve().parents[2]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))


def _load(name: str, rel: str):
    """Load a module by relative path from repo root, bypassing top-level imports."""
    mod_path = root / rel
    spec = importlib.util.spec_from_file_location(name, mod_path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore
    sys.modules[spec.name] = mod  # type: ignore
    assert spec.loader is not None  # type: ignore
    spec.loader.exec_module(mod)  # type: ignore
    return mod


class FakeRedis:
    """Minimal in-memory Redis stub for unit tests."""

    def __init__(self):
        self.kv = {}
        self.streams = {}
        self.seq = 0

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, ex=None, nx=False, px=None):
        if nx and key in self.kv:
            return False
        self.kv[key] = value
        return True

    def delete(self, *keys):
        n = 0
        for key in keys:
            if key in self.kv:
                del self.kv[key]
                n += 1
        return n

    def xadd(self, key, fields, maxlen=None, approximate=None):
        self.seq += 1
        sid = f"{1700000000000 + self.seq}-0"
        self.streams.setdefault(key, []).append((sid, dict(fields)))
        return sid


def _stub_module(name: str, **attrs):
    """Register a lightweight stub module under the given name."""
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


class DummyStore:
    """Minimal ActiveSymbolGuardStore stub."""

    def __init__(self, redis_obj, **kwargs):
        self.r = redis_obj
        self.docs = {}

    def load_raw(self, symbol):
        return dict(self.docs.get(symbol.upper()) or {})

    def load_view(self, symbol):
        return dict(self.docs.get(symbol.upper()) or {})

    def mark_released(self, *, symbol, expected_sid='', release_reason='', writer='', extra_patch=None, **kwargs):
        symbol = symbol.upper()
        doc = dict(self.docs.get(symbol) or {'symbol': symbol, 'sid': expected_sid, 'guard_version': 1})
        doc.update(extra_patch or {})
        doc['guard_status'] = 'released'
        doc['release_reason'] = release_reason
        doc['sid'] = expected_sid or doc.get('sid') or ''
        self.docs[symbol] = doc
        return {'applied': True, 'reason': 'released', 'doc': doc}


class DummyDiagnostics:
    """Minimal diagnostics stub returning configurable exchange truth."""

    def __init__(self, exchange_truth=None, triage_symbol='BTCUSDT', triage_sid='sid-1'):
        self.exchange_truth = exchange_truth or {
            'symbol': 'BTCUSDT', 'position_amt': 0.0,
            'open_plain_orders': 0, 'open_algo_orders': 0, 'is_reliable': True,
        }
        self.symbol = triage_symbol
        self.sid = triage_sid

    def debug_symbol(self, symbol, include_exchange=False):
        return {
            'guard_raw': {'symbol': symbol, 'sid': self.sid, 'guard_status': 'active'},
            'guard_view': {'symbol': symbol, 'sid': self.sid, 'guard_status': 'active'},
            'exchange_truth': dict(self.exchange_truth),
        }


class DummyPolicy:
    """Minimal incident policy stub."""

    def triage_symbol(self, symbol, include_exchange=False):
        return {
            'summary': {'symbol': symbol, 'sid': 'sid-1', 'severity': 'warning'},
            'policy': {'fingerprint': f'fp:{symbol}'},
        }

    def triage_sid(self, sid, include_exchange=False):
        return {
            'summary': {'symbol': 'BTCUSDT', 'sid': sid, 'severity': 'warning'},
            'policy': {'fingerprint': f'fp:{sid}'},
        }


def test_apply_and_revoke_hold_record_audit():
    """hold_symbol → is_active=True; revoke → is_active gone; 2 audit records emitted."""
    _stub_module('services.active_symbol_guard_diagnostics', ActiveSymbolGuardDiagnostics=DummyDiagnostics)
    _stub_module('services.active_symbol_guard_incident_policy', ActiveSymbolGuardIncidentPolicyEngine=DummyPolicy)
    _stub_module('services.active_symbol_guard_semantics', guard_view=lambda doc, **kwargs: dict(doc or {}))
    _stub_module('services.active_symbol_guard_store', ActiveSymbolGuardStore=DummyStore)
    _stub_module('services.binance_futures_client', BinanceFuturesClient=object)
    _stub_module('services.execution_metrics',
                 EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_ACTION_TOTAL=None,
                 EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_AUDIT_TOTAL=None)

    mod = _load('services.active_symbol_guard_runbook_p12_test1', 'services/active_symbol_guard_runbook.py')
    r = FakeRedis()
    ex = mod.ActiveSymbolGuardRunbookExecutor(r, diagnostics=DummyDiagnostics(), policy=DummyPolicy(), store=DummyStore(r))

    applied = ex.apply_hold_symbol(symbol='BTCUSDT', operator='alice', ticket='INC-1', reason='investigation', ttl_sec=60)
    assert applied['ok'] is True
    assert applied['hold']['ticket'] == 'INC-1'
    assert ex.hold_state('BTCUSDT')['is_active'] is True

    revoked = ex.revoke_hold_symbol(symbol='BTCUSDT', operator='alice', ticket='INC-1', reason='resolved')
    assert revoked['ok'] is True
    assert revoked['result'] == 'revoked'
    assert ex.hold_state('BTCUSDT') == {}
    # Both apply and revoke must leave an audit trail
    assert len(r.streams.get('orders:active_symbol_guard:audit', [])) == 2


def test_guarded_force_release_blocks_when_exchange_not_flat():
    """force_release must return ok=False and NOT modify guard when position > 0."""
    _stub_module('services.active_symbol_guard_diagnostics', ActiveSymbolGuardDiagnostics=DummyDiagnostics)
    _stub_module('services.active_symbol_guard_incident_policy', ActiveSymbolGuardIncidentPolicyEngine=DummyPolicy)
    _stub_module('services.active_symbol_guard_semantics', guard_view=lambda doc, **kwargs: dict(doc or {}))
    _stub_module('services.active_symbol_guard_store', ActiveSymbolGuardStore=DummyStore)
    _stub_module('services.binance_futures_client', BinanceFuturesClient=object)
    _stub_module('services.execution_metrics',
                 EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_ACTION_TOTAL=None,
                 EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_AUDIT_TOTAL=None)

    mod = _load('services.active_symbol_guard_runbook_p12_test2', 'services/active_symbol_guard_runbook.py')
    r = FakeRedis()
    # position_amt=0.25 → exchange is NOT flat
    diag = DummyDiagnostics(exchange_truth={
        'symbol': 'BTCUSDT', 'position_amt': 0.25,
        'open_plain_orders': 0, 'open_algo_orders': 0, 'is_reliable': True,
    })
    store = DummyStore(r)
    store.docs['BTCUSDT'] = {'symbol': 'BTCUSDT', 'sid': 'sid-1', 'guard_status': 'active'}
    ex = mod.ActiveSymbolGuardRunbookExecutor(r, diagnostics=diag, policy=DummyPolicy(), store=store)

    out = ex.guarded_force_release(symbol='BTCUSDT', operator='alice', ticket='INC-2', reason='unsafe')
    assert out['ok'] is False
    assert out['reason'] == 'exchange_truth_not_safe'
    # Guard must NOT have been released
    assert store.docs['BTCUSDT']['guard_status'] == 'active'


def test_escalation_ack_and_renew_require_fingerprint_state():
    """ack → state persisted; renew → renew_count incremented; renew without ack → ack_missing."""
    _stub_module('services.active_symbol_guard_diagnostics', ActiveSymbolGuardDiagnostics=DummyDiagnostics)
    _stub_module('services.active_symbol_guard_incident_policy', ActiveSymbolGuardIncidentPolicyEngine=DummyPolicy)
    _stub_module('services.active_symbol_guard_semantics', guard_view=lambda doc, **kwargs: dict(doc or {}))
    _stub_module('services.active_symbol_guard_store', ActiveSymbolGuardStore=DummyStore)
    _stub_module('services.binance_futures_client', BinanceFuturesClient=object)
    _stub_module('services.execution_metrics',
                 EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_ACTION_TOTAL=None,
                 EXECUTION_ACTIVE_SYMBOL_GUARD_RUNBOOK_AUDIT_TOTAL=None)

    mod = _load('services.active_symbol_guard_runbook_p12_test3', 'services/active_symbol_guard_runbook.py')
    r = FakeRedis()
    ex = mod.ActiveSymbolGuardRunbookExecutor(r, diagnostics=DummyDiagnostics(), policy=DummyPolicy(), store=DummyStore(r))

    ack = ex.escalation_ack(symbol='BTCUSDT', operator='alice', ticket='INC-3', reason='owned', ttl_sec=30)
    assert ack['ok'] is True
    fp = ack['fingerprint']
    state = ex.escalation_state(fp)
    assert state['ack_status'] == 'acked'

    renewed = ex.escalation_renew(symbol='BTCUSDT', operator='bob', ticket='INC-3A', reason='extend', ttl_sec=45)
    assert renewed['ok'] is True
    assert renewed['state']['renew_count'] == 1

    # Attempting renew on a non-existent fingerprint must fail gracefully
    missing = ex.escalation_renew(fingerprint='fp:missing', operator='carol', ticket='INC-4', reason='should_fail')
    assert missing['ok'] is False
    assert missing['reason'] == 'ack_missing'
