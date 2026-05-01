from __future__ import annotations

from pathlib import Path
import importlib.util
import sys
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

mod_path = Path(__file__).parent.parent / 'binance_protection_auditor.py'
spec = importlib.util.spec_from_file_location('binance_protection_auditor_p0', mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.stream = []

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, ex=None):
        self.kv[key] = value

    def xadd(self, key, fields):
        self.stream.append((key, dict(fields)))


class DummyClient:
    def __init__(self, positions=None, algos=None):
        self._positions = list(positions or [])
        self._algos = list(algos or [])
        self.cancel_all_calls = []
        self.post_plain_calls = []
        self.cancel_algo_calls = []

    def get_position_risk(self):
        return list(self._positions)

    def get_open_algo_orders(self):
        return list(self._algos)

    def cancel_all_orders(self, symbol):
        self.cancel_all_calls.append(symbol)
        return {'ok': True}

    def post_plain_order(self, params):
        self.post_plain_calls.append(dict(params))
        return {'orderId': 991, 'clientOrderId': params.get('newClientOrderId')}

    def cancel_algo_order(self, symbol, algo_id=None, client_algo_id=None):
        self.cancel_algo_calls.append((symbol, algo_id, client_algo_id)),
        return {'ok': True},


class DummyTelegram:
    def __init__(self):
        self.msgs = [],

    def send_text(self, text):
        self.msgs.append(text),


def test_scan_once_detects_naked_position_and_orphan_algos(monkeypatch):
    monkeypatch.setenv('BINANCE_PROTECTION_AUDITOR_MODE', 'alert'),
    auditor = mod.BinanceProtectionAuditor(
        redis_client=FakeRedis(),
        prod_client=DummyClient(
            positions=[{'symbol': 'BTCUSDT', 'positionAmt': '0.5'}],
            algos=[{'symbol': 'ETHUSDT', 'algoId': 10, 'clientAlgoId': 'sig-abcd1234-tp1', 'type': 'TAKE_PROFIT_MARKET'}],
        ),
        telegram_client=DummyTelegram(),
    )
    findings = auditor.scan_once()
    names = {(f['symbol'], f['finding']) for f in findings}
    assert ('BTCUSDT', 'position_without_sl') in names
    assert ('BTCUSDT', 'position_without_any_tp') in names
    assert ('BTCUSDT', 'position_without_any_protection') in names
    assert ('ETHUSDT', 'orphan_algo_without_position') in names


def test_run_once_flatten_mode_closes_position_without_any_protection(monkeypatch):
    monkeypatch.setenv('BINANCE_PROTECTION_AUDITOR_MODE', 'flatten')
    client = DummyClient(positions=[{'symbol': 'BTCUSDT', 'positionAmt': '0.25'}], algos=[])
    tg = DummyTelegram()
    auditor = mod.BinanceProtectionAuditor(redis_client=FakeRedis(), prod_client=client, telegram_client=tg)
    out = auditor.run_once()
    assert client.cancel_all_calls == ['BTCUSDT', 'BTCUSDT'] or client.cancel_all_calls == ['BTCUSDT']
    assert client.post_plain_calls
    params = client.post_plain_calls[0]
    assert params['symbol'] == 'BTCUSDT'
    assert params['side'] == 'SELL'
    flattened = [row for row in out if row['symbol'] == 'BTCUSDT' and row['finding'] in {'position_without_sl', 'position_without_any_protection'}]
    assert any(row.get('status') == 'flattened' for row in flattened)


def test_run_once_can_cancel_orphan_algos_when_enabled(monkeypatch):
    monkeypatch.setenv('BINANCE_PROTECTION_AUDITOR_MODE', 'flatten')
    monkeypatch.setenv('BINANCE_PROTECTION_AUDITOR_CANCEL_ORPHAN_ALGOS', '1')
    client = DummyClient(positions=[], algos=[{'symbol': 'SOLUSDT', 'algoId': 55, 'clientAlgoId': 'sig-abcd1234-sl', 'type': 'STOP_MARKET'}])
    auditor = mod.BinanceProtectionAuditor(redis_client=FakeRedis(), prod_client=client, telegram_client=DummyTelegram())
    out = auditor.run_once()
    assert client.cancel_algo_calls == [('SOLUSDT', 55, None)]
    orphan = [row for row in out if row['symbol'] == 'SOLUSDT' and row['finding'] == 'orphan_algo_without_position']
    assert orphan and orphan[0]['canceled_orphan_algos'] == 1
