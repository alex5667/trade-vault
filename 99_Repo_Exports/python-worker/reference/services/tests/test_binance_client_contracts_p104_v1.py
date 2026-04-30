from pathlib import Path
import importlib.util
import sys
import pytest

mod_path = Path(__file__).parent.parent / 'binance_futures_client.py'
spec = importlib.util.spec_from_file_location('binance_futures_client_p104', mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


def test_plain_order_rejects_close_position_locally(monkeypatch):
    client = mod.BinanceFuturesClient(api_key='k', api_secret='s')
    monkeypatch.setenv('BINANCE_POSITION_MODE', 'oneway')
    with pytest.raises(ValueError):
        client.post_plain_order({
            'symbol': 'BTCUSDT'
            'side': 'BUY'
            'type': 'MARKET'
            'closePosition': True
        })


def test_algo_trailing_requires_valid_activate_price(monkeypatch):
    client = mod.BinanceFuturesClient(api_key='k', api_secret='s')
    monkeypatch.setenv('BINANCE_POSITION_MODE', 'oneway')
    with pytest.raises(ValueError):
        client.post_algo_order({
            'symbol': 'BTCUSDT'
            'side': 'SELL'
            'type': 'TRAILING_STOP_MARKET'
            'quantity': '0.01'
            'callbackRate': '0.5'
            'activatePrice': '0'
            'clientAlgoId': 'a1'
        })


def test_replace_untriggered_algo_order_cancel_then_post(monkeypatch):
    calls = []
    client = mod.BinanceFuturesClient(api_key='k', api_secret='s')

    def fake_request(method, path, params=None, signed=False):
        calls.append((method, path, dict(params or {})))
        if path == '/fapi/v1/algoOrder' and method == 'POST':
            return {'algoId': 77}
        return {'ok': True}

    monkeypatch.setenv('BINANCE_POSITION_MODE', 'oneway')
    monkeypatch.setattr(client, '_request', fake_request)
    out = client.replace_untriggered_algo_order(
        'BTCUSDT'
        client_algo_id='old-a'
        new_params={
            'symbol': 'BTCUSDT'
            'side': 'SELL'
            'type': 'STOP_MARKET'
            'quantity': '0.01'
            'triggerPrice': '100.0'
            'clientAlgoId': 'new-a'
        }
    )
    assert out['algoId'] == 77
    assert calls[0][0:2] == ('DELETE', '/fapi/v1/algoOrder')
    assert calls[1][0:2] == ('POST', '/fapi/v1/algoOrder')
