from pathlib import Path
import importlib.util
import sys

mod_path = Path(__file__).parent.parent / "binance_futures_client.py"
spec = importlib.util.spec_from_file_location("binance_futures_client", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


def test_post_order_routes_algo_and_maps_compat_fields(monkeypatch):
    calls = []

    client = mod.BinanceFuturesClient(api_key="k", api_secret="s")

    def fake_request(method, path, params=None, signed=False):
        calls.append((method, path, dict(params or {}), signed))
        return {"algoId": 123}

    monkeypatch.setattr(client, "_request", fake_request)

    client.post_order({
        "symbol": "BTCUSDT",
        "side": "SELL",
        "type": "STOP_MARKET",
        "stopPrice": "100.0",
        "newClientOrderId": "cid-1",
    })

    assert calls
    method, path, params, signed = calls[0]
    assert method == "POST"
    assert path == "/fapi/v1/algoOrder"
    assert params["triggerPrice"] == "100.0"
    assert params["clientAlgoId"] == "cid-1"
    assert signed is True


def test_post_order_routes_plain_market(monkeypatch):
    calls = []
    client = mod.BinanceFuturesClient(api_key="k", api_secret="s")

    def fake_request(method, path, params=None, signed=False):
        calls.append((method, path, dict(params or {}), signed))
        return {"orderId": 456}

    monkeypatch.setattr(client, "_request", fake_request)

    client.post_order({
        "symbol": "BTCUSDT",
        "side": "BUY",
        "type": "MARKET",
        "quantity": "0.01",
    })

    method, path, params, signed = calls[0]
    assert path == "/fapi/v1/order"
    assert params["type"] == "MARKET"
