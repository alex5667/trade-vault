from pathlib import Path
import importlib.util
import sys

mod_path = Path(__file__).parent.parent / "services" / "binance_futures_client.py"
spec = importlib.util.spec_from_file_location("binance_futures_client", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


def test_user_stream_methods_route_to_listenkey_endpoints():
    client = mod.BinanceFuturesClient(api_key="k", api_secret="s")
    calls = []
    def fake_request(method, path, params=None, signed=False):
        calls.append((method, path, params, signed))
        return {"listenKey": "lk-1"} if method == "POST" else {}
    client._request = fake_request
    assert client.start_user_stream() == "lk-1"
    client.keepalive_user_stream("lk-1")
    client.close_user_stream("lk-1")
    assert calls[0][1] == "/fapi/v1/listenKey"
    assert calls[1][0] == "PUT"
    assert calls[2][0] == "DELETE"


def test_ambiguous_execution_error_detection():
    client = mod.BinanceFuturesClient(api_key="k", api_secret="s")
    exc = mod.BinanceAPIError(503, {"msg": "Unknown error, please check your request or try again later."})
    assert client.is_ambiguous_execution_error(exc) is True
    exc2 = mod.BinanceAPIError(0, {"ambiguous": True, "msg": "timed out"})
    assert client.is_ambiguous_execution_error(exc2) is True
