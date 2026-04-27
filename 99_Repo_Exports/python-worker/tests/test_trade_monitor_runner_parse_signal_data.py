from __future__ import annotations

import json
import importlib.util
from pathlib import Path
from typing import Any, Callable, Dict


def _load_parse_signal() -> Callable[[Dict[str, str]], Dict[str, Any]]:
    """
    Load runners/trade_monitor_runner.py by filepath to avoid assumptions about
    package layout (__init__.py) or PYTHONPATH in different environments.
    """
    root = Path(__file__).resolve().parents[1]  # .../python-worker
    runner = root / "runners" / "trade_monitor_runner.py"
    spec = importlib.util.spec_from_file_location("_tm_runner_mod", str(runner))
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    fn = getattr(mod, "_parse_signal", None)
    assert callable(fn)
    return fn


def test_parse_signal_canonical_data_envelope_flattens_payload():
    parse_signal = _load_parse_signal()

    env = {
        "signal_id": "sig-can-1",
        "ts_ms": 1700000000000,
        "kind": "volatility",
        "symbol": "BTCUSDT",
        "payload": {
            # TradeMonitor._normalize_signal uses sid or signal_id
            "sid": "sig-can-1",
            # your pipeline uses timeframe in some places; we keep it as-is here
            "timeframe": "1m",
            "trail_after_tp1": 0,
            "trail_after_tp1_reason": "LOW_MOMO",
        },
    }
    fields = {"data": json.dumps(env, ensure_ascii=False, separators=(",", ":"))}
    d = parse_signal(fields)

    # Flattened payload should be present at top-level
    assert d["sid"] == "sig-can-1"
    assert d["trail_after_tp1"] in (0, "0")
    assert d["trail_after_tp1_reason"] == "LOW_MOMO"
    assert d["symbol"] == "BTCUSDT"
    assert d["kind"] == "volatility"


def test_parse_signal_data_non_json_falls_back_to_flat():
    parse_signal = _load_parse_signal()
    d = parse_signal({"data": "not-json", "foo": "bar"})
    # fail-open: keep whatever we can
    assert d.get("foo") == "bar"


def test_parse_signal_payload_json_backcompat():
    parse_signal = _load_parse_signal()
    d = parse_signal({"payload_json": json.dumps({"sid": "sig-2", "tf": "1m"}, separators=(",", ":"))})
    assert d["sid"] == "sig-2"
    assert d["tf"] == "1m"
