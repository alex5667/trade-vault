"""P0.5 — signal_id must use canonical generate_signal_id algorithm, not MD5."""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common.normalization import SIGNAL_ID_ALGO_V1, generate_signal_id
from services.signal_preprocess import preprocess_signal_for_publish


def _run(**kwargs) -> dict:
    signal = {"symbol": "BTCUSDT", "direction": "LONG", "entry": 100.0, **kwargs}
    return preprocess_signal_for_publish(signal, "BTCUSDT", "CryptoOrderFlow", logger=None)


def test_signal_id_format_matches_canonical():
    out = _run()
    expected = generate_signal_id(
        symbol="BTCUSDT",
        ts_ms=out["ts_ms"],
        direction="LONG",
        kind="crypto-of",
    )
    assert out["signal_id"] == expected


def test_signal_id_not_md5():
    out = _run()
    assert ":" in out["signal_id"], "signal_id must contain ':' separators (canonical format)"
    assert len(out["signal_id"]) != 16, "signal_id must not be 16-char MD5 hex"


def test_id_algo_set():
    out = _run()
    assert out.get("id_algo") == SIGNAL_ID_ALGO_V1


def test_existing_signal_id_not_overwritten():
    out = _run(signal_id="crypto-of:BTCUSDT:1714234567890:L")
    assert out["signal_id"] == "crypto-of:BTCUSDT:1714234567890:L"
    assert out["sid"] == out["signal_id"]


def test_sid_mirrors_signal_id():
    out = _run()
    assert out["sid"] == out["signal_id"]
