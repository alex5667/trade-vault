"""Regressions for binance_iceberg_detector:

1. _get_atr must return float (not bytes) when r_core uses
   decode_responses=False. Previously returned raw bytes → `atr > 0` raised
   TypeError on every signal, silently dropping ATR-based SL/TP computation.

2. sid alignment: payload["sid"] (generated via generate_signal_id) must equal
   the outer sid used for the `signals:{sid}` SET key and outbox envelope.
   Earlier code built a parallel "signal:{symbol}:iceberg:{ts}" sid which
   diverged from payload.sid, breaking trade_close_joiner SID joins.
"""
from __future__ import annotations

import time
import types
from unittest.mock import MagicMock

import pytest


class _FakeRedisBytes:
    """Mimics redis.Redis(decode_responses=False) — HGET/GET returns bytes."""

    def __init__(
        self,
        store: dict[tuple[str, str], bytes] | None = None,
        string_store: dict[str, bytes] | None = None,
    ):
        self._store = store or {}
        self._strings = string_store or {}

    def hget(self, key, field):  # noqa: D401 — mimics redis API
        if isinstance(key, bytes):
            key = key.decode()
        if isinstance(field, bytes):
            field = field.decode()
        return self._store.get((key, field))

    def get(self, key):
        if isinstance(key, bytes):
            key = key.decode()
        return self._strings.get(key)


def test_get_atr_decodes_bytes_to_float():
    from services.binance_iceberg_detector import BinanceIcebergDetector

    fake_core = _FakeRedisBytes(store={("candles:BTCUSDT:1h", "atr"): b"123.456"})
    det = BinanceIcebergDetector.__new__(BinanceIcebergDetector)
    det.r_core = fake_core  # type: ignore[attr-defined]
    det.symbol = "BTCUSDT"

    atr = det._get_atr()
    assert atr is not None
    assert isinstance(atr, float)
    assert atr == pytest.approx(123.456)


def test_get_atr_returns_none_on_missing_key():
    from services.binance_iceberg_detector import BinanceIcebergDetector

    fake_core = _FakeRedisBytes(store={})
    det = BinanceIcebergDetector.__new__(BinanceIcebergDetector)
    det.r_core = fake_core  # type: ignore[attr-defined]
    det.symbol = "BTCUSDT"

    assert det._get_atr() is None


def test_get_atr_returns_none_on_garbage_value():
    from services.binance_iceberg_detector import BinanceIcebergDetector

    fake_core = _FakeRedisBytes(store={("candles:BTCUSDT:1h", "atr"): b"not_a_number"})
    det = BinanceIcebergDetector.__new__(BinanceIcebergDetector)
    det.r_core = fake_core  # type: ignore[attr-defined]
    det.symbol = "BTCUSDT"

    assert det._get_atr() is None


def test_get_atr_comparison_after_decode_does_not_raise():
    """The original bug: `atr > 0` raised TypeError because atr was bytes."""
    from services.binance_iceberg_detector import BinanceIcebergDetector

    fake_core = _FakeRedisBytes(store={("candles:BTCUSDT:1h", "atr"): b"42.0"})
    det = BinanceIcebergDetector.__new__(BinanceIcebergDetector)
    det.r_core = fake_core  # type: ignore[attr-defined]
    det.symbol = "BTCUSDT"

    atr = det._get_atr()
    # Original code did `if atr and atr > 0:` — must not raise on float.
    assert atr is not None and atr > 0
    # And arithmetic with it must work (ATR-based SL formula).
    sl_dist = 2.0 * atr
    assert sl_dist == pytest.approx(84.0)


def test_atr_falls_back_to_canonical_string_keys(monkeypatch):
    """In prod, ATR is written to `atr:{SYMBOL}:{TF}` (string), NOT to the
    legacy `candles:{symbol}:1h` hash. The detector must fall through to the
    canonical key, otherwise iceberg ATR-distance gating is permanently dead.
    """
    from services.binance_iceberg_detector import BinanceIcebergDetector

    fake_core = _FakeRedisBytes(
        store={},  # legacy hash empty
        string_store={"atr:BTCUSDT:1m": b"42.5"},
    )
    det = BinanceIcebergDetector.__new__(BinanceIcebergDetector)
    det.r_core = fake_core  # type: ignore[attr-defined]
    det.symbol = "BTCUSDT"

    # Default ladder is "1h,5m,1m" — 1h/5m absent, 1m wins.
    assert det._get_atr() == pytest.approx(42.5)


def test_atr_ladder_prefers_longer_tf(monkeypatch):
    """When multiple TFs are written, the ladder respects order (1h > 5m > 1m)."""
    from services.binance_iceberg_detector import BinanceIcebergDetector

    fake_core = _FakeRedisBytes(
        store={},
        string_store={
            "atr:BTCUSDT:1m": b"1.0",
            "atr:BTCUSDT:5m": b"5.0",
            "atr:BTCUSDT:1h": b"60.0",
        },
    )
    det = BinanceIcebergDetector.__new__(BinanceIcebergDetector)
    det.r_core = fake_core  # type: ignore[attr-defined]
    det.symbol = "BTCUSDT"

    assert det._get_atr() == pytest.approx(60.0)


def test_atr_ladder_env_override(monkeypatch):
    """ICEBERG_ATR_TF_LADDER env reorders the lookup."""
    from services.binance_iceberg_detector import BinanceIcebergDetector

    monkeypatch.setenv("ICEBERG_ATR_TF_LADDER", "1m,5m,1h")
    fake_core = _FakeRedisBytes(
        store={},
        string_store={
            "atr:BTCUSDT:1m": b"1.0",
            "atr:BTCUSDT:1h": b"60.0",
        },
    )
    det = BinanceIcebergDetector.__new__(BinanceIcebergDetector)
    det.r_core = fake_core  # type: ignore[attr-defined]
    det.symbol = "BTCUSDT"

    # Ladder starts with 1m → wins even though 1h is also present.
    assert det._get_atr() == pytest.approx(1.0)


def test_atr_legacy_hash_still_wins_when_present(monkeypatch):
    """Backward-compat: if the legacy hash candles:{symbol}:1h holds an ATR,
    it takes precedence over the canonical string keys."""
    from services.binance_iceberg_detector import BinanceIcebergDetector

    fake_core = _FakeRedisBytes(
        store={("candles:BTCUSDT:1h", "atr"): b"99.0"},
        string_store={"atr:BTCUSDT:1m": b"1.0"},
    )
    det = BinanceIcebergDetector.__new__(BinanceIcebergDetector)
    det.r_core = fake_core  # type: ignore[attr-defined]
    det.symbol = "BTCUSDT"

    assert det._get_atr() == pytest.approx(99.0)


def test_atr_returns_none_when_nothing_resolves():
    from services.binance_iceberg_detector import BinanceIcebergDetector

    fake_core = _FakeRedisBytes(store={}, string_store={})
    det = BinanceIcebergDetector.__new__(BinanceIcebergDetector)
    det.r_core = fake_core  # type: ignore[attr-defined]
    det.symbol = "BTCUSDT"

    assert det._get_atr() is None


def test_build_payload_emits_sid_signal_id_trace_id_all_equal(monkeypatch):
    """payload.sid == payload.signal_id == payload.trace_id (single source)."""
    monkeypatch.setenv("ICEBERG_SID_RANDOM_SUFFIX", "0")
    from services.binance_iceberg_detector import _build_iceberg_signal_payload

    st = types.SimpleNamespace(refresh_count=3, visible_qty=12.5, since_ts=time.time() - 2.0)
    p = _build_iceberg_signal_payload(
        symbol="BTCUSDT",
        direction="LONG",
        price=100.0,
        state=st,
        level_info={"kind": "bid", "price": 99.5},
        atr=2.0,
    )
    assert p["sid"] == p["signal_id"] == p["trace_id"]
    assert p["sid"]  # non-empty
