# tests/test_signal_outcome.py
"""
Unit tests for signal_outcome dataclass, factory, and writer.

Tests:
  1. from_trade_closed — field extraction from TradeClosed
  2. is_win label computation (r_multiple threshold)
  3. Missing/None field graceful handling
  4. to_dict() — flat stringified output for Redis Stream
  5. Writer.emit_to_redis — mock XADD verification
  6. Writer fail-open — exceptions don't propagate
"""
import pytest
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch


# ---------- Minimal TradeClosed stub for testing ----------

@dataclass
class _FakeTradeClosed:
    """Minimal TradeClosed stub matching domain.models.TradeClosed fields."""
    sid: str = "sig-001"
    order_id: str = "ord-001"
    symbol: str = "BTCUSDT"
    strategy: str = "CryptoOrderFlow"
    source: str = "CryptoOrderFlow"
    tf: str = "5m"
    direction: str = "LONG"

    entry_price: float = 50000.0
    entry_ts_ms: int = 1700000000000
    sl: float = 49500.0
    tp1_price: float = 50500.0
    tp_levels: List[float] = field(default_factory=lambda: [50500.0, 51000.0])
    atr: float = 120.0
    entry_tag: str = "reversal"
    regime: str = "trending"
    scenario: str = "continuation"
    signal_payload: Dict[str, Any] = field(default_factory=dict)

    exit_price: float = 50800.0
    exit_ts_ms: int = 1700003600000
    pnl_net: float = 15.5
    pnl_gross: float = 16.0
    fees: float = 0.5
    r_multiple: float = 1.5
    one_r_money: float = 10.0
    risk_usd: float = 25.0

    close_reason: str = "TP2"
    tp1_hit: bool = True
    tp2_hit: bool = True
    tp3_hit: bool = False
    trailing_started: bool = True
    trailing_active: bool = False
    trailing_moves: int = 3
    duration_ms: int = 3600000

    mfe_pnl: float = 20.0
    mae_pnl: float = -5.0
    giveback: float = 4.5
    missed_profit: float = 2.0

    is_virtual: bool = False
    meta_enforce_cov_bucket: str = "A"


# ---------- Tests: from_trade_closed ----------

class TestFromTradeClosed:
    """Тесты фабричной функции from_trade_closed."""

    def test_basic_extraction(self):
        """Все ключевые поля извлекаются корректно."""
        from domain.signal_outcome import from_trade_closed

        closed = _FakeTradeClosed()
        outcome = from_trade_closed(closed)

        assert outcome is not None
        assert outcome.sid == "sig-001"
        assert outcome.order_id == "ord-001"
        assert outcome.symbol == "BTCUSDT"
        assert outcome.strategy == "CryptoOrderFlow"
        assert outcome.source == "CryptoOrderFlow"
        assert outcome.tf == "5m"
        assert outcome.direction == "LONG"
        assert outcome.entry_price == 50000.0
        assert outcome.entry_ts_ms == 1700000000000
        assert outcome.sl == 49500.0
        assert outcome.tp1_price == 50500.0
        assert outcome.atr == 120.0
        assert outcome.entry_tag == "reversal"
        assert outcome.regime == "trending"
        assert outcome.scenario == "continuation"
        assert outcome.exit_price == 50800.0
        assert outcome.exit_ts_ms == 1700003600000
        assert outcome.pnl_net == 15.5
        assert outcome.pnl_gross == 16.0
        assert outcome.fees == 0.5
        assert outcome.r_multiple == 1.5
        assert outcome.one_r_money == 10.0
        assert outcome.risk_usd == 25.0
        assert outcome.close_reason == "TP2"
        assert outcome.tp1_hit is True
        assert outcome.tp2_hit is True
        assert outcome.tp3_hit is False
        assert outcome.trailing_started is True
        assert outcome.trailing_moves == 3
        assert outcome.duration_ms == 3600000
        assert outcome.mfe_pnl == 20.0
        assert outcome.mae_pnl == -5.0
        assert outcome.giveback == 4.5
        assert outcome.missed_profit == 2.0
        assert outcome.is_virtual is False
        assert outcome.meta_enforce_cov_bucket == "A"

    def test_win_label_positive(self):
        """r_multiple >= 1.0 → is_win=True."""
        from domain.signal_outcome import from_trade_closed

        closed = _FakeTradeClosed(r_multiple=1.0)
        outcome = from_trade_closed(closed)
        assert outcome is not None
        assert outcome.is_win is True

        closed2 = _FakeTradeClosed(r_multiple=2.5)
        outcome2 = from_trade_closed(closed2)
        assert outcome2 is not None
        assert outcome2.is_win is True

    def test_loss_label(self):
        """r_multiple < 1.0 → is_win=False."""
        from domain.signal_outcome import from_trade_closed

        closed = _FakeTradeClosed(r_multiple=0.5)
        outcome = from_trade_closed(closed)
        assert outcome is not None
        assert outcome.is_win is False

        closed2 = _FakeTradeClosed(r_multiple=-1.0)
        outcome2 = from_trade_closed(closed2)
        assert outcome2 is not None
        assert outcome2.is_win is False

    def test_zero_r_multiple_is_loss(self):
        """r_multiple=0 → is_win=False."""
        from domain.signal_outcome import from_trade_closed

        closed = _FakeTradeClosed(r_multiple=0.0)
        outcome = from_trade_closed(closed)
        assert outcome is not None
        assert outcome.is_win is False

    def test_missing_fields_defaults(self):
        """Gracefully handles object with minimal attrs (None/missing → defaults)."""
        from domain.signal_outcome import from_trade_closed

        # Create bare-minimum object
        class _BareClose:
            sid = "bare-001"
            order_id = "ord-bare"
            symbol = "ETHUSDT"
            r_multiple = 0.0

        outcome = from_trade_closed(_BareClose())
        assert outcome is not None
        assert outcome.sid == "bare-001"
        assert outcome.symbol == "ETHUSDT"
        assert outcome.entry_price == 0.0
        assert outcome.sl == 0.0
        assert outcome.atr == 0.0
        assert outcome.is_win is False
        assert outcome.direction == "LONG"  # default

    def test_tp1_fallback_from_tp_levels(self):
        """If tp1_price=0 but tp_levels has entries, use tp_levels[0]."""
        from domain.signal_outcome import from_trade_closed

        closed = _FakeTradeClosed(tp1_price=0.0, tp_levels=[55000.0, 56000.0])
        outcome = from_trade_closed(closed)
        assert outcome is not None
        assert outcome.tp1_price == 55000.0

    def test_regime_from_signal_payload(self):
        """If regime attr is empty, fallback to signal_payload['regime']."""
        from domain.signal_outcome import from_trade_closed

        closed = _FakeTradeClosed(
            regime="",
            scenario="",
            signal_payload={"regime": "ranging", "scenario": "breakout"},
        )
        outcome = from_trade_closed(closed)
        assert outcome is not None
        assert outcome.regime == "ranging"
        assert outcome.scenario == "breakout"

    def test_fail_open_on_exception(self):
        """If extraction fails entirely, return None (don't raise)."""
        from domain.signal_outcome import from_trade_closed

        # Pass something that will cause issues in float() conversion
        class _Bad:
            sid = "bad"
            order_id = "bad"
            symbol = "BAD"
            entry_price = object()  # can't float() this

        result = from_trade_closed(_Bad())
        assert result is None


# ---------- Tests: to_dict ----------

class TestToDict:
    """Тесты сериализации для Redis Stream."""

    def test_flat_string_values(self):
        """All values in to_dict() must be strings (Redis Stream requirement)."""
        from domain.signal_outcome import from_trade_closed

        closed = _FakeTradeClosed()
        outcome = from_trade_closed(closed)
        assert outcome is not None

        d = outcome.to_dict()
        for key, val in d.items():
            assert isinstance(val, str), f"Key '{key}' has non-string value: {type(val)}"

    def test_boolean_encoding(self):
        """Booleans encode as '1'/'0' (not 'True'/'False')."""
        from domain.signal_outcome import from_trade_closed

        closed = _FakeTradeClosed(tp1_hit=True, tp3_hit=False, is_virtual=True)
        outcome = from_trade_closed(closed)
        d = outcome.to_dict()

        assert d["tp1_hit"] == "1"
        assert d["tp3_hit"] == "0"
        assert d["is_virtual"] == "1"

    def test_all_keys_present(self):
        """to_dict() should contain all dataclass fields."""
        from domain.signal_outcome import SignalOutcome

        expected_keys = {
            "sid", "order_id", "symbol", "strategy", "source", "tf", "direction",
            "entry_price", "entry_ts_ms", "sl", "tp1_price", "atr",
            "entry_tag", "regime", "scenario",
            "exit_price", "exit_ts_ms", "pnl_net", "pnl_gross", "fees",
            "r_multiple", "one_r_money", "risk_usd",
            "close_reason",
            "tp1_hit", "tp2_hit", "tp3_hit",
            "trailing_started", "trailing_active", "trailing_moves", "duration_ms",
            "mfe_pnl", "mae_pnl", "giveback", "missed_profit",
            "is_win", "is_virtual", "meta_enforce_cov_bucket",
        }

        outcome = SignalOutcome(sid="test")
        d = outcome.to_dict()
        assert expected_keys.issubset(set(d.keys())), f"Missing keys: {expected_keys - set(d.keys())}"


# ---------- Tests: Writer ----------

class TestSignalOutcomeWriter:
    """Тесты SignalOutcomeWriter — Redis emit и fail-open."""

    def test_emit_to_redis_calls_xadd(self):
        """Writer calls XADD on the Redis connection with correct stream key."""
        from services.signal_outcome_writer import SignalOutcomeWriter
        from domain.signal_outcome import from_trade_closed

        writer = SignalOutcomeWriter()
        mock_redis = MagicMock()
        writer._redis = mock_redis

        closed = _FakeTradeClosed()
        outcome = from_trade_closed(closed)
        assert outcome is not None

        result = writer.emit_to_redis(outcome)
        assert result is True
        mock_redis.xadd.assert_called_once()

        call_args = mock_redis.xadd.call_args
        stream_key = call_args[0][0]
        assert stream_key == "signals:outcomes"

        data = call_args[0][1]
        assert data["sid"] == "sig-001"
        assert data["symbol"] == "BTCUSDT"

    def test_emit_to_redis_fail_open(self):
        """If Redis raises, writer returns False (no exception)."""
        from services.signal_outcome_writer import SignalOutcomeWriter
        from domain.signal_outcome import from_trade_closed

        writer = SignalOutcomeWriter()
        mock_redis = MagicMock()
        mock_redis.xadd.side_effect = ConnectionError("REDIS DOWN")
        writer._redis = mock_redis

        closed = _FakeTradeClosed()
        outcome = from_trade_closed(closed)
        result = writer.emit_to_redis(outcome)
        assert result is False  # fail-open, no exception

    def test_emit_full_fail_open(self):
        """Combined emit() catches all exceptions from both Redis and DB."""
        from services.signal_outcome_writer import SignalOutcomeWriter
        from domain.signal_outcome import from_trade_closed

        writer = SignalOutcomeWriter()
        # Mock Redis to fail
        writer._redis = MagicMock()
        writer._redis.xadd.side_effect = Exception("boom")

        closed = _FakeTradeClosed()
        outcome = from_trade_closed(closed)

        # This should NOT raise
        writer.emit(outcome)  # no exception = pass

    def test_singleton_returns_same_instance(self):
        """get_signal_outcome_writer() returns the same instance on repeated calls."""
        import services.signal_outcome_writer as mod

        # Reset singleton for test isolation
        mod._instance = None

        w1 = mod.get_signal_outcome_writer()
        w2 = mod.get_signal_outcome_writer()
        assert w1 is w2

        # cleanup
        mod._instance = None
