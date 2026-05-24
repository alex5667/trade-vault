"""Unit tests for ``services.trailing_state_writer``."""

import json
import os
from unittest.mock import MagicMock, patch

import pytest

from services.trailing_state_writer import (
    PgWriter,
    _normalize_row,
    _parse_stream_fields,
    pick_dsn,
)


# ────────────────────────────────────────────────────────────────────────────
# _normalize_row
# ────────────────────────────────────────────────────────────────────────────
class TestNormalizeRow:
    def _full_payload(self) -> dict:
        return {
            "sid": "of:BTCUSDT:1700000000000:LONG",
            "position_id": "pos-1",
            "symbol": "BTCUSDT",
            "side": "LONG",
            "from_state": "ARMED",
            "to_state": "TRAILING",
            "event_type": "transition",
            "reason_code": "tp1_hit",
            "profile": "shock_trending_v1",
            "ts_ms": "1700000123456",
            "price": "65000.5",
            "old_sl": "64900.0",
            "new_sl": "64950.0",
            "high_watermark": "65100.0",
            "low_watermark": "64800.0",
            "atr_value": "150.25",
            "atr_mult": "1.5",
        }

    def test_valid_full_payload(self):
        row, reason = _normalize_row(self._full_payload())
        assert reason == ""
        assert row is not None
        assert row["sid"] == "of:BTCUSDT:1700000000000:LONG"
        assert row["position_id"] == "pos-1"
        assert row["symbol"] == "BTCUSDT"
        assert row["side"] == "LONG"
        assert row["from_state"] == "ARMED"
        assert row["to_state"] == "TRAILING"
        assert row["event_type"] == "transition"
        assert row["reason_code"] == "tp1_hit"
        assert row["profile"] == "shock_trending_v1"
        assert row["ts_ms"] == 1700000123456
        assert row["price"] == 65000.5
        assert row["old_sl"] == 64900.0
        assert row["new_sl"] == 64950.0
        assert row["high_watermark"] == 65100.0
        assert row["low_watermark"] == 64800.0
        assert row["atr_value"] == 150.25
        assert row["atr_mult"] == 1.5

    def test_valid_minimal_payload(self):
        """Only required fields present — optional numerics should be None."""
        payload = {
            "sid": "sid-min",
            "symbol": "ETHUSDT",
            "side": "SHORT",
            "to_state": "ARMED",
            "event_type": "arm",
            "reason_code": "init",
            "ts_ms": "1700000000000",
        }
        row, reason = _normalize_row(payload)
        assert reason == ""
        assert row is not None
        assert row["from_state"] is None
        assert row["price"] is None
        assert row["old_sl"] is None
        assert row["new_sl"] is None
        assert row["high_watermark"] is None
        assert row["low_watermark"] is None
        assert row["atr_value"] is None
        assert row["atr_mult"] is None
        assert row["position_id"] is None
        assert row["profile"] is None

    def test_missing_sid_returns_reason(self):
        p = self._full_payload()
        del p["sid"]
        row, reason = _normalize_row(p)
        assert row is None
        assert reason == "missing_sid"

    def test_missing_ts_ms(self):
        p = self._full_payload()
        del p["ts_ms"]
        row, reason = _normalize_row(p)
        assert row is None
        assert reason == "missing_ts_ms"

    def test_missing_symbol(self):
        p = self._full_payload()
        del p["symbol"]
        row, reason = _normalize_row(p)
        assert row is None
        assert reason == "missing_symbol"

    def test_empty_required_field_treated_as_missing(self):
        p = self._full_payload()
        p["side"] = "   "
        row, reason = _normalize_row(p)
        assert row is None
        assert reason == "missing_side"

    def test_invalid_numeric_old_sl_returns_none(self):
        p = self._full_payload()
        p["old_sl"] = "not-a-number"
        row, reason = _normalize_row(p)
        assert reason == ""
        assert row is not None
        assert row["old_sl"] is None
        # Other numerics still parse
        assert row["new_sl"] == 64950.0

    def test_payload_jsonb_serialized(self):
        p = self._full_payload()
        row, reason = _normalize_row(p)
        assert reason == ""
        assert row is not None
        # payload column must be a JSON string
        assert isinstance(row["payload"], str)
        decoded = json.loads(row["payload"])
        assert decoded["sid"] == p["sid"]
        assert decoded["symbol"] == p["symbol"]
        assert decoded["price"] == "65000.5"  # raw string preserved
        # Stable ordering via sort_keys
        assert list(decoded.keys()) == sorted(decoded.keys())

    def test_bad_ts_ms_zero(self):
        p = self._full_payload()
        p["ts_ms"] = "0"
        row, reason = _normalize_row(p)
        assert row is None
        assert reason == "missing_ts_ms"

    def test_parse_stream_fields_decodes_bytes(self):
        fields = {b"sid": b"abc", b"symbol": b"BTCUSDT"}
        out = _parse_stream_fields(fields)
        assert out == {"sid": "abc", "symbol": "BTCUSDT"}


# ────────────────────────────────────────────────────────────────────────────
# PgWriter
# ────────────────────────────────────────────────────────────────────────────
class TestPgWriter:
    def _row(self, **overrides) -> dict:
        base = {
            "ts_ms": 1700000000000,
            "sid": "sid-1",
            "position_id": "pos-1",
            "symbol": "BTCUSDT",
            "side": "LONG",
            "from_state": "ARMED",
            "to_state": "TRAILING",
            "event_type": "transition",
            "price": 65000.0,
            "old_sl": 64900.0,
            "new_sl": 64950.0,
            "high_watermark": 65100.0,
            "low_watermark": None,
            "atr_value": 150.0,
            "atr_mult": 1.5,
            "reason_code": "tp1_hit",
            "profile": "default",
            "payload": "{}",
        }
        base.update(overrides)
        return base

    def test_insert_rows_empty_returns_zero(self):
        pg = PgWriter("dummy-dsn")
        assert pg.insert_rows([]) == 0

    def test_insert_rows_calls_executemany(self):
        rows = [self._row(), self._row(sid="sid-2")]
        fake_cur = MagicMock()
        fake_conn = MagicMock()
        fake_conn.cursor.return_value = fake_cur

        pg = PgWriter("dummy-dsn")
        with patch.object(pg, "_connect", return_value=fake_conn):
            count = pg.insert_rows(rows)

        assert count == 2
        # executemany called exactly once with the trailing-state SQL
        assert fake_cur.executemany.call_count == 1
        sql_arg, rows_arg = fake_cur.executemany.call_args[0]
        assert "trailing_state_transitions" in sql_arg
        assert "to_timestamp(%(ts_ms)s" in sql_arg
        assert rows_arg == rows

    def test_insert_rows_commits_on_success(self):
        fake_cur = MagicMock()
        fake_conn = MagicMock()
        fake_conn.cursor.return_value = fake_cur

        pg = PgWriter("dummy-dsn")
        with patch.object(pg, "_connect", return_value=fake_conn):
            pg.insert_rows([self._row()])

        fake_conn.commit.assert_called_once()
        fake_conn.close.assert_called_once()

    def test_insert_rows_returns_count(self):
        rows = [self._row(sid=f"sid-{i}") for i in range(5)]
        fake_cur = MagicMock()
        fake_conn = MagicMock()
        fake_conn.cursor.return_value = fake_cur

        pg = PgWriter("dummy-dsn")
        with patch.object(pg, "_connect", return_value=fake_conn):
            count = pg.insert_rows(rows)
        assert count == 5


# ────────────────────────────────────────────────────────────────────────────
# pick_dsn
# ────────────────────────────────────────────────────────────────────────────
class TestPickDsn:
    _KEYS = ("ANALYTICS_DB_DSN", "TRADES_DB_DSN", "TIMESCALE_DSN", "DATABASE_URL")

    @pytest.fixture(autouse=True)
    def _clean_env(self, monkeypatch):
        for k in self._KEYS:
            monkeypatch.delenv(k, raising=False)
        yield

    def test_pick_dsn_analytics_first(self, monkeypatch):
        monkeypatch.setenv("ANALYTICS_DB_DSN", "postgresql://analytics")
        monkeypatch.setenv("TRADES_DB_DSN", "postgresql://trades")
        monkeypatch.setenv("TIMESCALE_DSN", "postgresql://timescale")
        assert pick_dsn() == "postgresql://analytics"

    def test_pick_dsn_falls_back_to_trades(self, monkeypatch):
        monkeypatch.setenv("TRADES_DB_DSN", "postgresql://trades")
        assert pick_dsn() == "postgresql://trades"

    def test_pick_dsn_falls_back_to_timescale(self, monkeypatch):
        monkeypatch.setenv("TIMESCALE_DSN", "postgresql://timescale")
        assert pick_dsn() == "postgresql://timescale"

    def test_pick_dsn_empty_when_none_set(self):
        assert pick_dsn() == ""
