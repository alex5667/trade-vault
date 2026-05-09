import datetime as dt
import importlib.util
import json
import os
from unittest.mock import MagicMock, patch

from services.archivers.stream_archiver import (
    PgCfg,
    PgWriter,
    StreamArchiver,
)

# Load migration module directly to bypass any package shadowing
_MIG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "orderflow_services", "of_gate_history_migration_v1.py"
)
_mig_spec = importlib.util.spec_from_file_location("_of_gate_mig", _MIG_PATH)
_mig = importlib.util.module_from_spec(_mig_spec)
_mig_spec.loader.exec_module(_mig)

normalize_ts_ms = _mig.normalize_ts_ms
build_of_gate_row = _mig.build_of_gate_row


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_archiver() -> StreamArchiver:
    """Build StreamArchiver with mocked Redis and PgWriter (no real connections)."""
    r = MagicMock()
    pg = MagicMock(spec=PgWriter)
    return StreamArchiver(r, pg)


STREAM_ID = "1700000000123-0"


def _base_payload(**overrides):
    base = {
        "symbol": "BTCUSDT",
        "scenario_v4": "bucket:breakout",
        "schema_version": 4,
        "ok": 1,
        "ok_soft": 0,
        "reason_code": "ok_hard",
        "ts_ms": 1700000000123,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# of_gate_row
# ---------------------------------------------------------------------------

class TestOfGateRow:

    def test_basic_fields(self):
        archiver = _make_archiver()
        payload = _base_payload()
        row = archiver.of_gate_row(STREAM_ID, payload)

        assert row[0] == STREAM_ID                     # stream_id
        assert row[1] == 1700000000123                 # ts_ms
        assert isinstance(row[2], dt.datetime)         # ts
        assert row[3] == "BTCUSDT"                     # symbol
        assert row[4] == "bucket:breakout"             # scenario_v4
        assert row[5] == 4                             # schema_version
        assert row[6] == 1                             # ok
        assert row[7] == 0                             # ok_soft
        assert row[8] is None                          # missing_legs (absent)
        assert row[9] == "ok_hard"                     # reason_code
        assert json.loads(row[10])["symbol"] == "BTCUSDT"  # payload_json

    def test_ok_defaults_to_zero(self):
        archiver = _make_archiver()
        payload = {"symbol": "ETHUSDT", "ts_ms": 1700000000000}
        row = archiver.of_gate_row(STREAM_ID, payload)
        assert row[6] == 0   # ok
        assert row[7] == 0   # ok_soft

    def test_scenario_fallback(self):
        """scenario_v4 falls back to 'scenario' key then 'na'."""
        archiver = _make_archiver()
        payload = _base_payload(scenario_v4=None, scenario="legacy_scenario")
        row = archiver.of_gate_row(STREAM_ID, payload)
        assert row[4] == "legacy_scenario"

        row2 = archiver.of_gate_row(STREAM_ID, {"symbol": "X", "ts_ms": 1700000000000})
        assert row2[4] == "na"

    def test_missing_legs_json_string(self):
        """missing_legs as JSON string is parsed to dict."""
        archiver = _make_archiver()
        ml = json.dumps({"missing": ["ob_l3", "funding"]})
        payload = _base_payload(missing_legs=ml)
        row = archiver.of_gate_row(STREAM_ID, payload)
        parsed = json.loads(row[8])
        assert "missing" in parsed

    def test_missing_legs_invalid_string(self):
        """missing_legs as invalid JSON string is wrapped in _raw."""
        archiver = _make_archiver()
        payload = _base_payload(missing_legs="not-valid-json")
        row = archiver.of_gate_row(STREAM_ID, payload)
        parsed = json.loads(row[8])
        assert "_raw" in parsed

    def test_missing_legs_dict(self):
        """missing_legs as dict is serialised to JSON."""
        archiver = _make_archiver()
        payload = _base_payload(missing_legs={"legs": ["a", "b"]})
        row = archiver.of_gate_row(STREAM_ID, payload)
        assert json.loads(row[8]) == {"legs": ["a", "b"]}

    def test_ts_ms_from_stream_id_fallback(self):
        """Without ts_ms in payload, uses stream_id timestamp."""
        archiver = _make_archiver()
        payload = {"symbol": "ETHUSDT"}
        row = archiver.of_gate_row("1700111222333-0", payload)
        assert row[1] == 1700111222333

    def test_reason_code_fallback(self):
        """reason_code falls back to 'reason' then 'na'."""
        archiver = _make_archiver()
        payload = _base_payload(reason_code=None, reason="soft_veto")
        row = archiver.of_gate_row(STREAM_ID, payload)
        assert row[9] == "soft_veto"


# ---------------------------------------------------------------------------
# of_gate_quarantine_row
# ---------------------------------------------------------------------------

class TestOfGateQuarantineRow:

    SOURCE = "quarantined:metrics:of_gate"

    def test_basic_fields(self):
        archiver = _make_archiver()
        payload = {
            "symbol": "SOLUSDT",
            "scenario_v4": "bucket:absorption",
            "schema_version": 4,
            "ok": 0,
            "ok_soft": 0,
            "dq_code": "ts_future",
            "err": "ts 9999999999999 > now+5s",
            "ts_ms": 1700000000000,
        }
        row = archiver.of_gate_quarantine_row(self.SOURCE, STREAM_ID, payload)

        assert row[0] == STREAM_ID            # stream_id
        assert row[3] == self.SOURCE          # source_stream
        assert row[4] == "SOLUSDT"            # symbol
        assert row[5] == "bucket:absorption"  # scenario_v4
        assert row[6] == 4                    # schema_version
        assert row[7] == 0                    # ok
        assert row[8] == 0                    # ok_soft
        assert row[9] == "ts_future"          # dq_code
        assert "ts 9999" in row[10]           # err (truncated to 500)
        assert json.loads(row[11])["symbol"] == "SOLUSDT"

    def test_dq_code_fallback_chain(self):
        """dq_code: dq_code > dq_why > why > err > sentinel."""
        archiver = _make_archiver()

        # Explicit dq_code wins
        row = archiver.of_gate_quarantine_row(self.SOURCE, STREAM_ID,
                                              {"dq_code": "schema_mismatch", **_base_payload()})
        assert row[9] == "schema_mismatch"

        # dq_why
        row2 = archiver.of_gate_quarantine_row(self.SOURCE, STREAM_ID,
                                               {"dq_why": "stale_l2", **_base_payload()})
        assert row2[9] == "stale_l2"

        # why
        row3 = archiver.of_gate_quarantine_row(self.SOURCE, STREAM_ID,
                                               {"why": "future_ts", **_base_payload()})
        assert row3[9] == "future_ts"

        # sentinel
        row4 = archiver.of_gate_quarantine_row(self.SOURCE, STREAM_ID, _base_payload())
        assert row4[9] == "dq_unknown"

    def test_nullable_fields(self):
        """Fields like symbol/schema_version are nullable in quarantine table."""
        archiver = _make_archiver()
        payload = {"dq_code": "missing_symbol", "ts_ms": 1700000000000}
        row = archiver.of_gate_quarantine_row(self.SOURCE, STREAM_ID, payload)
        assert row[4] is None    # symbol
        assert row[5] is None    # scenario_v4
        assert row[6] is None    # schema_version

    def test_err_truncated_to_500(self):
        archiver = _make_archiver()
        long_err = "x" * 1000
        payload = {"dq_code": "parse_error", "err": long_err, "ts_ms": 1700000000000}
        row = archiver.of_gate_quarantine_row(self.SOURCE, STREAM_ID, payload)
        assert len(row[10]) == 500

    def test_dq_code_truncated_to_120(self):
        archiver = _make_archiver()
        long_code = "a" * 200
        payload = {"dq_code": long_code, "ts_ms": 1700000000000}
        row = archiver.of_gate_quarantine_row(self.SOURCE, STREAM_ID, payload)
        assert len(row[9]) == 120


# ---------------------------------------------------------------------------
# StreamArchiver config
# ---------------------------------------------------------------------------

class TestStreamArchiverP3Config:

    def test_of_gate_defaults_disabled(self):
        archiver = _make_archiver()
        assert archiver.of_gate_enabled is False
        assert archiver.of_gate_q_enabled is False

    def test_of_gate_streams(self):
        archiver = _make_archiver()
        assert archiver.of_gate_stream == "metrics:of_gate"
        assert archiver.of_gate_q_stream == "quarantined:metrics:of_gate"

    def test_of_gate_consumer_defaults(self):
        archiver = _make_archiver()
        assert archiver.of_gate_cg == "of_gate_metrics_archiver"
        assert archiver.of_gate_consumer == "archiver_of_gate_1"
        assert archiver.of_gate_q_cg == "of_gate_quarantine_archiver"
        assert archiver.of_gate_q_consumer == "archiver_of_gate_q_1"

    def test_of_gate_env_override(self, monkeypatch):
        monkeypatch.setenv("OF_GATE_METRICS_ARCHIVE_ENABLED", "1")
        monkeypatch.setenv("OF_GATE_QUARANTINE_ARCHIVE_ENABLED", "1")
        monkeypatch.setenv("OF_GATE_METRICS_ROLLUPS_AUTO_MIGRATE", "1")
        archiver = _make_archiver()
        assert archiver.of_gate_enabled is True
        assert archiver.of_gate_q_enabled is True
        assert archiver.of_gate_rollups_auto_migrate is True


# ---------------------------------------------------------------------------
# PgWriter.insert_of_gate_metrics - row shape
# ---------------------------------------------------------------------------

class TestPgWriterInserts:

    def test_insert_of_gate_metrics_empty(self):
        pg = PgWriter(PgCfg(dsn="postgresql://fake"))
        with patch.object(pg, "_conn") as mock_conn:
            result = pg.insert_of_gate_metrics([])
        assert result == 0
        mock_conn.assert_not_called()

    def test_insert_of_gate_metrics_quarantine_empty(self):
        pg = PgWriter(PgCfg(dsn="postgresql://fake"))
        with patch.object(pg, "_conn") as mock_conn:
            result = pg.insert_of_gate_metrics_quarantine([])
        assert result == 0
        mock_conn.assert_not_called()

    def test_insert_of_gate_metrics_returns_count(self):
        pg = PgWriter(PgCfg(dsn="postgresql://fake"))
        rows = [("id1", 1700000000, object(), "BTCUSDT", "na", 4, 1, 0, None, "ok_hard", "{}")]
        with patch.object(pg, "_conn") as mock_conn:
            ctx = MagicMock()
            mock_conn.return_value.__enter__ = MagicMock(return_value=ctx)
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)
            cursor = MagicMock()
            ctx.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
            ctx.cursor.return_value.__exit__ = MagicMock(return_value=False)
            with patch("services.archivers.stream_archiver.execute_values"):
                result = pg.insert_of_gate_metrics(rows)
        assert result == 1

    def test_insert_of_gate_metrics_quarantine_returns_count(self):
        pg = PgWriter(PgCfg(dsn="postgresql://fake"))
        rows = [("id1", 1700000000, object(), "quarantined:metrics:of_gate",
                 "BTCUSDT", "na", 4, 0, 0, "ts_future", None, "{}")]
        with patch.object(pg, "_conn") as mock_conn:
            ctx = MagicMock()
            mock_conn.return_value.__enter__ = MagicMock(return_value=ctx)
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)
            cursor = MagicMock()
            ctx.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
            ctx.cursor.return_value.__exit__ = MagicMock(return_value=False)
            with patch("services.archivers.stream_archiver.execute_values"):
                result = pg.insert_of_gate_metrics_quarantine(rows)
        assert result == 1


# ---------------------------------------------------------------------------
# of_gate_history_migration_v1 helpers
# ---------------------------------------------------------------------------

class TestNormalizeTs:

    def test_ms_passthrough(self):
        assert normalize_ts_ms(1700000000000) == 1700000000000

    def test_sec_to_ms(self):
        assert normalize_ts_ms(1700000000) == 1700000000000

    def test_none_returns_none(self):
        assert normalize_ts_ms(None) is None

    def test_ns_to_ms(self):
        v = 1700000000000 * 1_000_000
        assert normalize_ts_ms(v) == 1700000000000


class TestBuildOfGateRow:

    def test_full_payload(self):
        payload = {
            "symbol": "ETHUSDT",
            "scenario_v4": "bucket:extreme",
            "schema_version": 4,
            "ok": 1,
            "ok_soft": 1,
            "reason_code": "ok_soft",
            "ts_ms": 1700000000123,
            "missing_legs": json.dumps({"legs": ["ob_l3"]}),
        }
        row = build_of_gate_row("1700000000123-0", payload)
        assert row[3] == "ETHUSDT"
        assert row[4] == "bucket:extreme"
        assert row[6] == 1   # ok
        assert row[7] == 1   # ok_soft
        assert row[9] == "ok_soft"
        ml = json.loads(row[8])
        assert ml["legs"] == ["ob_l3"]

    def test_missing_ts_uses_stream_id(self):
        row = build_of_gate_row("1700111222333-0", {"symbol": "BTCUSDT"})
        assert row[1] == 1700111222333

    def test_no_ok_fields(self):
        row = build_of_gate_row("1700000000000-0", {"symbol": "XRPUSDT", "ts_ms": 1700000000000})
        assert row[6] == 0
        assert row[7] == 0
