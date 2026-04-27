import pytest
import datetime as dt
import json
from unittest.mock import MagicMock
from services.archivers.stream_archiver import StreamArchiver, PgWriter, PgCfg

@pytest.fixture
def archiver():
    r = MagicMock()
    pg = PgWriter(PgCfg(dsn="dummy"))
    svc = StreamArchiver(r, pg)
    return svc

def test_conf_score_row_parsing(archiver):
    payload = {
        "schema_version": 1,
        "producer": "signal_pipeline_v1",
        "sid": "sig-1234",
        "symbol": "BTCUSDT",
        "ts_event_ms": 1708420000000,
        "confidence_raw": 0.85,
        "confidence_final": 0.90,
        "evidence_map": {"rsi_agree": 1.0, "spread_bps": 2.5},
        "context": {"tf": "1m"}
    }
    
    row = archiver.conf_score_row("1708420000000-0", payload)
    
    assert row[0] == "1708420000000-0"
    assert row[1] == 1708420000000
    assert row[2] == dt.datetime.fromtimestamp(1708420000000 / 1000.0, tz=dt.timezone.utc)
    assert row[3] == "sig-1234"
    assert row[4] == "BTCUSDT"
    assert row[5] == 1 # schema_version
    assert row[6] == "signal_pipeline_v1" # producer
    assert row[7] == 0.85 # conf_raw
    assert row[8] == 0.90 # conf_final
    
    # evidence_map should be JSON
    evidence = json.loads(row[9])
    assert evidence["rsi_agree"] == 1.0
    
    # context_json
    context = json.loads(row[10])
    assert context["tf"] == "1m"

def test_conf_score_row_fallback(archiver):
    payload = {
        "signal_id": "sig-999",
        "symbol": "ETHUSDT",
        "confidence": 0.75,
    }
    row = archiver.conf_score_row("1708420000000-0", payload)
    
    assert row[3] == "sig-999"
    assert row[5] == 1 # defaults to 1
    assert row[7] == 0.75 # conf_raw
    assert row[8] == 0.75 # conf_final

def test_evidence_map_fallback(archiver):
    payload = {
        "sid": "sig-1",
        "evidence": {"div_match": 1.0}
    }
    row = archiver.conf_score_row("1708420000000-0", payload)
    evidence = json.loads(row[9])
    assert evidence["div_match"] == 1.0

def test_bad_schema_version(archiver):
    with pytest.raises(ValueError, match="schema_version_not_accepted"):
        archiver.conf_score_row("1708420000000-0", {"schema_version": 999})
