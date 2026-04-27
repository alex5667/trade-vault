import pytest
from services.orderflow.evidence_contract import (
    normalize_evidence_payload, EVIDENCE_SCHEMA_VERSION, EvidencePayload
)

def test_normalize_evidence_payload_basic():
    # Test typical success path
    res = normalize_evidence_payload(
        producer="test_producer",
        sid="test_sid",
        ts_event_ms=1600000000000,
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        entry=50000.0,
        evidence_raw={
            "data_health": 1.0,
            "book_stale_ms": 150,
            "dq_ok": True,
            "market_mode": "trend"
        },
        strict_unknown=True
    )
    
    # Assert payload content
    payload = res.payload
    assert payload.schema_version == EVIDENCE_SCHEMA_VERSION
    assert payload.producer == "test_producer"
    assert payload.sid == "test_sid"
    assert payload.ts_event_ms == 1600000000000
    assert payload.symbol == "BTCUSDT"
    assert payload.tf == "1m"
    assert payload.market_mode == "trend"
    
    # Assert evidence map
    emap = payload.evidence_map
    assert emap["data_health"] == 1.0
    assert emap["book_stale_ms"] == 150.0
    assert emap["dq_ok"] == 1.0
    assert emap["market_mode_id"] == 1.0

    # No unknown keys or dropped warnings
    assert not res.unknown_keys
    assert not res.dropped
    assert res.warnings == ["bad_ts:too_old"]

def test_normalize_evidence_payload_aliases_and_strict():
    # Test strict mode and alias mapping
    res = normalize_evidence_payload(
        producer="test_producer",
        sid="123",
        ts_event_ms=1600000000000,
        symbol="ETHUSDT",
        tf="5m",
        direction="SHORT",
        entry=2000.0,
        evidence_raw={
            "iceberg": 1.0,         # Should be aliased to iceberg_strict
            "unknown_metric": 5.0   # Should be dropped if strict_unknown is True
        },
        strict_unknown=True
    )
    
    emap = res.payload.evidence_map
    assert "iceberg_strict" in emap
    assert "iceberg" not in emap
    
    assert "unknown_metric" in res.unknown_keys
    assert "unknown_metric" in res.dropped
    assert res.dropped["unknown_metric"] == "unknown_key"

def test_normalize_evidence_payload_legacy_confirmations():
    res = normalize_evidence_payload(
        producer="test_producer",
        sid="456",
        ts_event_ms=1600000000000,
        symbol="SOLUSDT",
        tf="tick",
        direction="LONG",
        entry=150.0,
        confirmations_legacy=["sweep=1", "rsi=True"],
        strict_unknown=False
    )
    
    emap = res.payload.evidence_map
    # sweep -> sweep_any, rsi -> rsi_agree
    assert emap["sweep_any"] == 1.0
    assert emap["rsi_agree"] == 1.0
